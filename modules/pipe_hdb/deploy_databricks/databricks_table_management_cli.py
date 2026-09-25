from databricks.sdk import WorkspaceClient
w = WorkspaceClient()   # picks up the DEFAULT profile from ~/.databrickscfg automatically

import sys
import time
from pathlib import Path

from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = "3a82455c9b1702b1"
CATALOG_NAME = "macroecons"
# SQL_DIR = Path(__file__).parent / "sql"
SQL_DIR = Path("modules/pipe_hdb/deploy_databricks/sql")

def warm_warehouse():
    print("RUN: warehouses start (SQL API needs a live warehouse)")
    w.warehouses.start(WAREHOUSE_ID).result()

# warm_warehouse()

def run_sql(sql_statement: str):
    response = w.statement_execution.execute_statement(
        warehouse_id = WAREHOUSE_ID,
        statement = sql_statement,
        wait_timeout="30s"
    )
    response

def run_sql_file(sql_file_path):
    sql_statement = sql_file_path.read_text()
    run_sql(sql_statement)


def describe(fqn: str):
    t = w.tables.get(fqn)
    columns = [f"{c.name}:{c.type_text}" + (" NOT NULL" if c.nullable is False else "") for c in t.columns]
    properties = {k: v for k, v in (t.properties or {}).items() if k in (
        "delta.columnMapping.mode", "delta.minReaderVersion", "delta.minWriterVersion")}
    print(f"VALIDATE: full_name={t.full_name} format={t.data_source_format} type={t.table_type}")
    print(f"  columns: {columns}")
    print(f"  properties: {properties}")
    return t

# before that i used the write to create the table no good


# the sql definition IS the creation nothing else.

schema = 't1'
z_catalog = 'macroecons'
z_tbl_name = 'datagov__resale_flat_prices'

# i think i cannot programmatically pass hings inside maybe i can

createOrEvolve_table(
    printSchema =  z_sql_schema, 
    catalog =       z_catalog, 
    schema =     z_tier, 
    tbl_name =      z_tbl_name
    )

fqn = 'macroecons.t1.datagov__resale_flat_prices'
sql_file = SQL_DIR / 'create_tables' / f'{fqn}.sql'
run_sql_file(sql_file)

describe(fqn)

run_sql(f"DROP TABLE {fqn}")
describe(fqn)
run_sql_file(sql_file)

####### 
fqn = 'macroecons.t1.datagov__resale_flat_prices'
sql_file = SQL_DIR / 'create_tables' / f'{fqn}.sql'
run_sql_file(sql_file)
describe(fqn)

run_sql(f"DROP TABLE {fqn}")
describe(fqn)

run_sql_file(sql_file)
describe(fqn)


fqn = 'macroecons.t2.datagov__resale_flat_prices'
sql_file = SQL_DIR / 'create_tables' / f'{fqn}.sql'
run_sql_file(sql_file)
# describe(fqn)

run_sql(f"DROP TABLE {fqn}")
describe(fqn)

# run_sql_file(sql_file)
# describe(fqn)
# also how can i see dropped tables files and how many mbs each holds
# its complicated but we can ignore that





def add_column(fqn: str, col: str, type_: str):
    warm_warehouse()
    run_sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col} {type_})")
    describe(fqn)


def set_type(fqn: str, col: str, type_: str):
    warm_warehouse()
    print("Delta only allows SAFE/widening type changes (e.g. INT -> BIGINT) via ALTER COLUMN TYPE -")
    print("narrowing or incompatible changes raise natively, not caught here client-side.")
    run_sql(f"ALTER TABLE {fqn} ALTER COLUMN {col} TYPE {type_}")
    describe(fqn)


def drop_column(fqn: str, col: str):
    warm_warehouse()

    print(f"checking delta.columnMapping.mode on {fqn} first - DROP COLUMN needs 'name' or 'id'")
    t = w.tables.get(fqn)
    mapping_mode = (t.properties or {}).get("delta.columnMapping.mode", "none")
    if mapping_mode not in ("name", "id"):
        print(f"REFUSING: delta.columnMapping.mode is {mapping_mode!r} on {fqn} - DROP COLUMN needs 'name' or 'id'.")
        print("Not enabling it automatically as a side effect of a drop - it's a real, somewhat")
        print("one-way protocol upgrade and deserves its own explicit step. Enable it first, e.g.:")
        print(f"  run_sql(\"ALTER TABLE {fqn} SET TBLPROPERTIES ('delta.columnMapping.mode'='name', "
              f"'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')\")")
        sys.exit(1)

    if col not in [c.name for c in t.columns]:
        print(f"{fqn}: {col!r} not present - nothing to drop.")
        return

    print(f"About to drop column {col!r} from {fqn!r} - metadata only, old parquet files keep")
    print("the data (still reachable via time travel until VACUUM). Re-adding the name later")
    print("creates a NEW empty column, the old values do not come back.")
    confirm = input("Type the column name to confirm: ")
    if confirm != col:
        print(f"Mismatch ({confirm!r} != {col!r}) - aborted, nothing dropped.")
        sys.exit(1)

    run_sql(f"ALTER TABLE {fqn} DROP COLUMN {col}")
    describe(fqn)


def purge(fqn: str):
    warm_warehouse()

    print(f"About to PERMANENTLY drop table {fqn!r} - the whole table, not a column, unrecoverable")
    print("after Unity Catalog's recovery window (7 days by default, UNDROP TABLE ... until then -")
    print("unless this catalog/schema has a shorter or disabled retention set).")
    first = input("Type the full fqn to confirm (1/2): ")
    if first != fqn:
        print(f"Mismatch ({first!r} != {fqn!r}) on first confirmation - aborted, nothing dropped.")
        sys.exit(1)
    second = input("Type it again to confirm (2/2): ")
    if second != fqn:
        print(f"Mismatch ({second!r} != {fqn!r}) on second confirmation - aborted, nothing dropped.")
        sys.exit(1)

    run_sql(f"DROP TABLE {fqn}")
    print(f"{fqn} dropped. Recoverable via: UNDROP TABLE {fqn}   (until the recovery window lapses)")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)

    p = sub.add_parser("create"); p.add_argument("tier", choices=["t1", "t2"])
    p = sub.add_parser("add-column"); p.add_argument("fqn"); p.add_argument("col"); p.add_argument("type")
    p = sub.add_parser("drop-column"); p.add_argument("fqn"); p.add_argument("col")
    p = sub.add_parser("set-type"); p.add_argument("fqn"); p.add_argument("col"); p.add_argument("type")
    p = sub.add_parser("purge"); p.add_argument("fqn")
    p = sub.add_parser("describe"); p.add_argument("fqn")

    args = parser.parse_args()

    if args.action == "create":
        create(args.tier)
    elif args.action == "add-column":
        add_column(args.fqn, args.col, args.type)
    elif args.action == "drop-column":
        drop_column(args.fqn, args.col)
    elif args.action == "set-type":
        set_type(args.fqn, args.col, args.type)
    elif args.action == "purge":
        purge(args.fqn)
    elif args.action == "describe":
        describe(args.fqn)
