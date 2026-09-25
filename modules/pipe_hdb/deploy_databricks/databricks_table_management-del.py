from databricks.sdk import WorkspaceClient
w = WorkspaceClient()   # picks up the DEFAULT profile from ~/.databrickscfg automatically

"""TABLE-level management only: CREATE TABLE, ADD/DROP COLUMN, type changes, property
checks. Catalog/schema/volume provisioning stays in databricks_provision.sh - this
script never touches CREATE CATALOG/CREATE SCHEMA/volumes.

Same six actions and same safety checks as the shell twins (databricks_table_management.sh
/ _cli.sh), rebuilt on databricks-sdk instead of `databricks api post` + jq - real
Python objects back (TableInfo, StatementResponse) instead of parsing CLI JSON output.
Chosen over databricks-connect for the same reason the shell scripts chose SQL-via-
warehouse over Spark-native: DDL is text, not a DataFrame operation - no cluster,
no Spark session, no databricks-connect/pyspark dependency conflict.

column mapping: every CREATE bakes in delta.columnMapping.mode='name' from creation
(free - no data exists yet). See sql/t1_create.sql / t2_create.sql for the DDL itself -
this script reads those files rather than embedding DDL as Python strings.
"""

import sys
import time
from pathlib import Path

import pyarrow as pa
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sql import StatementState

WAREHOUSE_ID = "3a82455c9b1702b1"
CATALOG_NAME = "macroecons"

# __file__ only exists when this runs as a script/%run - pasted inline into an
# interactive session (ipython/jupyter), there is no __file__ at all (NameError).
# Falls back to a path relative to the repo root, matching where this file actually
# lives - so "run inline from the root" (paste, no %run) still resolves correctly.
try:
    SQL_DIR = Path(__file__).parent / "sql"
except NameError:
    SQL_DIR = Path("modules/pipe_hdb/deploy_databricks/sql")


def warm_warehouse():
    print("RUN: warehouses start (SQL API needs a live warehouse)")
    w.warehouses.start(WAREHOUSE_ID).result()


def run_sql(statement: str):
    """Run one SQL statement to completion. execute_statement's own wait_timeout maxes
    out at 50s - anything still PENDING/RUNNING after that gets polled via
    get_statement instead of assumed done, unlike a bare fire-and-forget call."""
    print(f"RUN SQL: {statement}")
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=WAREHOUSE_ID, wait_timeout="30s")

    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else "no error message"
        raise Exception(f"SQL failed ({resp.status.state}): {msg}\nstatement: {statement}")

    return resp


def run_sql_file(sql_file):
    """Read a .sql file and run it - the file-based twin of run_sql, matching the
    shell scripts' run_sql_file. Use with build_create_ddl/write_create_ddl below:
    generate the DDL, write it to sql/create_tables/, then run it from the file."""
    return run_sql(Path(sql_file).read_text())


def describe(fqn: str):
    t = w.tables.get(fqn)
    columns = [f"{c.name}:{c.type_text}" + (" NOT NULL" if c.nullable is False else "") for c in t.columns]
    properties = {k: v for k, v in (t.properties or {}).items() if k in (
        "delta.columnMapping.mode", "delta.minReaderVersion", "delta.minWriterVersion")}
    print(f"VALIDATE: full_name={t.full_name} format={t.data_source_format} type={t.table_type}")
    print(f"  columns: {columns}")
    print(f"  properties: {properties}")
    return t


def _pa_type_to_sql(t: pa.DataType) -> str:
    """pyarrow DataType -> Databricks SQL type text. Extend as new types show up -
    same spirit as helper_pyiceberg_io/helper_deltalake_io's own pa-type mappings,
    just targeting SQL text instead of a catalog API call."""
    if pa.types.is_string(t): return "STRING"
    if pa.types.is_int64(t): return "BIGINT"
    if pa.types.is_int32(t): return "INT"
    if pa.types.is_float64(t): return "DOUBLE"
    if pa.types.is_float32(t): return "FLOAT"
    if pa.types.is_boolean(t): return "BOOLEAN"
    if pa.types.is_date32(t) or pa.types.is_date64(t): return "DATE"
    if pa.types.is_timestamp(t): return "TIMESTAMP"
    raise NotImplementedError(f"no SQL type mapping for pyarrow type {t!r} - add one to _pa_type_to_sql()")


def build_create_ddl(schema_source, fqn: str, column_mapping: bool = True) -> str:
    """Build (not run) a CREATE TABLE ... DDL string from a pa.Schema/pa.Table - the
    DDL-generation half of createOrEvolve_table's create branch, split out so the SQL
    text can be inspected, written to a .sql file, and run later via run_sql_file,
    instead of only ever running immediately."""
    if isinstance(schema_source, pa.Table):
        arrow_schema = schema_source.schema
    elif isinstance(schema_source, pa.Schema):
        arrow_schema = schema_source
    else:
        raise TypeError(f"schema_source must be a pa.Schema or pa.Table, got {type(schema_source)}")

    col_defs = ",\n    ".join(
        f"{f.name} {_pa_type_to_sql(f.type)}" + ("" if f.nullable else " NOT NULL")
        for f in arrow_schema)

    tblproperties = ""
    if column_mapping:
        tblproperties = (
            "\nTBLPROPERTIES (\n"
            "    'delta.columnMapping.mode'   = 'name',\n"
            "    'delta.minReaderVersion'     = '2',\n"
            "    'delta.minWriterVersion'     = '5'\n"
            ")"
        )

    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n    {col_defs}\n)\nUSING DELTA{tblproperties}\n"


def write_create_ddl(schema_source, fqn: str, out_dir=None, column_mapping: bool = True) -> Path:
    """build_create_ddl(...) + write it to sql/create_tables/{fqn}.sql - matches the
    naming convention already in place there. Returns the file path, does NOT run it -
    pair with run_sql_file(path) to actually execute it, same two-step shape as the
    .sql files you've already been hand-writing into that folder."""
    out_dir = Path(out_dir) if out_dir else Path(SQL_DIR) / "create_tables"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{fqn}.sql"
    out_file.write_text(build_create_ddl(schema_source, fqn, column_mapping=column_mapping))
    print(f"wrote {out_file}")
    return out_file


def createOrEvolve_table(schema_source, catalog: str, schema: str, tbl_name: str):
    """
    Provisioning only - never called implicitly from a write.
    Evolve here ONLY adds columns, never deletes, never changes a type - same
    contract as helper_pyiceberg_io.createOrEvolve_table / helper_deltalake_io's twin
    (schema_source: pa.Schema | pa.Table, same input style across all three engines).

    catalog/schema/tbl_name build the fqn as catalog.schema.tbl_name (Unity Catalog's
    own 3-level naming - "schema" here is UC's term, e.g. 't1', not a pyarrow schema).
    tbl_name should already carry whatever naming convention you want, e.g.
    'datagov__resale_flat_prices'.

    Unlike create(tier) above (which reads a fixed sql/*.sql file per tier), this
    builds the DDL from a real pa.Schema at call time - use this one when the schema
    isn't one of the two fixed t1/t2 files, or when driving it from Python directly.
    """
    if isinstance(schema_source, pa.Table):
        arrow_schema = schema_source.schema
    elif isinstance(schema_source, pa.Schema):
        arrow_schema = schema_source
    else:
        raise TypeError(f"schema_source must be a pa.Schema or pa.Table, got {type(schema_source)}")

    fqn = f"{catalog}.{schema}.{tbl_name}"
    warm_warehouse()

    try:
        t = w.tables.get(fqn)
        exists = True
    except NotFound:
        exists = False

    if not exists:
        print(f"{fqn} does not exist yet - creating")
        col_defs = ", ".join(
            f"{f.name} {_pa_type_to_sql(f.type)}" + ("" if f.nullable else " NOT NULL")
            for f in arrow_schema)
        stmt = (f"CREATE TABLE {fqn} ({col_defs}) USING DELTA "
                f"TBLPROPERTIES ('delta.columnMapping.mode'='name', "
                f"'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')")
        run_sql(stmt)
        describe(fqn)
        return

    # REFUSE - type change on an existing column (matches the pyiceberg/deltalake twins)
    current_types = {c.name: c.type_text.upper() for c in t.columns}
    type_changes = [
        f"{f.name}: {current_types[f.name]} -> {_pa_type_to_sql(f.type)}"
        for f in arrow_schema
        if f.name in current_types and current_types[f.name] != _pa_type_to_sql(f.type)
    ]
    if type_changes:
        raise Exception(f"{fqn}: type change(s) requested, refusing: {type_changes}")

    # EVOLVE - add columns only
    missing = [f for f in arrow_schema if f.name not in current_types]
    if missing:
        print(f"evolving schema - adding {[f.name for f in missing]}")
        col_defs = ", ".join(f"{f.name} {_pa_type_to_sql(f.type)}" for f in missing)
        run_sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")
    else:
        print(f"{fqn} already instantiated - nothing is changed.")

    describe(fqn)


def create(tier: str):
    if tier not in ("t1", "t2"):
        print(f"unknown tier {tier!r} - only t1/t2 have a DDL file defined ({SQL_DIR}/)")
        sys.exit(1)

    ddl_file = Path(SQL_DIR) / f"{tier}_create.sql"   # Path(...) tolerates SQL_DIR being
    # reassigned as a bare str mid-session (interactive pasting can do that) - Path()
    # accepts either a str or an existing Path, so this never breaks the way a bare
    # `SQL_DIR / ...` does when SQL_DIR silently became a string.
    warm_warehouse()
    run_sql(ddl_file.read_text())
    describe(f"{CATALOG_NAME}.{tier}.datagov__resale_flat_prices")


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
