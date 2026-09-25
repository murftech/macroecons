#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

# TABLE-level management only: CREATE TABLE, ADD/DROP COLUMN, type changes, property
# checks. Catalog/schema/volume provisioning stays in databricks_provision.sh (scope
# split 2026-09-25) - this script never touches CREATE CATALOG/CREATE SCHEMA/volumes.
#
# Plain SQL via the Statement Execution API against a warehouse (same run_sql/
# warm_warehouse mechanism databricks_provision.sh already used for CREATE TABLE) -
# officially Databricks' recommended way to run SQL from outside Databricks
# (docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial). No Spark, no JVM, no
# Databricks Connect needed - chosen over a Spark-native approach for exactly that
# reason. The local-only twin (needs a live spark session, used for local JVM Delta
# instead) is helper_sparkdelta_io.createOrEvolve_table / safe_drop_column.
#
# column mapping: every CREATE here bakes in delta.columnMapping.mode='name' from
# creation (free - no data exists yet). Verified 2026-09-25 (local JVM Delta): plain
# Spark ALTER TABLE ADD COLUMNS works fine on a column-mapped table even with Row
# Tracking + Deletion Vectors on (the real macroecons.t1's own feature set) - delta-rs
# could NOT do this (ADD COLUMN raises, and a plain append silently nulls data), which
# is why local provisioning is migrating off it too, separately.

WAREHOUSE_ID=3a82455c9b1702b1
CATALOG_NAME=macroecons

usage() {
  echo "Usage: [create <t1|t2> | add-column <fqn> <col> <type> | drop-column <fqn> <col> | set-type <fqn> <col> <type> | describe <fqn> | purge <fqn>]"
  echo "  create <t1|t2>                - CREATE TABLE IF NOT EXISTS, delta.columnMapping.mode='name' from creation"
  echo "  add-column <fqn> <col> <type> - ALTER TABLE ADD COLUMNS"
  echo "  drop-column <fqn> <col>       - SAFE: type the column name ONCE to confirm (refuses if column mapping isn't already on)"
  echo "  set-type <fqn> <col> <type>   - ALTER TABLE ALTER COLUMN ... TYPE (Delta refuses unsafe/narrowing changes natively)"
  echo "  describe <fqn>                - DESCRIBE-equivalent: schema + delta.columnMapping/protocol properties"
  echo "  purge <fqn>                   - DOUBLE SAFE: DROP TABLE, type the full fqn TWICE to confirm - the whole table, not a column"
  echo "  <fqn> is the full 3-level name, e.g. ${CATALOG_NAME}.t1.datagov__resale_flat_prices"
  exit 1
}

main() {
  case "$1" in
  create)      shift; do_create "$@" ;;
  add-column)  shift; do_add_column "$@" ;;
  drop-column) shift; do_drop_column "$@" ;;
  set-type)    shift; do_set_type "$@" ;;
  describe)    shift; do_describe "$@" ;;
  purge)       shift; do_purge "$@" ;;
  *)           usage ;;
  esac
}

run_sql() {
    local stmt="$1"
    echo "RUN SQL: ${stmt}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${stmt}\"}"
}

# reads a real .sql file (multi-line, real comments, no bash-string escaping) and
# JSON-encodes it properly via jq -Rs - safer than run_sql's naive interpolation for
# anything with newlines/quotes in it, which every DDL file has (SQL comments).
run_sql_file() {
    local file="$1"
    echo "RUN SQL FILE: ${file}"
    local payload
    payload=$(jq -n --arg wid "${WAREHOUSE_ID}" --rawfile stmt "${file}" '{warehouse_id: $wid, statement: $stmt}')
    databricks api post /api/2.0/sql/statements --json "${payload}"
}

warm_warehouse() {
    echo "RUN: warehouses start (SQL API needs a live warehouse)"
    databricks warehouses start "${WAREHOUSE_ID}" >/dev/null
}


# ── TABLE DDL - lives in sql/ as real .sql files (2026-09-25, moved out of this script
# and out of databricks_provision.sh before that) - use sql/ for anything SQL going
# forward, reusable across scripts rather than re-embedded per script.
SQL_DIR="modules/pipe_hdb/deploy_databricks/sql"   # relative to the git root (macroecons/) - the script already cd's there above

do_create() {
    local tier="$1"
    if [ -z "$tier" ]; then echo "usage: create <t1|t2>"; exit 1; fi
    warm_warehouse
    case "$tier" in
      t1) run_sql_file "${SQL_DIR}/t1_create.sql" ;;
      t2) run_sql_file "${SQL_DIR}/t2_create.sql" ;;
      *)  echo "unknown tier '${tier}' - only t1/t2 have a DDL file defined (${SQL_DIR}/)"; exit 1 ;;
    esac
    do_describe "${CATALOG_NAME}.${tier}.datagov__resale_flat_prices"
}

do_add_column() {
    local fqn="$1" col="$2" type="$3"
    if [ -z "$fqn" ] || [ -z "$col" ] || [ -z "$type" ]; then echo "usage: add-column <fqn> <col> <type>"; exit 1; fi
    warm_warehouse
    run_sql "ALTER TABLE ${fqn} ADD COLUMNS (${col} ${type})"
    do_describe "$fqn"
}

do_drop_column() {
    local fqn="$1" col="$2"
    if [ -z "$fqn" ] || [ -z "$col" ]; then echo "usage: drop-column <fqn> <col>"; exit 1; fi
    warm_warehouse

    echo "checking delta.columnMapping.mode on ${fqn} first - DROP COLUMN needs 'name' or 'id'"
    mapping_mode=$(databricks tables get "${fqn}" -o json | jq -r '.properties["delta.columnMapping.mode"] // "none"')
    if [ "$mapping_mode" != "name" ] && [ "$mapping_mode" != "id" ]; then
        echo "REFUSING: delta.columnMapping.mode is '${mapping_mode}' on ${fqn} - DROP COLUMN needs 'name' or 'id'."
        echo "Not enabling it automatically as a side effect of a drop - it's a real, somewhat"
        echo "one-way protocol upgrade and deserves its own explicit step. Enable it first, e.g.:"
        echo "  databricks api post /api/2.0/sql/statements --json '{\"warehouse_id\":\"'\"${WAREHOUSE_ID}\"'\",\"statement\":\"ALTER TABLE ${fqn} SET TBLPROPERTIES ('\''delta.columnMapping.mode'\''='\''name'\'', '\''delta.minReaderVersion'\''='\''2'\'', '\''delta.minWriterVersion'\''='\''5'\'')\"}'"
        exit 1
    fi

    echo "About to drop column '${col}' from '${fqn}' - metadata only, old parquet files keep"
    echo "the data (still reachable via time travel until VACUUM). Re-adding the name later"
    echo "creates a NEW empty column, the old values do not come back."
    read -r -p "Type the column name to confirm: " confirm
    if [ "$confirm" != "$col" ]; then
        echo "Mismatch ('${confirm}' != '${col}') - aborted, nothing dropped."
        exit 1
    fi

    run_sql "ALTER TABLE ${fqn} DROP COLUMN ${col}"
    do_describe "$fqn"
}

do_set_type() {
    local fqn="$1" col="$2" type="$3"
    if [ -z "$fqn" ] || [ -z "$col" ] || [ -z "$type" ]; then echo "usage: set-type <fqn> <col> <type>"; exit 1; fi
    warm_warehouse
    echo "Delta only allows SAFE/widening type changes (e.g. INT -> BIGINT) via ALTER COLUMN TYPE -"
    echo "narrowing or incompatible changes raise natively, not caught here client-side."
    run_sql "ALTER TABLE ${fqn} ALTER COLUMN ${col} TYPE ${type}"
    do_describe "$fqn"
}

do_purge() {
    local fqn="$1"
    if [ -z "$fqn" ]; then echo "usage: purge <fqn>"; exit 1; fi
    warm_warehouse

    echo "About to PERMANENTLY drop table '${fqn}' - the whole table, not a column, unrecoverable"
    echo "after Unity Catalog's recovery window (7 days by default, UNDROP TABLE ${fqn} until then -"
    echo "unless this catalog/schema has a shorter or disabled retention set)."
    read -r -p "Type the full fqn to confirm (1/2): " first
    if [ "$first" != "$fqn" ]; then
        echo "Mismatch ('${first}' != '${fqn}') on first confirmation - aborted, nothing dropped."
        exit 1
    fi
    read -r -p "Type it again to confirm (2/2): " second
    if [ "$second" != "$fqn" ]; then
        echo "Mismatch ('${second}' != '${fqn}') on second confirmation - aborted, nothing dropped."
        exit 1
    fi

    run_sql "DROP TABLE ${fqn}"
    echo "${fqn} dropped. Recoverable via: UNDROP TABLE ${fqn}   (until the recovery window lapses)"
}

do_describe() {
    local fqn="$1"
    if [ -z "$fqn" ]; then echo "usage: describe <fqn>"; exit 1; fi
    echo "VALIDATE: schema + column-mapping/protocol properties of ${fqn}"
    databricks tables get "${fqn}" -o json \
      | jq -c '{full_name, data_source_format, table_type,
                columns: [.columns[] | "\(.name):\(.type_text)\(if .nullable == false then " NOT NULL" else "" end)"],
                properties: (.properties // {} | with_entries(select(
                    .key == "delta.columnMapping.mode" or
                    .key == "delta.minReaderVersion" or
                    .key == "delta.minWriterVersion")))}'
}


main "$@"
