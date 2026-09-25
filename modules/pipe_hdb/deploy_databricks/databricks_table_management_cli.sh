#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

# Flat, no functions on purpose - read top to bottom, one if-block per action, no
# jumping to a definition elsewhere in the file. Twin of databricks_table_management.sh
# (the function/dispatch version) - same commands, same behavior, different shape.
#
# DDL generation stays Python-only (databricks_table_management.py's build_create_ddl/
# write_create_ddl/createOrEvolve_table) - this script never builds a CREATE TABLE
# from a schema, it only RUNS a .sql file you already wrote/generated. `run-file`
# takes any path under sql/create_tables/ - by convention those are named
# <fqn>.sql (e.g. macroecons.t1.datagov__resale_flat_prices.sql), so the fqn for the
# post-run describe is derived straight from the filename, not passed separately.

WAREHOUSE_ID=3a82455c9b1702b1
CATALOG_NAME=macroecons
SQL_DIR="modules/pipe_hdb/deploy_databricks/sql"   # relative to the git root (macroecons/) - already cd'd there above

ACTION="$1"

if [ "$ACTION" != "run-file" ] && [ "$ACTION" != "add-column" ] && [ "$ACTION" != "drop-column" ] \
   && [ "$ACTION" != "set-type" ] && [ "$ACTION" != "purge" ] && [ "$ACTION" != "describe" ]; then
    echo "Usage: [run-file <sql_path> | add-column <fqn> <col> <type> | drop-column <fqn> <col> | set-type <fqn> <col> <type> | purge <fqn> | describe <fqn>]"
    echo "  run-file <sql_path>  - run any .sql file (e.g. ${SQL_DIR}/create_tables/<fqn>.sql)."
    echo "                         DDL itself is generated in Python (build_create_ddl/write_create_ddl"
    echo "                         in databricks_table_management.py), not here."
    echo "  <fqn> is the full 3-level name, e.g. ${CATALOG_NAME}.t1.datagov__resale_flat_prices"
    exit 1
fi

# describe never needs the warehouse (it's a Unity Catalog metadata call, not SQL) -
# every other action does, since they all go through the Statement Execution API.
if [ "$ACTION" != "describe" ]; then
    echo "RUN: warehouses start (SQL API needs a live warehouse)"
    databricks warehouses start "${WAREHOUSE_ID}" >/dev/null
fi


if [ "$ACTION" = "run-file" ]; then
    SQL_FILE="$2"
    if [ -z "$SQL_FILE" ]; then echo "usage: run-file <sql_path>"; exit 1; fi
    if [ ! -f "$SQL_FILE" ]; then echo "no such file: ${SQL_FILE}"; exit 1; fi

    echo "RUN SQL FILE: ${SQL_FILE}"
    PAYLOAD=$(jq -n --arg wid "${WAREHOUSE_ID}" --rawfile stmt "${SQL_FILE}" '{warehouse_id: $wid, statement: $stmt}')
    databricks api post /api/2.0/sql/statements --json "${PAYLOAD}"

    # convention: files are named <fqn>.sql - derive it for the describe step below
    FQN="$(basename "$SQL_FILE" .sql)"
fi


if [ "$ACTION" = "add-column" ]; then
    FQN="$2"; COL="$3"; TYPE="$4"
    if [ -z "$FQN" ] || [ -z "$COL" ] || [ -z "$TYPE" ]; then echo "usage: add-column <fqn> <col> <type>"; exit 1; fi

    STMT="ALTER TABLE ${FQN} ADD COLUMNS (${COL} ${TYPE})"
    echo "RUN SQL: ${STMT}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${STMT}\"}"
fi


if [ "$ACTION" = "drop-column" ]; then
    FQN="$2"; COL="$3"
    if [ -z "$FQN" ] || [ -z "$COL" ]; then echo "usage: drop-column <fqn> <col>"; exit 1; fi

    echo "checking delta.columnMapping.mode on ${FQN} first - DROP COLUMN needs 'name' or 'id'"
    MAPPING_MODE=$(databricks tables get "${FQN}" -o json | jq -r '.properties["delta.columnMapping.mode"] // "none"')
    if [ "$MAPPING_MODE" != "name" ] && [ "$MAPPING_MODE" != "id" ]; then
        echo "REFUSING: delta.columnMapping.mode is '${MAPPING_MODE}' on ${FQN} - DROP COLUMN needs 'name' or 'id'."
        echo "Not enabling it automatically as a side effect of a drop - it's a real, somewhat"
        echo "one-way protocol upgrade and deserves its own explicit step. Enable it first, e.g.:"
        echo "  databricks api post /api/2.0/sql/statements --json '{\"warehouse_id\":\"'\"${WAREHOUSE_ID}\"'\",\"statement\":\"ALTER TABLE ${FQN} SET TBLPROPERTIES ('\''delta.columnMapping.mode'\''='\''name'\'', '\''delta.minReaderVersion'\''='\''2'\'', '\''delta.minWriterVersion'\''='\''5'\'')\"}'"
        exit 1
    fi

    echo "About to drop column '${COL}' from '${FQN}' - metadata only, old parquet files keep"
    echo "the data (still reachable via time travel until VACUUM). Re-adding the name later"
    echo "creates a NEW empty column, the old values do not come back."
    read -r -p "Type the column name to confirm: " CONFIRM
    if [ "$CONFIRM" != "$COL" ]; then
        echo "Mismatch ('${CONFIRM}' != '${COL}') - aborted, nothing dropped."
        exit 1
    fi

    STMT="ALTER TABLE ${FQN} DROP COLUMN ${COL}"
    echo "RUN SQL: ${STMT}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${STMT}\"}"
fi


if [ "$ACTION" = "set-type" ]; then
    FQN="$2"; COL="$3"; TYPE="$4"
    if [ -z "$FQN" ] || [ -z "$COL" ] || [ -z "$TYPE" ]; then echo "usage: set-type <fqn> <col> <type>"; exit 1; fi

    echo "Delta only allows SAFE/widening type changes (e.g. INT -> BIGINT) via ALTER COLUMN TYPE -"
    echo "narrowing or incompatible changes raise natively, not caught here client-side."
    STMT="ALTER TABLE ${FQN} ALTER COLUMN ${COL} TYPE ${TYPE}"
    echo "RUN SQL: ${STMT}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${STMT}\"}"
fi


if [ "$ACTION" = "purge" ]; then
    FQN="$2"
    if [ -z "$FQN" ]; then echo "usage: purge <fqn>"; exit 1; fi

    echo "About to PERMANENTLY drop table '${FQN}' - the whole table, not a column, unrecoverable"
    echo "after Unity Catalog's recovery window (7 days by default, UNDROP TABLE ${FQN} until then -"
    echo "unless this catalog/schema has a shorter or disabled retention set)."
    read -r -p "Type the full fqn to confirm (1/2): " FIRST
    if [ "$FIRST" != "$FQN" ]; then
        echo "Mismatch ('${FIRST}' != '${FQN}') on first confirmation - aborted, nothing dropped."
        exit 1
    fi
    read -r -p "Type it again to confirm (2/2): " SECOND
    if [ "$SECOND" != "$FQN" ]; then
        echo "Mismatch ('${SECOND}' != '${FQN}') on second confirmation - aborted, nothing dropped."
        exit 1
    fi

    STMT="DROP TABLE ${FQN}"
    echo "RUN SQL: ${STMT}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${STMT}\"}"
    echo "${FQN} dropped. Recoverable via: UNDROP TABLE ${FQN}   (until the recovery window lapses)"
fi


if [ "$ACTION" = "describe" ]; then
    FQN="$2"
    if [ -z "$FQN" ]; then echo "usage: describe <fqn>"; exit 1; fi
fi


# every mutating action (not purge - the table's gone) ends by showing the result.
if [ "$ACTION" = "run-file" ] || [ "$ACTION" = "add-column" ] || [ "$ACTION" = "drop-column" ] \
   || [ "$ACTION" = "set-type" ] || [ "$ACTION" = "describe" ]; then
    echo "VALIDATE: schema + column-mapping/protocol properties of ${FQN}"
    databricks tables get "${FQN}" -o json \
      | jq -c '{full_name, data_source_format, table_type,
                columns: [.columns[] | "\(.name):\(.type_text)\(if .nullable == false then " NOT NULL" else "" end)"],
                properties: (.properties // {} | with_entries(select(
                    .key == "delta.columnMapping.mode" or
                    .key == "delta.minReaderVersion" or
                    .key == "delta.minWriterVersion")))}'
fi
