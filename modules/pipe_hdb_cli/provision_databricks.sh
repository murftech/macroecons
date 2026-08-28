#!/bin/bash
set -e

# Provision catalog, schema, and volume on Databricks.
# Run once before first deployment. Idempotent.

cd "$(dirname "$0")"

#### config ####
CATALOG=macroecons
SCHEMA=macroecons_hdb_cli
VOLUME=reports
WAREHOUSE_ID=3a82455c9b1702b1

usage() {
  echo "Usage: ./provision_databricks.sh [bootstrap|sql <name>|destroy]"
  echo "  bootstrap - create catalog + schema + volume"
  echo "  destroy   - delete volume + schema (catalog left in place)"
  echo "  sql <name> - run a SQL statement (or list available)"
  exit 1
}

#### SQL statements ####
# Free Edition's catalogs REST API fails; SQL Statement Execution API works.
SQL_NAMES="create_catalog drop_catalog create_schema drop_schema create_volume
           show_catalogs show_schemas show_tables show_volumes
           describe_schema describe_table count_rows sample_rows table_history"

sql_text() {
  case "$1" in
  create_catalog) echo "CREATE CATALOG IF NOT EXISTS ${CATALOG}" ;;
  drop_catalog) echo "DROP CATALOG IF EXISTS ${CATALOG} CASCADE" ;;
  create_schema) echo "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}" ;;
  drop_schema) echo "DROP SCHEMA IF EXISTS ${CATALOG}.${SCHEMA} CASCADE" ;;
  create_volume) echo "CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}" ;;
  show_catalogs) echo "SHOW CATALOGS" ;;
  show_schemas) echo "SHOW SCHEMAS IN ${CATALOG}" ;;
  show_tables) echo "SHOW TABLES IN ${CATALOG}.${SCHEMA}" ;;
  show_volumes) echo "SHOW VOLUMES IN ${CATALOG}.${SCHEMA}" ;;
  describe_schema) echo "DESCRIBE SCHEMA EXTENDED ${CATALOG}.${SCHEMA}" ;;
  describe_table) echo "DESCRIBE TABLE EXTENDED ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  count_rows) echo "SELECT COUNT(*) AS n FROM ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  sample_rows) echo "SELECT * FROM ${CATALOG}.${SCHEMA}.hdb_silver LIMIT 10" ;;
  table_history) echo "DESCRIBE HISTORY ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  *) return 1 ;;
  esac
}

statement_json() {
  cat <<EOF
{
  "warehouse_id": "${WAREHOUSE_ID}",
  "statement": "$1",
  "wait_timeout": "30s"
}
EOF
}

run_sql() {
  local stmt
  if ! stmt=$(sql_text "$1"); then
    echo "unknown statement '$1'. available:" >&2
    echo "${SQL_NAMES}" | tr -s ' \n' '\n' | sed 's/^/  /' >&2
    return 1
  fi
  echo "  SQL> ${stmt}" >&2
  databricks api post /api/2.0/sql/statements --json "$(statement_json "${stmt}")"
}

# Swallow "already exists" errors; resources are idempotent on their own.
run_idempotent() {
  local what="$1"
  shift
  local output
  if output=$("$@" 2>&1); then
    echo "  created ${what}"
  elif echo "${output}" | grep -qiE 'already exists|ALREADY_EXISTS'; then
    echo "  ${what} already exists, skipping"
  else
    echo "  FAILED creating ${what}:" >&2
    echo "${output}" >&2
    return 1
  fi
}

do_bootstrap() {
  echo "== bootstrap =="
  run_sql create_catalog
  run_idempotent "schema ${CATALOG}.${SCHEMA}" databricks schemas create "${SCHEMA}" "${CATALOG}"
  run_idempotent "volume ${CATALOG}.${SCHEMA}.${VOLUME}" \
    databricks volumes create "${CATALOG}" "${SCHEMA}" "${VOLUME}" MANAGED
  local host
  host=$(databricks auth describe -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['details']['host'])" 2>/dev/null || echo "<workspace>")
  echo "Here:
      ${host}/explore/data"
}

do_destroy() {
  echo "== destroy =="
  databricks volumes delete "${CATALOG}.${SCHEMA}.${VOLUME}" 2>/dev/null && echo "  deleted volume" || echo "  volume already gone"
  databricks schemas delete "${CATALOG}.${SCHEMA}" 2>/dev/null && echo "  deleted schema" || echo "  schema already gone"
  echo "  catalog ${CATALOG} left in place"
}

main() {
  case "$1" in

  bootstrap) do_bootstrap ;;

  sql)
    if [ -z "$2" ]; then
      echo "available statements:"
      echo "${SQL_NAMES}" | tr -s ' \n' '\n' | sed 's/^/  /'
    else
      run_sql "$2"
    fi
    ;;

  destroy) do_destroy ;;

  *) usage ;;

  esac
}

main "$@"
