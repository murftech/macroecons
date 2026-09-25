#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


# UC layout (medallion = schema-per-layer, all under one domain catalog):
#   macroecons.t0.landing                     Volume - landed raw CSVs (tier 0, pre-bronze)
#   macroecons.t1 / t2 / t3                    layer schemas - the bronze/silver/gold tables
WAREHOUSE_ID=3a82455c9b1702b1
CATALOG_NAME=macroecons
LANDING_SCHEMA=t0
LANDING_VOLUME=landing
LAYER_SCHEMAS="t1 t2 t3"


usage() {
  echo "Usage: [login|catalog|schemas|volume|all]"
  echo "  login   - ensure authenticated to murftech7@gmail.com"
  echo "  catalog - CREATE CATALOG IF NOT EXISTS via the SQL API (REST create is rejected on Free Edition)"
  echo "  schemas - CREATE SCHEMA IF NOT EXISTS for: ${LANDING_SCHEMA} ${LAYER_SCHEMAS}"
  echo "  volume  - create the ${CATALOG_NAME}.${LANDING_SCHEMA}.${LANDING_VOLUME} MANAGED volume"
  echo "  all     - run login, catalog, schemas, volume in sequence"
  echo "TABLE-level provisioning (CREATE TABLE, ADD/DROP COLUMN, type changes) moved to"
  echo "the sibling script databricks_table_management.sh (scope split 2026-09-25) -"
  echo "this script only ever provisions catalog/schema/volume, never a table."
  exit 1
}

main() {
  case "$1" in
  login)   do_login ;;
  catalog) do_catalog ;;
  schemas) do_schemas ;;
  volume)  do_volume ;;
  all)     do_login; do_catalog; do_schemas; do_volume ;;
  *)       usage ;;
  esac
}

do_login() {
  source "cloud_databricks_shared/databricks_login.sh"
  do_login "$@"
}


# one SQL statement per call to /api/2.0/sql/statements. all are IF NOT EXISTS,
# so the whole script is safe to re-run.
run_sql() {
    local stmt="$1"
    echo "RUN SQL: ${stmt}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${stmt}\"}"
}

warm_warehouse() {
    echo "RUN: warehouses start (SQL API needs a live warehouse)"
    databricks warehouses start "${WAREHOUSE_ID}" >/dev/null
}


do_catalog() {
    warm_warehouse
    run_sql "CREATE CATALOG IF NOT EXISTS ${CATALOG_NAME}"
    echo "Here: https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/${CATALOG_NAME}?o=7474643839559941"
}

do_schemas() {
    warm_warehouse
    run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${LANDING_SCHEMA}"
    for s in ${LAYER_SCHEMAS}; do
        run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${s}"
    done
    echo "Here: https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/${CATALOG_NAME}?o=7474643839559941"
}

do_volume() {
    echo "RUN: databricks volumes create (errors if it already exists - that's fine, skip)"
    databricks volumes create "${CATALOG_NAME}" "${LANDING_SCHEMA}" "${LANDING_VOLUME}" MANAGED
    echo "Here: https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/volumes/${CATALOG_NAME}/${LANDING_SCHEMA}/${LANDING_VOLUME}?o=7474643839559941"
}



main "$@"
