#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


WAREHOUSE_ID=3a82455c9b1702b1
# CATALOG_NAME=databricks_class
# SCHEMA_NAME=schema_class
# VOLUME_NAME=outputs

CATALOG_NAME=macroecons
SCHEMA_NAME=macroecons_hdb_cli
# VOLUME_NAME=outputs

CATALOG_NAME=macroecons
SCHEMA_NAME=raw
# VOLUME_NAME=outputs

EXPLORE_URL=https://dbc-b01338b1-a584.cloud.databricks.com/explore/data?o=7474643839559941


# teardown is the EXACT REVERSE of provision: volume -> schema -> catalog.
# a child can't be dropped after its parent is gone, and dropping a parent
# with --force / CASCADE would silently take children with it.
#
# inline testers:
# databricks volumes delete  ${CATALOG_NAME}.${SCHEMA_NAME}.${VOLUME_NAME}
# databricks schemas delete  ${CATALOG_NAME}.${SCHEMA_NAME} --force
# databricks catalogs delete ${CATALOG_NAME} --force
# databricks auth logout --profile $DATABRICKS_PROFILE


usage() {
  echo "Usage: [login|volume|schema|catalog|all|logout]"
  echo "  login   - ensure authenticated to ${DATABRICKS_PROFILE} (needed to delete anything)"
  echo "  volume  - delete the volume"
  echo "  schema  - delete the schema"
  echo "  catalog - DROP CATALOG via the SQL API"
  echo "  all     - Run in order: login, delete volume, delete schema, delete catalog"
  exit 1
}

main() {
  case "$1" in
  login)   do_login ;;
  volume)  delete_volume ;;
  schema)  delete_schema ;;
  catalog) delete_catalog ;;
  all)     do_login; delete_volume; delete_schema; delete_catalog ;;
  *)       usage ;;
  esac
}

do_login() {
  source "cloud_databricks_shared/databricks_login.sh"
  do_login "$@"
}

delete_volume() {
    echo "RUN: databricks volumes delete"  
    databricks volumes delete "${CATALOG_NAME}.${SCHEMA_NAME}.${VOLUME_NAME}"
    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/volumes/databricks_class/schema_class/outputs?o=7474643839559941"
}

delete_schema() {
    echo "RUN: databricks schemas delete"
    databricks schemas delete "${CATALOG_NAME}.${SCHEMA_NAME}" --force
    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/databricks_class/schema_class?o=7474643839559941"
}

SQL_CATALOG_DOWN=$(cat <<EOF
{
  "warehouse_id": "${WAREHOUSE_ID}",
  "statement": "DROP CATALOG IF EXISTS ${CATALOG_NAME} CASCADE"
}
EOF
)

delete_catalog() {

    echo "RUN: warehouses start, ensuring warehouse is up"
    databricks warehouses start "${WAREHOUSE_ID}" >/dev/null

    echo "CHECK: look before you send SQL JSON"
    echo "$SQL_CATALOG_DOWN"
    echo "RUN: api post /api/2.0/sql/statements"
    databricks api post /api/2.0/sql/statements --json "$SQL_CATALOG_DOWN"
    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/explore/data/databricks_class?o=7474643839559941"
}


main "$@"
