#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


MODULE=modules/pipe_hdb
DEPLOY_LOCAL="${MODULE}/deploy_local"
RUN="uv run --project ${MODULE} --group iceberg --group delta python"


usage() {
  echo "Usage: [delta|iceberg|all] [--env dev|production]"
  echo "  delta   - provision the local DELTA lakehouse catalog/tables (provision_deltalake_catalog.py)"
  echo "  iceberg - provision the local ICEBERG catalog/tables (provision_pyiceberg_catalog.py)"
  echo "  all     - run both, delta then iceberg"
  exit 1
}

main() {
  case "$1" in
  delta)   shift; do_delta "$@" ;;
  iceberg) shift; do_iceberg "$@" ;;
  all)     shift; do_delta "$@"; do_iceberg "$@" ;;
  *)       usage ;;
  esac
}

do_delta() {
    echo "RUN: provision_deltalake_catalog.py $*"
    $RUN "${DEPLOY_LOCAL}/provision_deltalake_catalog.py" "$@"
}

do_iceberg() {
    echo "RUN: provision_pyiceberg_catalog.py $*"
    $RUN "${DEPLOY_LOCAL}/provision_pyiceberg_catalog.py" "$@"
}


main "$@"
