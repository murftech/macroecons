#!/bin/bash
set -e

# forces cwd to the repo root regardless of where this script is invoked from - the
# docker build context needs the full repo, not just modules/pipe_hdb/
cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

IMAGE=hdb-pipeline

usage() {
  echo "Usage: ./modules/pipe_hdb/deploy_desktop.sh [build|run]"
  echo "  build   - build the pipeline image locally (hdb-pipeline:local)"
  echo "  run     - run the locally built pipeline image"
  exit 1
}

case "$1" in

  build)
    docker build -f modules/pipe_hdb/Dockerfile -t ${IMAGE}:local .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
  ;;

  run)
    # docker run --rm ${IMAGE}:local
    docker run --rm -v "/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/macroecons/pipe_hdb:/app/output" ${IMAGE}:local    
    ;;

  *)
    usage
    ;;
esac
