#!/bin/bash
set -e

# forces cwd to the repo root regardless of where this script is invoked from - the
# docker build context needs both streamlit/ and modules/pipe_hdb/ (siblings)
cd "$(dirname "$0")"
while [ ! -d .vscode ] && [ "$PWD" != "/" ]; do cd ..; done

IMAGE=hdb-streamlit
COMPOSE_FILE=streamlit/docker-compose.yml

usage() {
  echo "Usage: ./streamlit/deploy_docker.sh [build|up|down]"
  echo "  build - build the streamlit image locally (hdb-streamlit:local)"
  echo "  up    - run it locally via docker compose (see streamlit/docker-compose.yml)"
  echo "  down  - stop and remove the local docker compose container"
  echo "(cloud submit/deploy commands live in ./streamlit/deploy_gcp.sh)"
  exit 1
}

case "$1" in
  build)
    docker build -f streamlit/Dockerfile -t ${IMAGE}:local .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
    ;;

  up)
    docker compose -f ${COMPOSE_FILE} up -d
    echo 'given url: 
    http://localhost:8081'
    ;;

  down)
    docker compose -f ${COMPOSE_FILE} down
    ;;
    
  *)
    usage
    ;;
esac
