#!/bin/bash
# set -e
set -euo pipefail

SECONDS=0
trap 'printf "\nTotal Elapsed Time: %dm%02ds\n" $((SECONDS/60)) $((SECONDS%60))' EXIT

cd "$(dirname "$0")/../.."
echo Running from PWD $PWD


# Concept Guide:
# -n	true if the string that follows has non-zero length (-z is its opposite)
# "${RUNNING_IN_CONTAINER:-}"	the ARGs setted value, else "" if None.
# essentially it is testing: If RUNNING_IN_CONTAINER exist, use python, is it doesnt exist use uv run.

if [ -n "${RUNNING_IN_CONTAINER:-}" ]; then
  RUN="python"
else
  RUN="uv run --project modules/pipe_hdb_spark python"
fi

STEP="${1:-all}"
case "$STEP" in
  ingest)  $RUN modules/pipe_hdb_spark/src/0_import_datagov.py ;;

  report)  $RUN modules/pipe_hdb_spark/src/2_report_firstbq.py ;;

  # persist) $RUN modules/pipe_hdb_spark/3_persist_outputs_wip.py ;;

  all)     $RUN modules/pipe_hdb_spark/src/0_import_datagov.py
           $RUN modules/pipe_hdb_spark/src/2_report_firstbq.py ;;
          #  $RUN modules/pipe_hdb_spark/3_persist_outputs.py ;;

  *) echo "usage: ./run_pipeline.sh [ingest|report|persist|all]" >&2; exit 1 ;;

esac

# $RUN modules/pipe_hdb_spark/0_import_datagov.py
# $RUN modules/pipe_hdb_spark/2_report_firstbq.py
# $RUN modules/pipe_hdb_spark/3_persist_outputs.py

# i feel this is much cleaner, because, it doesn t make people open a python file to run a python file...
# it is very clean which file to open to run the pipleines.
# next question is, can this be replaced by airflow, prefetc or whatever. Or should it even be?