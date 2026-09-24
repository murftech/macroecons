#!/bin/bash
# pandas + pyarrow mirror of pipe_hdb/run.sh - same call-task shape, pandas twins of each step.
set -e

cd "$(dirname "$0")/../.."
echo "running from $PWD"

SRC_DIR=modules/pipe_hdb_pandas_mirror/src
ORIGINAL_SRC_DIR=modules/pipe_hdb/src     # steps with no pandas twin run straight from the original repo

if [ -n "${RUNNING_IN_CONTAINER:-}" ]; then
  RUN="python"
else
  RUN="uv run --project modules/pipe_hdb_pandas_mirror python"
fi

#### EXPLAIN
# $1 is the call task; shift it away so python's argparse only sees the step's own flags.
# see pipe_hdb/run.sh for the full walkthrough of $@ -> sys.argv.
#### EXPLAIN

echo "call task is:"
CALL_TASK="${1:-}"
echo $CALL_TASK

shift

echo "RUN chosen TASK now:"

case "$CALL_TASK" in
  # land has no spark in it, so there is no twin: the original runs under this mirror's venv.
  land)    $RUN $ORIGINAL_SRC_DIR/0_land_csv.py "$@" ;;
  import)  $RUN $SRC_DIR/1_import_to_t1_pandas.py "$@" ;;
  stage)   $RUN $SRC_DIR/2_stage_to_t2_pandas.py "$@" ;;

  # the default chain - each step on its own defaults.
  all)     $RUN $ORIGINAL_SRC_DIR/0_land_csv.py
           $RUN $SRC_DIR/1_import_to_t1_pandas.py
           $RUN $SRC_DIR/2_stage_to_t2_pandas.py ;;

  *)
    cat >&2 <<'EOF'
usage: sh ./run_pandas.sh {land|import|stage|all} [--flags...]

  land    pipe_hdb/src/0_land_csv.py   fetch datagov CSVs -> landing (original, shared)
  import  1_import_to_t1_pandas.py     landing CSV -> t1 (bronze, string cols, month-partitioned)
  stage   2_stage_to_t2_pandas.py      t1 -> t2 (silver, typed + derived + business shape)

  ./run_pandas.sh <CALL_TASK> --help     list that step's flags
  ./run_pandas.sh <CALL_TASK>            run it on defaults
  ./run_pandas.sh all                    land + import + stage, all on defaults
EOF
    exit 1 ;;
esac
