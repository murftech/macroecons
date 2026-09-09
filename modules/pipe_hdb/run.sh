#!/bin/bash
set -e

cd "$(dirname "$0")/../.."
echo "running from $PWD"

SRC_DIR=modules/pipe_hdb/src
# echo $SRC_DIR

if [ -n "${RUNNING_IN_CONTAINER:-}" ]; then
  RUN="python"
else
  RUN="uv run --project modules/pipe_hdb --group iceberg python"
fi
# echo $RUN

#### EXPLAIN
# $1 is always the one word specified to key the call task that sh is defined
# $2 and onwards are then the word chunks to specified what arguments to pass 
# Hence for $@ to be = $1 $2 $3 ... $n, to be only arguments to pass to the programme without the call task, we use SHIFT ONCE.
#### EXPLAIN

# echo "full arg string after the .sh call:"
# echo "$@"

echo "call task is:"
CALL_TASK="${1:-}"
echo $CALL_TASK

shift

# SUB do i really need true? i dont think so

#### EXPLAIN
# Shift is needed because in python argparse every single token given in command line MUST be accounted for. 
# Why: typo-catching. Run --startMnth 2024-01 (misspelled) and you want an error, that alerts 
# hey what is this argument you passed useless for? Did you make a mistake? not a silent ignore that falls back to the default month.
# Hence, you must not give the irrelevant CALL TASK name into python script, hence shift it away always
#### EXPLAIN


# echo "arg strings now, after one shift:"
# echo "$@"
# echo "$1 | $2 | $3"

#### EXPLAIN
# # EXPLAIN: the link between $@ and programme being called
# every language exposes has a receiver for .sh current argument string which is emmitted by "$@""
# the argument string is whatever is written AFTER .sh

# Python receives argument string always in a python list object named sys.argv in this defined format:
# # sys.argv == [SCRIPT_CALLED.py, '$1', '$2', '$3', ..., '$n']
# then sys.argv[0] = SCRIPT_VALLED.py
# sys.argv[1] = $1
# sys.argv[2] = $2
# EXAMPLE:
# Then run python 1_import_to_t1.py --spark_engine java --startMonth 2024-01
# within python
# Then sys.argv == ['modules/pipe_hdb/src/1_import_to_t1.py', '--spark_engine', 'java', '--startMonth', '2024-01']
# print(sys.argv)
# parser = argparse.ArgumentParser()
# parser.add_argument('--startMonth', default = thisMonth)
# Once startMonth is on the parser's arguments list, python will search the sys.argv/$@ for '--startMonth',
# if it exists it will take the NEXT string as the runtime value, if it doesnt find, it will go to default
#### EXPLAIN

echo "RUN chosen TASK now:"

case "$CALL_TASK" in
  land)    $RUN $SRC_DIR/0_land_csv.py "$@" ;;
  import)  $RUN $SRC_DIR/1_import_to_t1.py "$@" ;;
  stage)   $RUN $SRC_DIR/2_stage_to_t2.py "$@" ;;
  report)  $RUN $SRC_DIR/2.a_report_firstbq.py "$@" ;;
# next handle report
  # persist) $RUN $SRC_DIR/3_persist_outputs_wip.py "$@" ;;

  # the default chain - each step on its own defaults. for a custom sequence,
  # write a recipe script that calls this one step by step.
  all)     $RUN $SRC_DIR/0_land_csv.py
           $RUN $SRC_DIR/1_import_to_t1.py
           $RUN $SRC_DIR/2_stage_to_t2.py ;;

  *)
    cat >&2 <<'EOF'
usage: sh ./run.sh {land|import|stage|report|all} [--flags...]

  land    0_land_csv.py       fetch datagov CSVs -> landing
  import  1_import_to_t1.py   landing CSV -> t1 (bronze, string cols, month-partitioned)
  stage   2_stage_to_t2.py    t1 -> t2 (silver, typed + derived + business shape)
  report  2.a_report_firstbq.py   build the plotly report

  ./run.sh <CALL_TASK> --help     list that step's flags
  ./run.sh <CALL_TASK>            run it on defaults
  ./run.sh all                    land + import + stage, all on defaults
EOF
    exit 1 ;;
esac

# LP: about --help
# at parse_args()	argparse scans sys.argv, finds --help, prints the help, calls sys.exit(0)
