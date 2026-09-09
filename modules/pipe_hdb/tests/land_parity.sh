#!/bin/bash
# land_parity.sh - fire the LOCAL land and the DATABRICKS land AT THE SAME TIME,
# wait for both to finish, then list what each produced so you can eyeball parity.
#
#   local      : make land / make backfill  -> datalake/landing/datagov/resale_flat_prices/
#   databricks : deploy + run job, wait     -> /Volumes/macroecons/t0/landing/datagov/resale_flat_prices/
#
# usage:
#   ./tests/land_parity.sh              # update  -> 2017_onwards only, both sides
#   ./tests/land_parity.sh backfill     # backfill -> the 4 historical eras, both sides
#
# NOT run automatically anywhere - run it whenever you want a fresh both-sides check.

set -u
cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done          # -> repo root

MODE=${1:-update}                                                  # update | backfill
MODULE=modules/pipe_hdb
DEPLOY="$MODULE/deploy_databricks/databricks_deploy.sh"

# must match databricks_deploy.sh
CATALOG=macroecons
LANDING_SCHEMA=t0
LANDING_VOLUME=landing
case "$MODE" in
  update)   JOB=update;   LOCAL_TARGET=land     ;;
  backfill) JOB=backfill; LOCAL_TARGET=backfill ;;
  *) echo "usage: $0 [update|backfill]"; exit 1 ;;
esac

LOCAL_DIR="datalake/landing/datagov/resale_flat_prices"
VOL_DIR="dbfs:/Volumes/$CATALOG/$LANDING_SCHEMA/$LANDING_VOLUME/datagov/resale_flat_prices"

LOG_LOCAL=$(mktemp)
LOG_DBX=$(mktemp)
trap 'rm -f "$LOG_LOCAL" "$LOG_DBX"' EXIT

echo "══════════════════════════════════════════════════════════════════════"
echo " land parity   mode=$MODE   local:'make $LOCAL_TARGET'  ||  databricks job '$JOB'"
echo "══════════════════════════════════════════════════════════════════════"


# ── 1. fire both, in parallel ───────────────────────────────────────────────

( make -C "$MODULE" "$LOCAL_TARGET" ) >"$LOG_LOCAL" 2>&1 &
PID_LOCAL=$!
echo "[local]      pid $PID_LOCAL  -> $LOG_LOCAL"

# 'deploy' = export + sync + create-or-reset + run-now (fires the job, returns)
( "$DEPLOY" "$JOB" deploy ) >"$LOG_DBX" 2>&1 &
PID_DBX=$!
echo "[databricks] pid $PID_DBX  -> $LOG_DBX"
echo


# ── 2. wait for both commands to return ────────────────────────────────────

wait $PID_LOCAL; RC_LOCAL=$?
wait $PID_DBX;   RC_DBX=$?

echo "----- local  (exit $RC_LOCAL) -----------------------------------------"
tail -n 6 "$LOG_LOCAL"
echo
echo "----- databricks deploy  (exit $RC_DBX) -----------------------------"
tail -n 12 "$LOG_DBX"
echo


# ── 3. the databricks JOB is still running in the cloud - poll it ──────────
# 'deploy' only fires run-now and returns. grab the latest run for this job and
# wait for a terminal state before comparing the Volume.

JOB_NAME=$( [ "$JOB" = update ] && echo pipe_hdb_update || echo orchestrate_pipe_hdb_spark )
JOB_ID=$(databricks jobs list -o json 2>/dev/null \
  | python3 -c "import json,sys; print(next((j['job_id'] for j in json.load(sys.stdin) if j['settings']['name']=='$JOB_NAME'),''))")

if [ -n "$JOB_ID" ]; then
  echo "polling job $JOB_NAME (id $JOB_ID) for its latest run ..."
  for i in $(seq 1 60); do          # 60 * 10s = 10 min cap
    RUN=$(databricks jobs list-runs --job-id "$JOB_ID" --limit 1 -o json 2>/dev/null)
    LIFE=$(echo "$RUN"   | python3 -c "import json,sys; r=json.load(sys.stdin).get('runs',[{}])[0]; print(r.get('state',{}).get('life_cycle_state',''))" 2>/dev/null)
    RESULT=$(echo "$RUN" | python3 -c "import json,sys; r=json.load(sys.stdin).get('runs',[{}])[0]; print(r.get('state',{}).get('result_state',''))" 2>/dev/null)
    printf '  [%02d] %s %s\n' "$i" "${LIFE:-?}" "$RESULT"
    case "$LIFE" in
      TERMINATED|SKIPPED|INTERNAL_ERROR) break ;;
    esac
    sleep 10
  done
else
  echo "!! could not find job id for '$JOB_NAME' - skipping poll"
  RESULT=UNKNOWN
fi
echo


# ── 4. compare what each side landed ──────────────────────────────────────

echo "═══ landed files ════════════════════════════════════════════════════"
echo "-- local     $LOCAL_DIR"
ls -la "$LOCAL_DIR" 2>/dev/null | tail -n +2 || echo "   (missing)"
echo
echo "-- databricks $VOL_DIR"
databricks fs ls "$VOL_DIR" 2>/dev/null || echo "   (fs ls failed)"
echo


# ── 5. verdict ───────────────────────────────────────────────────────────

LOCAL_N=$(ls "$LOCAL_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
VOL_N=$(databricks fs ls "$VOL_DIR" 2>/dev/null | grep -c '\.csv$')
echo "csv count   local=$LOCAL_N   databricks=$VOL_N"

PASS=1
[ "$RC_LOCAL" -ne 0 ]           && { echo "FAIL: local 'make $LOCAL_TARGET' exit $RC_LOCAL"; PASS=0; }
[ "$RC_DBX"   -ne 0 ]           && { echo "FAIL: databricks deploy exit $RC_DBX"; PASS=0; }
[ "${RESULT:-}" != "SUCCESS" ] && [ -n "$JOB_ID" ] && { echo "FAIL: databricks job result=$RESULT"; PASS=0; }
[ "$LOCAL_N" -eq 0 ]           && { echo "FAIL: nothing landed locally"; PASS=0; }
[ "$LOCAL_N" != "$VOL_N" ]     && { echo "FAIL: csv count mismatch ($LOCAL_N vs $VOL_N)"; PASS=0; }

echo
[ "$PASS" -eq 1 ] && echo "PARITY OK" || { echo "PARITY FAILED"; exit 1; }
