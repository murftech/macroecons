#!/bin/bash
# set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


# LOCAL
MODULE_DEV=modules/pipe_hdb
SRC_DEV=${MODULE_DEV}/src
# REMOTE
DATABRICKS_PROFILE=murftech7@gmail.com
MODULE_DESTINATION=/Workspace/Users/${DATABRICKS_PROFILE}/deployments/pipe_hdb
SRC_DESTINATION=${MODULE_DESTINATION}/src

# UC layout (medallion = schema-per-layer, all under the macroecons domain catalog):
#   macroecons.t0.landing                             Volume - landed raw CSVs (tier 0)
#   macroecons.t1.datagov_resale_flat_prices (+_iceberg)   Delta / Iceberg (bronze)
#   macroecons.t2.datagov_resale_flat_prices (+_iceberg)   Delta / Iceberg (silver)
#   macroecons.t3.*                                   (later)
CATALOG_NAME=macroecons
LANDING_SCHEMA=t0            # tier-0 schema that holds the landing Volume
LANDING_VOLUME=landing       # the Volume itself
TIER1_SCHEMA=t1              # t1 (bronze) tables go here, NOT in LANDING_SCHEMA
TIER2_SCHEMA=t2              # t2 (silver) tables go here

# ── TWO JOBS ─────────────────────────────────────────────────────────────────
#   backfill  = full history rebuild. all 5 eras -> t1, full 1990-01.. window -> t2.
#               8 tasks. run rarely / by hand. mirrors `make fresh`-ish.
#   update    = the routine daily run. mirrors `make run` = land + import + stage,
#               each on its DEFAULTS: --mode update (2017_onwards only), --eras
#               2017_onwards, stage current-month-only. 3 serial tasks, no fan-out.
JOB_BACKFILL_NAME=orchestrate_pipe_hdb_spark
JOB_UPDATE_NAME=pipe_hdb_update

# Explore: https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941
JOBS_URL="https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941"


usage() {
  cat >&2 <<EOF
Usage:
  ./databricks_deploy.sh <action>                 (job-agnostic)
      login    - ensure authenticated to ${DATABRICKS_PROFILE}
      export   - uv export the databricks dep group -> src/requirements-databricks.txt
      sync     - push src/ to the databricks workspace
      unsync   - delete src/ from the workspace

  ./databricks_deploy.sh <job> <action>           <job> = backfill | update
      show     - render + jq-validate that job's JSON (no API call)
      create   - create the job (blocks if the name already exists)
      reset    - update the existing job to the current JSON
      deploy   - export + sync + create-or-reset + run   (the one-shot)
      run      - run-now
      delete   - delete the job

  e.g.  ./databricks_deploy.sh update deploy
        ./databricks_deploy.sh backfill run
EOF
  exit 1
}

main() {
  case "$1" in
    login)  login "${@:2}"; return ;;
    export) uv_export;       return ;;
    sync)   sync;            return ;;
    unsync) unsync;          return ;;
  esac

  # every remaining action is per-job: pick JOB_NAME + JOB_JSON, then dispatch.
  case "$1" in
    backfill) JOB_NAME=$JOB_BACKFILL_NAME; JOB_JSON=$(job_json_backfill) ;;
    update)   JOB_NAME=$JOB_UPDATE_NAME;   JOB_JSON=$(job_json_update)   ;;
    *)        usage ;;
  esac

  case "$2" in
    show)   echo "$JOB_JSON" | jq . ;;
    create) create ;;
    reset)  reset ;;
    run)    run ;;
    delete) delete ;;
    deploy) uv_export; sync; create_or_reset; run ;;
    *)      usage ;;
  esac
}

login() {
  source "cloud_databricks_shared/databricks_login.sh"
  do_login "$@"
}


######## PUSH SCRIPTS ##########
uv_export() {
    echo "RUN: uv export"
    (
      cd "${MODULE_DEV}" &&
      uv export --only-group databricks --no-hashes -o src/requirements-databricks.txt
      )
}

# Without docker, immutability is fully defined by:
# "environment_version": "X" in JOB_JSON,
# and uv.lock and pyproject.toml > requirements-databricks.txt

sync() {
    echo "Validate DEV >> DESTINATION:  ${SRC_DEV} >> ${SRC_DESTINATION}"
    echo "RUN: databricks sync"
    databricks sync --full "${SRC_DEV}" "${SRC_DESTINATION}"
    echo "Here: https://dbc-b01338b1-a584.cloud.databricks.com/browse/folders/4314226011913546?o=7474643839559941"
}

unsync() {
    echo "RUN: databricks workspace delete"
    databricks workspace delete "${MODULE_DESTINATION}" --recursive
}


################################
# JOB CONFIG
################################

# EXPLAIN:
# Parameters in a TASK are fixed per-deploy values.
# Parameters in the JOB are user-editable at run time (shown in the Databricks UI).
# Here: catalog is hardcoded in the task; landing_schema is a job parameter, so a
# one-off run can be pointed at another schema from the UI without a redeploy.
#
# schedule is embedded in the JSON. `update` ships UNPAUSED (verified green 2026-09-10,
# it is the one meant to run daily at 03:00 SGT); `backfill` stays PAUSED - run by hand.

# ── shared task fragments ────────────────────────────────────────────────────
# $1 = task_key   $2 = --mode value (update|backfill)   $3 = depends_on JSON array ('[]' for none)
land_task() {
  cat <<EOF
    {
      "task_key": "$1",
      "depends_on": $3,
      "environment_key": "default_env",
      "max_retries": 2,
      "min_retry_interval_millis": 60000,
      "spark_python_task": {
        "python_file": "${SRC_DESTINATION}/0_land_csv.py",
        "parameters": ["--mode", "$2",
                       "--catalog", "${CATALOG_NAME}",
                       "--schema", "{{job.parameters.landing_schema}}",
                       "--volume", "${LANDING_VOLUME}"]
      }
    }
EOF
}

# $1 = task_key   $2 = --eras value   $3 = depends_on JSON array
import_task() {
  cat <<EOF
    {
      "task_key": "$1",
      "depends_on": $3,
      "environment_key": "default_env",
      "max_retries": 2,
      "min_retry_interval_millis": 60000,
      "spark_python_task": {
        "python_file": "${SRC_DESTINATION}/1_import_to_t1.py",
        "parameters": ["--eras", "$2",
                       "--write_format", "delta,iceberg",
                       "--catalog", "${CATALOG_NAME}",
                       "--schema", "{{job.parameters.landing_schema}}",
                       "--volume", "${LANDING_VOLUME}"]
      }
    }
EOF
}

# $1 = task_key   $2 = depends_on JSON array   $3 = extra params CSV (e.g. '"--startMonth","1990-01",') or ''
stage_task() {
  cat <<EOF
    {
      "task_key": "$1",
      "depends_on": $2,
      "environment_key": "default_env",
      "max_retries": 2,
      "min_retry_interval_millis": 60000,
      "spark_python_task": {
        "python_file": "${SRC_DESTINATION}/2_stage_to_t2.py",
        "parameters": [$3"--write_format", "delta,iceberg",
                       "--catalog", "${CATALOG_NAME}",
                       "--schema", "{{job.parameters.landing_schema}}",
                       "--volume", "${LANDING_VOLUME}"]
      }
    }
EOF
}

# ── job envelope (name filled by the caller) ─────────────────────────────────
# $1 = job name   $2 = tasks JSON array body (without the surrounding [])   $3 = PAUSED|UNPAUSED
job_envelope() {
  cat <<EOF
{
  "name": "$1",
  "edit_mode": "UI_LOCKED",
  "timeout_seconds": 1800,
  "schedule": {
    "quartz_cron_expression": "0 0 3 * * ?",
    "timezone_id": "Asia/Singapore",
    "pause_status": "$3"
  },
  "email_notifications": {
    "on_failure": ["${DATABRICKS_PROFILE}"],
    "no_alert_for_skipped_runs": true
  },
  "parameters": [
    {"name": "landing_schema", "default": "${LANDING_SCHEMA}"},
    {"name": "run_date", "default": "{{job.start_time.iso_date}}"}
  ],
  "environments": [
    { "environment_key": "default_env",
      "spec": { "environment_version": "5",
                "dependencies": ["-r ${SRC_DESTINATION}/requirements-databricks.txt"] } }
  ],
  "tasks": [
$2
  ]
}
EOF
}

# ── BACKFILL: land x2 (parallel) -> 5 imports (serial) -> stage_all ──────────
#   serial imports: concurrent .overwrite() on one Delta table throws
#   DELTA_CONCURRENT_APPEND even for disjoint months; the 5 eras are tiny, so a
#   chain costs ~nothing. import_2017_onwards runs first and .create()s both t1
#   tables with the full column set; older eras align DOWN (1990-2014 lack
#   remaining_lease) and .overwrite() their own months.
#
#     land_backfill ┐
#     land_update  ─┴─> import_2017_onwards -> import_1990_1999 -> import_2000_2012Feb
#                           (creates t1)          -> import_2012Mar_2014 -> import_2015_2016 -> stage_all
job_json_backfill() {
  job_envelope "${JOB_BACKFILL_NAME}" "$(cat <<EOF
$(land_task land_backfill backfill '[]'),
$(land_task land_update   update   '[]'),
$(import_task import_2017_onwards  2017_onwards  '[{"task_key":"land_backfill"},{"task_key":"land_update"}]'),
$(import_task import_1990_1999     1990_1999     '[{"task_key":"import_2017_onwards"}]'),
$(import_task import_2000_2012Feb  2000_2012Feb  '[{"task_key":"import_1990_1999"}]'),
$(import_task import_2012Mar_2014  2012Mar_2014  '[{"task_key":"import_2000_2012Feb"}]'),
$(import_task import_2015_2016     2015_2016     '[{"task_key":"import_2012Mar_2014"}]'),
$(stage_task  stage_all  '[{"task_key":"import_2015_2016"}]'  '"--startMonth", "1990-01", ')
EOF
)" PAUSED
}

# ── UPDATE: the routine daily run - mirrors `make run` on defaults ───────────
#     land_update -> import_update (--eras 2017_onwards) -> stage_update (current month only)
job_json_update() {
  job_envelope "${JOB_UPDATE_NAME}" "$(cat <<EOF
$(land_task   land_update    update        '[]'),
$(import_task import_update   2017_onwards  '[{"task_key":"land_update"}]'),
$(stage_task  stage_update    '[{"task_key":"import_update"}]'  '')
EOF
)" UNPAUSED
}


######## ORCHESTRATE JOB (all use $JOB_NAME / $JOB_JSON set by main) ##########

get_job_id() {
  databricks jobs list -o json \
    | python3 -c "import json,sys; print(next((j['job_id'] for j in json.load(sys.stdin) if j['settings']['name']=='$JOB_NAME'), ''))"
}

create() {
    local job_id; job_id=$(get_job_id)
    if [ -n "$job_id" ]; then
      echo "job [$JOB_NAME] already exists as id [$job_id] - 'create' would duplicate it. use 'reset'."
      return 0
    fi
    echo "$JOB_JSON" | jq .   # jq fails loudly on a bad heredoc, before the API call
    echo "RUN: databricks jobs create  [$JOB_NAME]"
    databricks jobs create --json "$JOB_JSON"
    echo "Here: ${JOBS_URL}"
}

reset() {
    local job_id; job_id=$(get_job_id)
    [ -z "$job_id" ] && { echo "no job named $JOB_NAME - run 'create' first"; return 1; }
    echo "RUN: databricks jobs reset  [$JOB_NAME id=$job_id]"
    databricks jobs reset --json "$(echo "$JOB_JSON" | jq --argjson id "$job_id" '{job_id: $id, new_settings: .}')"
    echo "Here: ${JOBS_URL}"
}

create_or_reset() {
    local job_id; job_id=$(get_job_id)
    if [ -z "$job_id" ]; then create; else reset; fi
}

run() {
    local job_id; job_id=$(get_job_id)
    [ -z "$job_id" ] && { echo "no job named $JOB_NAME - run 'create' first"; return 1; }
    echo "RUN: databricks jobs run-now  [$JOB_NAME id=$job_id]"
    databricks jobs run-now "$job_id"
    echo "Here: ${JOBS_URL}"
}

delete() {
    local job_id; job_id=$(get_job_id)
    [ -z "$job_id" ] && { echo "no job named $JOB_NAME - nothing to delete"; return 0; }
    echo "RUN: databricks jobs delete  [$JOB_NAME id=$job_id]"
    databricks jobs delete "$job_id"
    echo "Here: ${JOBS_URL}"
}

################################

main "$@"
