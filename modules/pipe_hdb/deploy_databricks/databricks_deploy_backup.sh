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
JOB_NAME=orchestrate_pipe_hdb_spark
CATALOG_NAME=databricks_class
SCHEMA_NAME=schema_class
VOLUME_NAME=outputs

# Here Explore Workspace URL: https://dbc-b01338b1-a584.cloud.databricks.com/browse?o=7474643839559941

# LP: Home and Users/murftech7@gmail.com are the same folder
# LP: Home is an alias. Expand either and you get the identical five entries because they 
# resolve to the same path. Nothing is duplicated there — the sidebar just shows both the shortcut and the full tree.


usage() {
  echo "Usage: [login|export|sync|unsync|create|reset|run|delete|dockercombine|sync_n_run]"
  echo "  login         - ensure authenticated to ${DATABRICKS_PROFILE}"
  echo "  export        - uv export the databricks dep group -> src/requirements-databricks.txt"
  echo "  sync          - push source code to databricks workspace"
  echo "  unsync        - delete source code from databricks workspace"
  echo "  create        - create job with current src x JOB_JSON"
  echo "  reset         - update job with current src x JOB_JSON"
  echo "  run           - run job"
  echo "  delete        - delete job"
  echo "  dockercombine - run in order: export, sync, create"
  echo "  sync_n_run    - run in order: export, sync, reset, run"
  exit 1
}

main() {
  case "$1" in
  login)  login ;;
  export) uv_export ;;
  sync)   sync ;;
  unsync) unsync ;;
  create) create ;;
  reset)  reset ;;
  run)    run ;;
  delete) delete ;;
  dockercombine) uv_export; sync; create ;;
  sync_n_run) uv_export; sync; reset; run ;;
  # all)    login; uv_export; sync ;;
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


################################
sync() {
    echo "Validate DEV >> DESTINATION:  ${SRC_DEV} >> ${SRC_DESTINATION}"

    echo "RUN: databricks sync"
    databricks sync --full "${SRC_DEV}" "${SRC_DESTINATION}"

    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/browse/folders/4314226011913546?o=7474643839559941"
}

################################
unsync() {
    echo "RUN: databricks workspace delete"
    databricks workspace delete "${MODULE_DESTINATION}" --recursive

    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/browse/folders/4314226011913546?o=7474643839559941"
}

######## ORCHESTRATE JOB ##########

# get job_id via job name
# claude created, not digested yet
get_job_id() {
  databricks jobs list -o json \
    | python3 -c "import json,sys; print(next((j['job_id'] for j in json.load(sys.stdin) if j['settings']['name']=='$JOB_NAME'), ''))"
}


################################
# JOB CONFIG
################################

# EXPLAIN:
# Parameters in TASK are fixed runtime environment varaiables
# Paramters in JOBS are user editable runtime variables

# To Demo editable runtime arguments on databricks UI -
# hardcode the catalog in TASK parameter, but let user vary the schema in JOBS parameter.
# AL: directing table write to another schema: Succeeded.

# EXPLAIN:
# embed schedule inside JOB_JSON.


JOB_JSON=$(cat <<EOF
{
  "name": "$JOB_NAME",
  "edit_mode": "UI_LOCKED",
  "timeout_seconds": 1800,

  "schedule": {
    "quartz_cron_expression": "0 0 3 * * ?",
    "timezone_id": "Asia/Singapore",
    "pause_status": "UNPAUSED"
  },

  "email_notifications": {
    "on_failure": ["${DATABRICKS_PROFILE}"],
    "no_alert_for_skipped_runs": true
  },

  "parameters": [
    {"name": "schema", "default": "${SCHEMA_NAME}"},
    {"name": "run_date", "default": "{{job.start_time.iso_date}}"}
  ],

  "environments": [
    {
      "environment_key": "default_env",
      "spec": {
        "environment_version": "5",
        "dependencies": ["-r ${SRC_DESTINATION}/requirements-databricks.txt"]
      }
    }
  ],

  "tasks": [

    {
      "task_key": "0_import_datagov",
      "environment_key": "default_env",
      "max_retries": 2,
      "min_retry_interval_millis": 60000,
      "spark_python_task": {
        "python_file": "${SRC_DESTINATION}/0_import_datagov.py",
        "parameters": ["--catalog", "${CATALOG_NAME}", "--schema", "{{job.parameters.schema}}", "--volume", "${VOLUME_NAME}"]
      }
    },

    {
      "task_key": "2_report_firstbq",
      "depends_on": [{"task_key": "0_import_datagov"}],
      "environment_key": "default_env",
      "spark_python_task": {
        "python_file": "${SRC_DESTINATION}/2_report_firstbq.py",
        "parameters": ["--catalog", "${CATALOG_NAME}", "--schema", "{{job.parameters.schema}}", "--volume", "${VOLUME_NAME}", "--run-date", "{{job.parameters.run_date}}"]

      }
    }
  ]
}
EOF
)

#########################

create() {
    
    local job_id
    job_id=$(get_job_id)

    if [ -n "$job_id" ]; then
      echo "An id [$job_id] with job name [$JOB_NAME] already exists  - "
      echo "databricks job create will duplicate [$JOB_NAME] across multiple ids. Command blocked. Use 'update' instead."
      return 0
    fi

    echo "$JOB_JSON" | jq .
    echo "^^ jq: pretty-print, validate JOB_JSON syntax (jq fails loudly on a bad heredoc, rather than during API call)\n"
    
    echo "RUN: databricks jobs create"
    # main
    databricks jobs create --json "$JOB_JSON"

    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941"
}

#########################

reset() {
    local job_id
    job_id=$(get_job_id)

    # guard for job_name does not exist, else error message could be just about json arg failing
    # EXPLAIN: If zero lenght job_id, then give error message
    [ -z "$job_id" ] && { echo "no job named $JOB_NAME - run 'create' first"; return 1; }

    echo "RUN: databricks jobs reset"
    # claude created, not digested yet
    # main
    databricks jobs reset --json "$(echo "$JOB_JSON" | jq --argjson id "$job_id" '{job_id: $id, new_settings: .}')"
    
    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941"
}

#########################

run() {
    local job_id
    job_id=$(get_job_id)

    # guard for job_name does not exist
    # EXPLAIN: If zero lenght job_id, then give error message
    [ -z "$job_id" ] && { echo "no job named $JOB_NAME - run 'create' first"; return 1; }

    echo "RUN: databricks jobs run-now"
    databricks jobs run-now $job_id

    echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941"
}

#########################

delete() {
  echo "RUN: databricks jobs delete"
  databricks jobs delete $(get_job_id)

echo "Here:
      https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941"  
}

################################

main "$@"
