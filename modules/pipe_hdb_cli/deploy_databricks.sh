#!/bin/bash
set -e

# Deploy pipeline: upload code and manage job on Databricks.
# Run provision_databricks.sh once first, then this for each deployment.

cd "$(dirname "$0")"

#### config ####
CATALOG=macroecons
SCHEMA=macroecons_hdb_cli
VOLUME=reports
JOB_NAME=orchestrate_pipe_hdb
SRC_LOCAL=../pipe_hdb_databricks/src
STATE_DIR=.state
WAREHOUSE_ID=3a82455c9b1702b1
USER_EMAIL=murftech7@gmail.com
SCHEDULE_CRON="0 0 1 * * ?"
SCHEDULE_TZ="Asia/Singapore"
SCHEDULE_PAUSE="PAUSED"

usage() {
  echo "Usage: ./deploy_databricks.sh [upload|job|deploy|run|status|destroy|all]"
  echo "  upload   - sync source code to workspace"
  echo "  job      - create or update the job"
  echo "  deploy   - upload + job"
  echo "  run      - trigger one job run"
  echo "  status   - show job id, URL, and recent runs"
  echo "  destroy  - delete job (leaves catalog + schema + volume)"
  echo "  all      - deploy + run"
  exit 1
}

#### helpers ####

# Find job by name; no state file means name-based lookup is the only identity.
lookup_job_id() {
  databricks jobs list -o json 2>/dev/null |
    python3 -c "
import json,sys
name=sys.argv[1]
jobs=[j for j in json.load(sys.stdin) if j.get('settings',{}).get('name')==name]
print(jobs[0]['job_id'] if jobs else '')
" "${JOB_NAME}"
}

# Compute workspace src path for syncing and job config substitution.
workspace_src_path() {
  local user
  user=$(databricks current-user me -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
  echo "/Workspace/Users/${user}/pipe_hdb_cli/src"
}

#### actions ####

do_upload() {
  echo "== upload =="
  local dst host
  dst=$(workspace_src_path)
  echo "  ${SRC_LOCAL} -> ${dst}"
  databricks sync --full "${SRC_LOCAL}" "${dst}"
  host=$(databricks auth describe -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['details']['host'])" 2>/dev/null || echo "<workspace>")
  echo "Here (uploaded code):
      ${host}/browse${dst}"
  echo "Here (workspace):
      ${host}/browse"
}

do_job() {
  echo "== job =="
  mkdir -p "${STATE_DIR}"
  local src_remote rendered job_id
  src_remote=$(workspace_src_path)
  rendered="${STATE_DIR}/job.rendered.json"

  # Substitute config variables into job.json template.
  sed -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__CATALOG__|${CATALOG}|g" \
    -e "s|__SCHEMA__|${SCHEMA}|g" \
    -e "s|__VOLUME__|${VOLUME}|g" \
    -e "s|__SRC__|${src_remote}|g" \
    -e "s|__USER_EMAIL__|${USER_EMAIL}|g" \
    -e "s|__SCHEDULE_CRON__|${SCHEDULE_CRON}|g" \
    -e "s|__SCHEDULE_TZ__|${SCHEDULE_TZ}|g" \
    -e "s|__SCHEDULE_PAUSE__|${SCHEDULE_PAUSE}|g" \
    configs/job.json >"${rendered}"

  job_id=$(lookup_job_id)
  host=$(databricks auth describe -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['details']['host'])" 2>/dev/null || echo "<workspace>")

  if [ -z "${job_id}" ]; then
    echo "  no job named ${JOB_NAME} found, creating"
    databricks jobs create --json "@${rendered}"
    job_id=$(lookup_job_id)
  else
    echo "  job ${job_id} exists, overwriting settings"
    python3 -c "
import json,sys
settings=json.load(open(sys.argv[1]))
settings.pop('_comment_', None)
json.dump({'job_id': int(sys.argv[2]), 'new_settings': settings}, sys.stdout)
" "${rendered}" "${job_id}" >"${STATE_DIR}/job.reset.json"
    databricks jobs reset --json "@${STATE_DIR}/job.reset.json"
  fi

  echo "Here:
      ${host}/jobs/${job_id}"
}

do_run() {
  echo "== run =="
  local job_id host
  job_id=$(lookup_job_id)
  [ -z "${job_id}" ] && {
    echo "no job named ${JOB_NAME} - run './deploy_databricks.sh job' first" >&2
    exit 1
  }
  # Killing this shell doesn't stop the run; use `databricks jobs cancel-run <run_id>`.
  host=$(databricks auth describe -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['details']['host'])" 2>/dev/null || echo "<workspace>")
  databricks jobs run-now "${job_id}"
  echo "Here:
      ${host}/jobs/${job_id}"
}

do_status() {
  echo "== status =="
  local job_id host
  job_id=$(lookup_job_id)
  [ -z "${job_id}" ] && {
    echo "no job named ${JOB_NAME} deployed"
    return 0
  }
  host=$(databricks auth describe -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['details']['host'])" 2>/dev/null || echo "<workspace>")
  echo "  job id  : ${job_id}"
  echo "  recent runs:"
  databricks jobs list-runs --job-id "${job_id}" --limit 5
  echo "Here:
      ${host}/jobs/${job_id}"
  echo "Here (all jobs):
      ${host}/jobs"
}

do_destroy() {
  echo "== destroy =="
  local job_id
  job_id=$(lookup_job_id)
  if [ -n "${job_id}" ]; then
    databricks jobs delete "${job_id}" && echo "  deleted job ${job_id}"
  else
    echo "  no job to delete"
  fi
  rm -rf "${STATE_DIR}"
}

main() {
  case "$1" in

  upload) do_upload ;;
  job) do_job ;;
  deploy)
    do_upload
    do_job
    ;;
  run) do_run ;;
  status) do_status ;;
  destroy) do_destroy ;;
  all)
    do_upload
    do_job
    do_run
    ;;
  *) usage ;;

  esac
}

main "$@"
