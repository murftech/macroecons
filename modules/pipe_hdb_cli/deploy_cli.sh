#!/bin/bash
set -e

#### pipe_hdb on Databricks, WITHOUT Asset Bundles ####
# the same three-task pipeline as ../pipe_hdb_databricks, provisioned imperatively with
# plain `databricks` CLI calls instead of `bundle deploy`. this exists to make the
# declarative-vs-imperative trade visible: same result, and you can read exactly what each
# step costs you. modelled on ../pipe_hdb/deploy_aws.sh's shape on purpose.
#
# the python is NOT duplicated - SRC_LOCAL points at the bundle module's src/ directory.
# there is no "bundle root" here, so nothing constrains where source may live.

cd "$(dirname "$0")"

#### config - the imperative stand-in for databricks.yml's variables: block ####
CATALOG=macroecons
SCHEMA=macroecons_hdb_cli # _cli suffix so this cannot collide with the bundle's schema
VOLUME=reports
JOB_NAME=hdb-pipeline-daily-cli
SRC_LOCAL=../pipe_hdb_databricks/src
STATE_DIR=.state
# serverless SQL warehouse used only for the statements in configs/sql/statements.json.
# it auto-starts on first use and idles back down on its own - find yours with:
#   databricks warehouses list
WAREHOUSE_ID=3a82455c9b1702b1

usage() {
  echo "Usage: ./deploy_cli.sh [bootstrap|upload|job|deploy|run|status|destroy|all|sql <name>]"
  echo "  bootstrap - create catalog + schema + volume (idempotent; safe to re-run)"
  echo "  sql       - run a named statement from configs/sql/statements.json;"
  echo "              with no name, lists what is available"
  echo "  upload    - sync src/ into the workspace"
  echo "  job       - create the job, or overwrite it if a job of this name already exists"
  echo "  deploy    - upload + job (the recurring loop; bootstrap is one-time)"
  echo "  run       - trigger the job once"
  echo "  status    - print the job id, its URL, and recent runs"
  echo "  destroy   - delete job, volume, schema (catalog is left alone, see note below)"
  echo "  all       - bootstrap + deploy + run"
  exit 1
}

#### helpers ####

# the imperative answer to idempotency. `bundle deploy` diffs against stored state and
# simply does nothing when a resource already matches; here every create is unconditional,
# so each one needs its "already exists" error swallowed by hand. this is the same
# run_idempotent pattern as ../pipe_hdb/configs_aws_sh/setup_aws_iam.sh.
run_idempotent() {
  local what="$1"
  shift
  local output
  if output=$("$@" 2>&1); then
    echo "  created ${what}"
  elif echo "${output}" | grep -qiE 'already exists|ALREADY_EXISTS'; then
    echo "  ${what} already exists, skipping"
  else
    echo "  FAILED creating ${what}:" >&2
    echo "${output}" >&2
    return 1
  fi
}

#### SQL statements, in the script rather than a data file ####
# same shape as streamlit/deploy_aws.sh's task_definition_json(): the config vars at the
# top of this file interpolate natively, so there are no __TOKENS__ to substitute and no
# lookup file to keep in sync. the SQL is readable exactly where it is used.
#
# why SQL at all: `databricks catalogs create` is rejected on Free Edition ("Metastore
# storage root URL does not exist") because the catalogs REST API demands an explicit
# MANAGED LOCATION. the SQL Statement Execution API reaches the same operation by a
# different route and succeeds - so this script needs no UI step anywhere.
SQL_NAMES="create_catalog drop_catalog create_schema drop_schema create_volume
           show_catalogs show_schemas show_tables show_volumes
           describe_schema describe_table count_rows sample_rows table_history"

sql_text() {
  case "$1" in
  create_catalog) echo "CREATE CATALOG IF NOT EXISTS ${CATALOG}" ;;
  drop_catalog) echo "DROP CATALOG IF EXISTS ${CATALOG} CASCADE" ;;
  create_schema) echo "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}" ;;
  drop_schema) echo "DROP SCHEMA IF EXISTS ${CATALOG}.${SCHEMA} CASCADE" ;;
  create_volume) echo "CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}" ;;
  show_catalogs) echo "SHOW CATALOGS" ;;
  show_schemas) echo "SHOW SCHEMAS IN ${CATALOG}" ;;
  show_tables) echo "SHOW TABLES IN ${CATALOG}.${SCHEMA}" ;;
  show_volumes) echo "SHOW VOLUMES IN ${CATALOG}.${SCHEMA}" ;;
  describe_schema) echo "DESCRIBE SCHEMA EXTENDED ${CATALOG}.${SCHEMA}" ;;
  describe_table) echo "DESCRIBE TABLE EXTENDED ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  count_rows) echo "SELECT COUNT(*) AS n FROM ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  sample_rows) echo "SELECT * FROM ${CATALOG}.${SCHEMA}.hdb_silver LIMIT 10" ;;
  table_history) echo "DESCRIBE HISTORY ${CATALOG}.${SCHEMA}.hdb_silver" ;;
  *) return 1 ;;
  esac
}

# the request body for the Statement Execution API, heredoc'd the same way.
# NOTE a heredoc does no JSON escaping - a statement containing a double quote or a
# backslash would produce invalid JSON. every statement above uses SQL's own single-quote
# string literals, so this is safe here; reach for `jq -n --arg` if that ever changes.
statement_json() {
  cat <<EOF
{
  "warehouse_id": "${WAREHOUSE_ID}",
  "statement": "$1",
  "wait_timeout": "30s"
}
EOF
}

run_sql() {
  local stmt
  if ! stmt=$(sql_text "$1"); then
    echo "unknown statement '$1'. available:" >&2
    echo "${SQL_NAMES}" | tr -s ' \n' '\n' | sed 's/^/  /' >&2
    return 1
  fi
  # echo the SQL to STDERR, not stdout - keeps stdout pure JSON so the result stays
  # pipeable:  ./deploy_cli.sh sql show_catalogs | jq -r '.result.data_array[][]'
  echo "  SQL> ${stmt}" >&2
  databricks api post /api/2.0/sql/statements --json "$(statement_json "${stmt}")"
}

# with no state file mapping a config key to a server id, the only way to find "the job I
# made last time" is to search by name. this is precisely the identity mapping that
# .databricks/bundle/dev/resources.json does for the bundle, reimplemented badly - two jobs
# sharing a name would make this ambiguous, and nothing stops that from happening.
lookup_job_id() {
  databricks jobs list -o json 2>/dev/null |
    python3 -c "
import json,sys
name=sys.argv[1]
jobs=[j for j in json.load(sys.stdin) if j.get('settings',{}).get('name')==name]
print(jobs[0]['job_id'] if jobs else '')
" "${JOB_NAME}"
}

# the workspace path src/ gets uploaded to. the bundle derived this from bundle.name +
# target and injected it into task definitions automatically (the TranslatePaths mutator);
# imperatively it has to be computed here and textually substituted into the JSON below.
workspace_src_path() {
  local user
  user=$(databricks current-user me -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
  echo "/Workspace/Users/${user}/pipe_hdb_cli/src"
}

#### actions ####

do_bootstrap() {
  echo "== bootstrap =="
  # `databricks catalogs create` ALWAYS fails on Free Edition ("Metastore storage root URL
  # does not exist") because the catalogs REST API demands an explicit MANAGED LOCATION.
  # the SQL Statement Execution API reaches the same operation by a different route and
  # succeeds - so this whole script stays CLI-only, no UI step. CREATE CATALOG IF NOT
  # EXISTS is idempotent on its own, hence no run_idempotent wrapper here.
  run_sql create_catalog
  run_idempotent "schema ${CATALOG}.${SCHEMA}" databricks schemas create "${SCHEMA}" "${CATALOG}"
  run_idempotent "volume ${CATALOG}.${SCHEMA}.${VOLUME}" \
    databricks volumes create "${CATALOG}" "${SCHEMA}" "${VOLUME}" MANAGED
}

do_upload() {
  echo "== upload =="
  local dst
  dst=$(workspace_src_path)
  # `databricks sync` is the same file-transfer primitive the bundle uses for its files/
  # directory, just exposed standalone. deliberately NOT `workspace import-dir`, which
  # treats .py files as notebooks and strips their extensions - that would break
  # spark_python_task, which needs a real file at a real .py path.
  echo "  ${SRC_LOCAL} -> ${dst}"
  databricks sync --full "${SRC_LOCAL}" "${dst}"
}

do_job() {
  echo "== job =="
  mkdir -p "${STATE_DIR}"
  local src_remote rendered job_id
  src_remote=$(workspace_src_path)
  rendered="${STATE_DIR}/job.rendered.json"

  # textual substitution standing in for the bundle's ${var.x} / ${workspace.x} resolution.
  sed -e "s|__JOB_NAME__|${JOB_NAME}|g" \
    -e "s|__CATALOG__|${CATALOG}|g" \
    -e "s|__SCHEMA__|${SCHEMA}|g" \
    -e "s|__VOLUME__|${VOLUME}|g" \
    -e "s|__SRC__|${src_remote}|g" \
    configs/job.json >"${rendered}"

  job_id=$(lookup_job_id)
  if [ -z "${job_id}" ]; then
    echo "  no job named ${JOB_NAME} found, creating"
    databricks jobs create --json "@${rendered}"
  else
    # `reset` overwrites all settings, which is the closest imperative equivalent to what a
    # redeploy does. note there is no diff and no plan: this fires an update every single
    # time, whether or not anything actually changed.
    echo "  job ${job_id} exists, overwriting its settings"
    python3 -c "
import json,sys
settings=json.load(open(sys.argv[1]))
settings.pop('_comment_', None)
json.dump({'job_id': int(sys.argv[2]), 'new_settings': settings}, sys.stdout)
" "${rendered}" "${job_id}" >"${STATE_DIR}/job.reset.json"
    databricks jobs reset --json "@${STATE_DIR}/job.reset.json"
  fi
}

do_run() {
  echo "== run =="
  local job_id
  job_id=$(lookup_job_id)
  [ -z "${job_id}" ] && {
    echo "no job named ${JOB_NAME} - run './deploy_cli.sh job' first" >&2
    exit 1
  }
  # same lesson as the bundle: this triggers server-side work. killing this shell does not
  # stop the run - use `databricks jobs cancel-run <run_id>` for that.
  databricks jobs run-now "${job_id}"
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
  echo "  job url : ${host}/jobs/${job_id}"
  echo "  recent runs:"
  databricks jobs list-runs --job-id "${job_id}" --limit 5
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
  databricks volumes delete "${CATALOG}.${SCHEMA}.${VOLUME}" 2>/dev/null && echo "  deleted volume" || echo "  volume already gone"
  databricks schemas delete "${CATALOG}.${SCHEMA}" 2>/dev/null && echo "  deleted schema" || echo "  schema already gone"
  # the catalog is deliberately NOT deleted - it is shared with the bundle module, and
  # nothing here records that this script did not create it. `bundle destroy` has the same
  # blind spot for the same reason: no state, no ownership.
  echo "  catalog ${CATALOG} left in place on purpose"
  rm -rf "${STATE_DIR}"
}

main() {
  case "$1" in

  bootstrap) do_bootstrap ;;
  upload) do_upload ;;
  job) do_job ;;

  # ad-hoc: ./deploy_cli.sh sql count_rows   (bare `sql` lists what is available)
  sql)
    if [ -z "$2" ]; then
      echo "available statements:"
      echo "${SQL_NAMES}" | tr -s ' \n' '\n' | sed 's/^/  /'
    else
      run_sql "$2"
    fi
    ;;

  deploy)
    do_upload
    do_job
    ;;

  run) do_run ;;
  status) do_status ;;
  destroy) do_destroy ;;

  all)
    do_bootstrap
    do_upload
    do_job
    do_run
    ;;

  *) usage ;;

  esac
}

#### what this script has to do by hand that `bundle deploy` did for free ####
# 1. idempotency      - run_idempotent per resource, vs a state diff that skips no-op work
# 2. identity         - lookup_job_id searches by NAME; the bundle stores server ids in
#                       resources.json and updates the exact object it created
# 3. path resolution  - workspace_src_path + sed, vs the TranslatePaths mutator
# 4. ordering         - the case blocks sequence catalog->schema->volume->job manually; the
#                       bundle derives that from ${resources.*} references (visible as
#                       "depends_on" in `databricks bundle plan -o json`)
# 5. deletion         - nothing here notices a resource removed from configs/job.json;
#                       destroy only removes what this script hardcodes
# 6. dry run          - there is no `plan`. the only way to know what this will change is
#                       to read it
# 7. environments     - no dev/prod split; SCHEMA/JOB_NAME would have to be parameterised
#                       and every caller updated

main "$@"
