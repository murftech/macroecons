#!/bin/bash
# set -e
# set this only if it is sh script



#### 4.5 PROBE - characterise the runtime BEFORE porting anything ####
# one task, prints only, writes nothing. no "dependencies" key on purpose:
# we want WAREHOUSE_IDat environment_version ships on its own, not our own pins played back.

# PROBE_NAME=probe_env

# PROBE_JSON=$(cat <<EOF
# {
#   "name": "${PROBE_NAME}",
#   "timeout_seconds": 900,
#   "environments": [
#     { "environment_key": "default_env",
#       "spec": { "environment_version": "5" } }
#   ],
#   "tasks": [
#     { "task_key": "probe",
#       "environment_key": "default_env",
#       "spark_python_task": { "python_file": "${SRC_REMOTE}/probe.py" } }
#   ]
# }
# EOF
# )

# echo "$PROBE_JSON" | jq .                      # look before you send

# databricks jobs create --json "$PROBE_JSON"

# PROBE_ID=$(databricks jobs list -o json | python3 -c "import json,sys; print([j['job_id'] for j in json.load(sys.stdin) if j['settings']['name']=='probe_env'][0])")
# echo "job ${PROBE_ID}"

# RUN_ID=$(databricks jobs run-now ${PROBE_ID} -o json | jq -r '.run_id')
# TASK_RUN_ID=$(databricks jobs get-run ${RUN_ID} -o json | jq -r '.tasks[0].run_id')
# databricks jobs get-run-output ${TASK_RUN_ID} -o json | jq -r '.logs'



# question how are the tables defined as Delta tables. 

# #### 7. VERIFY post run ####

databricks jobs list-runs --job-id ${JOB_ID} --limit 3
databricks tables list ${CATALOG} ${SCHEMA_NAME}
databricks fs ls dbfs:/Volumes/${CATALOG}/${SCHEMA_NAME}/${VOLUME}

# pull a query programmitically, proves, existence of table.
SQL_JSON=$(cat <<EOF
{
  "warehouse_id": "${WAREHOUSE_ID}",
  "statement": "SELECT COUNT(*) AS n FROM ${CATALOG}.${SCHEMA_NAME}.hdb_silver"
}
EOF
)

databricks api post /api/2.0/sql/statements --json "$SQL_JSON" | jq -r '.result.data_array[][]'



# already have in line above
# #### 8. TEARDOWN - exact reverse of creation order ####
# databricks jobs delete ${JOB_ID}
# databricks workspace delete ${SRC_REMOTE} --recursive
# databricks volumes delete ${CATALOG}.${SCHEMA_NAME}.${VOLUME}
# databricks schemas delete ${CATALOG}.${SCHEMA_NAME} --force
# databricks catalogs delete ${CATALOG} --force
