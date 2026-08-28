#!/bin/bash
# set -e
# set this only if it is sh script

# the commands is path agnostic.
# also databricks login is always on.

# databricks auth describe
# databricks auth login --host https://dbc-b01338b1-a584.cloud.databricks.com

# ###########
# #### Catalog, SCHEMA_NAME, Tables, Volumes
# ###########
# # Here
# # https://dbc-b01338b1-a584.cloud.databricks.com/explore/data?o=7474643839559941

# databricks catalogs list
# # databricks catalogs delete macroecons --force

# databricks SCHEMA_NAMEs list macroecons
# # databricks SCHEMA_NAMEs delete macroecons.dev_murftech7_macroecons_hdb

# databricks tables list macroecons dev_murftech7_macroecons_hdb
# # databricks tables delete macroecons.dev_murftech7_macroecons_hdb.hdb_silver

# databricks volumes list macroecons dev_murftech7_macroecons_hdb
# # databricks volumes delete macroecons.dev_murftech7_macroecons_hdb.reports




# ###########
# #### Jobs and Tasks and Pipelines
# ###########
# # Here:
# # https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941

# databricks jobs list
# databricks jobs delete 483971807457204

# databricks pipelines list-pipelines
# # databricks pipelines delete 06c82161-4af2-4d42-9369-cf55b5543bef
# # databricks pipelines delete 258c8876-612b-40b5-8dad-dca8b369c892
# # databricks pipelines delete 62ccc8b0-e6ad-4424-a677-b5e3b43b8a6c



# ############
# ### Dashboards
# ############

# # Here
# # https://dbc-b01338b1-a584.cloud.databricks.com/sql/dashboards?o=7474643839559941
# databricks lakeview list
# # databricks lakeview trash 01f18b6f0700178da2f25102d0c65e17
# # databricks lakeview trash 01f18b77efc51d5db98dfaf322e3af5a
# # databricks lakeview trash 01f18b777abb1aa6b156e67b99fe9696

# ############
# ### Workspace
# ############

# # Here
# # https://dbc-b01338b1-a584.cloud.databricks.com/browse?o=7474643839559941
# databricks workspace list /Workspace/Users/murftech7@gmail.com
# # databricks workspace delete /Workspace/Users/murftech7@gmail.com/.bundle/dev --recursive





# Now we gonna
# create unity catalog
# Create task
# Create Job
# Then deploy

# All by CLI

# Tell me the steps in general




################################################################################
### THE WHOLE PIPELINE, IMPERATIVELY, LINE BY LINE
### no bundle, no functions, no json files. paste one line at a time.
################################################################################

#### 0. variables - paste these first, everything below reuses them ####
CATALOG=databricks_class
SCHEMA_NAME=schema_class
VOLUME=outputs
JOB_NAME=orchestrate_pipe_hdb
WH=3a82455c9b1702b1
USER_EMAIL=murftech7@gmail.com

SRC_LOCAL=/Users/murftech/Dropbox/Datarepo/macroecons/modules/pipe_hdb_databricks/src
SRC_REMOTE=/Workspace/Users/${USER_EMAIL}/deployments/pipe_hdb_sh/src


# LP: Home and Users/murftech7@gmail.com are the same folder
# LP: Home is an alias. Expand either and you get the identical five entries because they 
# resolve to the same path. Nothing is duplicated there — the sidebar just shows both the shortcut and the full tree.

# sanity check they all took
echo "${CATALOG} | ${SCHEMA_NAME} | ${VOLUME} | ${SRC_REMOTE}"

# always warm up the warehouse, else the code cannot run in one chunk sometimes
databricks warehouses start ${WH}

#### 1. CATALOG - must exist before the SCHEMA_NAME. SQL only (REST is rejected here) ####
# `databricks catalogs create` fails on Free Edition ("Metastore storage root URL does not
# exist"). the SQL Statement Execution API is a different code path and works.
# heredoc into a VARIABLE, not a file - $(...) captures what the heredoc writes.
# the closing EOF must be at column 0, and the ) goes on its own line after it.


CATALOG_JSON=$(cat <<EOF
{
  "warehouse_id": "${WH}",
  "statement": "CREATE CATALOG IF NOT EXISTS ${CATALOG}"
}
EOF
)

echo "$CATALOG_JSON" # always look before you send
databricks api post /api/2.0/sql/statements --json "$CATALOG_JSON"
# LP: expose the api console, do POST action, communicate to the /api/2.0/sql/statements defined API, pass the valid JSON in for execution.
# Collection → POST creates a member → member has a URL → GET reads it. Standard REST, and once you see that shape you can predict the rest of the platform's API from the path alone.
databricks catalogs list
# databricks catalogs delete ${CATALOG} --force

# LP:
# Required fields self-document. {} is a legitimate discovery tool. It will bump an error for the next field you should fill in.
# Optional fields are invisible. Nothing will ever volunteer that wait_timeout exists — and if you misspell it you get no error, just the default.
# The path is the hard part. No CLI command, no error message, and no local SCHEMA_NAME will tell you /api/2.0/sql/statements exists. That's the one piece that genuinely requires the API reference.



#### 2. SCHEMA_NAME - must exist before the volume, and before the job RUNS ####
echo $SCHEMA_NAME
echo $CATALOG

# databricks schemas create
# Discover:
# Usage:
#   databricks SCHEMA_NAMEs create NAME CATALOG_NAME [flags]
databricks schemas create ${SCHEMA_NAME} ${CATALOG}
databricks schemas list ${CATALOG}
# databricks schemas delete ${CATALOG}.${SCHEMA_NAME} --force
# databricks schemas list ${CATALOG}

#### 3. VOLUME - needs the SCHEMA_NAME. only needed before the RUN, not before job create ####
# purpose, for landing parquet files or output files. Just like S3 buckets

echo ${CATALOG} ${SCHEMA_NAME} ${VOLUME}
databricks volumes create ${CATALOG} ${SCHEMA_NAME} ${VOLUME} MANAGED
databricks volumes list ${CATALOG} ${SCHEMA_NAME}

# databricks volumes delete --help
# Usage:
#   databricks volumes delete NAME [flags]
# databricks volumes delete ${CATALOG}.${SCHEMA_NAME}.${VOLUME}
# databricks volumes list ${CATALOG} ${SCHEMA_NAME}


#### 4. UPLOAD THE CODE ####
# The PUSH!!!
# databricks sync
# Usage:
#   databricks sync [flags] SRC DST
# source to destination
echo ${SRC_LOCAL} 
echo ${SRC_REMOTE}

# this simply just copy and pastes a chosen folder contents byte for byte into the chosen target folder in databricks workspace.
# i can actually paste any nonsense?
databricks sync --full ${SRC_LOCAL} ${SRC_REMOTE}
databricks workspace list ${SRC_REMOTE}
# databricks workspace delete ${SRC_REMOTE} --recursive

# Immutability is given by: 
# "environment_version": "4",
# and uv.lock and pyproject.toml


#### 5. JOB - ochectrator ####

# Parameters in TASK are fixed runtime environment varaiables
# Paramters in JOBS are user editable runtime variables
# databricks jobs create --help
# embeds schedule inside, not trhough an external file

# bake the catalog, but user vary the schema, just for fun
# test out if i can direct it to another schema.. zz.

JOB_JSON=$(cat <<EOF
{
  "name": "${JOB_NAME}",
  "edit_mode": "UI_LOCKED",
  "timeout_seconds": 1800,

  "schedule": {
    "quartz_cron_expression": "0 0 3 * * ?",
    "timezone_id": "Asia/Singapore",
    "pause_status": "UNPAUSED"
  },

  "email_notifications": {
    "on_failure": ["${USER_EMAIL}"],
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
        "dependencies": ["-r ${SRC_REMOTE}/requirements.txt"]
      }
    }
  ],

  "tasks": [
  
    {
      "task_key": "ingest_bronze",
      "environment_key": "default_env",
      "max_retries": 2,
      "min_retry_interval_millis": 60000,
      "spark_python_task": {
        "python_file": "${SRC_REMOTE}/01_ingest_bronze.py",
        "parameters": ["--catalog", "${CATALOG}", "--schema", "{{job.parameters.schema}}", "--src-dir", "${SRC_REMOTE}", "--run-date", "{{job.parameters.run_date}}"]
      }
    },

    {
      "task_key": "report_plotly",
      "depends_on": [{"task_key": "ingest_bronze"}],
      "environment_key": "default_env",
      "spark_python_task": {
        "python_file": "${SRC_REMOTE}/02_report_plotly.py",
        "parameters": ["--catalog", "${CATALOG}", "--schema", "{{job.parameters.schema}}", "--volume", "${VOLUME}", "--src-dir", "${SRC_REMOTE}"]
      }
    },

    {
      "task_key": "publish_outputs",
      "depends_on": [{"task_key": "report_plotly"}],
      "environment_key": "default_env",
      "spark_python_task": {
        "python_file": "${SRC_REMOTE}/03_publish_outputs.py",
        "parameters": ["--catalog", "${CATALOG}", "--schema", "{{job.parameters.schema}}", "--volume", "${VOLUME}"]
      }
    }
  ]
}
EOF
)

# check the payload before sending. piping through `jq .` does double duty: it pretty-prints
# AND fails loudly on malformed json, so you catch a bad heredoc here rather than from the API.
# validate valid json syntax
echo "$JOB_JSON" | jq .



# databricks jobs create --json "$JOB_JSON"
# if you keep running this, you will create more and more job ids with the same name and same config.
# Here
# https://dbc-b01338b1-a584.cloud.databricks.com/jobs?o=7474643839559941

# # grab the id back - there is no state file, so name lookup is the only way
JOB_ID=$(databricks jobs list -o json | python3 -c "import json,sys; print([j['job_id'] for j in json.load(sys.stdin) if j['settings']['name']=='${JOB_NAME}'][0])")
echo ${JOB_ID}
# 515023237597153
# databricks jobs delete ${JOB_ID}

# update job
databricks jobs reset --json "$(echo "$JOB_JSON" | jq --argjson id ${JOB_ID} '{job_id: $id, new_settings: .}')"





# #### 6. RUN - everything above must exist by now. this is where mistakes surface ####
databricks jobs run-now ${JOB_ID}
# Here
# https://dbc-b01338b1-a584.cloud.databricks.com/jobs/515023237597153?o=7474643839559941
# nothing complex.


# question how are the tables defined as Delta tables. 

# #### 7. VERIFY ####
databricks jobs list-runs --job-id ${JOB_ID} --limit 3
databricks tables list ${CATALOG} ${SCHEMA_NAME}
databricks fs ls dbfs:/Volumes/${CATALOG}/${SCHEMA_NAME}/${VOLUME}

# pull a query programmitically, proves, existence of table.
SQL_JSON=$(cat <<EOF
{
  "warehouse_id": "${WH}",
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
