import datetime
import time
from pathlib import Path

import streamlit as st

BUCKET_ID = 'murftech-macroecons' # same name on both GCS and S3
REPORT_FOLDER = 'DBMaster/annotations/reports/macroecons/pipe_hdb'

GCP_PROJECT = 'macroecons'
GCP_REGION = 'asia-southeast1'
JOB_NAME = 'hdb-pipeline'

####
# import gcp-side managers
####
from google.cloud import storage
bucket_client = storage.Client()

from google.cloud import run_v2
jobs_client = run_v2.JobsClient()
executions_client = run_v2.ExecutionsClient()

########################
#### for st_iframe (Charts from HTML)
#########################

def fetch_html_bytes(filename):
    from google.cloud import storage

    bucket = storage.Client().bucket(BUCKET_ID)
    blob = bucket.get_blob(f'{REPORT_FOLDER}/{filename}')

    if blob is None:
        print('file for chart is missing, check about it')
        return None
    return blob.download_as_text()



########################
#### for the interactive charts (the data itself, not pre-rendered HTML)
#########################

def fetch_parquet_bytes():
    # not implemented on this provider yet - the interactive charts were built local-first
    # and the bucket read lands with the docker/aws/gcp steps. returning None makes the
    # Explore tab show its empty state instead of raising.
    return None


########################
#### for the saved street-name sets
#########################

def read_saved_sets():
    # not implemented on this provider yet - saved sets were built local-first. the bucket
    # (or dynamodb) backed version lands with the docker/aws/gcp steps. an empty dict makes
    # the app show "no saved sets yet" rather than raising.
    return {}


def write_saved_sets(saved_sets):
    # deliberately a no-op rather than a silent success that loses data - the app checks
    # SAVED_SETS_WRITABLE below before offering the save controls at all
    raise NotImplementedError('saved sets are not wired up on this provider yet')


SAVED_SETS_WRITABLE = False


#######################
##### for show code snippet
#######################
# at dockerfile time, it was already synced copied exactly state
CODE_PATH = Path('modules/pipe_hdb/2_report_firstbq.py')



########################
#### for st.button('Refresh data')
#########################

# module-level, Do not inside trigger_pipeline(), because get_last_run_time() below needs job_path too
job_path = jobs_client.job_path(GCP_PROJECT, GCP_REGION, JOB_NAME)


def trigger_pipeline():
    # returns the execution_name, for the caller to pass to poll_pipeline()
    operation = jobs_client.run_job(name=job_path)
    return operation.metadata.name


########################
#### for status_placeholder = st.empty() while waiting for the pipeline run
#########################

def get_last_run_time():
    request = run_v2.ListExecutionsRequest(parent=job_path, page_size=5)

    # fixed UTC+8 offset (Singapore has no DST) explicit, avoid relying on the host's timezone (likely UTC)
    SGT = datetime.timezone(datetime.timedelta(hours=8), name='SGT')

    # results come back newest-first; skip any still-running execution
    # (completion_time is None until it actually finishes) to find the last one that did
    for execution in executions_client.list_executions(request=request):
        if execution.completion_time is not None:
            return execution.completion_time.astimezone(SGT)
    return None


def poll_pipeline(execution_name, status_placeholder):
    # execution_name will come from return of trigger_pipeline()

    MAX_POLL_ATTEMPTS = 40  # 40 * 3s = 2 min safety cap, in case something never reports completion

    execution = executions_client.get_execution(name=execution_name)

    attempts = 0
    with status_placeholder, st.spinner('Waiting for google cloud run to finish...'):
        while execution.completion_time is None and attempts < MAX_POLL_ATTEMPTS:
            time.sleep(3)
            execution = executions_client.get_execution(name=execution_name)
            attempts += 1

    if execution.completion_time is not None:
        if execution.failed_count > 0:
            status_placeholder.error('google cloud run failed - check the Cloud Run Job logs for details.')
        else:
            status_placeholder.success('google cloud run finished - charts above are up to date.')
            st.rerun()
    else:
        status_placeholder.warning('Still running after 2 minutes - reload the page later to check for updated charts.')
