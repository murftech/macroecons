import datetime
import os
import time
from pathlib import Path

import streamlit as st

BUCKET_ID = 'murftech-macroecons'  # same name on both GCS and S3
REPORT_FOLDER = 'DBMaster/annotations/reports/macroecons/pipe_hdb'

ECS_CLUSTER = 'macroecons'
AWS_REGION = 'ap-southeast-1'
IMAGE = 'hdb-pipeline'

####
# import aws-side managers
####

import boto3

# for fetching information from ECS tasks
jobs_client = boto3.client('ecs', region_name=AWS_REGION)

# for fetching information from HTML output
bucket_client = boto3.client('s3', region_name=AWS_REGION)

########################
#### for st_iframe (Charts from HTML)
#########################

def fetch_html_bytes(filename):
    try:
        blob = bucket_client.get_object(Bucket=BUCKET_ID, Key=f'{REPORT_FOLDER}/{filename}')
    except bucket_client.exceptions.ClientError:
        print('file for chart is missing, check about it')
        return None
    return blob['Body'].read().decode('utf-8')


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

def trigger_pipeline():

    SUBNET_ID = os.environ['SUBNET_ID']
    SECURITY_GROUP_ID = os.environ['SECURITY_GROUP_ID']
    # aws network variables:
    # Lineage: shared folder > aws_config.sh > deploy_aws.sh > app_spec_json + create egs > inject values into environment variables
    # > retrieve with os.environ[]
    # these selections are exactly the same if you'd minmally run an FARGATE task in UI.

    response = jobs_client.run_task(
        cluster=ECS_CLUSTER,
        taskDefinition=IMAGE,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [SUBNET_ID],
                'securityGroups': [SECURITY_GROUP_ID],
                'assignPublicIp': 'ENABLED',
            }
        },
    )
    return response['tasks'][0]['taskArn']


########################
#### for status_placeholder = st.empty() while waiting for the pipeline run
#########################

def get_last_run_time():
    # fixed UTC+8 offset (Singapore has no DST) explicit, avoid relying on the host's timezone (likely UTC)
    SGT = datetime.timezone(datetime.timedelta(hours=8), name='SGT')

    # ECS stopped tasks hisotry expires in 60 minutes. Not queriable.
    # hence, take output html last modified time as proxy.
    try:
        head = bucket_client.head_object(Bucket=BUCKET_ID, Key=f'{REPORT_FOLDER}/1-firstbq-overlay.html')
    except bucket_client.exceptions.ClientError:
        return None
    return head['LastModified'].astimezone(SGT)

def poll_pipeline(execution_name, status_placeholder):
    # execution_name will come from return of trigger_pipeline()
    MAX_POLL_ATTEMPTS = 40  # 40 * 3s = 2 min safety cap, in case something never reports completion

    # Ask AWS about the task 
    # → get back a list-shaped response (built for querying many at once)
    # → grab the first (and only) one out of that list.

    task = jobs_client.describe_tasks(cluster=ECS_CLUSTER, tasks=[execution_name])['tasks'][0]

    attempts = 0
    with status_placeholder, st.spinner('Waiting for AWS EGS task to finish...'):
        while task['lastStatus'] != 'STOPPED' and attempts < MAX_POLL_ATTEMPTS:
            time.sleep(3)
            task = jobs_client.describe_tasks(cluster=ECS_CLUSTER, tasks=[execution_name])['tasks'][0]
            attempts += 1

    if task['lastStatus'] == 'STOPPED':
        exit_code = task['containers'][0].get('exitCode')
        if exit_code != 0:
            status_placeholder.error(f'AWS ECS task failed - check CloudWatch Logs for details. ({task["containers"][0].get("reason", "no reason given")})')
        else:
            status_placeholder.success('AWS ECS task finished - charts above are up to date.')
            st.rerun()
    else:
        status_placeholder.warning('Still running after 2 minutes - reload the page later to check for updated charts.')
