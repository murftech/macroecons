#!/bin/bash
set -e

######## easy placement of the only point of the script ###
CRON_SCHEDULE="0 1 * * *"
##############
# GCP syncs with standard 5-field cron, and consumes this string as-is.
# AWS wants 6 fields and rejects some "*" for "?", We convert aws inline during function calll
SCHEDULE_TIMEZONE="Asia/Singapore"

# the schedule's own name - reused as-is on both clouds (EventBridge and Cloud Scheduler)
SCHEDULE_NAME=hdb-pipeline-daily

# roles must already be setup, run these for setup independently
# sh modules/pipe_hdb/configs_aws_sh/setup_aws_iam.sh schedule
# sh modules/pipe_hdb/configs_gcp_sh/setup_gcp_iam.sh all


cd "$(dirname "$0")"
while [ ! -d .vscode ] && [ "$PWD" != "/" ]; do cd ..; done

# Avail fixed configs of the AWS project from a shared location
source "cloud_aws_shared/aws_config.sh"
echo account id is $ACCOUNT_ID
echo region is $REGION
echo cluster is $CLUSTER
echo subnet id is $SUBNET_ID
echo security group id is $SECURITY_GROUP_ID

# Avail fixed configs of the GCP project from a shared location
source "cloud_gcp_shared/gcp_config.sh"
echo gcp project is $GCP_PROJECT
echo gcp region is $GCP_REGION
echo gcp job name is $GCP_JOB_NAME

usage() {
  echo "Usage: ./modules/pipe_hdb/schedules.sh [aws|delete-aws|gcp|delete-gcp]"
  echo "  aws        - create the daily EventBridge schedule (or update it, if it already exists)"
  echo "  delete-aws - delete the EventBridge schedule itself (not the IAM role - see teardown_aws_iam.sh for that)"
  echo "  gcp        - create the daily Cloud Scheduler job (or update it, if it already exists)"
  echo "  delete-gcp - delete the Cloud Scheduler job itself (not the service account or its run.invoker binding)"
  exit 1
}

main() {
case "$1" in

#########
### AWS
#########
  aws)
    AWS_CRON=$(echo "${CRON_SCHEDULE}" | awk '{$5="?"; print $0, "*"}')

    # must match the role name setup_aws_iam.sh actually creates - this string is interpolated
    # straight into a role ARN in schedule_aws_json below, and AWS only resolves it by name
    SCHEDULER_ROLE_NAME=hdb-pipeline-scheduler-role

    if aws scheduler get-schedule \
         --name ${SCHEDULE_NAME} \
         --region ${REGION} >/dev/null 2>&1; then
      AWS_VERB=update-schedule
      echo "${SCHEDULE_NAME} exists - updating it to match this script"
    else
      AWS_VERB=create-schedule
      echo "${SCHEDULE_NAME} not found - creating it"
    fi

    aws scheduler ${AWS_VERB} \
      --name ${SCHEDULE_NAME} \
      --schedule-expression "cron(${AWS_CRON})" \
      --schedule-expression-timezone "${SCHEDULE_TIMEZONE}" \
      --flexible-time-window '{"Mode": "OFF"}' \
      --target "$(schedule_aws_json)" \
      --region ${REGION}

    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/scheduler/home?region=ap-southeast-1#schedules/default/hdb-pipeline-daily"
    ;;

  delete-aws)
    aws scheduler delete-schedule \
      --name ${SCHEDULE_NAME} \
      --region ${REGION}
    echo "Here
      https://ap-southeast-1.console.aws.amazon.com/scheduler/home?region=ap-southeast-1#schedules/default/hdb-pipeline-daily"
    ;;

#########
### GCP
#########
  gcp)

    SCHEDULER_ROLE=cloud-scheduler@${GCP_PROJECT}.iam.gserviceaccount.com

    # try error for create or update  
    if gcloud scheduler jobs describe ${SCHEDULE_NAME} \
         --location=${GCP_REGION} \
         --project=${GCP_PROJECT} >/dev/null 2>&1; then
      GCP_VERB=update
      echo "${SCHEDULE_NAME} exists - updating it to match this script"
    else
      GCP_VERB=create
      echo "${SCHEDULE_NAME} not found - creating it"
    fi
    
    # --uri below is how we target the job: Cloud Scheduler has no Cloud Run target type, needs this API string type identifier
    gcloud scheduler jobs ${GCP_VERB} http ${SCHEDULE_NAME} \
      --location=${GCP_REGION} \
      --schedule="${CRON_SCHEDULE}" \
      --time-zone="${SCHEDULE_TIMEZONE}" \
      --uri="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT}/jobs/${GCP_JOB_NAME}:run" \
      --http-method=POST \
      --oauth-service-account-email="${SCHEDULER_ROLE}" \
      --project=${GCP_PROJECT}

    echo "Here:
      https://console.cloud.google.com/cloudscheduler/jobs/edit/${GCP_REGION}/${SCHEDULE_NAME}?authuser=1&project=${GCP_PROJECT}"
    ;;

  delete-gcp)
    gcloud scheduler jobs delete ${SCHEDULE_NAME} \
      --location=${GCP_REGION} \
      --project=${GCP_PROJECT} \
      --quiet

    echo "Here:
      https://console.cloud.google.com/cloudscheduler?authuser=1&project=${GCP_PROJECT}"
    ;;

  *)
    usage
    ;;

esac
}


# content appendixed:

# same subnet/SG/launch-type shape as modules/pipe_hdb/deploy_aws.sh's own `run` case -
# this is just the scheduled equivalent of that same run-task call
schedule_aws_json() {
  cat <<EOF
{
  "Arn": "arn:aws:ecs:${REGION}:${ACCOUNT_ID}:cluster/${CLUSTER}",
  "RoleArn": "arn:aws:iam::${ACCOUNT_ID}:role/${SCHEDULER_ROLE_NAME}",
  "EcsParameters": {
    "TaskDefinitionArn": "arn:aws:ecs:${REGION}:${ACCOUNT_ID}:task-definition/hdb-pipeline",
    "LaunchType": "FARGATE",
    "TaskCount": 1,
    "NetworkConfiguration": {
      "awsvpcConfiguration": {
        "Subnets": ["${SUBNET_ID}"],
        "SecurityGroups": ["${SECURITY_GROUP_ID}"],
        "AssignPublicIp": "ENABLED"
      }
    }
  }
}
EOF
}

main "$@"
