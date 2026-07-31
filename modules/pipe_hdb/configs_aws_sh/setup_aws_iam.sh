#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

source "cloud_aws_shared/aws_config.sh"

echo account id is $ACCOUNT_ID

IAM_DIR=modules/pipe_hdb/configs_aws

usage() {
  echo "Usage: ./modules/pipe_hdb/configs_aws_sh/setup_aws_iam.sh [execution|task|s3|attach|schedule|all]"
  echo "  execution      - create hdb-pipeline-execution-role + attach AWS's managed policy"
  echo "  task           - create hdb-pipeline-task-role identity for hdb_pipeline"
  echo "  s3             - create the hdb-pipeline-to-s3-access policy"
  echo "  attach         - attach that S3 policy to the task role"
  echo "  schedule       - create hdb-pipeline-scheduler-role, used by schedules.sh (EventBridge Scheduler)"
  echo "  all            - run execution/task/s3/attach in sequence (schedule is opt-in, not included)"
  echo "(run this from the repo root - it needs the aws_iam/ JSON files as relative paths)"
  exit 1
}

main() {
case "$1" in
  execution)
    create_execution_role
    ;;
  task)
    create_task_role
    ;;
  s3)
    create_s3_policy
    ;;
  attach)
    attach_s3_policy
    ;;
  schedule)
    create_scheduler_role
    ;;
  all)
    create_execution_role
    create_task_role
    create_s3_policy
    attach_s3_policy
    ;;
  *)
    usage
    ;;
esac
}

# the reason to put the functions outside of case, is because we need dont want to rewrite the statements for all

# you can open the files to understand here
# modules/pipe_hdb/configs_aws/config_aws_iam/trust_policy.json
# modules/pipe_hdb/configs_aws/config_aws_iam/s3_policy.json

# task-role is about permissions for what INSIDE your application code does, 
# execution-role is about permissions that ECS itself needs to handle the OUTSIDE of the image. 


create_execution_role() {
  # create a execution role with yet empty permissions
  aws iam create-role \
    --role-name hdb-pipeline-execution-role \
    --assume-role-policy-document file://cloud_aws_shared/trust_policy.json

  # attach an standard amazon prepackaged permission set to it, that lets ECS pull the hdb-pipeline image
  # This grants generic permission to pull any image from ECR in your account, including hdb-pipeline, AND writing logs to cloudwatch.
  aws iam attach-role-policy \
    --role-name hdb-pipeline-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-pipeline-execution-role?section=permissions"
  
  # AmazonECSTaskExecutionRolePolicy >
  # "Action": [
  #   "ecr:GetAuthorizationToken",
  #   "ecr:BatchCheckLayerAvailability",
  #   "ecr:GetDownloadUrlForLayer",
  #   "ecr:BatchGetImage",
  #   "logs:CreateLogStream",
  #   "logs:PutLogEvents"
  # ],
  # "Resource": "*"
}

# both task-role and execution-role objects are consumed in task_definition.json alongside hdb-pipeline image
# the task will use execution-role to be allowed to pull from ECR, plus write to cloudwatch and
# use task-role to assume an identity which allows writing to S3, and 


# The below is needed only if our code tries to connect outside of the application at runtime.
create_task_role() {
  # creates the task role the running container shall assume at task run time.
  aws iam create-role \
    --role-name hdb-pipeline-task-role \
    --assume-role-policy-document file://cloud_aws_shared/trust_policy.json
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-pipeline-task-role"
}

create_s3_policy() {
  # defines the actual S3 read/write permissions on murftech-macroecons that run_pipeline.py's boto3 upload calls need
  # to be attached to any task-role that requires it.
  # this is what run_pipeline.py's s3.upload_file() actually depend on at runtime
  aws iam create-policy \
    --policy-name hdb-pipeline-to-s3-access \
    --policy-document file://${IAM_DIR}/s3_policy.json
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn%3Aaws%3Aiam%3A%3A717741926071%3Apolicy%2Fhdb-pipeline-to-s3-access"
  # there are weird characters because the path is encoded    
}

attach_s3_policy() {
  # Attach s3 allowance policy to the selected role
  aws iam attach-role-policy \
    --role-name hdb-pipeline-task-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-to-s3-access
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-pipeline-task-role?section=permissions"
}

########
# scheduler side (used by modules/pipe_hdb/schedules.sh, not the pipeline task itself) -
# trusted by scheduler.amazonaws.com, not ecs-tasks.amazonaws.com like every role above
########

# runs an aws command, tolerating ONE specific expected error string as "already in the
# desired state, continue" rather than aborting - any OTHER error still fails normally
# (set -e still applies; this only returns 0 for the exact expected case)
run_idempotent() {
  local expected_error="$1"
  shift
  local output
  if ! output=$("$@" 2>&1); then
    if echo "$output" | grep -q "$expected_error"; then
      echo "(already exists, skipping) $output"
    else
      echo "$output" >&2
      return 1
    fi
  else
    echo "$output"
  fi
}

create_scheduler_role() {
  run_idempotent "EntityAlreadyExists" aws iam create-role \
    --role-name hdb-pipeline-scheduler-role \
    --assume-role-policy-document file://${IAM_DIR}/scheduler_trust_policy.json

  # ecs:RunTask + iam:PassRole on the two roles above - lets EventBridge Scheduler
  # actually launch hdb-pipeline the same way a human running `deploy_aws.sh run` would
  run_idempotent "EntityAlreadyExists" aws iam create-policy \
    --policy-name hdb-pipeline-scheduler-access \
    --policy-document file://${IAM_DIR}/scheduler_policy.json
  echo "Here:
    https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn%3Aaws%3Aiam%3A%3A717741926071%3Apolicy%2Fhdb-pipeline-scheduler-access?section=permissions"
  # attach-role-policy is naturally idempotent already (re-attaching an already-attached
  # policy is a no-op success, not an error) - no wrapper needed here
  aws iam attach-role-policy \
    --role-name hdb-pipeline-scheduler-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-scheduler-access

  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-pipeline-scheduler-role?section=permissions"
}

main "$@"