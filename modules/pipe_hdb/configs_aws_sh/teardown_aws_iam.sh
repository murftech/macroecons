#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .vscode ] && [ "$PWD" != "/" ]; do cd ..; done

source "cloud_aws_shared/aws_config.sh"

usage() {
  echo "Usage: ./modules/pipe_hdb/configs_aws_sh/teardown_aws_iam.sh [execution|task|s3|detach|schedule|all]"
  echo "  execution  - detach AWS's managed policy + delete hdb-pipeline-execution-role"
  echo "  task       - delete hdb-pipeline-task-role (must already be detached from any policy)"
  echo "  s3         - delete the hdb-pipeline-to-s3-access policy (must already be detached from any role)"
  echo "  detach     - detach the S3 policy from the task role (undo of attach)"
  echo "  schedule   - detach + delete hdb-pipeline-scheduler-role and its policy (undo of setup's 'schedule')"
  echo "  all        - run detach, s3, task, execution in the correct order (schedule is opt-in, not included)"
  echo " 'all' runs in dependency order: detach -> delete policy -> delete task role -> delete execution role)"
  exit 1
}

main() {
case "$1" in
  execution)
    delete_execution_role
    ;;
  task)
    delete_task_role
    ;;
  s3)
    delete_s3_policy
    ;;
  detach)
    detach_s3_policy
    ;;
  schedule)
    delete_scheduler_role
    ;;
  all)
    detach_s3_policy
    delete_s3_policy
    delete_task_role
    delete_execution_role
    ;;
  *)
    usage
    ;;
esac
}


delete_execution_role() {
  aws iam detach-role-policy \
    --role-name hdb-pipeline-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
  aws iam delete-role --role-name hdb-pipeline-execution-role
  echo "Here (confirm hdb-pipeline-execution-role is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles"
}

delete_task_role() {
  aws iam delete-role --role-name hdb-pipeline-task-role
  echo "Here (confirm hdb-pipeline-task-role is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles"
}

delete_s3_policy() {
  aws iam delete-policy \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-to-s3-access
  echo "Here (confirm hdb-pipeline-s3-access is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/policies"
}

detach_s3_policy() {
  aws iam detach-role-policy \
    --role-name hdb-pipeline-task-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-to-s3-access
  echo "Here (role still exists, confirm hdb-pipeline-s3-access is no longer listed under it):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-pipeline-task-role?section=permissions"
}

# runs an aws command, tolerating ONE specific expected error string as "already in the
# desired state, continue" rather than aborting - any OTHER error still fails normally
run_idempotent() {
  local expected_error="$1"
  shift
  local output
  if ! output=$("$@" 2>&1); then
    if echo "$output" | grep -q "$expected_error"; then
      echo "(already gone, skipping) $output"
    else
      echo "$output" >&2
      return 1
    fi
  else
    echo "$output"
  fi
}

# mirrors create_scheduler_role() in setup_aws_iam.sh - that one does create+attach as a
# single combined step (not split into execution/task/attach-style granular actions), so
# this undoes it the same way: detach -> delete policy -> delete role, all in one go
delete_scheduler_role() {
  run_idempotent "NoSuchEntity" aws iam detach-role-policy \
    --role-name hdb-pipeline-scheduler-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-scheduler-access
  run_idempotent "NoSuchEntity" aws iam delete-policy \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-pipeline-scheduler-access
  run_idempotent "NoSuchEntity" aws iam delete-role --role-name hdb-pipeline-scheduler-role
  echo "Here (confirm hdb-pipeline-scheduler-role and hdb-pipeline-scheduler-access are both gone):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles"
}

main "$@"