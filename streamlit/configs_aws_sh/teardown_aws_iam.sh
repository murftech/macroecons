#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .vscode ] && [ "$PWD" != "/" ]; do cd ..; done

source "cloud_aws_shared/aws_config.sh"

usage() {
  echo "Usage: ./streamlit/configs_aws_sh/teardown_aws_iam.sh [detach|policy|task|execution|infra|all]"
  echo ""
  echo "Must run in sequence (task-role chain - policy/task each need detach to have run first):"
  echo "  detach     - 1st: detach hdb-streamlit-task-access from hdb-streamlit-task-role (undo of attach)"
  echo "  policy     - 2nd: delete all non-default versions, then delete hdb-streamlit-task-access itself"
  echo "  task       - 2nd: delete hdb-streamlit-task-role (policy and task can then run in either order)"
  echo ""
  echo "Independent - each detaches its own managed policy inline, run standalone in any order:"
  echo "  execution  - detach AWS's managed policy + delete hdb-streamlit-execution-role"
  echo "  infra      - detach AWS's managed Express policy + delete ecsInfrastructureRoleForExpressServices"
  echo ""
  echo "  all        - runs detach, policy, task, execution, infra in that order"
  exit 1
}

detach_task_policy() {
  aws iam detach-role-policy \
    --role-name hdb-streamlit-task-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access
  echo "Here (role still exists, confirm hdb-streamlit-task-access is no longer listed under it):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-streamlit-task-role?section=permissions"
}

delete_task_policy() {
  # a customer-managed policy can't be deleted while non-default versions still exist -
  # clear those first, then delete-policy removes the policy along with its default version
  for VERSION in $(aws iam list-policy-versions \
      --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access \
      --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text); do
    aws iam delete-policy-version \
      --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access \
      --version-id ${VERSION}
  done
  aws iam delete-policy --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access
  echo "Here (confirm hdb-streamlit-task-access is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn%3Aaws%3Aiam%3A%3A717741926071%3Apolicy%2Fhdb-streamlit-task-access"
}

delete_task_role() {
  aws iam delete-role --role-name hdb-streamlit-task-role
  echo "Here (confirm hdb-streamlit-task-role is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-streamlit-task-role?section=permissions"
}

delete_execution_role() {
  aws iam detach-role-policy \
    --role-name hdb-streamlit-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
  aws iam delete-role --role-name hdb-streamlit-execution-role
  echo "Here (confirm hdb-streamlit-execution-role is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-streamlit-execution-role?section=permissions"
}

delete_infra_role() {
  aws iam detach-role-policy \
    --role-name ecsInfrastructureRoleForExpressServices \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices
  aws iam delete-role --role-name ecsInfrastructureRoleForExpressServices
  echo "Here (confirm ecsInfrastructureRoleForExpressServices is gone from the list - its own detail page no longer exists):
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/ecsInfrastructureRoleForExpressServices?section=permissions"
}

case "$1" in
  detach)
    detach_task_policy
    ;;
  policy)
    delete_task_policy
    ;;
  task)
    delete_task_role
    ;;
  execution)
    delete_execution_role
    ;;
  infra)
    delete_infra_role
    ;;
  all)
    detach_task_policy
    delete_task_policy
    delete_task_role
    delete_execution_role
    delete_infra_role
    ;;
  *)
    usage
    ;;
esac
