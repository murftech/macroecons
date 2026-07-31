#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

source "cloud_aws_shared/aws_config.sh"

IAM_DIR=streamlit/configs_aws

usage() {
  echo "Usage: ./streamlit/configs_aws_sh/setup_aws_iam.sh [execution|task|policy|policy2|attach|infra|all]"
  echo "  execution          - create hdb-streamlit-execution-role + attach AWS's managed policy (image pull + logs)"
  echo "  task          - create hdb-streamlit-task-role (assumed by the running container)"
  echo "  policy            - create the custom task policy (S3 read + trigger/poll the ECS pipeline task)"
  echo "  policy2           - push an updated streamlit_task_policy.json live as a new policy version (policy already exists)"
  echo "  attach            - attach that policy to the task role"
  echo "  infra         - create ecsInfrastructureRoleForExpressServices + attach AWS's managed Express policy"
  echo "  all                - run all five in sequence"
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
  policy)
    create_task_policy
    ;;
  policy2)
    update_task_policy
    ;;
  attach)
    attach_task_policy
    ;;
  infra)
    create_infra_role
    ;;
  all)
    create_execution_role
    create_task_role
    create_task_policy
    attach_task_policy
    create_infra_role
    ;;
  *)
    usage
    ;;
esac
}


# the reason to put the functions outside of case, is because we dont want to rewrite the statements for all

# you can open the files to understand here
# cloud_aws_shared/trust_policy.json (shared with modules/pipe_hdb - same trust statement, used by every task-assumed role)
# streamlit/configs_aws/streamlit_task_policy.json
# streamlit/configs_aws/ecs_express_infra_role_trust_policy.json

# task-role is about permissions for what INSIDE your application code does,
# execution-role is about permissions that ECS itself needs to handle the OUTSIDE of the image.


########
# execution side (what can ECS do outside of image)
########
create_execution_role() {
  # create a execution role with yet empty permissions
  aws iam create-role \
    --role-name hdb-streamlit-execution-role \
    --assume-role-policy-document file://cloud_aws_shared/trust_policy.json

  # attach an standard amazon prepackaged permission set to it, that lets ECS pull images from ECR and later stream the running container's logs to CloudWatch
    aws iam attach-role-policy \
    --role-name hdb-streamlit-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-streamlit-execution-role?section=permissions"

}


########
# task side (what can internal code do to external environments)
########

create_task_role() {
  # this is needed since our code does connect outside of the application at runtime (S3 reads + triggering the ECS pipeline task)
  # creates the task role the running container shall assume when live.
  aws iam create-role \
    --role-name hdb-streamlit-task-role \
    --assume-role-policy-document file://cloud_aws_shared/trust_policy.json
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/hdb-streamlit-task-role?section=permissions"
}

create_task_policy() {
  # defines 4 external acting permissions: S3 read, S3 list, ecs:RunTask/DescribeTasks, iam:PassRole
  # S3 read: fetch_html_bytes()
  # ecs:RunTask/DescribeTasks: trigger_pipeline()/poll_pipeline()
  # iam:PassRole: trigger_pipeline()
  # to be attached to our streamlit's task-role
  # How?: Consumed directly by: aws ecs create-express-gateway-service, the Service Up time
  # Not passed through task-definition.jsons, because modern design where the spinup is able to inline things like subnet, remote tag, etc
  aws iam create-policy \
    --policy-name hdb-streamlit-task-access \
    --policy-document file://${IAM_DIR}/streamlit_task_policy.json
}

update_task_policy() {
  # pushes an updated streamlit_task_policy.json content live, for when the file changes after
  # the policy already exists (create-policy only works the first time - EntityAlreadyExists otherwise)
  # --set-as-default makes this new version take effect immediately - no reattach needed,
  # since hdb-streamlit-task-role is already attached to this policy's ARN, not to a specific version
  # the syntax is in same shape as above
  aws iam create-policy-version \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access \
    --policy-document file://${IAM_DIR}/streamlit_task_policy.json \
    --set-as-default
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/policies/details/arn%3Aaws%3Aiam%3A%3A717741926071%3Apolicy%2Fhdb-streamlit-task-access"
}

attach_task_policy() {
  # Attach the task policy to the selected role
  aws iam attach-role-policy \
    --role-name hdb-streamlit-task-role \
    --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/hdb-streamlit-task-access
    
}


########
# infra side (what to provision for a always-running container)
########

create_infra_role() {  
  # creates an empty infra role
  aws iam create-role \
    --role-name ecsInfrastructureRoleForExpressServices \
    --assume-role-policy-document file://${IAM_DIR}/ecs_express_infra_role_trust_policy.json

  # AmazonECSInfrastructureRoleforExpressGatewayServices is the policy you must attach when spinning up an instance.
  # Infra roles attach important infra configs like loadbalancer, cloudwatch, and a host of other stuff
  # This also attaches dozens of important general configs which EGS needs turned on for it to work.
  # AWS packaged them and manages them for us, so it is always in sync with their blackbox EGS
  # This step is still required because of auditability/accountability -
  # we can see and are explicitly authorizing exactly what permissions this role gets, rather than AWS silently creating it for us.
  # Consumed by: aws ecs create-express-gateway-service
  aws iam attach-role-policy \
    --role-name ecsInfrastructureRoleForExpressServices \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices
  echo "Here:
      https://us-east-1.console.aws.amazon.com/iam/home#/roles/details/ecsInfrastructureRoleForExpressServices?section=permissions"
}

main "$@"