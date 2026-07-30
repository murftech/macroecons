#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .vscode ] && [ "$PWD" != "/" ]; do cd ..; done


# Avail fixed configs of the AWS project from a shared location
source "cloud_aws_shared/aws_config.sh"
echo account id is $ACCOUNT_ID
echo region is $REGION
echo cluster is $CLUSTER
echo subnet id is $SUBNET_ID
echo security group id is $SECURITY_GROUP_ID

# Define futher required configs
IMAGE=hdb-pipeline

#### used for [tag/push] ####
REMOTE_TAG=${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${IMAGE}:aws

#### used for [register] ####
TASK_JSON_FILE=modules/pipe_hdb/configs_aws/task_definition.json 


usage() {
  echo "Usage: ./modules/pipe_hdb/deploy_aws.sh [dockerlogin|build|tag|push|dockercombine|register|run]"
  echo "  build        - build the pipeline image locally, native arm64 allowed (hdb-pipeline:local)"
  echo "  tag          - tag the local image with the full ECR remote tag"
  echo "  dockerlogin  - authenticate Docker to ECR (token expires ~12h, re-run whenever it's stale)"
  echo "  push         - push the tagged image to ECR"
  echo "  dockercombine- run build, tag, dockerlogin, push in sequence"

  echo "  register     - register/re-register latest image to named task definition, embedding the task with task_definition.json specified configs"
  echo "  run - looks up the default VPC/subnet/security group, then runs the hdb-pipeline task on Fargate"
  exit 1
}

main() {
  case "$1" in

  build)
    docker build -f modules/pipe_hdb/Dockerfile -t ${IMAGE}:local .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
    ;;

  tag)
    docker tag ${IMAGE}:local ${REMOTE_TAG}
    ;;

  dockerlogin)
    # explain >
    # get-login-password: short-lived (~12h) token generated from AWS ECR
    # --region: which region's ECR to target
    # docker login: call docker login into webhosts
    # --username AWS: this username is default fixed by AWS
    # --password-stdin: takes stdin as password from the pipe
    # hostname: ID to connect to AWS IAM account

    aws ecr get-login-password --region ${REGION} \
      | docker login \
      --username AWS \
      --password-stdin \
      ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
    ;;  

  push)
    docker push ${REMOTE_TAG}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecr/repositories/private/717741926071/hdb-pipeline/_/details?region=ap-southeast-1"
    ;;

  dockercombine)
    docker build -f modules/pipe_hdb/Dockerfile -t ${IMAGE}:local .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
    docker tag ${IMAGE}:local-awsrdy ${REMOTE_TAG}
    aws ecr get-login-password --region ${REGION} \
      | docker login \
      --username AWS \
      --password-stdin \
      ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
    docker push ${REMOTE_TAG}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecr/repositories/private/717741926071/hdb-pipeline/_/details?region=ap-southeast-1"
    ;;

  register)
    # to stop the scrolling pager and exit the command
    export AWS_PAGER=""
    # task_definition.json's image field is a placeholder - jq swaps in the same REMOTE_TAG
    # that push/dockercombine actually push to, so this can't drift out of sync with ECR
    # the way it did before (file said :latest, script had moved on to pushing :aws)

    # dynamically join remote tag to sync whichever docker tag given.
    COMPLETE_JSON=$(jq --arg img "${REMOTE_TAG}" '.containerDefinitions[0].image = $img' ${TASK_JSON_FILE})
    aws ecs register-task-definition \
      --cli-input-json "${COMPLETE_JSON}" \
      --region ${REGION}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/task-definitions/hdb-pipeline?region=ap-southeast-1"
    ;;

  run)
    # to stop the scrolling pager and exit the command  
    export AWS_PAGER=""

    echo "Using Subnet: ${SUBNET_ID}, Security Group: ${SECURITY_GROUP_ID}"
    # printed by network_lookup.sh, hardcoded in shared location aws_config.sh
    # assignPublicIp=ENABLED: needed so the task can reach the internet for (data.gov.sg API + S3)
    
    aws ecs run-task \
      --cluster ${CLUSTER} \
      --task-definition ${IMAGE} \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${SUBNET_ID}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
      --region ${REGION}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/clusters/macroecons/tasks?region=ap-southeast-1"
    echo "Here Landed Objects:
      https://ap-southeast-1.console.aws.amazon.com/s3/buckets/murftech-macroecons?region=ap-southeast-1&prefix=DBMaster/annotations/reports/macroecons/pipe_hdb/&showversions=false"
    ;;

  *)
    usage
    ;;

esac
}

# place all the helper functions out in appendix

#### used for [run] - commented out in favor of the hardcoded values above, kept here to
#### reuse if we prefer the VPC/subnet/SG to be discovered dynamically ####
# get_vpc() {
#   aws ec2 describe-vpcs \
#     --filters "Name=is-default,Values=true" \
#     --query 'Vpcs[0].VpcId' \
#     --output text \
#     --region ${REGION}
# }
#
# get_first_subnet() {
#   local vpc_id="$1"
#   aws ec2 describe-subnets \
#     --filters "Name=vpc-id,Values=${vpc_id}" \
#     --query 'Subnets[0].SubnetId' \
#     --output text \
#     --region ${REGION}
# }
#
# get_default_sg() {
#   local vpc_id="$1"
#   aws ec2 describe-security-groups \
#     --filters "Name=vpc-id,Values=${vpc_id}" "Name=group-name,Values=default" \
#     --query 'SecurityGroups[0].GroupId' \
#     --output text \
#     --region ${REGION}
# }

main "$@"