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
IMAGE=hdb-streamlit
REMOTE_TAG=${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${IMAGE}:aws-amd64

# services runtime are also configed by task definitions
task_definition_json() {
    cat <<EOF
  {
    "image": "${REMOTE_TAG}",
    "containerPort": 8080,
    "environment": [
      {"name": "S3_BUCKET", "value": "murftech-macroecons"},
      {"name": "SUBNET_ID", "value": "${SUBNET_ID}"},
      {"name": "SECURITY_GROUP_ID", "value": "${SECURITY_GROUP_ID}"}
    ]
  }
EOF
}

# local cache to sync ARN ID re-produced after running create EGS
ARN_FILE=streamlit/.state/.service_arn

##################
##################

usage() {
  echo "Usage: ./streamlit/deploy_aws.sh [buildx|tag|dockerlogin|push|dockercombine|create|update|url|delete]"
  echo "  buildx        - build the streamlit image locally (hdb-streamlit:amd64)"
  echo "  tag           - tag the local image with the full ECR remote tag"
  echo "  dockerlogin   - authenticate Docker to ECR (token expires ~12h, re-run whenever it's stale)"
  echo "  push          - push the tagged image to ECR"
  echo "  dockercombine - run dockerlogin, build, tag, push in sequence"
  echo "  create        - MAIN Spins up Infra for the first time. Ingests task_definition_json and latest image version into task definition"
  echo "  update        - MAIN update task_definition_json and/or latest image version into task definition and redeploy onto infra, keeping the service live through the cutover process."
  echo "  url           - get streamlit's url"
  echo "  delete        - teardown service and infrastructure."
  exit 1
}

main() {
case "$1" in

  buildx)
    docker buildx build --platform linux/amd64 -f streamlit/Dockerfile -t ${IMAGE}:local-amd64 --load .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
    # sub make sure to understand why build cannot. And make sure to ab test it.

    # Learning Point: why buildx?  
    # generic FARGATE can run with mac's native ARM64
    # EGS FARGATE can only choose AMD64, hence docker needs to build with nonnative AMD64 within mac.
    # plain docker build --platform linux/amd64 had silently still built arm64 before due to bugs.
    # use docker buildx to explicitly ensure x-platform building
    # however buildx builds in isolated output (dontknowwhere), and does not persist image in docker
    # --load . required to export image onto docker.
    ;;

  tag)
    docker tag ${IMAGE}:local-amd64 ${REMOTE_TAG}
    ;;
    
  dockerlogin)
    aws ecr get-login-password --region ${REGION} \
    | docker login \
    --username AWS \
    --password-stdin \
    ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
    ;;

  push)
    docker push ${REMOTE_TAG}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecr/repositories/private/717741926071/hdb-streamlit?region=ap-southeast-1"
    ;;

  dockercombine)
    aws ecr get-login-password --region ${REGION} | \
      docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com
    docker buildx build --platform linux/amd64 -f streamlit/Dockerfile -t ${IMAGE}:local-amd64 --load .
    # clean up the dangling image left behind by the previous build under this same tag
    docker image prune -f
    docker tag ${IMAGE}:local-amd64 ${REMOTE_TAG}
    docker push ${REMOTE_TAG}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecr/repositories/private/717741926071/hdb-streamlit?region=ap-southeast-1"
    ;;

  create)
    # desired given name to service
    SERVICE_NAME=hdb-streamlit
    # IAM configs availed by setup_aws_iam.sh
    EXECUTION_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/hdb-streamlit-execution-role
    TASK_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/hdb-streamlit-task-role
    INFRA_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/ecsInfrastructureRoleForExpressServices


    STDOUT=$(
      aws ecs create-express-gateway-service \
      --service-name ${SERVICE_NAME} \
      --cluster ${CLUSTER} \
      --execution-role-arn ${EXECUTION_ROLE_ARN} \
      --task-role-arn ${TASK_ROLE_ARN} \
      --infrastructure-role-arn ${INFRA_ROLE_ARN} \
      --primary-container "$(task_definition_json)" \
      --health-check-path "/_stcore/health" \
      --region ${REGION})
    
    # SAVE refeshed Service ARN
    # STDOUT=$ is needed to catch the stdout from 
    # we need the response to take out the service ARN automatically on every create. Removes need to Manaully retrieve in UI
    echo "$STDOUT"
    echo "$STDOUT" | jq -r '.service.serviceArn' > ${ARN_FILE}
    echo "Service ARN saved to ${ARN_FILE}"

    echo "Here:
      service deployment-
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/clusters/macroecons/services?region=ap-southeast-1
      task definition created -
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/task-definitions/macroecons-hdb-streamlit?status=ACTIVE&region=ap-southeast-1
      "
    ;;

  update)
  
    EXECUTION_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/hdb-streamlit-execution-role
    # only updates task_definition_json configs in place with service still on. Once finished, service switches over to them.
    # stil takes time though
    aws ecs update-express-gateway-service \
      --service-arn "$(cat ${ARN_FILE})" \
      --execution-role-arn ${EXECUTION_ROLE_ARN} \
      --primary-container "$(task_definition_json)" \
      --region ${REGION}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/clusters/macroecons/express-services/hdb-streamlit/resources?region=ap-southeast-1"
    ;;
  url)
    # quickly retrieve url
    aws ecs describe-express-gateway-service \
      --service-arn "$(cat ${ARN_FILE})" \
      --query 'service.activeConfigurations[0].ingressPaths[0].endpoint' \
      --output text \
      --region ${REGION}
    ;;

  delete)
    # delete service via arn
    aws ecs delete-express-gateway-service \
      --service-arn "$(cat ${ARN_FILE})" \
      --region ${REGION}
    echo "Here:
      https://ap-southeast-1.console.aws.amazon.com/ecs/v2/clusters/macroecons/express-services/hdb-streamlit/resources?region=ap-southeast-1"
    ;;

  *)
    usage
    ;;

esac
}

main "$@"
