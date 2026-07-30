#!/bin/bash
set -e

REGION=ap-southeast-1

usage() {
  echo "Usage: ./modules/pipe_hdb/aws_ecs/network_lookup.sh [vpc|subnets|sg|all]"
  echo "  vpc      - print the default VPC ID"
  echo "  subnets  - print subnet IDs inside the default VPC"
  echo "  sg       - print the default security group ID inside the default VPC"
  echo "  all      - print all three, labeled"
  exit 1
}

get_vpc() {
  aws ec2 describe-vpcs \
    --filters "Name=is-default,Values=true" \
    --query 'Vpcs[0].VpcId' \
    --output text \
    --region ${REGION}
}

get_subnets() {
  local vpc_id="$1"
  aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${vpc_id}" \
    --query 'Subnets[].SubnetId' \
    --output text \
    --region ${REGION}
}

get_default_sg() {
  local vpc_id="$1"
  aws ec2 describe-security-groups \
    --filters "Name=vpc-id,Values=${vpc_id}" "Name=group-name,Values=default" \
    --query 'SecurityGroups[0].GroupId' \
    --output text \
    --region ${REGION}
}

case "$1" in
  vpc)
    get_vpc
    ;;
  subnets)
    VPC_ID=$(get_vpc)
    get_subnets "${VPC_ID}"
    ;;
  sg)
    VPC_ID=$(get_vpc)
    get_default_sg "${VPC_ID}"
    ;;
  all)
    VPC_ID=$(get_vpc)
    echo "VPC ID:     ${VPC_ID}"
    echo "Subnets:    $(get_subnets "${VPC_ID}")"
    echo "Default SG: $(get_default_sg "${VPC_ID}")"
    ;;
  *)
    usage
    ;;
esac
