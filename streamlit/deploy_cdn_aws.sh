#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

source "cloud_aws_shared/aws_config.sh"

DOMAIN=hdb-streamlit-aws.murftech.dev
DIST_CONFIG=streamlit/configs_aws/cloudfront_distribution.json

# CloudFront only accepts ACM certs from us-east-1, regardless of what region the app itself runs in
CERT_REGION=us-east-1

# local cache of the cert ARN, written by request-cert, read by validation-record/cert-status
# (same reasoning as streamlit/.state/.service_arn - the ARN doesn't exist until after the request,
# and every later command needs to reference that exact same cert, not a freshly requested one)
CERT_ARN_FILE=streamlit/.state/.cert_arn

# same reasoning again, but for the distribution's own Id - written by create-distribution, read by distribution-config
DIST_ID_FILE=streamlit/.state/.distribution_id

usage() {
  echo "Usage: ./streamlit/deploy_cloudfront.sh [request-cert|validation-record|cert-status|create-distribution|distribution-config]"
  echo "  request-cert        - request the ACM cert for ${DOMAIN} in ${CERT_REGION}, cache its ARN"
  echo "  validation-record   - print the DNS CNAME (name+value) needed to prove domain ownership"
  echo "  cert-status         - print PENDING_VALIDATION or ISSUED"
  echo "  create-distribution - create the CloudFront distribution from ${DIST_CONFIG}, cache its Id"
  echo "  distribution-config - pull the live distribution config back down, to check it matches ${DIST_CONFIG}"
  echo "(each step still needs a manual DNS record added at Porkbun in between - request-cert and"
  echo " create-distribution aren't chainable into an 'all', unlike setup_aws_iam.sh)"
  exit 1
}

request_cert() {
  RESPONSE=$(aws acm request-certificate \
    --domain-name ${DOMAIN} \
    --validation-method DNS \
    --region ${CERT_REGION})
  echo "$RESPONSE"
  echo "$RESPONSE" | jq -r '.CertificateArn' > ${CERT_ARN_FILE}
  echo "Cert ARN saved to ${CERT_ARN_FILE}"
}

validation_record() {
  aws acm describe-certificate \
    --certificate-arn "$(cat ${CERT_ARN_FILE})" \
    --region ${CERT_REGION} \
    --query 'Certificate.DomainValidationOptions[0].ResourceRecord'
}

cert_status() {
  aws acm describe-certificate \
    --certificate-arn "$(cat ${CERT_ARN_FILE})" \
    --region ${CERT_REGION} \
    --query 'Certificate.Status'
}

create_distribution() {
  export AWS_PAGER=""
  RESPONSE=$(aws cloudfront create-distribution \
    --distribution-config file://${DIST_CONFIG})
  echo "$RESPONSE"
  echo "$RESPONSE" | jq -r '.Distribution.Id' > ${DIST_ID_FILE}
  echo "Distribution Id saved to ${DIST_ID_FILE}"
}

distribution_config() {
  export AWS_PAGER=""
  aws cloudfront get-distribution-config \
    --id "$(cat ${DIST_ID_FILE})"
}

case "$1" in
  request-cert)
    request_cert
    ;;
  validation-record)
    validation_record
    ;;
  cert-status)
    cert_status
    ;;
  create-distribution)
    create_distribution
    ;;
  distribution-config)
    distribution_config
    ;;
  *)
    usage
    ;;
esac
