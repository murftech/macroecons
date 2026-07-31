#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

# Avail fixed configs of the GCP project from a shared location
source "cloud_gcp_shared/gcp_config.sh"
echo gcp project is $GCP_PROJECT
echo gcp region is $GCP_REGION
echo gcp artifact repo is $GCP_ARTIFACT_REPO

IMAGE=hdb-streamlit
REMOTE_TAG=${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/${GCP_ARTIFACT_REPO}/${IMAGE}:latest

echo remote tag is:
echo $REMOTE_TAG

usage() {
  echo "Usage: ./streamlit/deploy_gcp.sh [submit|deploy]"
  echo "  submit - build via Cloud Build and push to Artifact Registry"
  echo "  deploy - deploy the pushed image to Cloud Run"
  echo "(local Docker Desktop build/up/down commands live in ./streamlit/deploy_docker.sh)"
  exit 1
}

case "$1" in
  submit)
    gcloud builds submit \
    --config=streamlit/configs_gcp/cloudbuild.yaml \
    --project=${GCP_PROJECT} \
    --substitutions=_IMAGE_TAG=${REMOTE_TAG} .
    echo "Here: 
      https://console.cloud.google.com/artifacts/docker/macroecons/asia-southeast1/repo-macroecons?authuser=1&project=macroecons"
# should resolve into:
# gcloud builds submit \
# --config=streamlit/configs_gcp/cloudbuild.yaml \
# --project=macroecons \
# --substitutions=_IMAGE_TAG=asia-southeast1-docker.pkg.dev/macroecons/repo-macroecons/hdb-streamlit:latest .
    ;;
    
  deploy)
    gcloud run deploy ${IMAGE} \
      --image=${REMOTE_TAG} \
      --project=${GCP_PROJECT} \
      --region=${GCP_REGION} \
      --allow-unauthenticated
    echo "Here:
      https://console.cloud.google.com/run/services?authuser=1&project=macroecons"
    ;;

  *)
    usage
    ;;
esac
