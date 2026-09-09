#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

# Avail fixed configs of the GCP project from a shared location
source "cloud_gcp_shared/gcp_config.sh"
echo gcp project is $GCP_PROJECT
echo gcp region is $GCP_REGION
echo gcp artifact repo is $GCP_ARTIFACT_REPO
echo gcp job name is $GCP_JOB_NAME

# GCP_JOB_NAME doubles as the image name - the Cloud Run Job and the image it runs share it
REMOTE_TAG=${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/${GCP_ARTIFACT_REPO}/${GCP_JOB_NAME}:latest

echo remote tag is:
echo $REMOTE_TAG

usage() {
  echo "Usage: ./modules/pipe_hdb/deploy_gcp.sh [submit|deploy|execute]"
  echo "  submit  - build pipeline via Cloud Build and push to Artifact Registry"
  echo "  deploy  - create/update the Cloud Run Job that runs the pushed image (see streamlit's 'Refresh data' button)"
  echo "  execute - manually trigger the deployed Job right now, without going through streamlit"
  exit 1
}

case "$1" in

  submit)
    gcloud builds submit \
      --config=modules/pipe_hdb/configs_gcp/cloudbuild.yaml \
      --project=${GCP_PROJECT} \
      --substitutions=_IMAGE_TAG=${REMOTE_TAG} .
    echo "Here: 
      https://console.cloud.google.com/artifacts/docker/macroecons/asia-southeast1/repo-macroecons?authuser=1&project=macroecons"
# should resolve into:
# gcloud builds submit \
# --config=modules/pipe_hdb/configs_gcp/cloudbuild.yaml \
# --project=macroecons \
# --substitutions=_IMAGE_TAG=asia-southeast1-docker.pkg.dev/macroecons/repo-macroecons/hdb-pipeline:latest .
    ;;

  deploy)
    gcloud run jobs deploy ${GCP_JOB_NAME} \
      --image=${REMOTE_TAG} \
      --project=${GCP_PROJECT} \
      --region=${GCP_REGION} \
      --set-env-vars=GCS_BUCKET=murftech-macroecons
    
    echo "Here: 
      https://console.cloud.google.com/run/jobs?authuser=1&project=macroecons"
    ;;

  execute)
    gcloud run jobs execute ${GCP_JOB_NAME} \
      --project=${GCP_PROJECT} \
      --region=${GCP_REGION}
    echo "Here:
      https://console.cloud.google.com/run/jobs/details/asia-southeast1/hdb-pipeline/executions?authuser=1&project=macroecons"
      ;;

  *)
    usage
    ;;
    
esac
