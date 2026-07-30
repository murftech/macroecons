#!/bin/bash
# Shared GCP values, the direct parallel of cloud_aws_shared/aws_config.sh.
# Source this AFTER the cd-to-repo-root loop, e.g.:
#   source "cloud_gcp_shared/gcp_config.sh"
#
# every name here is GCP_-prefixed on purpose, not for tidiness: schedules.sh sources BOTH
# this file and aws_config.sh, and aws_config.sh already defines a bare REGION
# (ap-southeast-1). an unprefixed REGION here would silently overwrite it and point the
# EventBridge/ECS calls at the wrong cloud's region.

GCP_PROJECT=macroecons
GCP_REGION=asia-southeast1

# Artifact Registry repo that deploy_gcp.sh pushes images into
GCP_ARTIFACT_REPO=repo-macroecons

# pipe_hdb's Cloud Run Job name - it doubles as the image name (see GCP_REMOTE_TAG in
# deploy_gcp.sh). streamlit's equivalent is hdb-streamlit, so if streamlit/ ever sources
# this file it should set its own name locally rather than reuse this one.
GCP_JOB_NAME=hdb-pipeline
