#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

# Avail fixed configs of the GCP project from a shared location
source "cloud_gcp_shared/gcp_config.sh"
echo gcp project is $GCP_PROJECT
echo gcp region is $GCP_REGION
echo gcp job name is $GCP_JOB_NAME

# the account id is immutable once created and becomes the email - keep it in its own
# variable and derive the address, so the two can't drift apart
SCHEDULER_SA_ID=cloud-scheduler
SCHEDULER_SA=${SCHEDULER_SA_ID}@${GCP_PROJECT}.iam.gserviceaccount.com

# same reason deploy_gcp.sh echoes its REMOTE_TAG - this derived address is the one string
# schedules.sh must match in --oauth-service-account-email, so print what was actually built
echo scheduler service account is $SCHEDULER_SA

usage() {
  echo "Usage: ./modules/pipe_hdb/configs_gcp_sh/setup_gcp_iam.sh [schedule|invoker|all]"
  echo "  schedule  - create the cloud-scheduler service account (identity only, grants nothing)"
  echo "  invoker   - grant that account roles/run.invoker on the hdb-pipeline Cloud Run Job"
  echo "  all       - run schedule then invoker in sequence"
  echo "(pairs with modules/pipe_hdb/schedules.sh gcp, which names this account by email)"
  exit 1
}

main() {
case "$1" in
  schedule)
    create_scheduler_sa
    ;;
  invoker)
    grant_run_invoker
    ;;
  all)
    create_scheduler_sa
    grant_run_invoker
    ;;
  *)
    usage
    ;;
esac
}



# runs a gcloud command, tolerating ONE specific expected error string as "already in the
# desired state, continue" rather than aborting - any OTHER error still fails normally
# (set -e still applies; this only returns 0 for the exact expected case)
run_idempotent() {
  local expected_error="$1"
  shift
  local output
  if ! output=$("$@" 2>&1); then
    if echo "$output" | grep -q "$expected_error"; then
      echo "(already exists, skipping) $output"
    else
      echo "$output" >&2
      return 1
    fi
  else
    echo "$output"
  fi
}

create_scheduler_sa() {
  run_idempotent "already exists" gcloud iam service-accounts create ${SCHEDULER_SA_ID} \
    --project=${GCP_PROJECT} \
    --display-name=cloud-scheduler \
    --description=scheduler-permissions

  echo "Here:
    https://console.cloud.google.com/iam-admin/serviceaccounts?authuser=1&project=macroecons"
}

grant_run_invoker() {
  # run.invoker is the only role needed
  
  gcloud run jobs add-iam-policy-binding ${GCP_JOB_NAME} \
    --project=${GCP_PROJECT} \
    --region=${GCP_REGION} \
    --member="serviceAccount:${SCHEDULER_SA}" \
    --role="roles/run.invoker"

  # object > tick > permissions> show inherited role
  echo "Here:
    https://console.cloud.google.com/run/jobs?authuser=1&project=macroecons"
}

main "$@"
