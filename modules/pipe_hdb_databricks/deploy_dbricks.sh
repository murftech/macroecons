#!/bin/bash
set -e
 
# always run from the bundle root (where databricks.yml lives). The CLI actually
# walks upward looking for databricks.yml on its own, so this cd isn't strictly
# required the way deploy_aws.sh's "find .git" loop was - but keeping it explicit
# avoids surprises if this script is ever called from somewhere else in the repo.

cd "$(dirname "$0")"
# while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

TARGET=dev
JOB_KEY=hdb_pipeline
 
usage() {
  echo "Usage: ./deploy_databricks.sh [validate|plan|deploy|run|all|status|destroy]"
  echo "  validate - check databricks.yml + resources/*.yml are structurally valid (no deploy)"
  echo "  plan     - show what deploy WOULD add/change/delete against the live workspace (no deploy)"
  echo "  deploy   - push the bundle to the workspace: uploads src/, creates/updates the"
  echo "             schema, volume, and job (this one step replaces build/tag/dockerlogin/"
  echo "             push/register from deploy_aws.sh - no image, no task definition, no ECR)"
  echo "  run      - manually trigger the deployed job once, same idea as deploy_aws.sh's 'run'"
  echo "             but no cluster/subnet/security-group to specify - serverless has none of that"
  echo "  all      - validate, deploy, run in sequence (equivalent to deploy_aws.sh's dockercombine)"
  echo "  status   - print links to the deployed job/resources in the workspace"
  echo "  destroy  - tear down everything this bundle created (the CLI itself prompts to confirm)"
  exit 1
}
 
main() {
  case "$1" in
 
  validate)
    databricks bundle validate -t ${TARGET}
    ;;

  plan)
    # validate only proves the YAML is well-formed - it never asks the workspace anything.
    # plan is the one that diffs the resolved config against the deployed state and prints
    # "N to add, N to change, N to delete, N unchanged", so it's the check worth running
    # before touching a live workspace. deploy_aws.sh has no equivalent - with imperative
    # bash the only way to know what a deploy will change is to read the script.
    databricks bundle plan -t ${TARGET}
    ;;

  deploy)
    databricks bundle deploy -t ${TARGET}
    ;;
 
  run)
    databricks bundle run ${JOB_KEY} -t ${TARGET}
    ;;
 
  all)
    databricks bundle validate -t ${TARGET}
    databricks bundle deploy -t ${TARGET}
    databricks bundle run ${JOB_KEY} -t ${TARGET}
    ;;
 
  status)
    # prints workspace URLs for what's deployed - the Databricks equivalent of the
    # hardcoded "Here: https://...console.aws.amazon.com/..." echoes in deploy_aws.sh,
    # except the CLI generates the real, current links itself instead of us hand-typing them
    databricks bundle summary -t ${TARGET}
    ;;
 
  destroy)
    databricks bundle destroy -t ${TARGET}
    ;;
 
  *)
    usage
    ;;
 
  esac
}
 
# place all the helper functions out in appendix

# databricks auth login --host <your-workspace-url>
 
main "$@"