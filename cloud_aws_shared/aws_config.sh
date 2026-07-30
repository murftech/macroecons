#!/bin/bash

####
# hand-typed, stable, obvious if wrong.
# deploy-time only, or duplicated as a literal in streamlit/providers/aws.py on purpose.
####
ACCOUNT_ID=717741926071
REGION=ap-southeast-1
CLUSTER=macroecons

####
# discovered by network_lookup.sh, volatile, opaque if wrong (subnet-53c07a0a, sg-f08cfe82).
# these two are the ONLY values injected into the container - streamlit/deploy_aws.sh writes
# them into the task-def "environment" block, providers/aws.py reads them with os.environ[].
####

# same default-VPC values network_lookup.sh discovers - hardcoded here since they
# essentially never change; re-run network_lookup.sh and update these by hand if they ever do
SUBNET_ID=subnet-53c07a0a
SECURITY_GROUP_ID=sg-f08cfe82