# Rule of thumb: always build dockers from repo root
# build context is the repo root (needs packages/ and pyproject.toml, both outside
# modules/pipe_hdb/) - build with:
#   docker build -f modules/pipe_hdb/Dockerfile -t hdb-pipeline:local .
# or via Cloud Build: ./modules/pipe_hdb/deploy_gcp.sh submit

FROM python:3.13-slim

LABEL org.opencontainers.image.title="hdb-pipeline"
LABEL org.opencontainers.image.description="HDB resale data pipeline - fetches/cleans/charts data.gov.sg data, uploads output to S3/GCS. Runs as a one-shot AWS ECS Fargate task (modules/pipe_hdb/deploy_aws.sh run) or GCP Cloud Run Job. Safe to delete older/dangling copies once superseded by a newer build."
LABEL org.opencontainers.image.source="modules/pipe_hdb/Dockerfile"

WORKDIR /app

ENV RUNNING_IN_CONTAINER=true

# install dependencies
COPY modules/pipe_hdb/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy precode local package and install it
# COPY packages/ packages/
# COPY pyproject.toml .
# RUN pip install --no-cache-dir -e .

COPY modules/pipe_hdb modules/pipe_hdb

# create folders the scripts write to
RUN mkdir -p hive/t2 output

CMD ["python", "modules/pipe_hdb/run_pipeline.py"]

