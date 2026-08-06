# build context is the repo root (app.py's CODE_PATH reads modules/pipe_hdb/1_firstbq.py,
# which lives outside streamlit/) - run from the repo root, or use ./deploy_gcp.sh build:
#   docker build -f streamlit/Dockerfile -t hdb-streamlit:local .

FROM python:3.13-slim

LABEL org.opencontainers.image.title="hdb-streamlit"
LABEL org.opencontainers.image.description="Streamlit dashboard for the HDB resale pipeline - deployed to AWS ECS Express Mode (see streamlit/deploy_aws.sh) or run locally via docker-compose (see streamlit/deploy_docker.sh). Safe to delete older/dangling copies once superseded by a newer build."
LABEL org.opencontainers.image.source="streamlit/Dockerfile"

WORKDIR /app

# lets app.py tell "running in Docker locally" apart from "running natively on your Mac" -
# same convention modules/pipe_hdb/Dockerfile already uses
ENV RUNNING_IN_CONTAINER=true

COPY streamlit/requirements.txt .
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY streamlit/app.py .
COPY streamlit/helper_charts.py .
COPY streamlit/presets_street_sets.json .
COPY streamlit/providers providers
COPY streamlit/.streamlit .streamlit

# TWO reasons this whole directory is needed, not one:
#   1. app.py imports modules.pipe_hdb.helper_transform_for_plotly - the aggregation shared
#      with the pipeline, so both sides compute "median resale price" identically
#   2. the code-snippet viewer reads 2_report_firstbq.py directly (see CODE_PATH in app.py)
# do not trim this to a single file - the import would break at container start only
COPY modules/pipe_hdb modules/pipe_hdb

EXPOSE 8080

CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]

# essentially opening the terminal of the vm and running this exact line:
# streamlit run app.py --server.port=8080 --server.address=0.0.0.0
