# Rule of thumb: always build dockers from repo root
#   docker build -f modules/pipe_hdb/Dockerfile -t hdb-pipeline:local .
# or via Cloud Build: ./modules/pipe_hdb/deploy_gcp.sh submit

FROM python:3.13-slim
# have to match the pyproject.toml's definition

# run_pipeline.sh finds the repo root by walking up to .git, which does not
# exist in the image. This tells it where the root is instead.
ENV REPO_ROOT=/app

ENV RUNNING_IN_CONTAINER=true

WORKDIR /app

# copy source code
COPY modules/pipe_hdb modules/pipe_hdb

# create folders the scripts write to
RUN mkdir -p hive/t2 output


# official way to install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# run uv sync on the uv.lock to create venv in place with where uv.lock is
RUN cd modules/pipe_hdb && uv sync --no-dev --frozen

# locate the venv python (similar as source .venv/bin/activate)
ENV PATH="/app/modules/pipe_hdb/.venv/bin:$PATH"


### CMD is what to run DURING runtime, others above are what to run during build time.
# CMD ["python", "modules/pipe_hdb/run_pipeline.py"]
CMD ["bash", "modules/pipe_hdb/run_pipeline.sh"]


