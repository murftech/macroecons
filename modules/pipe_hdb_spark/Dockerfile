# Rule of thumb: always build dockers from repo root
#   docker build -f modules/pipe_hdb_spark/Dockerfile -t hdb-pipeline-spark:local .
# or via Cloud Build: ./modules/pipe_hdb_spark/deploy_gcp.sh submit

# ── stage 1: a source to copy the JVM out of ─────────────────────────────────
# YES, this must come before the python stage. Docker reads the file top to
# bottom, so `COPY --from=jre` below only resolves if `AS jre` is declared above
# it. Nothing else from this image survives - it exists purely to be copied from.
#
# Adoptium's own build, so the container gets the SAME JVM as
# `brew install --cask temurin@17` on the laptop. Debian's
# openjdk-17-jre-headless is a different build of OpenJDK; using it would mean
# "works locally" and "works in the container" were no longer the same claim.
#
# Pin the patch once this is working, so a rebuild next year gets this JVM and
# not whatever 17-jre points at then:  eclipse-temurin:17.0.20.1_1-jre
FROM eclipse-temurin:17-jre AS jre


# ── stage 2: the actual image ────────────────────────────────────────────────
FROM python:3.12-slim
# have to match the pyproject.toml's definition

# `pip install pyspark` ships Spark's 279 jars but NOT a JVM, so without this the
# first getOrCreate() dies with [JAVA_GATEWAY_EXITED]. This copies just the JDK
# directory out of stage 1 - ~250MB, none of that image's OS layers.
#
# Setting JAVA_HOME is safe HERE, unlike on the mac: the container has exactly
# one JVM at a known path, so there is nothing stale for it to point at.
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# run_pipeline.sh finds the repo root by walking up to .git, which does not
# exist in the image. This tells it where the root is instead.
ENV REPO_ROOT=/app

ENV RUNNING_IN_CONTAINER=true

WORKDIR /app

# copy source code
COPY modules/pipe_hdb_spark modules/pipe_hdb_spark

# create folders the scripts write to
RUN mkdir -p hive/t2 output


# official way to install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# run uv sync on the uv.lock to create venv in place with where uv.lock is
RUN cd modules/pipe_hdb_spark && uv sync --no-dev --frozen

# locate the venv python (similar as source .venv/bin/activate)
ENV PATH="/app/modules/pipe_hdb_spark/.venv/bin:$PATH"


### CMD is what to run DURING runtime, others above are what to run during build time.
# CMD ["python", "modules/pipe_hdb_spark/run_pipeline.py"]
CMD ["bash", "modules/pipe_hdb_spark/run_pipeline.sh"]


