import datetime
from pathlib import Path

import docker
import streamlit as st


########################
#### for st_iframe (Charts from HTML)
#########################

# same OneDrive folder providers/local.py reads from - docker-compose.yml already mounts
# it into this container at the identical absolute path, read-only
LOCAL_HTML_PATH = Path('/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/macroecons/pipe_hdb')


def fetch_html_bytes(filename):
    print(f'check LOCAL_HTML_PATH: {LOCAL_HTML_PATH}')
    filepath = LOCAL_HTML_PATH / filename
    if not filepath.exists():
        print('file for chart is missing, check about it')
        return None
    return filepath.read_text()


#######################
##### for show code snippet
#######################

# copied into /app at Dockerfile build time, same as providers/gcp.py
CODE_PATH = Path('modules/pipe_hdb/2_report_firstbq.py')


########################
#### for st.button('Refresh data')
#########################

# talks to the HOST's Docker daemon via the socket mounted in docker-compose.yml -
# this container can't reach the pipe_hdb venv or run its own subprocess, so instead
# it asks Docker itself to start hdb-pipeline:local as a sibling container

# sub study
DOCKER_CLIENT = docker.from_env()


def trigger_pipeline():
    # sub study
    # containers.run() with detach=False blocks until the pipe_hdb container finishes,
    # so - like the local provider - there's nothing left to poll afterward, always
    # returns None
    try:
        DOCKER_CLIENT.containers.run(
            'hdb-pipeline:local',
            volumes={
                # sibling containers are started against the HOST's Docker daemon, so
                # this must be the real host path, not a path inside this container
                str(LOCAL_HTML_PATH): {'bind': '/app/output', 'mode': 'rw'},
            },
            remove=True,
            detach=False,
        )
        st.success('Pipeline refreshed successfully.')
    except docker.errors.ContainerError as e:
        st.error(f'Pipeline failed - hdb-pipeline container exited with an error: {e}')
    return None


########################
#### for status_placeholder = st.empty() while waiting for the pipeline run
#########################

def get_last_run_time():
    # fixed UTC+8 offset (Singapore has no DST) - explicit rather than relying on the host's
    # local timezone, which is UTC by default inside the container
    SGT = datetime.timezone(datetime.timedelta(hours=8), name='SGT')

    # use the report file's mtime as a proxy for "when did the pipeline last write output"
    filepath = LOCAL_HTML_PATH / '1-firstbq-overlay.html'
    if not filepath.exists():
        return None
    return datetime.datetime.fromtimestamp(filepath.stat().st_mtime, tz=SGT)


def poll_pipeline(execution_name, status_placeholder):
    # trigger_pipeline() always returns None (containers.run() already blocked until
    # the sibling container finished), so this never actually runs - kept only so
    # app.py can call it unconditionally without branching on which provider is active
    pass
