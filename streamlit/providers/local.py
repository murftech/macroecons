import datetime
import subprocess
import sys
from pathlib import Path

import streamlit as st


########################
#### for st_iframe (Charts from HTML)
#########################

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

# repo root - used to find run_pipeline.py, and the pipeline source itself.
# this file lives at streamlit/providers/local.py, so up three levels to reach the repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# take the file in dev repo
CODE_PATH = REPO_ROOT / 'modules/pipe_hdb/1_firstbq.py'


########################
#### for st.button('Refresh data')
#########################

# the pipeline runs in its own isolated venv (built via modules/pipe_hdb/setup.sh),

def trigger_pipeline():
    # the subprocess already runs to completion by the time this returns, so there's
    # nothing for the caller to poll - always returns None
    PIPELINE_PYTHON = REPO_ROOT / 'modules/pipe_hdb/venv/bin/python'
    result = subprocess.run([str(PIPELINE_PYTHON), str(REPO_ROOT / 'modules/pipe_hdb/run_pipeline.py')], cwd=REPO_ROOT)

    if result.returncode == 0:
        st.success('Pipeline refreshed successfully.')
    else:
        st.error('Pipeline failed. Check the terminal running Streamlit for details.')
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
    # trigger_pipeline() always returns None locally (the subprocess already completed),
    # so this never actually runs - kept only so app_draft1.py can call it unconditionally
    # without branching on which provider is active
    pass
