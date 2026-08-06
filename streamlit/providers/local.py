import datetime
import json
import subprocess
import sys
from pathlib import Path

import streamlit as st


# repo root - used to find run_pipeline.py, the pipeline source, and the parquet.
# this file lives at streamlit/providers/local.py, so up three levels to reach the repo root.
# derived from __file__, not the working directory, so every path below resolves the same
# whether streamlit is launched from the repo root or from streamlit/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


########################
#### for st_iframe (Charts from HTML)
#########################

import platform

if platform.system() == 'Darwin':
    LOCAL_HTML_PATH = Path('/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/macroecons/pipe_hdb')
elif platform.system() == 'Windows':
    LOCAL_HTML_PATH = Path('C:/Users/Talesinc/OneDrive/DBMaster/annotations/reports/macroecons/pipe_hdb')

def fetch_html_bytes(filename):
    print(f'check LOCAL_HTML_PATH: {LOCAL_HTML_PATH}')
    filepath = LOCAL_HTML_PATH / filename
    if not filepath.exists():
        print('file for chart is missing, check about it')
        return None
    return filepath.read_text()


########################
#### for the interactive charts (the data itself, not pre-rendered HTML)
#########################

# despite sitting under hive/, this is a single parquet file and not a partitioned
# directory - 0_import_datagov.py writes it with a plain write_parquet()
LOCAL_PARQUET_PATH = REPO_ROOT / 'hive/t2/datagovhdb'


def fetch_parquet_bytes():
    # hands back raw bytes rather than a DataFrame so polars stays out of the providers
    # entirely - the aws/gcp versions will return bucket bytes in this same shape, and the
    # one place that parses them can then be provider-agnostic
    print(f'check LOCAL_PARQUET_PATH: {LOCAL_PARQUET_PATH}')
    if not LOCAL_PARQUET_PATH.exists():
        print('parquet for charts is missing, check about it')
        return None
    return LOCAL_PARQUET_PATH.read_bytes()


########################
#### for the saved street-name sets
#########################

# lives under localdata/ because the allowlist .gitignore tracks everything in /streamlit/**
# - a file of user-generated sets does not belong in commits. localdata/ is already ignored
SAVED_SETS_PATH = REPO_ROOT / 'localdata' / 'saved_sets.json'


def read_saved_sets():
    # {set name: [street_name, ...]}. missing file just means nobody has saved one yet,
    # which is an empty dict rather than an error
    if not SAVED_SETS_PATH.exists():
        return {}

    saved_sets = json.loads(SAVED_SETS_PATH.read_text())

    return saved_sets


def write_saved_sets(saved_sets):
    # whole-map write, so a save is a read-modify-write. fine for one local process; on
    # aws this becomes a race between simultaneous savers - see the branch notes
    SAVED_SETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SAVED_SETS_PATH.write_text(json.dumps(saved_sets, indent=2, sort_keys=True))


# the app hides the save/delete controls where this is False, so a viewer is never offered
# a button that cannot work
SAVED_SETS_WRITABLE = True


#######################
##### for show code snippet
#######################

# take the file in dev repo
CODE_PATH = REPO_ROOT / 'modules/pipe_hdb/2_report_firstbq.py'


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
