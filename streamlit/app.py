import streamlit as st

import os
import sys
print(sys.executable)



##########################
####### SET PAGE CANVAS
##########################


st.set_page_config(layout='wide')

st.markdown(
    '<style>'
    '[data-testid="stAppDeployButton"] { display: none; }'
    '[data-testid="stCode"] code { font-size: 80%; line-height: 80%; }'
    '</style>',
    unsafe_allow_html=True,
)



##########################
####### SET PAGE INTRODUCTION
##########################

st.markdown("""
This is a prototype to showcase engineer's knowledge in:

1) pulling <b style="color:#B8860B">live data.</b> Pulling data from <b style="color:#B8860B">API</b>
2) cleaning data with properly chosen ETL tools: <b style="color:#B8860B">polars</b> is chosen for this project
3) designing charts with <b style="color:#B8860B">ease of information digestion</b> and <b style="color:#B8860B">good physical UX</b> in mind
4) working ability of packaging the runtime project files into <b style="color:#B8860B">docker</b>, pushed onto a <b style="color:#B8860B">cloud platform</b> (like GCP), <b style="color:#B8860B">scheduled</b>, and output is viewable via a <b style="color:#B8860B">secure links</b> (using S3 bucket)
5) job is <b style="color:#B8860B">re-executable</b> via a button in <b style="color:#B8860B">clean</b> webpage url (Via <b style="color:#B8860B">Streamlit</b>), allowing viewing of the output in the same url
""", unsafe_allow_html=True)


st.caption('Credits to data.gov.sg > Data source: (https://data.gov.sg/collections/189/view)')


###################
### Branch Variable - the only cloud-specific branch in this whole file
###################
IS_GCP = 'K_SERVICE' in os.environ  # set automatically by Cloud Run, never present locally
# no AWS service auto-injects a distinguishing env var the way Cloud Run does, so - same
# convention run_pipeline.py already uses - presence of S3_BUCKET is the signal we're on AWS
IS_AWS = 'S3_BUCKET' in os.environ
IS_DOCKER = 'RUNNING_IN_CONTAINER' in os.environ

if IS_GCP:
    from providers.gcp import (
        CODE_PATH,
        fetch_html_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
elif IS_AWS:
    # checked before IS_DOCKER - RUNNING_IN_CONTAINER is true in both the AWS and generic
    # docker-compose images, so AWS must take precedence or this would never be reached
    from providers.aws import (
        CODE_PATH,
        fetch_html_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
elif IS_DOCKER:
    from providers.docker import (
        CODE_PATH,
        fetch_html_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
else:
    from providers.local import (
        CODE_PATH,
        fetch_html_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )


#######################
##### refresh button to run Cloud Run, or Subprocess Python in local folder
#######################

execution_name = None
if st.button('Refresh data 👍🏻'):
    with st.spinner('Triggering refresh...'):
        try:
            execution_name = trigger_pipeline()
        except Exception as e:
            st.error(f'Failed to refresh: {e}')

last_run = get_last_run_time()
st.caption(f'Pipeline last ran: {last_run.strftime("%Y-%m-%d | %H:%M:%S %Z")}' if last_run else 'Data last refreshed: unknown')

# slot for the polling status/result below (declared here so it renders up here, even
# though the actual poll loop runs at the bottom of the script, after the charts)
status_placeholder = st.empty()


#######################
##### show code snippet
#######################

def show_code_snippet(start_line, end_line, key):
    if st.button('Click here to view relevant code snippet that built this chart ↑ ', key=key):
        if not CODE_PATH.exists():
            st.warning(f'Source file not found at {CODE_PATH}')
            return
        all_lines = CODE_PATH.read_text().splitlines()
        snippet_lines = all_lines[start_line - 1:end_line]  # convert 1-indexed line numbers to a 0-indexed slice
        code_content = '\n'.join(snippet_lines)
        st.code(code_content, language='python')


###########################


def st_iframe(filename):
    content = fetch_html_bytes(filename)
    if content is None:
        st.markdown('[content is missing]')
    else:
        st.iframe(content)


# line ranges point into CODE_PATH (modules/pipe_hdb/2_report_firstbq.py) and have to be
# re-checked whenever that file is edited - they are positional, nothing validates them
st_iframe('1-firstbq-overlay.html')
show_code_snippet(70, 113, key='code_snippet_overlay')

st_iframe('1-firstbq-facet.html')
show_code_snippet(118, 150, key='code_snippet_facet')

st_iframe('1-firstbq-fixedaxis.html')


if execution_name:
    poll_pipeline(execution_name, status_placeholder)
