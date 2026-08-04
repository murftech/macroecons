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
        fetch_parquet_bytes,
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
        fetch_parquet_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
elif IS_DOCKER:
    from providers.docker import (
        CODE_PATH,
        fetch_html_bytes,
        fetch_parquet_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
else:
    from providers.local import (
        CODE_PATH,
        fetch_html_bytes,
        fetch_parquet_bytes,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )


#######################
##### the dataset behind the live charts
#######################

import io
from pathlib import Path

import polars as pl

# repo root on sys.path so `modules.pipe_hdb...` resolves. the pipeline scripts use
# sys.path.append('') for this, which relies on cwd being the repo root - true under
# deploy_local.sh and in the container, but not if streamlit is launched from elsewhere.
# deriving it from __file__ instead makes the import work regardless, same as REPO_ROOT
# in providers/local.py
sys.path.append(str(Path(__file__).resolve().parent.parent))

# the actual chart building code snippets
from helper_charts import make_facet, make_overlay

from modules.pipe_hdb.helper_transform_for_plotly import (

    # Defaults to be input into the widget sliders
    DEFAULT_FLAT_TYPES,
    DEFAULT_MAX_LEASE,
    DEFAULT_MIN_YEAR,

    # defaults for the sort order
    FLAT_TYPE_ORDER,

    # recomputes the plotter df on every widget slide
    build_median,

    # recomputes in every widget slide
    january_lines,
)

### cache the loaded resuable data
@st.cache_data(ttl=3600)
def load_dataframe():
    content = fetch_parquet_bytes()
    if content is None:
        return None

    df_source = pl.read_parquet(io.BytesIO(content))

    return df_source


#######################
##### refresh button to run Cloud Run, or Subprocess Python in local folder
#######################

execution_name = None
if st.button('Refresh data 👍🏻'):
    with st.spinner('Triggering refresh...'):
        try:
            execution_name = trigger_pipeline()
            # local/docker block until the pipeline finishes, so by here the parquet on
            # disk is already the new one and the cached copy is stale. aws/gcp return
            # immediately instead, and clear from inside poll_pipeline once it succeeds.
            if execution_name is None:
                load_dataframe.clear()
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


#######################
##### two views of the same pipeline output
#######################

# Explore  - this app reads the parquet and builds the figures now, per the controls
# Published report - the html the pipeline itself rendered and put in the bucket. kept
# because it is the visible proof of the pipeline -> docker -> cloud -> secure link story
# the introduction above advertises; the live tab alone would not demonstrate that.
tab_explore, tab_published = st.tabs(['Explore the data', 'Published report'])


with tab_explore:
    df_source = load_dataframe()

    if df_source is None:
        st.warning('No dataset available yet - run the pipeline with the button above.')
    else:
        with st.sidebar:
            st.header('Filters')
            st.caption('Applies to the **Explore the data** tab. The published report is '
                       'rendered by the pipeline with its own fixed filters.')

            flat_types = st.multiselect(
                'Flat type', FLAT_TYPE_ORDER, default=list(DEFAULT_FLAT_TYPES),
            )

            year_min = int(df_source['tx_year'].min())
            year_max = int(df_source['tx_year'].max())
            min_year = st.slider(
                'From year', year_min, year_max, DEFAULT_MIN_YEAR,
                help='transactions before this year are excluded',
            )

            # a range rather than two sliders: the interesting question is which lease BAND
            # a flat sits in, and the static report could only ever set the ceiling (75)
            lease_floor = int(df_source['remaining_lease_sold'].min())
            lease_ceiling = int(df_source['remaining_lease_sold'].max())
            min_lease, max_lease = st.slider(
                'Remaining lease (years)',
                lease_floor, lease_ceiling, (lease_floor, DEFAULT_MAX_LEASE),
                help='the static report fixes this at "up to 75" - drag the left handle to '
                     'isolate a band instead of everything below a ceiling',
            )

            towns = st.multiselect(
                'Town (blank = all)', sorted(df_source['town'].unique().to_list()),
            )

            # the honesty control: narrow the filters enough and a month can hold one or
            # two sales, whose median looks like signal but is not
            min_sales = st.number_input(
                'Minimum sales per month', min_value=1, max_value=100, value=1,
                help='drops months with too few transactions to have a meaningful median',
            )

        if not flat_types:
            st.info('Pick at least one flat type in the sidebar.')
        else:
            plotter = build_median(
                df_source,
                max_lease=max_lease,
                min_lease=min_lease,
                min_year=min_year,
                flat_types=flat_types,
                towns=towns or None,
                min_sales=min_sales,
            )

            if plotter.height == 0:
                st.warning('Nothing matches those filters - try widening them.')
            else:
                start_of_year_lines = january_lines(plotter)

                st.caption(
                    f'{plotter.height:,} month/flat-type points  |  '
                    f'{plotter["nb_sales"].sum():,} transactions  |  '
                    f'smallest month has {plotter["nb_sales"].min():,} sale(s)'
                )
            # where the charts are actually rendered!!!!!!
                st.plotly_chart(make_overlay(plotter, start_of_year_lines))
                st.plotly_chart(make_facet(plotter, start_of_year_lines))


with tab_published:
    # line ranges point into CODE_PATH (modules/pipe_hdb/2_report_firstbq.py) and have to
    # be re-checked whenever that file is edited - positional, nothing validates them
    st_iframe('1-firstbq-overlay.html')
    show_code_snippet(70, 113, key='code_snippet_overlay')

    st_iframe('1-firstbq-facet.html')
    show_code_snippet(118, 150, key='code_snippet_facet')

    st_iframe('1-firstbq-fixedaxis.html')


if execution_name:
    poll_pipeline(execution_name, status_placeholder)
