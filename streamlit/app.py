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
        read_saved_sets,
        write_saved_sets,
        SAVED_SETS_WRITABLE,
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
        read_saved_sets,
        write_saved_sets,
        SAVED_SETS_WRITABLE,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
elif IS_DOCKER:
    from providers.docker import (
        CODE_PATH,
        fetch_html_bytes,
        fetch_parquet_bytes,
        read_saved_sets,
        write_saved_sets,
        SAVED_SETS_WRITABLE,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )
else:
    from providers.local import (
        CODE_PATH,
        fetch_html_bytes,
        fetch_parquet_bytes,
        read_saved_sets,
        write_saved_sets,
        SAVED_SETS_WRITABLE,
        get_last_run_time,
        poll_pipeline,
        trigger_pipeline,
    )


#######################
##### the dataset behind the live charts
#######################

import html
import io
import json
from pathlib import Path

import polars as pl

# repo root on sys.path so `modules.pipe_hdb...` resolves. the pipeline scripts use
# sys.path.append('') for this, which relies on cwd being the repo root - true under
# deploy_local.sh and in the container, but not if streamlit is launched from elsewhere.
# deriving it from __file__ instead makes the import work regardless, same as REPO_ROOT
# in providers/local.py
sys.path.append(str(Path(__file__).resolve().parent.parent))

# the actual chart building code snippets
from helper_charts import make_facet

from modules.pipe_hdb.helper_transform_for_plotly import (

    # Defaults to be input into the widget sliders
    DEFAULT_MAX_LEASE,
    DEFAULT_MIN_YEAR,

    # recomputes the plotter df on every widget slide
    build_median,

    # recomputes in every widget slide
    january_lines,
)

#######################
##### curated presets vs user-saved sets
#######################

# PRESETS are yours: tracked in git, deployed with the app, identical on every platform,
# and NOT deletable from the UI. edit the json by hand, no restart needed - it is read on
# each rerun. validate names after editing, a street that does not exist filters to zero
# rows silently rather than erroring.
#
# user-saved sets live in the provider's mutable store instead (localdata/ locally, a
# bucket later) - anyone can add or delete those.
PRESETS_PATH = Path(__file__).resolve().parent / 'presets_street_sets.json'


def read_presets():
    if not PRESETS_PATH.exists():
        return {}

    presets = json.loads(PRESETS_PATH.read_text())

    return presets


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

            # bounds come from the data rather than being hardcoded, so they stay right as
            # the pipeline adds months. read them before the widgets, because the defaults
            # dict below needs lease_floor
            year_min = int(df_source['tx_year'].min())
            year_max = int(df_source['tx_year'].max())
            lease_floor = int(df_source['remaining_lease_sold'].min())
            lease_ceiling = int(df_source['remaining_lease_sold'].max())

            ###############
            ### one definition of "default", used by both the widgets and the reset button
            ###############

            # every filter is keyed, and its starting value is seeded into session_state
            # rather than passed as default=/value=. two reasons:
            #   - the per-filter reset can only write to session_state, so this is the only
            #     way the buttons and the widgets share ONE definition of default
            #   - passing default= AND writing the same key from a callback makes streamlit
            #     warn about a widget both defaulted and set via session state
            FILTER_DEFAULTS = {
                # a range, but only the lower handle is live - see pin_year_upper below
                'year_pick': (DEFAULT_MIN_YEAR, year_max),
                'lease_pick': (lease_floor, DEFAULT_MAX_LEASE),
                # seeded rather than blank so a first-time viewer lands on a real question
                # instead of all 26 towns at once. TWO towns, not one, because the control
                # is a multiselect and a single chip does not advertise that
                'town_pick': ['TAMPINES', 'WOODLANDS'],
                'street_pick': [],
            }

            for filter_key, filter_default in FILTER_DEFAULTS.items():
                st.session_state.setdefault(filter_key, filter_default)

            def reset_one_filter(key):
                # runs BEFORE the rerun, the only point a widget's key may be assigned -
                # same constraint the saved-set loader works under
                st.session_state[key] = FILTER_DEFAULTS[key]

            def is_at_default(key):
                current = st.session_state[key]
                default = FILTER_DEFAULTS[key]

                # multiselects are lists, and a list compares order-sensitively. deselecting
                # everything then reselecting it is not a change the viewer would call a
                # change, so compare the selection rather than the ordering
                if isinstance(default, list):
                    return set(current) == set(default)

                # the lease range comes back as a tuple, but streamlit has been known to
                # hand back a list for the same widget - normalise before comparing
                if isinstance(default, tuple):
                    return tuple(current) == tuple(default)

                return current == default

            def filter_reset(key):
                # a per-filter restore, placed under its own widget, so one control can be
                # put back without throwing away the rest of the selection.
                # only rendered when that filter is actually off its default - a button
                # that would do nothing is noise, and its absence doubles as a marker of
                # which filters the viewer has touched.
                # type='tertiary' renders as plain text rather than a bordered button -
                # six bordered buttons would dominate a sidebar that is mostly inputs
                if is_at_default(key):
                    return

                st.button('↺ Default', key=f'reset_{key}', type='tertiary',
                          on_click=reset_one_filter, args=(key,))

            def filter_label(title, key, help_text=None, right=None, ratio=(3, 1)):
                # the widget's own label is collapsed and rendered here instead, so the
                # reset can sit on the title row. splitting the TITLE rather than the widget
                # keeps the control at full sidebar width - a multiselect squeezed into 3/4
                # of ~300px wraps its chips onto extra rows.
                # st.columns holds the right-hand cell even while the button is hidden, so
                # the title does not shift sideways when a filter goes off default.
                # `right` lets a filter put something other than the reset in the right-hand
                # cell - towns uses it for its sort control, and so has no reset at all
                title_col, reset_col = st.columns(ratio, vertical_alignment='center')

                # collapsing the label also loses streamlit's '?' help icon, so the help
                # text becomes a native browser tooltip on the title instead.
                # html.escape is not optional here - a double quote anywhere in help_text
                # closes the title="..." attribute early and the whole span renders as
                # literal text on the page
                if help_text:
                    title_col.markdown(
                        f'<span title="{html.escape(help_text)}"><b>{title}</b> ⓘ</span>',
                        unsafe_allow_html=True,
                    )
                else:
                    title_col.markdown(f'**{title}**')

                with reset_col:
                    if right is not None:
                        right()
                    else:
                        filter_reset(key)

            # ordered widest scope to narrowest: town narrows which streets are offered,
            # so it comes first; street is the point of the whole tab, so it comes last,
            # nearest the saved sets that populate it.
            # there is no flat_type control - the facet chart always shows all four, which
            # is the comparison this tab exists to make. build_median's default covers it.

            # each town shows an approximate sales-per-month rate rather than a raw total.
            # a total only means something once you know how many months it spans, so it
            # changes meaning every time the year filter moves; a rate stays comparable.
            #
            # the counts respect the year and lease filters, but are read from session_state
            # rather than from min_year/min_lease - this widget renders BEFORE those two, so
            # their variables do not exist yet on this run. session_state holds last run's
            # values, which is what the viewer currently sees.
            # street is deliberately NOT applied: streets are chosen from within towns, so
            # feeding them back into the town counts would be circular.
            year_lo, _ = st.session_state['year_pick']
            lease_lo, lease_hi = st.session_state['lease_pick']

            in_scope = (
                df_source
                .filter(pl.col('tx_year') >= year_lo)
                .filter(pl.col('remaining_lease_sold').is_between(lease_lo, lease_hi))
            )

            # the denominator is the months actually present across the WHOLE selection, not
            # per town - dividing each town by its own month count would inflate the rate for
            # quiet towns that simply have gaps. counting real months also handles the
            # part-finished current year without special-casing it
            months_in_scope = max(1, in_scope['tx_monthdate'].n_unique())

            town_counts = in_scope.group_by('town').len()
            town_rate = {
                town: n / months_in_scope
                for town, n in zip(town_counts['town'], town_counts['len'])
            }

            # sort order is the viewer's choice, not a filter - it changes nothing about the
            # data, so it stays out of FILTER_DEFAULTS and gets no reset button.
            # A-Z by default because it holds still - Top Sales reshuffles the list every
            # time the year or lease filter moves, which is disorienting on first landing.
            #
            # read from session_state rather than from the control's return value: the
            # control renders inside filter_label, which runs AFTER the ordering decision
            # below. session_state already holds the new value on the rerun after a click.
            SORT_AZ, SORT_TOP_SALES = 'Sort A-Z', 'Top Sales'
            st.session_state.setdefault('town_sort_mode', SORT_AZ)

            def town_display(town):
                # the rate is what "Top Sales" sorts by, so it is shown only in that mode -
                # under A-Z it is a number with no bearing on the order, and 26 of them
                # crowd the list for nothing
                if st.session_state['town_sort_mode'] != SORT_TOP_SALES:
                    return town

                rate = town_rate.get(town, 0)
                # sub-10 rates need a decimal or they all collapse to the same integer
                shown = f'{rate:,.0f}' if rate >= 10 else f'{rate:.1f}'
                return f'{town}  (~{shown}/mo)'

            # a town filtered to zero rows drops out of town_counts entirely - keep any
            # already-selected town in the list, or streamlit raises on a value that is no
            # longer an option
            town_candidates = (
                set(town_counts['town'].to_list()) | set(st.session_state.get('town_pick', []))
            )

            def town_sort_control():
                # segmented_control highlights the active option itself, so which mode is
                # live is visible without reading the label
                st.segmented_control(
                    'Sort towns by', [SORT_AZ, SORT_TOP_SALES],
                    key='town_sort_mode', label_visibility='collapsed',
                )

            if st.session_state['town_sort_mode'] == SORT_TOP_SALES:
                town_options = sorted(town_candidates, key=lambda t: -town_rate.get(t, 0))
            else:
                town_options = sorted(town_candidates)

            filter_label('Towns', 'town_pick',
                         'leave blank for all 26 towns. /mo is the transaction count divided '
                         'by the number of months actually present under the current year '
                         'and lease filters. narrows which street names are offered below',
                         # 1:3 rather than the usual 3:1 - "Sort A-Z" and "Top Sales" need
                         # ~220px side by side and wrap onto separate lines below that.
                         # "Towns" is short enough to give up the room
                         right=town_sort_control, ratio=(1, 3))
            towns = st.multiselect(
                'Towns', town_options,
                key='town_pick', label_visibility='collapsed',
                # format_func changes only what is DISPLAYED - the value handed back is
                # still the plain town name, so no mapping back is needed
                format_func=town_display,
            )

            ###############
            ### street name, and the saved sets that populate it
            ###############

            # counted off in_scope (year + lease already applied) and narrowed by town, so
            # the number beside each street is the transactions it actually contributes.
            # a street with nothing left under the current filters simply does not appear
            # in the group_by, which is what drops it from the list
            street_scope = in_scope
            if towns:
                street_scope = street_scope.filter(pl.col('town').is_in(towns))

            street_counts = street_scope.group_by('street_name').len()
            street_size = dict(zip(street_counts['street_name'], street_counts['len']))

            # a loaded set can hold streets from anywhere, and a selected street can fall to
            # zero as other filters move - either way it has to stay in the options, or
            # streamlit raises on a value that is no longer offered. those show as (0),
            # which is the honest signal that the selection is currently yielding nothing
            street_options = sorted(
                set(street_size) | set(st.session_state.get('street_pick', []))
            )

            def street_display(street):
                return f'{street}  ({street_size.get(street, 0):,})'

            filter_label('Street Names', 'street_pick',
                         'leave blank for every street in the selected towns. the number is '
                         'total transactions under the current year and lease filters - '
                         'streets with none are not listed')
            streets = st.multiselect(
                'Street Names', street_options, key='street_pick',
                label_visibility='collapsed', format_func=street_display,
            )

            presets = read_presets()
            saved_sets = read_saved_sets()

            # one picker for both, but prefixed so it is obvious which are curated. a
            # preset and a user set may share a name without colliding, because the
            # displayed label - not the bare name - is what the selectbox returns
            PRESET_MARK = '★ '
            options = (
                [PRESET_MARK + name for name in sorted(presets)]
                + sorted(saved_sets)
            )

            # saving sits ABOVE loading - it acts on the street selection immediately above
            # it, so the two read as one motion. loading is a separate errand and can wait.
            # it is also its own expander rather than a divider inside the load panel: they
            # are opposite directions of travel (one writes a set, one reads one).
            # only appears once there are streets to save. with nothing picked the panel had
            # nothing to offer, and a disabled Save button reads as broken rather than as
            # "not yet" - its absence says the same thing without the explanation
            if SAVED_SETS_WRITABLE and streets:
                with st.expander('Save current selected streets', expanded=False):
                    new_name = st.text_input('Name this set',
                                             placeholder='e.g. Tampines targets')
                    # streets is non-empty to be here, so the name is the only thing left
                    if st.button('Save', disabled=not new_name):
                        saved_sets[new_name] = list(streets)
                        write_saved_sets(saved_sets)
                        st.success(f'Saved "{new_name}" with {len(streets)} street(s).')
                        st.rerun()

                    # no caption either way. the Save button is disabled until a name and at
                    # least one street exist, and the chips above already show what would be
                    # saved - the panel explains itself without prose
                    #
                    # no delete control by design. anyone can SAVE a set, but removing one
                    # is done in the backing store instead - locally that is
                    # localdata/saved_sets.json, a bucket object once this is on aws.
                    # an unauthenticated delete button would let any viewer destroy another
                    # viewer's work, and there are no user ids here to scope it by

            # the header names the action, not the contents - collapsed, "Saved streets"
            # reads as a label with no hint that clicking it does anything
            with st.expander(f'Load saved streets ({len(options)})', expanded=False):
                if not options:
                    st.write('No sets saved yet.')
                else:
                    def streets_for(label):
                        if label.startswith(PRESET_MARK):
                            return presets[label[len(PRESET_MARK):]]
                        return saved_sets[label]

                    # picking IS loading - no confirm button. on_change fires before the
                    # rerun, the only point a widget's session_state key may be assigned;
                    # doing it inline after the multiselect exists raises StreamlitAPIException.
                    # town_pick is cleared too, so the loaded streets are all valid options
                    def apply_chosen_view():
                        label = st.session_state['view_pick']
                        if label == PICK_PROMPT:
                            return

                        st.session_state['street_pick'] = list(streets_for(label))
                        st.session_state['town_pick'] = []

                    # a placeholder sits at index 0 so that choosing the FIRST real set
                    # fires on_change. without it that set is already selected on open, and
                    # clicking it changes nothing, so nothing loads
                    PICK_PROMPT = '— pick a set —'

                    # label collapsed - the expander header already says these are sets to
                    # load, so a second label above the dropdown just repeats it
                    chosen = st.selectbox(
                        'Load a set', [PICK_PROMPT] + options, key='view_pick',
                        on_change=apply_chosen_view, label_visibility='collapsed',
                    )

                    if chosen != PICK_PROMPT:
                        chosen_streets = streets_for(chosen)

                        # a street that is not in the data filters to zero rows without
                        # erroring, so surface the mismatch here rather than letting it read
                        # as "your filters are too narrow" further down
                        known = set(df_source['street_name'].unique().to_list())
                        unknown = sorted(set(chosen_streets) - known)

                        st.caption(f'Loaded {len(chosen_streets)} street(s).')
                        if unknown:
                            st.warning(f'Not found in the data: {", ".join(unknown)}')



            def pin_year_upper():
                # a range slider rather than a single handle, purely so the red fill runs
                # from the chosen year to the RIGHT edge - the years being KEPT. a single
                # slider fills from the left, painting the excluded years instead, which
                # reads as the opposite of what the filter does.
                # the upper handle is pinned here rather than left free: there is no
                # max-year filter, so a movable right handle would be a control that
                # silently does nothing. dragging it snaps straight back.
                lower, _ = st.session_state['year_pick']
                st.session_state['year_pick'] = (lower, year_max)

            filter_label('From year', 'year_pick',
                         'transactions before this year are excluded. the right handle is '
                         'fixed at the latest year in the data')
            min_year, _ = st.slider(
                'From year', year_min, year_max, key='year_pick',
                on_change=pin_year_upper, label_visibility='collapsed',
            )

            # a range rather than two sliders: the interesting question is which lease BAND
            # a flat sits in, and the static report could only ever set the ceiling (75)
            # plain quotes are fine now - filter_label html-escapes help_text
            filter_label('Remaining lease (years)', 'lease_pick',
                         'the static report fixes this at "up to 75" - drag the left handle '
                         'to isolate a band instead of everything below a ceiling')
            min_lease, max_lease = st.slider(
                'Remaining lease (years)',
                lease_floor, lease_ceiling, key='lease_pick',
                label_visibility='collapsed',
            )

            st.divider()
            st.caption('Months with less than 1 sale are not shown.')

        # flat_types is not passed - build_median defaults to all four charted types, which
        # is what the facet always shows. no "pick at least one flat type" guard is needed
        # any more, because there is no way to pick none.
        # min_sales is not passed either - build_median defaults to 1, which excludes
        # nothing. the grey bars now carry the sample size, so thin months stay visible
        # rather than being dropped
        plotter = build_median(
            df_source,
            max_lease=max_lease,
            min_lease=min_lease,
            min_year=min_year,
            towns=towns or None,
            streets=streets or None,
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
            # where the chart is actually rendered!!!!!!
            # config mirrors what 2_report_firstbq.py passes to to_html - st.plotly_chart
            # takes the same dict. displayModeBar hides the camera/zoom/pan/reset toolbar,
            # scrollZoom stops the wheel rescaling the axes while the page is being scrolled
            st.plotly_chart(
                make_facet(plotter, start_of_year_lines),
                config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True},
            )


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
