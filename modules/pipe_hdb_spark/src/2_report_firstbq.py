"""Spark port of 2_report_firstbq.py.

Only the DATA half changes. Everything from "charts" onward is plotly and is
carried over verbatim - plotly cannot consume a Spark DataFrame, and it does not
need to: build_median collapses ~239k rows to a few hundred, so the result is
collected to pandas and the rest of the script is unchanged.

THAT BOUNDARY IS THE POINT. Spark does the distributed part (filter, group,
median over the full dataset); pandas does the small part (a chart's worth of
rows). Collecting a large DataFrame with .toPandas() pulls every row into the
driver's memory and is how people OOM a cluster - it is safe here precisely
because the aggregation already shrank the data.

Reuses three helpers from helper_transform_for_plotly.py unchanged, because they
are pure Python with no polars in them: flat_type_order, ceil_tick, floor_tick.
Only build_median (polars) and january_lines (polars) needed reimplementing.
"""

import copy
import os
import platform
from pathlib import Path

import pandas as pd


# pure-Python helpers - no polars inside them, so they port for free
from helper_transform_for_plotly import (
    DEFAULT_FLAT_TYPES,
    DEFAULT_MAX_LEASE,
    DEFAULT_MIN_LEASE,
    DEFAULT_MIN_YEAR,
    ceil_tick,
    flat_type_order,
    floor_tick,
)


# ── start spark ───────────────────────────────────────────────────────────────

# ── start spark ───────────────────────────────────────────────────────────────

from helper_getspark import get_spark
# from modules.pipe_hdb_spark.src.helper_getspark import get_spark
spark = get_spark()

from helper_sparkutils import F, T, col, lit, when, to_date, year, bround, count, median
# from pyspark.sql.functions import only use this line further if i need something new. Also i would not want to edit the git repoed stuff.


# from modules.pipe_hdb_spark.src.helper_getspark import get_spark
# spark = get_spark()

# from modules.pipe_hdb_spark.src.helper_sparkutils import F, T, col, lit, when, to_date, year, bround, count, median



# ------------------
_HERE = Path(__file__).resolve().parent if '__file__' in globals() else Path.cwd()


def _repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / '.git').is_dir():
            return candidate
    return start


DATA_ROOT = os.environ.get('HDB_DATA_ROOT', str(_repo_root(_HERE) / 'hive/t2'))
IN_PARQUET = f'{DATA_ROOT}/datagovhdb_spark'
print(f'data root: {DATA_ROOT}')


# ── read ──────────────────────────────────────────────────────────────────────
# The directory, not a file inside it - Spark wrote part-*.parquet plus _SUCCESS
# and reads the whole directory as one table.
datagovhdb = spark.read.parquet(IN_PARQUET)


# ── aggregate ─────────────────────────────────────────────────────────────────

def build_median_spark(
    df,
    *,
    max_lease=DEFAULT_MAX_LEASE,
    min_lease=DEFAULT_MIN_LEASE,
    min_year=DEFAULT_MIN_YEAR,
    flat_types=DEFAULT_FLAT_TYPES,
    towns=None,
    streets=None,
    min_sales=1,
):
    """Spark twin of helper_transform_for_plotly.build_median.

    median() not percentile_approx(): Spark offers both, and the approximate one
    is the usual reflex for large data. Here the whole point of the shared
    definition is that every consumer computes the SAME number - an approximate
    median would make the Spark report and the streamlit app disagree, which is
    a correctness bug, not a performance trade.
    """
    scoped = (
        df
        .filter(col('remaining_lease_sold') <= max_lease)
        .filter(col('remaining_lease_sold') >= min_lease)
        .filter(col('tx_year') >= min_year)
        .filter(col('flat_type').isin(list(flat_types)))
    )

    if towns:
        scoped = scoped.filter(col('town').isin(list(towns)))
    if streets:
        scoped = scoped.filter(col('street_name').isin(list(streets)))

    aggregated = (
        scoped
        .groupBy('tx_monthdate', 'flat_type')
        .agg(
            median(col('resale_price')).alias('median_price'),
            count('*').alias('nb_sales'),
        )
        .filter(col('nb_sales') >= min_sales)
        .orderBy('flat_type', 'tx_monthdate')
    )

    # round to the nearest 1000 for the axis, keep a k-denominated copy for hover.
    #
    # bround(), NOT round(). Spark's round() is HALF_UP; polars' .round() is
    # HALF_EVEN (banker's rounding). With an even number of sales the median
    # lands on a .5 and the two disagree by exactly 1000 - measured at 21 of 368
    # rows here, silently, with no error from either engine. bround() is Spark's
    # HALF_EVEN and takes the mismatch to zero.
    return aggregated.withColumn(
        'median_price_k', bround(col('median_price') / 1000, 0)
    ).withColumn(
        'median_price', bround(col('median_price') / 1000, 0) * 1000
    )


bq_median = build_median_spark(datagovhdb)
print(f'aggregated to {bq_median.count():,} rows')


# ── collect: the Spark/pandas boundary ────────────────────────────────────────
# Small by construction (months x flat types). Everything below this line is the
# original script, unchanged - pandas indexing happens to match polars for the
# handful of operations the presentation code does (.unique(), .max(), .min()).
plotter = bq_median.toPandas()

# .toPandas() maps Spark's DateType to python datetime.date objects, giving an
# `object` dtype column - NOT datetime64. plotly copes, but `.dt` does not exist
# on object columns, so january_lines() below would fail. Convert once, here at
# the boundary, rather than defending against it in every consumer.
plotter['tx_monthdate'] = pd.to_datetime(plotter['tx_monthdate'])

spark.stop()          # nothing below needs Spark; free the JVM


def january_lines(plotter):
    """pandas twin of the polars version - x positions for the year-boundary
    vlines, sorted explicitly rather than trusting unique()'s ordering."""
    jan = plotter[plotter['tx_monthdate'].dt.month == 1]['tx_monthdate']
    return sorted(jan.unique())


############
#### charts
############


import plotly.express as px



####################################
####### additional information aggregates
####################################

# build_median() already returns the rounded median_price and the median_price_k hover column.
# NOTE: the polars original assigned `plotter = bq_median` here. In the Spark port
# plotter was already set above, to bq_median.toPandas() - reassigning it to the
# Spark DataFrame would send a Column into plotly and fail with the unhelpful
# "'Column' object is not callable".

# legend/facet order is declared per-figure via category_orders below instead of by
# re-sorting the dataframe - which means plotter is no longer mutated between the two charts
# FLAT_TYPE_ORDER is largest-first, so flat_type_order() returns descending as-is and
# descending=True is what yields ascending. swapping these two keeps the names honest and
# keeps this report's chart order unchanged by that constant's direction
FLAT_TYPE_DESC = flat_type_order(plotter['flat_type'].unique())
FLAT_TYPE_ASC = flat_type_order(plotter['flat_type'].unique(), descending=True)

start_of_year_lines = january_lines(plotter)

################################
######## overall chart #####
################################



##########
#### tedious blackbox scale maker - ceil_tick/floor_tick now imported from helper_transform_for_plotly.py
##########


fig_overlay = px.line(
    plotter,

    x='tx_monthdate',
    y='median_price',

    markers=True,

    color='flat_type',
    custom_data=['median_price_k'],

    title='Median Resale Price by Flat Type, Year-Monthly',

    labels={'tx_monthdate': 'Monthly',
            'median_price': 'Median Price (SGD)',
            'flat_type': 'Flat Type'},

    category_orders={'flat_type': FLAT_TYPE_DESC},

    template='plotly_dark',
)


for jan in start_of_year_lines:
    fig_overlay.add_vline(x=jan, line=dict(color='darkgreen', width=2))

fig_overlay.update_layout(
    hovermode='x unified',
    hoverlabel=dict(font=dict(size=12)),
    xaxis=dict(hoverformat='%Y %b'),
)
fig_overlay.update_traces(hovertemplate='%{customdata[0]:.0f}k')

# fig_overlay.update_traces(hovertemplate='%{y:.3s}')
fig_overlay.update_xaxes(tickfont=dict(size=25), tickangle=1)


########## 
#### tedious blackbox scale maker
########## 
fig_overlay.update_yaxes(dtick=100000, range=[0, ceil_tick(plotter['median_price'].max())], side='right')
########## 
#### tedious blackbox scale maker
########## 




fig_facet = px.line(
    plotter,

    x='tx_monthdate',
    y='median_price',

    facet_col='flat_type',
    facet_col_wrap=2,
    markers=True,
    custom_data=['median_price_k'],
    title='Median Resale Price individually by Flat Type, Year-Monthly',

    labels={'tx_monthdate': 'Monthly', 'median_price': 'Median Price (SGD)'},
    category_orders={'flat_type': FLAT_TYPE_ASC},
    template='plotly_dark',

    facet_col_spacing=0.1,
    facet_row_spacing=0.15,
    
)


fig_facet.update_xaxes(matches='x', showticklabels=True, tickfont=dict(size=18), tickangle=1,
                       dtick='M12', tickformat='%Y')

fig_facet.update_yaxes(matches=None, showticklabels=True, dtick=50000, side='right')

for jan in start_of_year_lines:
    fig_facet.add_vline(x=jan, line=dict(color='darkgreen', width=2))

fig_facet.update_layout(hovermode='x unified', hoverlabel=dict(font=dict(size=10)))
fig_facet.update_xaxes(hoverformat='%Y %b')
fig_facet.update_traces(hovertemplate='%{customdata[0]:.0f}k<extra></extra>')


##########
#### tedious blackbox scale maker
##########


fig_facet.update_yaxes(title='')

for trace in fig_facet.data:
    xaxis_key = trace.xaxis.replace('x', 'xaxis', 1)
    yaxis_key = trace.yaxis.replace('y', 'yaxis', 1)
    is_right_col = fig_facet.layout[xaxis_key].domain[0] > 0.5
    if is_right_col:
        fig_facet.layout[yaxis_key].update(title='Median Price (SGD)')
    
for trace in fig_facet.data:
    yaxis_key = trace.yaxis.replace('y', 'yaxis', 1)
    fig_facet.layout[yaxis_key].update(range=[floor_tick(min(trace.y)), ceil_tick(max(trace.y))])


##########
#### tedious blackbox scale maker
##########


# fig_facet.show()

fig_fixed_y_axis = copy.deepcopy(fig_facet)
fig_fixed_y_axis.update_layout(title='Same - Fixed y-axis')
fig_fixed_y_axis.update_yaxes(matches='y', showticklabels=True, dtick=100000, side='right')
fig_fixed_y_axis.update_yaxes(range=[floor_tick(plotter['median_price'].min()), ceil_tick(plotter['median_price'].max())])
#                                
# fig_fixed_y_axis.show()



####### HTML APP Markdown #####

# fig_overlay.update_layout(width=1400, height=600)
fig_overlay.update_layout(height=600)
fig_overlay.update_layout(dragmode=False)
fig_overlay.update_layout(legend=dict(itemclick=False, itemdoubleclick=False,
                                      x=0.1, xanchor='left', y=0.75, yanchor='bottom'))

# fig_facet.update_layout(width=1400, height=800, dragmode=False)
# fig_fixed_y_axis.update_layout(width=1400, height=800, dragmode=False)

# remove width to be responseive
fig_facet.update_layout(height=800, dragmode=False)
fig_fixed_y_axis.update_layout(height=800, dragmode=False)






from plotly.io import to_html

# html_out = (
#     to_html(fig_overlay,      full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False, 'scrollZoom': False}) +
#     to_html(fig_facet,        full_html=False, include_plotlyjs=False,  config={'displayModeBar': False, 'scrollZoom': False}) +
#     to_html(fig_fixed_y_axis, full_html=False, include_plotlyjs=False,  config={'displayModeBar': False, 'scrollZoom': False})
# )

# split into 3 separate files (was 1 concatenated html_out) so Streamlit can
# place other content (e.g. a code snippet) between each chart.
# each is now its own standalone document/iframe, so each needs its own
# Plotly.js include (include_plotlyjs='cdn' on all three, not just the first).
html_overlay = to_html(fig_overlay,             full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})
html_facet = to_html(fig_facet,                 full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})
html_fixed_y_axis = to_html(fig_fixed_y_axis,   full_html=False, include_plotlyjs='cdn', config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

##########
#### custom js injection - fixed tooltip y position
#### comment out js_injection = """...""" and uncomment js_injection = '' to disable
##########

js_injection = """
<script>
document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.js-plotly-plot').forEach(function(plot) {

        function fixTooltipY() {
            var hoverLayer = plot.querySelector('.hoverlayer');
            if (!hoverLayer) return;
            var figHeight = plot.clientHeight;
            var midY = figHeight * 0.5;
            hoverLayer.childNodes.forEach(function(node) {
                if (node.tagName === 'g') {
                    var transform = node.getAttribute('transform') || '';
                    var newTransform = transform.replace(
                        /translate/(([^,]+),([^)]+)/)/,
                        function(_, x, currentY) {
                            var cy = parseFloat(currentY);
                            var fixedY = cy < midY ? figHeight * 0.13 : figHeight * 0.57;
                            return 'translate(' + x + ',' + fixedY + ')';
                        }
                    );
                    if (newTransform !== transform) {
                        node.setAttribute('transform', newTransform);
                    }
                }
            });
        }

        plot.on('plotly_hover', function() {
            requestAnimationFrame(fixTooltipY);
        });

        var observer = new MutationObserver(function() {
            requestAnimationFrame(fixTooltipY);
        });

        var hoverLayer = plot.querySelector('.hoverlayer');
        if (hoverLayer) {
            observer.observe(hoverLayer, { attributes: true, subtree: true, attributeFilter: ['transform'] });
        }
    });
});
</script>
"""

# js_injection = ''   # uncomment to disable


# main_title only goes on the first file - it was one overall page heading,
# not a per-chart title, and Streamlit's own intro text now covers this anyway.
main_title = ('<h1 style="color: white; font-family: sans-serif; padding: 20px 0 0 20px;">'
              'HDB Resale Prices (Only Flats With Up to 75 Years Remaining Lease) </h1>')

def wrap_html(body_content, title=''):
    return ('<html><head><title></title><style>body' +
            '{ background-color: black; margin: 0; }</style></head><body>' +
            title +
            body_content +
            js_injection +
            '</body></html>')

# each figure now gets its own filename, so Streamlit can show them
# separately (e.g. with a code snippet placed between each one)
files_to_write = {
    '1-firstbq-overlay-spark.html': wrap_html(html_overlay, title=main_title),
    '1-firstbq-facet-spark.html': wrap_html(html_facet),
    '1-firstbq-fixedaxis-spark.html': wrap_html(html_fixed_y_axis),
}


for filename, content in files_to_write.items():
    if os.environ.get('RUNNING_IN_CONTAINER'):
        target_path = f'output/{filename}'
    elif platform.system() == 'Darwin':
        target_path = f'/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/macroecons/pipe_hdb/{filename}'
    elif platform.system() == 'Windows':
        target_path = f'C:/Users/Talesinc/OneDrive/DBMaster/annotations/reports/macroecons/pipe_hdb/{filename}'
    else:
        raise OSError(f'Unsupported platform: {platform.system()}')

    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    with open(target_path, 'w') as f:
        print('writing html to')
        print(target_path)
        f.write(content)
        print('writing html end')

print('script ended')

    # LP: if you write the below the html will be a little weid with no ending
    # f.write(html_out)




