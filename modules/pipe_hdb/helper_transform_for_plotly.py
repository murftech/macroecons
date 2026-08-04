"""Shared aggregation for the HDB resale charts.

Imported by BOTH sides:
  - modules/pipe_hdb/2_report_firstbq.py  -> renders static HTML for the bucket
  - the streamlit app              -> renders interactive figures

Deliberately polars-only: no plotly, no streamlit. The two consumers must agree on
the numbers (a disagreement there is a correctness bug - two answers to one question)
but are meant to disagree on presentation, so presentation stays out of this file.

Import from either side with the repo root as cwd:
    import sys; sys.path.append('')
    from modules.pipe_hdb.helper_transform_for_plotly import build_median
same idiom 0_import_datagov.py already uses for helper_hdb.

Named helper_* rather than carrying a step number because it is imported, not executed -
run_pipeline.py runs only the numbered scripts. A leading digit would also make the module
name unimportable, since Python identifiers cannot start with one.
"""

import math

import polars as pl
from polars import col


####################################
####### the original hardcoded scope, now the defaults
####################################

# these reproduce exactly what 2_report_firstbq.py used to filter on inline, so calling
# build_median() with no arguments gives the pre-existing charts unchanged
DEFAULT_MAX_LEASE = 75
# the source data bottoms out at 39 years remaining, so a floor of 0 excludes nothing and
# leaves the pipeline's charts unchanged. the app can raise it to isolate a lease band
DEFAULT_MIN_LEASE = 0
DEFAULT_MIN_YEAR = 2019
# the flat types this project charts, smallest to largest. drives both the ordering and
# the app's picker options, so it is the single place to widen or narrow scope.
#
# the source data holds three more, all deliberately out of scope:
#   1 ROOM (87 rows) and MULTI-GENERATION (88 rows) - too rare for a monthly median to
#     mean anything; most months have zero or one sale
#   EXECUTIVE (16,845 rows, 7% of the data) - not a data-quality problem but a different
#     market segment from the standard flats this project is about
#
# they remain in the parquet untouched. filtering here rather than at import keeps the raw
# data complete, so widening scope later is a one-line change and not a re-import
FLAT_TYPE_ORDER = ['2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM']

# every charted type is on by default - these coincide today, but the names mean different
# things: FLAT_TYPE_ORDER is what may be picked, this is what starts picked
DEFAULT_FLAT_TYPES = FLAT_TYPE_ORDER


def flat_type_order(flat_types, descending=False):
    # plotly needs an explicit category order; derive it from FLAT_TYPE_ORDER so a
    # selection of any size comes back in a sensible order rather than alphabetically
    ordered = [ft for ft in FLAT_TYPE_ORDER if ft in flat_types]

    if descending:
        ordered = ordered[::-1]

    return ordered


####################################
####### the aggregation itself
####################################

def build_median(
    df,
    *,
    max_lease=DEFAULT_MAX_LEASE,
    min_lease=DEFAULT_MIN_LEASE,
    min_year=DEFAULT_MIN_YEAR,
    flat_types=DEFAULT_FLAT_TYPES,
    towns=None,
    min_sales=1,
):
    """Median resale price per month per flat_type.

    Returns tx_monthdate, flat_type, median_price, nb_sales, median_price_k.

    min_lease/max_lease bracket remaining_lease_sold, so a band can be isolated rather than
    only a ceiling - the static report only ever set the ceiling (75).

    min_sales guards against the headline risk of making this interactive: narrow the
    filters enough (one town, one flat type) and a month can hold 1-2 transactions,
    whose "median" reads as signal but is noise. nb_sales is returned so the caller
    can surface the sample size rather than hide it.
    """
    scoped = (
        df
        .filter(col('remaining_lease_sold') <= max_lease)
        .filter(col('remaining_lease_sold') >= min_lease)
        .filter(col('tx_year') >= min_year)
        .filter(col('flat_type').is_in(list(flat_types)))
    )

    # towns is the dimension the static charts never used at all - None means all 26
    if towns:
        scoped = scoped.filter(col('town').is_in(list(towns)))

    median = (
        scoped
        .group_by('tx_monthdate', 'flat_type')
        .agg(
            col('resale_price').median().alias('median_price'),
            pl.len().alias('nb_sales'),
        )
        .filter(col('nb_sales') >= min_sales)  # no-op at the default of 1
        .sort('flat_type', 'tx_monthdate')
    )

    # round to the nearest 1000 for the axis, and keep a k-denominated copy for hover labels
    df_final = median.with_columns(
        (pl.col('median_price') / 1000).round(0) * 1000,
        ((pl.col('median_price') / 1000).round(0)).alias('median_price_k'),
    )

    # df_final.show(50)

    return df_final


def january_lines(plotter):
    # x positions for the year-boundary vlines.
    # polars' .unique() makes no ordering promise (pandas' preserved first-appearance
    # order), so sort explicitly rather than relying on whatever comes back
    jan_dates = (
        plotter
        .filter(col('tx_monthdate').dt.month() == 1)['tx_monthdate']
        .unique()
        .sort()
    )

    return jan_dates


##########
#### tedious blackbox scale maker
##########

def ceil_tick(val, dtick=50000):
    return (math.ceil(val / dtick) + 1) * dtick


def floor_tick(val, dtick=50000):
    return math.floor(val / dtick) * dtick
