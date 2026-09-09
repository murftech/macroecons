"""Unchanged from modules/pipe_hdb/helper_transform_for_plotly.py.

Deliberately polars-only, same as the original - the aggregation logic that both
the static report and (if you still run it) your Streamlit app depend on stays in
exactly one place, so there's exactly one definition of "median resale price" to
disagree with.
"""

import math

import polars as pl
from polars import col


DEFAULT_MAX_LEASE = 75
DEFAULT_MIN_LEASE = 0
DEFAULT_MIN_YEAR = 2019

FLAT_TYPE_ORDER = ['5 ROOM', '4 ROOM', '3 ROOM', '2 ROOM']
DEFAULT_FLAT_TYPES = FLAT_TYPE_ORDER


def flat_type_order(flat_types, descending=False):
    ordered = [ft for ft in FLAT_TYPE_ORDER if ft in flat_types]

    if descending:
        ordered = ordered[::-1]

    return ordered


def build_median(
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
    """Median resale price per month per flat_type."""
    scoped = (
        df
        .filter(col('remaining_lease_sold') <= max_lease)
        .filter(col('remaining_lease_sold') >= min_lease)
        .filter(col('tx_year') >= min_year)
        .filter(col('flat_type').is_in(list(flat_types)))
    )

    if towns:
        scoped = scoped.filter(col('town').is_in(list(towns)))

    if streets:
        scoped = scoped.filter(col('street_name').is_in(list(streets)))

    median = (
        scoped
        .group_by('tx_monthdate', 'flat_type')
        .agg(
            col('resale_price').median().alias('median_price'),
            pl.len().alias('nb_sales'),
        )
        .filter(col('nb_sales') >= min_sales)
        .sort('flat_type', 'tx_monthdate')
    )

    df_final = median.with_columns(
        (pl.col('median_price') / 1000).round(0) * 1000,
        ((pl.col('median_price') / 1000).round(0)).alias('median_price_k'),
    )

    return df_final


def january_lines(plotter):
    jan_dates = (
        plotter
        .filter(col('tx_monthdate').dt.month() == 1)['tx_monthdate']
        .unique()
        .sort()
    )

    return jan_dates


def ceil_tick(val, dtick=50000):
    return (math.ceil(val / dtick) + 1) * dtick


def floor_tick(val, dtick=50000):
    return math.floor(val / dtick) * dtick
