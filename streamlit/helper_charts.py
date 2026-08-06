"""Figure builder for the LIVE chart on the Explore tab.

One facet per flat type. The single-axis overlay was removed deliberately - it answers a
different question (how do the flat types compare) than the one this tab is for, and it
distracted from the per-flat-type trend.


Deliberately not shared with modules/pipe_hdb/2_report_firstbq.py. That file builds
standalone HTML documents for the bucket - cdn plotly, fixed pixel heights, a JS tooltip
hack, dragmode disabled - none of which apply here, where st.plotly_chart owns sizing and
theming and the user is meant to be able to zoom.

What IS shared is the arithmetic: both sides call
modules.pipe_hdb.helper_transform_for_plotly.build_median, so the live chart and the
published report can never disagree about what a median is.
"""

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from modules.pipe_hdb.helper_transform_for_plotly import (
    FLAT_TYPE_ORDER,
    ceil_tick,
    flat_type_order,
    floor_tick,
)


# how much of a facet's height the tallest sales bar may occupy
BAR_MAX_SHARE = 0.25
# solid rather than translucent - the bars render behind the line, so they no longer need
# transparency to stay out of its way
BAR_COLOUR = 'rgb(205, 175, 55)'


# two columns - the whole chart fits on screen without scrolling, which matters more than
# the extra width a single column gave each line
FACET_COLUMNS = 2
FACET_ROW_HEIGHT = 280

# category_orders is built from the FULL list of charted flat types, never from the types
# present in the current selection. plotly reserves a facet cell for every category named
# here, so filtering 2 ROOM down to nothing leaves its cell empty instead of promoting
# 3 ROOM into the top-left and shifting everything else around.
#
# facet_col_wrap fills left to right then down, so this list IS the reading order:
#   2 3
#   4 5
#
# note the double negative - FLAT_TYPE_ORDER is largest-first, so descending=True REVERSES
# it into smallest-first, which is the order wanted here. the flag describes what it does
# to the source list, not the result
FACET_ORDER = flat_type_order(FLAT_TYPE_ORDER, descending=True)


def _add_sales_bars(fig, plotter):
    """Draw nb_sales as bars behind each facet's median line.

    Counts and prices cannot share an axis - 30 sales against 600,000 dollars would flatten
    the bars to nothing - so the counts are scaled INTO the price axis, the same trick the
    commented-out block in 0_import_datagov.py uses.

    Every facet is scaled on its OWN busiest month, not a global maximum. A facet showing a
    quiet flat type would otherwise have invisible bars; the bars are there to compare
    months WITHIN a facet, not across them.
    """
    # each facet's axes are read off ITS OWN line trace rather than computed from a row/col
    # guess. plotly numbers facet rows bottom-up, so position 0 (the top facet) is NOT
    # row 1 - deriving it arithmetically put every bar in the wrong row.
    # px creates one line trace per flat type PRESENT, in category_orders order
    present = [ft for ft in FACET_ORDER
               if ft in plotter['flat_type'].unique().to_list()]
    axes_for = {ft: (trace.xaxis, trace.yaxis)
                for ft, trace in zip(present, fig.data)}

    for flat_type in present:
        facet = plotter.filter(pl.col('flat_type') == flat_type)
        x_axis, y_axis = axes_for[flat_type]

        # the y range has to be decided HERE and pinned below. left to autoscale, plotly
        # would grow the axis to fit the bars, which changes the height the bars were
        # scaled against - the scaling would chase its own tail
        y_floor = floor_tick(facet['median_price'].min())
        y_ceiling = ceil_tick(facet['median_price'].max())

        busiest = facet['nb_sales'].max()
        headroom = (y_ceiling - y_floor) * BAR_MAX_SHARE
        bar_heights = [n / busiest * headroom for n in facet['nb_sales']]

        fig.add_trace(
            go.Bar(
                x=facet['tx_monthdate'].to_list(),
                y=bar_heights,
                # base pins each bar to the axis floor - without it bars start at zero,
                # far below a range that begins around 400k, and nothing is visible
                base=y_floor,
                marker=dict(color=BAR_COLOUR),
                # the count is already in the line's hover, so a second hover entry per
                # point would just double up under hovermode='x unified'
                hoverinfo='skip',
                showlegend=False,
                # target the facet's own axes directly - 'y3' etc, not a row/col guess
                xaxis=x_axis,
                yaxis=y_axis,
            )
        )

        # 'y3' -> 'yaxis3', 'y' -> 'yaxis'. same conversion 2_report_firstbq.py uses
        fig.layout[y_axis.replace('y', 'yaxis', 1)].update(range=[y_floor, y_ceiling])

    # traces render in list order, so the bars added above would sit ON TOP of the lines.
    # put every Bar first so the lines stay readable
    fig.data = (
        tuple(t for t in fig.data if isinstance(t, go.Bar))
        + tuple(t for t in fig.data if not isinstance(t, go.Bar))
    )

    return fig


def _add_legend(fig):
    """Two legend entries: what the line is, and what the bars are.

    Every facet carries its own copy of both, so a default legend would list the same two
    things four times over. Only the first line and the first bar get an entry; the rest
    are suppressed. The facet titles already say which flat type is which, so the legend
    is free to explain the two MARKS instead of the four series.
    """
    for trace in fig.data:
        trace.showlegend = False

    first_line = next((t for t in fig.data if not isinstance(t, go.Bar)), None)
    if first_line is not None:
        first_line.showlegend = True
        first_line.name = 'Median resale price'

    first_bar = next((t for t in fig.data if isinstance(t, go.Bar)), None)
    if first_bar is not None:
        first_bar.showlegend = True
        first_bar.name = 'Transactions that month'

    return fig


def _add_year_lines(fig, start_of_year_lines):
    # same darkgreen january markers the static charts use, so the two views read alike
    for jan in start_of_year_lines:
        fig.add_vline(x=jan, line=dict(color='darkgreen', width=2))

    return fig


LABELS = {
    'tx_monthdate': 'Monthly',
    'median_price': 'Median Price (SGD)',
    'flat_type': 'Flat Type',
    'nb_sales': 'Sales in month',
}


####################################
####### one small chart per flat type
####################################

def make_facet(plotter, start_of_year_lines):
    fig = px.line(
        plotter,

        x='tx_monthdate',
        y='median_price',

        facet_col='flat_type',
        facet_col_wrap=FACET_COLUMNS,
        markers=True,
        custom_data=['median_price_k', 'nb_sales'],

        labels=LABELS,
        category_orders={'flat_type': FACET_ORDER},

        template='plotly_dark',

        facet_col_spacing=0.1,
        facet_row_spacing=0.15,
    )

    fig = _add_year_lines(fig, start_of_year_lines)

    # each facet keeps its own y range - with the flat types this far apart in price, a
    # shared axis flattens the small ones into straight lines.
    # matches=None must be set BEFORE _add_sales_bars pins each range, or a shared axis
    # would make the last facet's range win everywhere
    fig.update_yaxes(matches=None, showticklabels=True, side='right')
    fig.update_xaxes(matches='x', showticklabels=True, dtick='M12', tickformat='%Y')

    fig = _add_sales_bars(fig, plotter)
    fig = _add_legend(fig)

    fig.update_layout(
        hovermode='x unified',
        hoverlabel=dict(font=dict(size=10)),
        # fixed rows, because the cell count no longer shrinks with the selection - the
        # chart keeps the same height whatever is filtered, which is the point
        height=FACET_ROW_HEIGHT * max(1, -(-len(FACET_ORDER) // FACET_COLUMNS)),
        # extra top margin makes room for the legend sitting above the grid
        margin=dict(t=60),
        # horizontal, above the top-left facet - out of the plotting area entirely, so it
        # never covers data, and read before the charts rather than after.
        # itemclick disabled: clicking an entry would hide only the ONE trace that carries
        # the legend entry, leaving the other three facets untouched - which looks broken
        legend=dict(
            orientation='h',
            yanchor='bottom', y=1.04,
            xanchor='left', x=0,
            itemclick=False, itemdoubleclick=False,
        ),
        # overlay, not the default 'group' - grouping would narrow each bar and offset it
        # sideways to make room for a neighbour that does not exist
        barmode='overlay',
        # same as 2_report_firstbq.py: nothing on these charts is draggable. the axes are
        # set deliberately per facet, and a dragged-out view is a view the reader cannot
        # get back without knowing to double-click
        dragmode=False,
    )
    fig.update_xaxes(hoverformat='%Y %b')
    # selector matters: without it this hits the Bar traces too, and they carry no
    # custom_data - %{customdata[0]} is undefined on them, which kills the unified tooltip
    # for the whole facet rather than just for the bar
    fig.update_traces(
        hovertemplate='%{customdata[0]:.0f}k  (n=%{customdata[1]})<extra></extra>',
        selector=dict(type='scatter'),
    )

    return fig
