"""Figure builders for the LIVE charts.

Deliberately not shared with modules/pipe_hdb/2_report_firstbq.py. That file builds
standalone HTML documents for the bucket - cdn plotly, fixed pixel heights, a JS tooltip
hack, dragmode disabled - none of which apply here, where st.plotly_chart owns sizing and
theming and the user is meant to be able to zoom.

What IS shared is the arithmetic: both sides call
modules.pipe_hdb.helper_transform_for_plotly.build_median, so the live chart and the
published report can never disagree about what a median is.
"""

import plotly.express as px

from modules.pipe_hdb.helper_transform_for_plotly import (
    ceil_tick,
    flat_type_order,
)


####################################
####### shared bits between both figures
####################################

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
####### one axis, every flat type overlaid
####################################

def make_overlay(plotter, start_of_year_lines):
    order = flat_type_order(plotter['flat_type'].unique(), descending=True)

    fig = px.line(
        plotter,

        x='tx_monthdate',
        y='median_price',

        markers=True,

        color='flat_type',
        # nb_sales rides along in the hover so a median resting on 2 transactions is
        # visible as such, rather than looking like the same quality of data as one on 200
        custom_data=['median_price_k', 'nb_sales'],

        labels=LABELS,
        category_orders={'flat_type': order},

        template='plotly_dark',
    )

    fig = _add_year_lines(fig, start_of_year_lines)

    fig.update_layout(
        hovermode='x unified',
        hoverlabel=dict(font=dict(size=12)),
        xaxis=dict(hoverformat='%Y %b'),
        height=600,
        margin=dict(t=30),
    )
    fig.update_traces(hovertemplate='%{customdata[0]:.0f}k  (n=%{customdata[1]})')
    fig.update_yaxes(range=[0, ceil_tick(plotter['median_price'].max())], side='right')

    return fig


####################################
####### one small chart per flat type
####################################

def make_facet(plotter, start_of_year_lines):
    order = flat_type_order(plotter['flat_type'].unique())

    fig = px.line(
        plotter,

        x='tx_monthdate',
        y='median_price',

        facet_col='flat_type',
        facet_col_wrap=2,
        markers=True,
        custom_data=['median_price_k', 'nb_sales'],

        labels=LABELS,
        category_orders={'flat_type': order},

        template='plotly_dark',

        facet_col_spacing=0.1,
        facet_row_spacing=0.15,
    )

    fig = _add_year_lines(fig, start_of_year_lines)

    # each facet keeps its own y range - with the flat types this far apart in price, a
    # shared axis flattens the small ones into straight lines
    fig.update_yaxes(matches=None, showticklabels=True, side='right')
    fig.update_xaxes(matches='x', showticklabels=True, dtick='M12', tickformat='%Y')

    fig.update_layout(
        hovermode='x unified',
        hoverlabel=dict(font=dict(size=10)),
        height=280 * max(1, -(-len(order) // 2)),  # two per row, so ceil(n/2) rows
        margin=dict(t=30),
    )
    fig.update_xaxes(hoverformat='%Y %b')
    fig.update_traces(hovertemplate='%{customdata[0]:.0f}k  (n=%{customdata[1]})<extra></extra>')

    return fig
