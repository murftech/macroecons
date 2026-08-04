from polars import col, concat, lit, when
import polars as pl
import plotly.graph_objects as go
import copy


datagovhdb = pl.read_parquet('hive/t2/datagovhdb')

# dc.listvalues(datagovhdb, 'flat_type')


df_scope = (datagovhdb
              .filter(col('remaining_lease_sold') <= 75)
            #   .filter(col('remaining_lease_sold') >= 75)

              .filter(col('tx_year')>=2019)
              .filter(col('flat_type').is_in(['2 ROOM','3 ROOM','4 ROOM','5 ROOM']))
)

# BQ: for each flat_type controlled by median and sample size, are prices dropping or increasing?
# technical bq: for each flat_type controlled by median and sample size, are prices dropping or increasing?

# df_scope.glimpse()
bq_sel = df_scope.select('tx_year', 'tx_monthdate', 'flat_type', 'remaining_lease_sold', 'town', 'street_name', 'resale_price')

# bq_sel.show()

# BQ: for each flat_type, are median prices dropping or increasing across tx_monthdate?
bq_median = (
    bq_sel
    .group_by('tx_monthdate', 'flat_type')
    .agg(
        col('resale_price').median().alias('median_price'),
        pl.len().alias('nb_sales'),
    )
    .sort('flat_type', 'tx_monthdate')
)

# bq_median.show(1000)

# dc.listvalues(bq_median, 'flat_type')




############
#### charts 
############



import math
import plotly.express as px



####################################
####### additional information aggregates
####################################

plotter = bq_median.with_columns(
    (pl.col('median_price') / 1000).round(0) * 1000,
    ((pl.col('median_price') / 1000).round(0)).alias('median_price_k'),
)

# legend/facet order is declared per-figure via category_orders below instead of by
# re-sorting the dataframe - which means plotter is no longer mutated between the two charts
FLAT_TYPE_DESC = ['5 ROOM', '4 ROOM', '3 ROOM', '2 ROOM']
FLAT_TYPE_ASC = ['2 ROOM', '3 ROOM', '4 ROOM', '5 ROOM']

# polars' .unique() makes no ordering promise (pandas' preserved first-appearance order),
# so sort explicitly rather than relying on whatever order comes back
start_of_year_lines = (
    plotter.filter(pl.col('tx_monthdate').dt.month() == 1)['tx_monthdate'].unique().sort()
)

################################
######## overall chart #####
################################



########## 
#### tedious blackbox scale maker
########## 

def ceil_tick(val, dtick=50000):
    return (math.ceil(val / dtick) + 1) * dtick

def floor_tick(val, dtick=50000):
    return math.floor(val / dtick) * dtick
########## 
#### tedious blackbox scale maker
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

import os

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
    '1-firstbq-overlay.html': wrap_html(html_overlay, title=main_title),
    '1-firstbq-facet.html': wrap_html(html_facet),
    '1-firstbq-fixedaxis.html': wrap_html(html_fixed_y_axis),
}

import platform

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




