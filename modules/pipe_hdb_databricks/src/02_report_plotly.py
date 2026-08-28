"""Databricks version of modules/pipe_hdb/2_report_firstbq.py.

What changed and why:
  - Reads from the Delta table `catalog.schema.hdb_silver` (written by 01_ingest_bronze.py)
    instead of `pl.read_parquet('hive/t2/datagovhdb')`.
  - build_median() and all three Plotly figures are untouched - same helper module,
    same charts, same styling. This project's whole "polars only, plotly builds static
    html" design survives the move unchanged; only the storage layer changed.
  - The three HTML files land in a Unity Catalog Volume
    (/Volumes/catalog/schema/volume/YYYY-MM-DD/*.html) instead of output/ (in-container)
    or your OneDrive path (native run). A Volume is Databricks' answer to "a folder
    that isn't tied to one machine" - readable from the workspace UI, another job,
    a Databricks App, or downloaded locally.
  - Same __file__ gotcha as 01_ingest_bronze.py - spark_python_task's exec() wrapper
    doesn't define it, so this script's own directory arrives as --src-dir instead.
"""

import argparse
import copy
import os
import sys
from datetime import date

import polars as pl


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--volume', required=True)
    parser.add_argument('--src-dir', required=True)
    args = parser.parse_args()

    sys.path.append(args.src_dir)
    from helper_transform_for_plotly import (
        build_median,
        ceil_tick,
        flat_type_order,
        floor_tick,
        january_lines,
    )

    source_table = f"{args.catalog}.{args.schema}.hdb_silver"

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    datagovhdb = pl.from_pandas(spark.table(source_table).toPandas())

    bq_median = build_median(datagovhdb)

    import plotly.express as px

    plotter = bq_median

    FLAT_TYPE_DESC = flat_type_order(plotter['flat_type'].unique())
    FLAT_TYPE_ASC = flat_type_order(plotter['flat_type'].unique(), descending=True)

    start_of_year_lines = january_lines(plotter)

    # ── overall chart ───────────────────────────────────────────────────────
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
    fig_overlay.update_xaxes(tickfont=dict(size=25), tickangle=1)
    fig_overlay.update_yaxes(dtick=100000, range=[0, ceil_tick(plotter['median_price'].max())], side='right')

    # ── faceted chart ───────────────────────────────────────────────────────
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

    fig_fixed_y_axis = copy.deepcopy(fig_facet)
    fig_fixed_y_axis.update_layout(title='Same - Fixed y-axis')
    fig_fixed_y_axis.update_yaxes(matches='y', showticklabels=True, dtick=100000, side='right')
    fig_fixed_y_axis.update_yaxes(range=[floor_tick(plotter['median_price'].min()), ceil_tick(plotter['median_price'].max())])

    # ── html assembly (unchanged) ───────────────────────────────────────────
    fig_overlay.update_layout(height=600)
    fig_overlay.update_layout(dragmode=False)
    fig_overlay.update_layout(legend=dict(itemclick=False, itemdoubleclick=False,
                                          x=0.1, xanchor='left', y=0.75, yanchor='bottom'))
    fig_facet.update_layout(height=800, dragmode=False)
    fig_fixed_y_axis.update_layout(height=800, dragmode=False)

    from plotly.io import to_html

    html_overlay = to_html(fig_overlay, full_html=False, include_plotlyjs='cdn',
                            config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})
    html_facet = to_html(fig_facet, full_html=False, include_plotlyjs='cdn',
                          config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})
    html_fixed_y_axis = to_html(fig_fixed_y_axis, full_html=False, include_plotlyjs='cdn',
                                 config={'displayModeBar': False, 'scrollZoom': False, 'responsive': True})

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
                        /translate\\(([^,]+),([^)]+)\\)/,
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

    main_title = ('<h1 style="color: white; font-family: sans-serif; padding: 20px 0 0 20px;">'
                  'HDB Resale Prices (Only Flats With Up to 75 Years Remaining Lease) </h1>')

    def wrap_html(body_content, title=''):
        return ('<html><head><title></title><style>body' +
                '{ background-color: black; margin: 0; }</style></head><body>' +
                title +
                body_content +
                js_injection +
                '</body></html>')

    files_to_write = {
        '1-firstbq-overlay.html': wrap_html(html_overlay, title=main_title),
        '1-firstbq-facet.html': wrap_html(html_facet),
        '1-firstbq-fixedaxis.html': wrap_html(html_fixed_y_axis),
    }

    # one dated subfolder per run, so re-runs don't clobber yesterday's report and
    # 03_publish_outputs.py always knows exactly what today's run produced
    volume_root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"
    run_dir = f"{volume_root}/{date.today().isoformat()}"
    os.makedirs(run_dir, exist_ok=True)

    for filename, content in files_to_write.items():
        target_path = f"{run_dir}/{filename}"
        with open(target_path, 'w') as f:
            f.write(content)
        print(f'wrote {target_path}')

    # also refresh a stable "latest" copy so a bookmark/link never goes stale
    latest_dir = f"{volume_root}/latest"
    os.makedirs(latest_dir, exist_ok=True)
    for filename, content in files_to_write.items():
        with open(f"{latest_dir}/{filename}", 'w') as f:
            f.write(content)

    print('script ended')


if __name__ == '__main__':
    main()
