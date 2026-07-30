from polars import col, concat, lit, when
import polars as pl
import plotly.graph_objects as go

# import precode.pre_dateranger_pl as dr
# import precode.pre_dimchecks_pl as dc
# import precode.pre_readwrite_pl as rw


import sys
sys.path.append('')

from modules.pipe_hdb.helper_hdb import fetch_hdb_data



def sortcount(df, groupcols, showtop=10):
    matrix = (df.group_by(groupcols)
        .len()
        .rename({'len': 'count'})
        .with_columns((col('count') / col('count').sum()).alias('pcnt'))
        .sort('count', descending=True)
    )
    matrix.show(showtop)

    return matrix


# ── load data ─────────────────────────────────────────────────────────────────


# dataset_ids = [
#     'd_ebc5ab87086db484f88045b47411ebc5',
#     'd_43f493c6c50d54243cc1eab0df142d6a',
#     'd_ea9ed51da2787afaf8e51f827c304208',
#     'd_2d5ff9ea31397b66239f245f57751537',
#     # 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc',
# ]

# total_data = pl.concat([fetch_hdb_data(did) for did in dataset_ids])
# print(f"Total combined rows: {total_data.shape[0]:,}")
# the othre ids seem to be in compeltely different format

total_data = fetch_hdb_data('d_8b84c4ee58e3cfc0ece0d773c8ca6abc')

hdbdata = total_data.with_columns([
    col('month').str.to_date('%Y-%m').alias('tx_monthdate'),
    col('month').str.to_date('%Y-%m').dt.year().alias('tx_year'),
])

hdbdata.glimpse()

# hdbdata.select('tx_monthdate')
# hdbdata.select('tx_year')

hdbdata = hdbdata.with_columns(
    when(col('tx_year') >= 2021)
    .then(lit('after 2021'))
    .otherwise(lit('before 2021'))
    .alias('covid')
)

# hdbdata = hdbdata.filter(col('covid')=='after 2021')

sortcount(hdbdata, 'town')
sortcount(hdbdata, 'flat_type')
sortcount(hdbdata, 'floor_area_sqm')
sortcount(hdbdata, 'lease_commence_date')
sortcount(hdbdata, 'tx_year')


hdbdata = hdbdata.with_columns([
    (col('tx_year') - col('lease_commence_date')).alias('age_sold'),
    (99 - (col('tx_year') - col('lease_commence_date'))).alias('remaining_lease_sold'),
    (2025 - (col('tx_year') - col('lease_commence_date'))).alias('pretend_top_2025'),
])

print(hdbdata.shape)


hdb_sel = (
    hdbdata
    .select(['tx_year', 'tx_monthdate', 'covid', 'flat_type', 'resale_price',
             'age_sold', 'remaining_lease_sold', 'pretend_top_2025',
             'street_name', 'storey_range', 'town'])

    .sort('tx_monthdate', descending=True)
)

streetnames = sortcount(hdb_sel, 'street_name', 100)
townnames = sortcount(hdb_sel, 'town', 100)
townstreet = sortcount(hdb_sel, ['town', 'street_name'], 100)
sortcount(hdb_sel, 'flat_type', 100)

hdb_sel.write_parquet('hive/t2/datagovhdb')



# streetnames = dc.listvalues(hdb_sel, 'street_name')
# streetnames.write_csv('tmp/steetnames.csv')




# # ── target filter definitions ────────────────────────────────────────────────────────
# hdb_filters = {
#     'All':      lit(True),
#     'Town: Bedok':              col('town') == 'BEDOK',

#     'Town: Woodlands':          col('town') == 'WOODLANDS',
#     'Woodlands DR 16':          col('street_name') == 'WOODLANDS DR 16',

#     'Town: Tampines':           col('town') == 'TAMPINES',

#     'Tampines Target Streets':  col('street_name').is_in([
#                                     'TAMPINES AVE 7', 
#                                     'TAMPINES ST 23', 
#                                     'TAMPINES ST 24',
#                                     'TAMPINES ST 32', 
#                                     'TAMPINES ST 42',
#                                     ]),

#     'Tampines ST 22':           col('street_name') == 'TAMPINES ST 22',
# }

# hdb_filters['Tampines Target Streets']
# hdb_filters['Town: Tampines']








# # ── inflation adjustment ──────────────────────────────────────────────────────
# print('adjust prices for inflation')

# hdb_median = (
#     hdb_sel
#     .group_by(['tx_year', 'flat_type'])
#     .agg(
#         col('resale_price').median().alias('median_price'),
#         pl.len().alias('nb_sales'),
#     )
#     .sort(['flat_type', 'tx_year', 'median_price'])
# )

# locate_maxyear = (
#     hdb_median
#     .filter(col('tx_year') <= 2023)
#     .group_by('flat_type')
#     .agg(
#         col('median_price')
#         .sort_by('tx_year')
#         .last()
#         .alias('median_price_anchor')
#     )
# )

# # hdb_inflation = (
# #     hdb_median
# #     .join(locate_maxyear, on='flat_type', how='left')
# #     .with_columns(
# #         (col('median_price_anchor') / col('median_price')).alias('inflation')
# #     )
# #     .select(['tx_year', 'flat_type', 'inflation'])
# # )

# # hdb_inflated = (
# #     hdb_sel
# #     .join(hdb_inflation, on=['tx_year', 'flat_type'], how='left')
# #     .with_columns(
# #         (col('inflation') * col('resale_price')).alias('resale_price_proxy_2025')
# #     )
# # )

# # ── plotly interactive chart ──────────────────────────────────────────────────
# print('interactive plotly: median price by filter selection')

# filter_names = list(hdb_filters.keys())
# n_filters    = len(filter_names)
# traces_per   = 2

# cutoff = pl.date(2023, 1, 1)

# all_medians = {}
# all_counts  = {}

# for nm, expr in hdb_filters.items():
#     sel = hdb_sel.filter(col('tx_monthdate') >= cutoff).filter(expr)
#     all_medians[nm] = (
#         sel.group_by('tx_monthdate')
#         .agg(col('resale_price').median().alias('median_price'))
#         .sort('tx_monthdate')
#     )
#     all_counts[nm] = (
#         sel.group_by('tx_monthdate')
#         .agg(pl.len().alias('count'))
#         .sort('tx_monthdate')
#     )

# all_prices   = concat([df['median_price'] for df in all_medians.values()])
# all_cnts     = concat([df['count'] for df in all_counts.values()])
# scale_min    = (all_prices.drop_nulls().min() // 10000) * 10000
# scale_max    = -(-all_prices.drop_nulls().max() // 10000) * 10000
# scale_factor = (scale_max - scale_min) / all_cnts.max()

# fig = go.Figure()

# for i, nm in enumerate(filter_names):
#     med     = all_medians[nm].to_pandas()
#     cnt     = all_counts[nm].to_pandas()
#     visible = (i == 0)

#     fig.add_trace(go.Scatter(
#         x            = med['tx_monthdate'],
#         y            = med['median_price'],
#         mode         = 'lines+markers+text',
#         name         = nm,
#         text         = (med['median_price'] / 1000).round(0).astype(int).astype(str) + 'K',
#         textposition = 'top center',
#         line         = dict(color='steelblue', width=2),
#         marker       = dict(color='steelblue', size=6),
#         visible      = visible,
#         showlegend   = False,
#     ))

#     fig.add_trace(go.Bar(
#         x                = cnt['tx_monthdate'],
#         y                = cnt['count'] * scale_factor + scale_min,
#         name             = f'{nm} (n)',
#         text             = cnt['count'],
#         textposition     = 'inside',
#         insidetextanchor = 'start',
#         marker           = dict(color='rgba(0,0,0,0.25)'),
#         visible          = visible,
#         showlegend       = False,
#     ))

# buttons = []
# for i, nm in enumerate(filter_names):
#     vis = [False] * (n_filters * traces_per)
#     vis[i * traces_per]     = True
#     vis[i * traces_per + 1] = True
#     buttons.append(dict(method='restyle', args=['visible', vis], label=nm))

# fig.update_layout(
#     title   = 'HDB 4-Room Resale Prices (2023+) — Median & Volume',
#     xaxis   = dict(title='Transaction Month'),
#     yaxis   = dict(title='Resale Price (SGD)', range=[scale_min, 700000],
#                    tickformat=',d'),
#     barmode = 'overlay',
#     updatemenus=[dict(
#         type        = 'dropdown',
#         active      = 0,
#         buttons     = buttons,
#         x           = 0,
#         y           = 1.18,
#         xanchor     = 'left',
#         yanchor     = 'top',
#         bgcolor     = '#f0f0f0',
#         bordercolor = '#999',
#     )],
# )

# fig.show()
