
import argparse
import copy
from itertools import groupby
import os
import platform
import sys
from datetime import date

import precode.pre_dimchecks as dc
import precode.pre_dateranger as dr
import precode.pre_readwrite as rw
import precode.pre_datashapers as ds

# from helper_transform_for_plotly import flat_type_order



# ── start spark ───────────────────────────────────────────────────────────────

from sparkutils.getspark import get_spark, stop_spark
spark = get_spark('hdb_ingest')

from sparkutils.functions import F, T, col, lit, when, to_date, year, bround, count, median


from pyspark.sql.functions import desc

datagovhdb = spark.read.parquet('datalake/t2/datagov/resale_flat_prices')
datagovhdb.show()

STARTMONTH = '2024-11-01'
ENDMONTH = '2024-12-01'
datagovhdb_scope = datagovhdb.filter(col('tx_monthdate').between(STARTMONTH, ENDMONTH))

dc.dim(datagovhdb)
dc.dim(datagovhdb_scope)
dc.listvalues(datagovhdb, 'tx_monthdate')
dc.listvalues(datagovhdb_scope, 'tx_monthdate')

########

datagovhdb.printSchema()

# do this
flat_type_counts = datagovhdb.groupBy('flat_type').count()
flat_type_counts.show()

## end
df_2room = datagovhdb.filter(col('flat_type')=='2 ROOM')
df_4room = datagovhdb.filter(col('flat_type')=='4 ROOM')


# pseudocode:


# dr.make_yearmonth(datagovhdb, '')

countbyTowns = (df_2room
.groupby('tx_year', 'town').count()
.orderBy(desc('tx_year'), desc('count'))
)
countbyTowns.show(1000)

# calude task
# filter datagovhdb to the towns that topped countbyTowns, then break each town
# down by street, ordered by largest count per town.

# towns_of_interest = ['TAMPINES', 'PASIR RIS']
towns_of_interest = ['TAMPINES']


east_2_rooms = (df_2room
.filter(col('tx_year')>=2025)
.filter(col('town').isin(towns_of_interest))
)
east_2_rooms.show()

streetcounts_2rooms = (east_2_rooms
.groupBy('town', 'street_name', 'flat_type')
.agg(
    count('*').alias('count'),
    median('resale_price').alias('median_resale_price'),
    median('remaining_lease_sold').alias('median_remaining_lease')
)
.orderBy('town', desc('count'))
)
streetcounts_2rooms.show(1000)

# Generally its remainingg lease is 88 YEar
# 

# Hence i want to find the 4 rooms with remaining lease that is around this.and
# checj the ndeian resale price

df_4room = (datagovhdb
.filter(col('flat_type')=='4 ROOM')
.filter(col('remaining_lease_sold').between(85, 90))
)

df_4room.show()


streetcounts_4rooms = (df_4room
.filter(col('tx_year')>=2025)
.filter(col('town').isin(towns_of_interest))
.groupBy('town', 'street_name', 'flat_type')
.agg(
    count('*').alias('count'),
    median('resale_price').alias('median_resale_price'),
    median('remaining_lease_sold').alias('median_remaining_lease')
)
.orderBy('town', desc('count'))
)
streetcounts_4rooms.show(1000)



streetcounts_2rooms
df_4room 

