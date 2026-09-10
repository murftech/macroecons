###### IMPORT  ######
from pathlib import Path
import sys
import argparse

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

## NEW ###
# ── PICK THE PROVIDER — the only environment branch in this file ─────────────
if IS_DATABRICKS:
    from providers.databricks import add_args, get_spark_engine, get_landing_dir, write_tier
elif IS_LOCAL:
    from providers.local import add_args, get_spark_engine, get_landing_dir, write_tier


# ── HANDWRITTEN CONTRACT ─────────────────────────────────────────────────────────
ERA_CONTRACTS = {
    '1990_1999':    {'id': 'd_ebc5ab87086db484f88045b47411ebc5', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2000_2012Feb': {'id': 'd_43f493c6c50d54243cc1eab0df142d6a', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2012Mar_2014': {'id': 'd_2d5ff9ea31397b66239f245f57751537', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2015_2016':    {'id': 'd_ea9ed51da2787afaf8e51f827c304208', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2017_onwards': {'id': 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc', 'month_col': 'month', 'src_format': 'yyyy-mm'},
}

# only HIVE gets this, ICEBERG will exercise its schema evolution
COLUMNS_CONTRACT = ['month', 'town', 'flat_type', 'block', 'street_name', 'storey_range',
              'floor_area_sqm', 'flat_model', 'lease_commence_date', 'remaining_lease',
              'resale_price']


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────

print('show sys.argv')
print(sys.argv)
parser = argparse.ArgumentParser()
parser.add_argument('--spark_engine', choices=['sail', 'java'], default='sail')
parser.add_argument('--write_format', default='parquet,iceberg',
                    help="comma-separated: parquet,delta,iceberg. local writes parquet+iceberg; "
                         "databricks writes delta+iceberg (deploy passes --write_format delta,iceberg)")
parser.add_argument('--eras', default='2017_onwards',
                    help="comma-separated era keys or 'all'. currently available eras: " + ','.join(ERA_CONTRACTS))
add_args(parser)                # databricks: --catalog/--schema/--volume ; local: nothing
args = parser.parse_args()
print(args)


# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    # args.spark_engine = 'java'
    args.spark_engine = 'sail'
    args.write_format = 'parquet'
    # args.eras = '1990_1999,2000_2012Feb,2015_2016'
    args.eras = 'all'
    # args.write_format = 'iceberg'
    # args.write_format = 'parquet,iceberg'
    # print(args)

# resolve the engine ONCE, here: get_spark_engine() returns 'java' on databricks, passes
# --spark_engine through locally. below this line args.spark_engine IS the live engine.
args.spark_engine = get_spark_engine(args.spark_engine)


# ── VALIDATE ERA PARAMETER ─────────────────────────────────────────────────────────

if args.eras == 'all':
    eras = list(ERA_CONTRACTS)
else:
    eras = args.eras.split(',')

print('era python list to be parsed:')
print(eras)

unknown = [e for e in eras if e not in ERA_CONTRACTS]

if unknown:
    raise SystemExit(f"unknown era(s): {unknown}. keys: {', '.join(ERA_CONTRACTS)}")




# ── START SPARK ───────────────────────────────────────────────────────────────
from sparkutils.getspark import get_spark, stop_spark
spark = get_spark('1_import_to_t1', args.spark_engine)
from sparkutils.functions import col, lit, when, to_date, year, F


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────────────

ORIGIN, DATASET, TIER = 'datagov', 'resale_flat_prices', 't1'

# SOURCE: where the landed CSVs are read from
LANDING_DIR = get_landing_dir(args, ORIGIN, DATASET)
print(f'[read] landing dir : {LANDING_DIR}')


# ── REQUIRED FUNCTIONS ─────────────────────────────────────────────
COMPUTED_PARTITION = 'tx_monthdate'     # the derived partition column

from helper_transit import add_monthdate

def build_csv_path(era):
    # a tiny helper that builds the path to one landed CSV file, given an era key.
    return Path(LANDING_DIR) / f'{era}_{DATASET}_{ERA_CONTRACTS[era]["id"]}.csv'

############### RUN ###############
## sub: need to defend exverything below
## need to seetle to prefix with get_ and togg_ its important


era_dfs = []
for era in eras:
    contract = ERA_CONTRACTS[era]                      # {id, month_col, src_format} for this file
    csv = build_csv_path(era)
    if not csv.exists():
        print(f'[skip] {era}: not landed - {csv}')
        continue

    df = spark.read.csv(str(csv), header=True, inferSchema=False)   # source columns stay string
    df.printSchema()
    print('sample spark dataframe right after read_csv')
    df.show(3)
    df = add_monthdate(df, contract['month_col'], contract['src_format'], COMPUTED_PARTITION)
    n = df.count()
    if n == 0:
        print(f'[skip] {era}: 0 rows')
        continue
    print(f'[{era}] {n:,} rows')
    era_dfs.append(df)


if not era_dfs:
    stop_spark(spark)
    raise SystemExit('nothing written - every requested era was missing or empty')



##### here we will play with io #######

era_dfs
# so claude actually is trying to write a list not a sparkdf
era_dfs[1].show()


# also how can i line by line test write table to delta we think of it later

import shutil, os, importlib
from sparkutils.functions import col, lit

# comment out lines 155–162 (the write_tier call + stop_spark) while iterating
STUDY = 'datalake/_study'
shutil.rmtree(STUDY, ignore_errors=True)            # clean slate each run
by_era = {e: df for e, df in zip(eras, era_dfs)}    # assumes none skipped (all 5 landed)
PART   = [COMPUTED_PARTITION]                          # 'tx_monthdate'
HROOT  = f'{STUDY}/hive/t1'


import shutil
shutil.rmtree('datalake/_study', ignore_errors=True)

####### PYARROW DONE #########
import importlib, helper_pyarrow_io
importlib.reload(helper_pyarrow_io)
from helper_pyarrow_io import write_partition, write_partition_guarded, read, list_partitions   # re-bind — REQUIRED

# partitioning = ['town', 'tx_monthdate']
# partitioning = ['tx_monthdate']

partitioning = ['tx_monthdate', 'town']
target = by_era['1990_1999']
# target = by_era['1990_1999'].withColumn('extra', lit('die'))
target = by_era['1990_1999'].drop('block')
# target = by_era['1990_1999'].withColumn('floor_area_sqm', col('floor_area_sqm').cast('double'))
write_partition_guarded(target.toArrow(), HROOT, partitioning, show_partitions=True)


import pyarrow.dataset as ds
ds.write_dataset(
        data = by_era['1990_1999'].toArrow(),
        base_dir = HROOT,
        partitioning = partitioning,  
                        
        partitioning_flavor='hive', # the wrapper is so that i did not have to repeat these required defaults                   
        existing_data_behavior = 'delete_matching',   # the wrapper is so that i did not have to repeat these required defaults                   
        format = 'parquet' # the wrapper is so that i did not have to repeat these required defaults                   
    )
# the wrapper also curates an attached summary on every write.

stage1 = spark.read.parquet(HROOT)

import pyarrow.parquet as pq
# simple file parquet DO NOT use write_dataset
pq.write_table(by_era['1990_1999'].toArrow(), 'datalake/_study/hdb_stage.parquet')

# consumer script
stage2 = spark.read.parquet('datalake/_study/hdb_stage.parquet')
stage2.show()

############# PYICEBERG ##############


###########

tier=TIER, origin=ORIGIN, dataset=DATASET,
==> base_dir

part_cols ==> partitioning


write_partition_guarded(by_era['1990_1999'].toArrow(), base_dir, partitioning, show_partitions=True)



#################### now we must understand how write_tier cooples everything ####
# Anyway i have write_icerberg to master next
# should i have a write_java? i think shouldnt becasue anything can run in laptop is not big data


# if there is no need to wrap into write_tier please DO NOT just remove it i dont want triple layers. where i am confused two years later
# But wait i do this becasue of iceberg vs arrow vs delta, stupid shhit
# So i'd rather name it, write cloud then?
# togg_write?

# # ── WRITE t1 — provider owns the fork: files under datalake/ locally, managed
# #    catalog tables on databricks. bounds=None -> window is derived from the eras. ──
write_tier(era_dfs, tier=TIER, origin=ORIGIN, dataset=DATASET,
           write_format=args.write_format, part_cols=[COMPUTED_PARTITION],
           columns_contract=COLUMNS_CONTRACT, bounds=None, spark=spark, args=args)

Three things it tries to policy:
1) ENVIRONMENT: DATABRICKS/local
2) write_format
3) forcing a catalog, schema, tablename definition
4) is just simply what write_dataset does

# Othrs:

# Multi-frame orchestration. write_tier takes a list of 5 era DataFrames. Locally it loops them (each era .toArrow()'d and written separately, so iceberg evolves its schema per era); on Databricks it unionByNames them first. write_dataset takes exactly one table. This is genuine domain logic — the loop-vs-union decision, and per-era writes.
# OR
# columns_contract padding. Before the parquet write, every era's arrow table is padded up to the fixed 11-column list (1990s eras lack remaining_lease). Iceberg doesn't get padded — it evolves. That split is a decision write_tier makes; write_dataset knows nothing about it.
# bounds → span + the (n_out, months_in) return. Silver passes bounds=(startMonth, endMonth) → exact overwrite window; bronze passes None → derive from data. And it returns the row count + month list for the STATUS block.
# 3 for t1 and t2 differnces.
# An explicit bounds makes the databricks .overwrite(tx_monthdate >= '2015-01-01' AND <= '2015-03-01') clear the whole window — 
# so an empty month gets correctly emptied. !!!! wait what
# but bounds management can only take effect in databricks

# # ── EXIT ─────────────────────────────────────────────

# stop_spark(spark)

# print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# # argv: argument vector
# print(args)
