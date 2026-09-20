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


'''
Q: why is a import csv into warehouse as is seems to have such a long script?
So number of lines 13 → 134 buys you:
re-runnable without duplicating data, TWO query engines,
month-partitioned output, per-run file selection.
For a prototype the 13-liner may genuinely be enough — you can add features back one at a time when a real need shows up.
'''

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


era_dfs = {}
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
    df = df.withColumn('era', lit(era))   # pretend factor column - known, fixed, 5-value set (ERA_CONTRACTS keys)
    n = df.count()
    if n == 0:
        print(f'[skip] {era}: 0 rows')
        continue
    print(f'[{era}] {n:,} rows')
    era_dfs[era] = df

type(next(iter(era_dfs.values())))

if not era_dfs:
    stop_spark(spark)
    raise SystemExit('nothing written - every requested era was missing or empty')


# ── WRITE t1 — provider owns the fork: files under datalake/ locally, managed
#    catalog tables on databricks. bounds=None -> window is derived from the eras. ──

# we will study write tier in prod:

data_new = era_dfs['2015_2016']
data_old = era_dfs['1990_1999']

data_new.printSchema()
data_old.printSchema()

partition_cols = ['tx_monthdate']


# full run best squence needed by iceberg
import importlib, helper_pyiceberg_io
importlib.reload(helper_pyiceberg_io)
from helper_pyiceberg_io import (
    getOrCreate_catalog,
    custom_describe_catalog,
    createOrEvolve_table,
    double_safe_purge,
    write_partition_guarded,
    sail_read_iceberg,
    schema_to_code
)

catalog = getOrCreate_catalog('datalake/iceberg')
custom_describe_catalog(catalog)
# tier = 't1'
# origin = 'datagov'
# tbl_name = 'resale_flat_prices'
# tbl_fqn = tier + '.' + 'origin' + '__' + tbl_name
# tbl_fqn
catalog.load_table('t1.datagov__resale_flat_prices')

# write_partition_guarded?

#### demo partition evolution
write_partition_guarded(
    data_old.toArrow(), catalog, 't1.datagov__resale_flat_prices', partition_cols,
    on_newcols='evolve', on_missingcols='pad_null')

write_partition_guarded(
    data_new.toArrow(), catalog, 't1.datagov__resale_flat_prices', partition_cols,
    on_newcols='evolve', on_missingcols='pad_null')

write_partition_guarded(
    data_old.toArrow(), catalog, 't1.datagov__resale_flat_prices', partition_cols,
    on_newcols='evolve', on_missingcols='pad_null')

custom_describe_catalog(catalog)


TBL_FQN = 't1.datagov__resale_flat_prices'

def _reset_demo_table():
    # full_refresh=True only replaces ROWS (tbl.overwrite, no filter) - it never
    # touches tbl.update_schema(), so a column added by an earlier block (e.g.
    # remaining_lease, evolved in above) stays in the table's schema forever.
    # purge + re-create is the only way to actually clear schema too. Demo-only:
    # createOrEvolve_table stays out of the normal pipeline per the provisioning-
    # only rule - this is explicitly exempted scratch, not a pipeline call.
    double_safe_purge(catalog, TBL_FQN)
    helper_pyiceberg_io.createOrEvolve_table(data_old.toArrow(), partition_cols, catalog, 't1', 'datagov__resale_flat_prices')

# demo gaurds firing for on_newcols
_reset_demo_table()
write_partition_guarded(data_old.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
# sub: i should say guard is on_newcols = error, before writing the exception message..

_reset_demo_table()
write_partition_guarded(data_old.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='drop', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='drop', on_missingcols='error')

_reset_demo_table()
write_partition_guarded(data_old.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='error')


# demo gaurds firing for on_missingcols
_reset_demo_table()
write_partition_guarded(data_new.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
write_partition_guarded(data_old.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')

_reset_demo_table()
write_partition_guarded(data_new.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')
write_partition_guarded(data_old.toArrow(), catalog, TBL_FQN, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='pad_null')


# run till here


import importlib, helper_pyarrow_io
importlib.reload(helper_pyarrow_io)
from helper_pyarrow_io import write_partition, write_partition_guarded, read, list_partitions   # re-bind — REQUIRED

CATALOG_PATH  = 'datalake/hive/'
TIER_PATH  = 'datalake/hive/t1'
PATH_TO_TABLE = 'datalake/hive/t1/datagov__resale_flat_prices'
write_partition_guarded?

import shutil
shutil.rmtree(PATH_TO_TABLE)

# demo running the exact same line in production
# write_partition_guarded?
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')
# i have to understand how to make a provision_pyarrow... i think can only delete.
# how about schema contratr etc we cannot ma pyarrow only has metadata on write

# demo gaurds firing for on_newcols
shutil.rmtree(PATH_TO_TABLE)
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
# sub: i should say guard is on_newcols = error, before writing the exception message..

shutil.rmtree(PATH_TO_TABLE)
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='drop', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='drop', on_missingcols='error')

shutil.rmtree(PATH_TO_TABLE)
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='error')
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='error')


# demo gaurds firing for on_missingcols
shutil.rmtree(PATH_TO_TABLE)
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='error')

shutil.rmtree(PATH_TO_TABLE)
write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='pad_null')
write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='error', on_missingcols='pad_null')


# Appendix:

# all combinations
# # [on_newcols, on_missingcols]

# for iceberg warehousing:
# ['evolve', 'pad_null'] # for t1 to make sure data comes in always, and do not lose data
# ['evolve', 'error'] # contradicts both, evolution will make older era fail in production.
# ['drop', 'pad_null'] for T2 fixed schema and refeshable from t1, but do not want to stop job if missing cols, just warn
# ['drop', 'error']  for T2 fixed schema and refeshable from t1, but stop job immeidately if missing cols happen, dont even push
# ['error', 'pad_null'] # for T2, but does not make sense if you error one and allow another.
# ['error', 'error']  for T2 fixed schema and refeshable from t1

# for pyarrow file defined schema:
# ['evolve', 'pad_null'] # for t1 to make sure data comes in always, and do not lose data
# ['evolve', 'error'] # contradicts both, evolution will make older era fail in production.
# # T1 will never use drop what right so that was a nonsense thought!

# ['drop', 'pad_null'] for T2 fixed schema and refeshable from t1, but do not want to stop job if missing cols, just warn
# ['drop', 'error']  for T2 fixed schema and refeshable from t1, but stop job immeidately if missing cols happen, dont even push
# ['error', 'pad_null'] # for T2, but does not make sense if you error one and allow another.
# ['error', 'error']  for T2 fixed schema and refeshable from t1 
# # thats provided i have a T2 fixed schema! # hence yes, i have to have T2 fixed schema too for arrow. T1 doesnt need.

