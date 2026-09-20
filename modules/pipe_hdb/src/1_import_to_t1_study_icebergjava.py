###### IMPORT  ######
from pathlib import Path
import sys, os
import argparse

# must be set before ANY SparkSession is created (driver vs worker Python
# version mismatch otherwise breaks any DataFrame operation that runs a
# Python worker) - verified needed this exact way during helper_sparkcatalog_io's
# own validation run this session.
os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

## NEW ###
# ── PICK THE PROVIDER — the only environment branch in this file ─────────────
if IS_DATABRICKS:
    from providers.databricks import add_provider_args, provider_overwrite_spark_engine, get_landing_dir, dispatch_write
elif IS_LOCAL:
    from providers.local import add_provider_args, provider_overwrite_spark_engine, get_landing_dir, dispatch_write


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
add_provider_args(parser)                # databricks: --catalog/--schema/--volume ; local: --env
args = parser.parse_args()
print(args)


# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    args.spark_engine = 'java'
    # args.spark_engine = 'sail'
    # args.write_format = 'parquet'
    # args.eras = '1990_1999,2000_2012Feb,2015_2016'
    args.eras = '2015_2016'
    # args.eras = 'all'
    args.write_format = 'iceberg'
    # args.write_format = 'parquet,iceberg'
    # print(args)

# resolve the engine ONCE, here: provider_overwrite_spark_engine() returns 'java' on databricks, passes
# --spark_engine through locally. below this line args.spark_engine IS the live engine.
args.spark_engine = provider_overwrite_spark_engine(args.spark_engine)


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
# helper_sparkcatalog_io needs a REAL Iceberg-JVM catalog - a Spark session
# with the iceberg-spark-runtime JAR + a local Hadoop catalog configured. That
# has to happen BEFORE get_spark() ever runs: JAR loading only takes effect at
# JVM startup, and get_spark() itself already knows how to ADOPT a
# pre-existing session rather than creating a fresh one (see its own
# "Spark session already provided by environment" branch) - so pre-configuring
# it here, then calling get_spark() as normal, needs zero edits to sparkutils.
# Only do this for engine='java' + write_format containing 'iceberg' - Sail
# can't load JVM JARs at all, and there's no reason to pay the JAR-download
# cost for a run that isn't going to touch helper_sparkcatalog_io.
ICEBERG_WAREHOUSE_JVM = 'datalake/iceberg_jvm'   # deliberately separate from datalake/iceberg (the pyiceberg SqlCatalog) - two independent catalogs, zero collision risk even if a table name repeats
if args.spark_engine == 'java' and 'iceberg' in args.write_format.split(','):
    from pyspark.sql import SparkSession
    (
        SparkSession.builder
        .appName('1_import_to_t1')
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.local_iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.local_iceberg.type', 'hadoop')
        .config('spark.sql.catalog.local_iceberg.warehouse', ICEBERG_WAREHOUSE_JVM)
        .getOrCreate()
    )

from sparkutils.getspark import get_spark, stop_spark
spark = get_spark('1_import_to_t1', args.spark_engine)
from sparkutils.functions import col, lit, when, to_date, year, F


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────────────

ORIGIN, DATASET, TIER = 'datagov', 'resale_flat_prices', 't1_jvm'

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

### agreed shape check if write tier followed this at all. ###
# # ── WRITE t1 — provider owns the fork: files under datalake/ locally, managed
# #    catalog tables on databricks. bounds=None -> window is derived from the eras. ──

# # we will study write tier in prod:

# data_new = era_dfs['2015_2016']
# data_old = era_dfs['1990_1999']

# data_new.printSchema()
# data_old.printSchema()

# data_target = data_new
# data_target = data_old
# partition_cols = ['tx_monthdate']


# # full run best squence needed by iceberg
# import importlib, helper_pyiceberg_io
# importlib.reload(helper_pyiceberg_io)
# from helper_pyiceberg_io import (
#     getOrCreate_catalog,
#     custom_describe_catalog,
#     # createOrEvolve_table,
#     # double_safe_purge,
#     write_partition_guarded,
#     sail_read_iceberg,
#     schema_to_code
# )

# catalog = getOrCreate_catalog('datalake/iceberg')
# custom_describe_catalog(catalog)
# # tier = 't1'
# # origin = 'datagov'
# # tbl_name = 'resale_flat_prices'
# # tbl_fqn = tier + '.' + 'origin' + '__' + tbl_name
# # tbl_fqn
# catalog.load_table('t1.datagov__resale_flat_prices')

# # write_partition_guarded?

# write_partition_guarded(

#     data_old.toArrow(), catalog, 't1.datagov__resale_flat_prices', partition_cols,
#     on_newcols='evolve', on_missingcols='pad_null')
# write_partition_guarded(
#     data_new.toArrow(), catalog, 't1.datagov__resale_flat_prices', partition_cols,
#     on_newcols='evolve', on_missingcols='pad_null')

# custom_describe_catalog(catalog)

# # full run best squence needed by arrow
# import importlib, helper_pyarrow_io
# importlib.reload(helper_pyarrow_io)
# from helper_pyarrow_io import write_partition, write_partition_guarded, read, list_partitions   # re-bind — REQUIRED

# CATALOG_PATH  = 'datalake/hive/'
# TIER_PATH  = 'datalake/hive/t1'
# PATH_TO_TABLE = 'datalake/hive/t1/datagov__resale_flat_prices'
# # write_partition_guarded?

# # import shutil
# # shutil.rmtree(PATH_TO_TABLE)
# write_partition_guarded(data_old.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')
# write_partition_guarded(data_new.toArrow(), PATH_TO_TABLE, partition_cols, show_partitions=False, on_newcols='evolve', on_missingcols='pad_null')


# ── STUDY: exercise helper_sparkcatalog_io directly, bypassing dispatch_write ──
# dispatch_write doesn't route java-engine iceberg writes to helper_sparkcatalog_io
# yet (that wiring is a deliberate, separate, not-yet-done integration step) -
# so this calls it directly, same "study" pattern as the commented-out block
# above did for helper_pyiceberg_io before dispatch_write existed at all.

import importlib, helper_sparkcatalog_io
importlib.reload(helper_sparkcatalog_io)
from helper_sparkcatalog_io import create_table, write_partition_guarded

partition_keys = [COMPUTED_PARTITION]
FQN = f'local_iceberg.{TIER}.{ORIGIN}__{DATASET}'

spark.sql(f'CREATE NAMESPACE IF NOT EXISTS local_iceberg.{TIER}')

# widest schema first, same reasoning dispatch_write itself uses: the first
# write into an empty table defines its width, so later (narrower) eras only
# ever need on_missingcols='pad_null', never on_newcols territory.
ordered_eras = sorted(era_dfs.values(), key=lambda df: -len(df.columns))

if not spark.catalog.tableExists(FQN):
    create_table(ordered_eras[0], spark, FQN, partition_keys)
    ordered_eras = ordered_eras[1:]   # that first frame is already written by create_table

for df in ordered_eras:
    write_partition_guarded(df, spark, FQN, partition_keys,
                            on_newcols='evolve', on_missingcols='pad_null')

print(f'DONE: {FQN} now has {spark.table(FQN).count():,} rows')


# ── EXIT ─────────────────────────────────────────────

stop_spark(spark)

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
