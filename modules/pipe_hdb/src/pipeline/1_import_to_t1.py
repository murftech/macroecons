###### IMPORT  ######
from pathlib import Path
import sys
import argparse

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

# ── PICK THE PROVIDER — the only environment branch in this file ─────────────
if IS_DATABRICKS:
    from providers.databricks import add_provider_args, provider_overwrite_spark_engine, get_landing_dir, dispatch_write
elif IS_LOCAL:
    from providers.local import add_provider_args, provider_overwrite_spark_engine, get_landing_dir, dispatch_write, ENV, preprovision_local_jvm_spark

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
parser.add_argument('--write_format', choices=['parquet', 'iceberg', 'delta'], default='parquet')
parser.add_argument('--eras', default='2017_onwards',
                    help="comma-separated era keys or 'all'. currently available eras: " + ','.join(ERA_CONTRACTS))

add_provider_args(parser)                # databricks: --catalog/--schema/--volume ; local: --env


print('\n\n')
args = parser.parse_args()

# resolve the engine ONCE, here: provider_overwrite_spark_engine() returns 'java' on DATABRICKS, passes
# --spark_engine through locally. below this line args.spark_engine IS the live engine.
args.spark_engine = provider_overwrite_spark_engine(args.spark_engine)
# sub:  to use some classes or whatever thing to make this not run

print(f'resolved args: {args}')


# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    args.spark_engine = 'java'
    # args.spark_engine = 'sail'
    # args.eras = '1990_1999,2000_2012Feb,2015_2016'
    args.eras = '1990_1999'
    # args.eras = 'all'
    # args.write_format = 'iceberg'
    
    # claude work
    args.write_format = 'delta'
    # args.write_format = 'parquet'
    print(f'overwritten final args: {args}')



print('\n\n\n ######## BEGIN LOGIC ###########')

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

if IS_LOCAL:
    # jars/catalogs only load at JVM boot - must run before get_spark(), see its docstring
    if args.write_format in ('iceberg', 'delta') and args.spark_engine == 'java':
        print(f'local jvm {args.write_format} run needs spark config (jars, extensions, catalogs) before get_spark')
        preprovision_local_jvm_spark(args.env, args.write_format)

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

from reusable_functions.helper_transit import add_monthdate

def build_csv_path(era):
    # a tiny helper that builds the path to one landed CSV file, given an era key.
    return Path(LANDING_DIR) / f'{era}_{DATASET}_{ERA_CONTRACTS[era]["id"]}.csv'

####################################
############### RUN ###############
####################################
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


# ── real production write - on_newcols/on_missingcols default to ('evolve', 'pad_null')


# for testing schema evolution
# if args.write_format in ['iceberg', 'parquet']:
#     OVERWRITE_KEY = ['era', 'tx_monthdate']

# if args.write_format == 'delta':    
#     OVERWRITE_KEY = ['tx_monthdate']

OVERWRITE_KEY = ['tx_monthdate']


dispatch_write(era_dfs, tier=TIER, origin=ORIGIN, dataset=DATASET,
           write_format=args.write_format, overwrite_keys=OVERWRITE_KEY,
           show_partitions=False, spark=spark, args=args)


# ── EXIT ─────────────────────────────────────────────

stop_spark(spark)

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
