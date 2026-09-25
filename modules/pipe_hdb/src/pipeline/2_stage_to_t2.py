###### STAGE  ######
from pathlib import Path
import sys
import argparse
from datetime import date

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

## NEW ###
# ── PICK THE PROVIDER — the only environment branch in this file ─────────────
if IS_DATABRICKS:
    from providers.databricks import add_provider_args, provider_overwrite_spark_engine, togg_read, dispatch_write
elif IS_LOCAL:
    from providers.local import add_provider_args, provider_overwrite_spark_engine, togg_read, dispatch_write, preprovision_local_jvm_spark

from reusable_functions.helper_transit import sortcount


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────

thisMonth = date.today().strftime('%Y-%m')
print(f'this month is: {thisMonth}')

print('show sys.argv')
print(sys.argv)
parser = argparse.ArgumentParser()
parser.add_argument('--startMonth', default=thisMonth)
parser.add_argument('--endMonth',   default=thisMonth)
parser.add_argument('--spark_engine', choices=['sail', 'java'], default='sail')
parser.add_argument('--write_format', choices=['parquet', 'iceberg', 'delta'], default='parquet')
add_provider_args(parser)                # databricks: --catalog/--schema/--volume ; local: --env
args = parser.parse_args()

# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    args.startMonth = '2022-09'
    args.endMonth = '2022-10'
    # args.spark_engine = 'sail'
    args.spark_engine = 'java'
    # args.write_format = 'parquet'
    # args.write_format = 'iceberg'
    args.write_format = 'delta'
    # print(args)

# resolve the engine ONCE, here: provider_overwrite_spark_engine() returns 'java' on databricks, passes
# --spark_engine through locally. below this line args.spark_engine IS the live engine.
args.spark_engine = provider_overwrite_spark_engine(args.spark_engine)


print(args)


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────
ORIGIN, DATASET = 'datagov', 'resale_flat_prices'
SOURCE_TIER, TIER = 't1', 't2'
# sub: HERE I DONT PUT CATALOG, so choocse the same dont catalog in script 1, thy did i catalog?


# ── START SPARK ───────────────────────────────────────────────────────────────
# preprovision_local_jvm_spark MUST run before get_spark(): jars/catalogs only load at
# JVM boot time, never at session-adopt time - see providers.local's own docstring for why.
# covers the READ too: t1 is read in the same --write_format this run writes t2 in.
if IS_LOCAL:
    if args.write_format in ('iceberg', 'delta') and args.spark_engine == 'java':
        preprovision_local_jvm_spark(args.env, args.write_format)

from sparkutils.getspark import get_spark, stop_spark
spark = get_spark('hdb_stage', args.spark_engine)
from sparkutils.functions import col, lit, when, to_date, year



############### READ t1 + WINDOW ###############
# t1 is keyed by `tx_monthdate` (a DATE, always the 1st of the month). filter on
# it so the engine prunes to just the months asked for. because every value is
# day-01, an inclusive between on {month}-01 bounds is exact - the end month's own
# value is {endMonth}-01, which the upper bound includes.
lo = to_date(lit(f'{args.startMonth}-01'))
hi = to_date(lit(f'{args.endMonth}-01'))

# ONE format per run: t1 is read in the same format t2 is written in (script 1 wrote t1 in it too)
t1_src = togg_read(spark, args, tier=SOURCE_TIER, origin=ORIGIN, dataset=DATASET, write_format=args.write_format)

t1 = (
    t1_src
    .filter(col('tx_monthdate').between(lo, hi))
    .withColumn('tx_monthdate', to_date(col('tx_monthdate')))   # sail reads the partition as string; force DATE so all engines match
)

n_in = t1.count()
print(f'[read] {n_in:,} rows from t1 in {args.startMonth}..{args.endMonth}')
if n_in == 0:
    stop_spark(spark)
    raise SystemExit(f'no rows in {args.startMonth}..{args.endMonth}')


############### CAST TYPE ###############
# t1 keeps every column string; casting to real types is silver's job.
typed = (
    t1
    .withColumn('floor_area_sqm', col('floor_area_sqm').cast('double'))
    .withColumn('resale_price', col('resale_price').cast('double'))
    .withColumn('lease_commence_date', col('lease_commence_date').cast('long'))
)


############### DERIVE NEW COLUMNS ###############
derived = (
    typed
    .withColumn('tx_year', year(col('tx_monthdate')))
    .withColumn('covid', when(col('tx_year') >= 2021, lit('after 2021')).otherwise(lit('before 2021')))
    .withColumn('age_sold', col('tx_year') - col('lease_commence_date'))
    .withColumn('remaining_lease_sold', 99 - (col('tx_year') - col('lease_commence_date')))
    .withColumn('pretend_top_2025', 2025 - (col('tx_year') - col('lease_commence_date')))
)
derived.printSchema()

# sortcount(derived, 'town')
# sortcount(derived, 'flat_type')
# sortcount(derived, 'tx_year')
# sortcount(derived, ['town', 'street_name'], 100)

############### SELECT ###############
t2 = (
    derived
    .select(
        'tx_year', 'tx_monthdate', 'covid', 'flat_type', 'resale_price',
        'age_sold', 'remaining_lease_sold', 'pretend_top_2025',
        'street_name', 'storey_range', 'town',
    )
    .orderBy(col('tx_monthdate').desc())
)
t2.printSchema()


############### WRITE ###############


# for testing schema evolution
# if args.write_format in ['iceberg', 'parquet']:
#     OVERWRITE_KEY = ['era', 'tx_monthdate']

# if args.write_format == 'delta':    
#     OVERWRITE_KEY = ['tx_monthdate']

OVERWRITE_KEY = ['tx_monthdate']


dispatch_write(
    t2, tier=TIER, origin=ORIGIN, dataset=DATASET,
    write_format=args.write_format,
    overwrite_keys=OVERWRITE_KEY,
    spark=spark, args=args, show_partitions=True)


stop_spark(spark)


print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
