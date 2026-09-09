###### IMPORT  ######
from pathlib import Path
import sys; sys.path.append('.')
from datetime import date

# import modules.pipe_hdb.src.helper_transit as ht
# [n for n in dir(ht) if not n.startswith('_')]
from modules.pipe_hdb.src.helper_transit import sortcount


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────

thisMonth = date.today().strftime('%Y-%m')
print(f'this month is: {thisMonth}')

print('show sys.argv')
print(sys.argv)
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--startMonth', default=thisMonth)
parser.add_argument('--endMonth',   default=thisMonth)
parser.add_argument('--spark_engine', choices=['sail', 'java'], default='sail')
parser.add_argument('--write_format', choices=['hive', 'iceberg', 'both'], default='both')
args = parser.parse_args()
print(args)


# ── INLINE OVERWRITES ───────────────────────────────────────────────────────────

IN_IPYTHON = 'IPython' in sys.modules
if IN_IPYTHON:
    args.startMonth = '2022-09'
    args.endMonth = '2022-10'
    args.spark_engine = 'sail'
    args.write_format = 'hive'
    # args.write_format = 'iceberg'
    # args.write_format = 'both'
    # print(args)


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────
SCHEMA, DATASET = 'datagov', 'resale_flat_prices'

# read from t1 (bronze: every column string, month-partitioned)
HIVE_T1 = f'datalake/hive/t1/{SCHEMA}/{DATASET}'

# write to t2 (silver: typed + derived + business shape)
HIVE_T2         = f'datalake/hive/t2/{SCHEMA}/{DATASET}'
ICEBERG_T2_NAME = f't2.{SCHEMA}__{DATASET}'                   # catalog identity: catalog.schema__table
ICEBERG_T2_PATH = f'datalake/iceberg/t2/{SCHEMA}/{DATASET}'   # where the files physically go


# ── START SPARK ───────────────────────────────────────────────────────────────

from sparkutils.getspark import get_spark, stop_spark
spark = get_spark('hdb_stage', args.spark_engine)
from sparkutils.functions import col, lit, when, to_date, year, F



############### READ t1 + WINDOW ###############
# t1 is partitioned by `tx_monthdate` (a DATE, always the 1st of the month).
# filter on it so Spark prunes to just the months asked for. because every value
# is day-01, an inclusive between on {month}-01 bounds is exact - the end month's
# own partition value is {endMonth}-01, which the upper bound includes.
lo = to_date(lit(f'{args.startMonth}-01'))
hi = to_date(lit(f'{args.endMonth}-01'))

t1 = (
    spark.read.parquet(HIVE_T1)
    .filter(col('tx_monthdate').between(lo, hi))
    .withColumn('tx_monthdate', to_date(col('tx_monthdate')))   # sail reads the partition as string; force DATE so both engines match
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
derived.printSchema()
t2.printSchema()

############### WRITE ###############
# collect once: .toArrow() runs the plan and pulls every row to the driver as a
# pyarrow Table. nothing past this needs spark - both writers are engine-free and
# behave identically on sail and java. each does delete+append per tx_monthdate:
# only the months in this window are replaced, other months are kept.
arrow_table = t2.toArrow()

months_in = sorted({str(v) for v in arrow_table.column('tx_monthdate').to_pylist()})
print(f'[collected] {arrow_table.num_rows:,} rows, tx_monthdate partitions: {months_in}')

if args.write_format in ['hive', 'both']:
    import modules.pipe_hdb.src.helper_pyarrow_io as helper_pyarrow_io
    helper_pyarrow_io.write_partitioned(arrow_table, HIVE_T2, 'tx_monthdate')
    print(f'DONE:  hive    -> {HIVE_T2}')

if args.write_format in ['iceberg', 'both']:
    import modules.pipe_hdb.src.helper_iceberg_io as helper_iceberg_io
    helper_iceberg_io.replace_partitions(
        arrow_table, 'tx_monthdate', table_fqn=ICEBERG_T2_NAME, location=ICEBERG_T2_PATH)
    print(f'DONE:  iceberg -> {ICEBERG_T2_PATH}')


stop_spark(spark)

# ── STATUS  ───────────────────────────────────────────────────────────────
print('=' * 60)
print(f'  ran: startMonth={args.startMonth}  endMonth={args.endMonth}  '
      f'engine={args.spark_engine}  format={args.write_format}')
print(f'  out: {arrow_table.num_rows:,} rows | partitions {months_in}')
print('=' * 60)

