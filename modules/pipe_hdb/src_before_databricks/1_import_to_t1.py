###### IMPORT  ######
from pathlib import Path
import sys; sys.path.append('.')

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
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--spark_engine', choices=['sail', 'java'], default='sail')
parser.add_argument('--write_format', choices=['hive', 'iceberg', 'both'], default = 'both')
parser.add_argument('--eras', default='2017_onwards',
                    help="comma-separated era keys or 'all'. currently available eras: " + ','.join(ERA_CONTRACTS))

args = parser.parse_args()
print(args)


# ── INLINE OVERWRITES ───────────────────────────────────────────────────────────
IN_IPYTHON = 'IPython' in sys.modules  
if IN_IPYTHON:
    # args.spark_engine = 'java'
    args.spark_engine = 'sail'
    args.write_format = 'hive'
    # args.eras = '1990_1999,2000_2012Feb,2015_2016'
    args.eras = 'all'
    # args.write_format = 'iceberg'
    # args.write_format = 'both'
    # print(args)


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
spark = get_spark('hdb_transform', args.spark_engine)
from sparkutils.functions import col, lit, when, to_date, year, F

# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────────────
CATALOG, SCHEMA, DATASET = 't1', 'datagov', 'resale_flat_prices'

# SOURCE VARIABLES: where the landed CSVs are read from
LANDING_DIR = f'datalake/landing/{SCHEMA}/{DATASET}'

# TARGET_VARIABLES: where t1 is written
HIVE_T1            = f'datalake/hive/{CATALOG}/{SCHEMA}/{DATASET}'
ICEBERG_TABLE_NAME = f'{CATALOG}.{SCHEMA}__{DATASET}'                   # catalog identity: catalog.schema__table
ICEBERG_TABLE_PATH = f'datalake/iceberg/{CATALOG}/{SCHEMA}/{DATASET}'  # where the files physically go


# ── REQUIRED FUNCTIONS ─────────────────────────────────────────────
COMPUTED_PARTITION = 'tx_monthdate'     # the derived partition column

# import modules.pipe_hdb.src.helper_transit as ht
# [n for n in dir(ht) if not n.startswith('_')]
from modules.pipe_hdb.src.helper_transit import add_monthdate
import pyarrow as pa


def build_csv_path(era):
    # a tiny helper that builds the path to one landed CSV file, given an era key.
    return Path(LANDING_DIR) / f'{era}_{DATASET}_{ERA_CONTRACTS[era]["id"]}.csv'

wrote_any = False

############### RUN ###############
# testers
era = eras[0]
era
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

    arrow_native = df.toArrow()                     # per-era columns - for iceberg (table evolves)
    months = sorted({str(v) for v in arrow_native.column(COMPUTED_PARTITION).to_pylist()})
    print(f'[{era}] {n:,} rows, {len(months)} months {months[0]}..{months[-1]}')

    if args.write_format in ['hive', 'both']:
        # parquet manual schema evolution. via COLUMNS_CONTRACT
        arrow_hive = arrow_native
        for c in COLUMNS_CONTRACT:
            if c not in arrow_hive.column_names:
                arrow_hive = arrow_hive.append_column(c, pa.nulls(arrow_hive.num_rows, pa.string()))
        arrow_hive = arrow_hive.select([*COLUMNS_CONTRACT, COMPUTED_PARTITION])
        import modules.pipe_hdb.src.helper_pyarrow_io as helper_pyarrow_io
        helper_pyarrow_io.write_partitioned(arrow_hive, HIVE_T1, COMPUTED_PARTITION)
        print(f'  hive    -> {HIVE_T1}')

    if args.write_format in ['iceberg', 'both']:
        import modules.pipe_hdb.src.helper_iceberg_io as helper_iceberg_io
        helper_iceberg_io.replace_partitions(
            arrow_native, COMPUTED_PARTITION, table_fqn=ICEBERG_TABLE_NAME, location=ICEBERG_TABLE_PATH)
        print(f'  iceberg -> {ICEBERG_TABLE_PATH}')

    wrote_any = True

# ── EXIT ─────────────────────────────────────────────

stop_spark(spark)

if not wrote_any:
    raise SystemExit('nothing written - every requested era was missing or empty')
