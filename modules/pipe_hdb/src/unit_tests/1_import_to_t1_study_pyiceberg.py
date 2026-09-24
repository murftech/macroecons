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


if not era_dfs:
    stop_spark(spark)
    raise SystemExit('nothing written - every requested era was missing or empty')



##### here we will play with io #######

era_dfs
# era_dfs is a dict now, keyed by era - no more list-index confusion
era_dfs['1990_1999'].show()





##################################################################################################
############# PYICEBERG ##############
##################################################################################################


# sub put these in helper functions somewhere


# import sparkutils.getspark as m
# m.__file__


import importlib, helper_pyiceberg_io
importlib.reload(helper_pyiceberg_io)
from helper_pyiceberg_io import (
    getOrCreate_catalog,
    custom_describe_catalog,
    createOrEvolve_table,
    double_safe_purge,
    write_partition_guarded,
    sail_read_iceberg
)


def listimport(import_name):
    import inspect
    print([n for n, _ in inspect.getmembers(import_name, inspect.isfunction)])
listimport(helper_pyiceberg_io)

#### assume already ran helper_pyiceberg_provision.py

##### load catalog #####
catalog = getOrCreate_catalog('datalake/iceberg')
custom_describe_catalog(catalog)

#### define the new table destination in catalog ####

partition_cols = ['tx_monthdate']
data_out = era_dfs['2015_2016']
# data_out = era_dfs['1990_1999']
data_out.show()
data_out_arrow = data_out.toArrow()


SCHEMA_CONTRACT = era_dfs['2017_onwards'].toArrow().schema

# what namespaces(schema) can i define to?
catalog.list_namespaces()


# define by table
createOrEvolve_table(data_out_arrow, partition_cols, catalog, 't1', 'resale_flat_prices')

# define by schema
createOrEvolve_table(SCHEMA_CONTRACT, partition_cols, catalog, 't1', 'hellotable')

# define to another namespace
createOrEvolve_table(data_out_arrow, partition_cols, catalog, 't2', 'hellotable')

# define with no partition
# createOrEvolve_table(data_out_arrow, [], catalog, 't1', 'resale_flat_prices')

print('created tables')
custom_describe_catalog(catalog)

#### complete delete tables and data ####
double_safe_purge?
double_safe_purge(catalog, 't2.hellotable')
double_safe_purge(catalog, 't1.hellotable')
double_safe_purge(catalog, 't1.resale_flat_prices')

print('no more tables')
custom_describe_catalog(catalog)

#### runtime write call ####
createOrEvolve_table(data_out_arrow, partition_cols, catalog, 't1', 'resale_flat_prices')
# comment this out in production. how to people actually evolve table.
# write_partition_guarded?
write_partition_guarded(data_out_arrow, catalog, 't1.resale_flat_prices', partition_cols)
# write_partition_guarded(data_out_arrow, catalog, 't1.resale_flat_prices', partition_cols, full_refresh=True)

# sub: the write partition with properly defined specs. is gone :( please write it nicely again.
# sub the show partitions so lame wtf? get it to follow arrows' please...
# SUB: inside the create and evolve, i have not understand the syntax of add_fileds and identity trawsnforms etc
# i have to do it

# sub maybe, create a write_snapshot fucntion. i think it should be its simple function so i never have to write []
# this one is snapshow style data. with rollback


##### read back the table #####

# bare mechanisms for study:
tbl = catalog.load_table('t1.resale_flat_prices')
type(tbl)
type(tbl.scan())
type(tbl.scan().to_arrow())
type(spark.createDataFrame(tbl.scan().to_arrow()))
load_table = spark.createDataFrame(tbl.scan().to_arrow())
load_table.show()


sail_read_iceberg??

resale_flat_prices = sail_read_iceberg(spark, catalog, 't1.resale_flat_prices')
resale_flat_prices.show()

######## Ops BAU DONE ######!!!



################################################
#### schema evolutions and guards firing #####
################################################
type(tbl)

# tbl.append(arrow_table)

# tbl.metadata_location          # which file is actually current
# tbl.history()                  # snapshot list, parsed
# tbl.inspect.snapshots()        # → a pandas DataFrame, no manual JSON reading needed
# tbl.inspect.files()            # → same, for the manifest/data-file layer — decodes the avro for


#### actual real life writing now. wihtiout align and schema evoluaiont ####

# next do puposely schema evolutions. like drop col add col, add one partition key
# and see they all fire. and see the schema_id change


live = {f.file.file_path for f in tbl.scan().plan_files()}
all_files = glob.glob(f'{table_path}/data/**/*.parquet', recursive=True)
# anything in all_files but not in live = something got replaced this session



#### learning point of partitioning
import pyarrow.compute as pc

era_values = pc.unique(arrow_table.column('era')).to_pylist()
monthdate_values = pc.unique(arrow_table.column('tx_monthdate')).to_pylist()

from pyiceberg.expressions import EqualTo, In, And
iceberg_replace_func = And(
    In('era', era_values),
    In('tx_monthdate', monthdate_values),
)

# this does nothing if the parition was not specified into the load.table
tbl.overwrite(
    df=arrow_table,
    overwrite_filter=iceberg_replace_func
    )
# once it writes what does it look like????
# every write emits one json, which later we have to clean up
# every write emits one more .parquet
# Every write emits nb jsons equals to its nb paritions. exaclty one file in each folder leaf

# i solved the issue, the parition_list
# iceberg does not take parittion_list. i think delta doesnt too later we will check.
# there parittioning have to be like this. using condiions/
# so also if i just decided to change era to towns then what is stopping me?
# it just messes up the deletes.


# lern the minila for these too:
# tbl.append(arrow_table)
# tbl.delete(In(partition_col, months)) # this is the actual write

# only two matters, snapshot_properties is maybe only

# snapshot_properties={'source': 'run.sh import', 'era': era}
# ah thus is like my runtime shit. the businessdate.



##### whats next? #####

# where is the rollback actually? what are actually iceberg things? but only after i learn the write partition.


# import sqlite3
# con = sqlite3.connect(CATALOG_PATH)
# for row in con.execute('SELECT * FROM iceberg_namespace_properties'):
#     print(row)



# --- the bare pyiceberg call, no wrapper ---

import pyarrow.compute as pc
months = pc.unique(arrow_table.column(partition_col)).to_pylist()
months
# print('months:', sorted(str(m) for m in months))


# target = era_dfs['1990_1999'].withColumn('extra', lit('die'))
target = era_dfs['1990_1999'].drop('block')
# target = era_dfs['1990_1999'].withColumn('floor_area_sqm', col('floor_area_sqm').cast('double'))
# partition_cols = ['era', 'tx_monthdate']
# partitions = partitioning[0]
target = era_dfs['2015_2016']
# target = era_dfs['2015_2016'].drop('block')
target.show()
arrow_table = target.toArrow()


###########


# ## learn evolution , version control and snapshotting and rollback later

# # mark it, at any point you trust the state:
# tbl.manage_snapshots().create_tag(
#     snapshot_id=tbl.current_snapshot().snapshot_id,
#     tag_name='last_known_good'
# ).commit()

# # later, from anywhere, anytime - just the name:
# good = tbl.snapshot_by_name('last_known_good')
# tbl.manage_snapshots().rollback_to_snapshot(good.snapshot_id).commit()


# catalog.list_tables('t1')
# catalog.list_tables('t2')


# # do i ever need these?
# # metadata_uid = catalog.load_table('t1.resale_flat_prices').metadata_location
# # metadata_uid
# # catalog.list_tables('t1')

# import re
# from pathlib import Path

# metadata_json_path = catalog.load_table('t1.resale_flat_prices').metadata_location
# metadata_json_path
# # 00016-32e68a5c-5026-4e68-8832-93893de5246d
# # Ver: 16
# # commit_uuid: 32e68a5c-5026-4e68-8832-93893de5246d

# import json
# with open(tbl.metadata_location.replace('file://', '')) as f:
#     raw = json.load(f)
# raw['schemas']            # same info, but as raw dicts straight from disk
# # raw['partition-specs']

# parse_metadata_location(metadata_json_path)

# tbl.metadata_location
# parse_metadata_location('file://datalake/iceberg2/t1/resale_flat_prices/metadata/00016-32e68a5c-5026-4e68-8832-93893de5246d.metadata.json')

# def parse_metadata_location(raw: str) -> dict:
#     label, sep, uri = raw.partition(': ')
#     if not sep or '://' not in uri:
#         uri, label = raw, None

#     scheme, _, path = uri.partition('://')
#     p = Path(path)

#     filename = p.name
#     m = re.match(r'(\d+)-([0-9a-f-]+)\.metadata\.json$', filename)
#     version = int(m.group(1)) if m else None
#     commit_uuid = m.group(2) if m else None

#     table_path = p.parent.parent   # up from metadata/<file> to the table root
#     table_name = table_path.name
#     namespace = table_path.parent.name

#     return {
#         'label': label,
#         'scheme': scheme,
#         'path': str(p),
#         'metadata_filename': filename,
#         'version': version,
#         'commit_uuid': commit_uuid,
#         'table_path': str(table_path),
#         'namespace': namespace,
#         'table_name': table_name,
#     }

####### DROP TABLE #######
# SHOULD rarely be used
# catalog.drop_table('t1.resale_flat_prices')
# # catalog.list_tables('t1')
# # # if i know drop table without registering it back, it will be nonsense, and leave orhaned files that i must manually filesystem clean up later
# # # i should only drop if i simply dont want it in my catalog
# # catalog.register_table('t1.resale_flat_prices',metadata_uid)
# # catalog.list_tables('t1')
##############

# just dont ever run this



#################### now we must understand how write_tier cooples everything ####
# Anyway i have write_icerberg to master next
# should i have a write_java? i think shouldnt becasue anything can run in laptop is not big data


# if there is no need to wrap into write_tier please DO NOT just remove it i dont want triple layers. where i am confused two years later
# But wait i do this becasue of iceberg vs arrow vs delta, stupid shhit
# So i'd rather name it, write cloud then?
# togg_write?





# # ── WRITE t1 — provider owns the fork: files under datalake/ locally, managed
# #    catalog tables on databricks. bounds=None -> window is derived from the eras. ──

era_dfs


# write_tier(era_dfs, tier=TIER, origin=ORIGIN, dataset=DATASET,
#            write_format=args.write_format, part_cols=[COMPUTED_PARTITION],
#            columns_contract=COLUMNS_CONTRACT, bounds=None, spark=spark, args=args)

Three things it tries to policy:
# 1) ENVIRONMENT: DATABRICKS/local
# 2) write_format
# 3) forcing a catalog, schema, tablename definition
# 4) is just simply what write_dataset does

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
