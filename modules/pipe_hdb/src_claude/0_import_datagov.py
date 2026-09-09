"""Step 0, ported to run unchanged on local, docker, and Databricks serverless.

Same pipeline and same output columns as 0_import_datagov.py. What changed:

1. THE PATH MACHINERY IS GONE. The old version computed a repo root by walking up
   looking for .git, with a __file__ guard and an env override. That was written
   speculatively, before there was a cloud target to test against, and it was wrong
   in both directions: in docker there is no .git, so it fell back to the SCRIPT's
   directory and wrote to /app/modules/pipe_hdb_sparkbricks/src/hive/t2 rather than
   /app/hive/t2 (which is why the Dockerfile's `mkdir -p hive/t2` never mattered),
   and on Databricks it would have done the same thing in the workspace.

   The replacement is simpler than what it replaced. There are exactly two cases:
     - Databricks: the location is not inferred at all, it ARRIVES as --catalog /
       --schema / --volume, the same way pipe_hdb_databricks does it.
     - local + docker: a bare relative path, because run_pipeline.sh cds to the repo
       root first. This is what modules/pipe_hdb has always done and it has always
       worked; the Spark port abandoned it for no reason it could name.

2. THE WRITE BRANCHES. A parquet directory locally, a governed Delta table on
   Databricks. Nothing else in the script knows which.

3. get_spark COMES FROM the rewritten getspark.py (the previous version is
   parked as getspark_original.py). That one calls
   spark.sparkContext.setLogLevel(), which does not exist on Spark Connect.

WHAT DID NOT CHANGE, deliberately: the CSV is still landed and still read by Spark.
pipe_hdb_databricks avoids the file entirely (polars reads the https URL straight
into memory, so no path is ever needed) and that would have deleted this whole
question - but it would also mean Spark is not doing the ingest, which is the one
thing this module exists to demonstrate. Landing raw and pointing compute at it is
also the shape that survives real data: it is replayable without re-hitting a
rate-limited API whose signed URL expires in minutes, and it is auditable.

NEXT REFACTOR (agreed, not done here): split the fetch from the transform into two
tasks. The fetch needs no Spark and currently pays ~3s of session startup for
nothing, and the two halves have different failure modes.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import requests


####################################
####### where am I running
####################################

# presence of --catalog/--schema IS the switch. explicit in the job JSON rather than
# sniffed from the environment, so the same script run by hand on the laptop and run
# by the scheduler on Databricks differ in exactly one visible place: the arguments.
#
# every argument is optional, so a bare `python 0_import_datagov2.py` behaves exactly
# as it did before this port - which is what keeps local and docker green.

parser = argparse.ArgumentParser()
parser.add_argument('--catalog', help='Unity Catalog catalog. Present => running on Databricks.')
parser.add_argument('--schema', help='Unity Catalog schema. Present => running on Databricks.')
parser.add_argument('--volume', default='outputs', help='UC volume that holds the landed raw CSV.')
parser.add_argument('--src-dir', help='Optional. sys.path[0] is already this directory on all '
                                      'three platforms (verified by probe.py), so this is a '
                                      'belt-and-braces override, not a requirement.')
parser.add_argument('--spark_engine', default='sail', choices=['sail', 'java'],
                     help='Which Spark engine to use.')

args = parser.parse_args()

ON_DATABRICKS = bool(args.catalog and args.schema)

if args.src_dir:
    sys.path.insert(0, args.src_dir)

# ── start spark ───────────────────────────────────────────────────────────────

from sparkutils.getspark import get_spark, stop_spark

if ON_DATABRICKS:
    spark = get_spark('hdb_ingest', engine='java')
else:
    spark = get_spark('hdb_ingest', args.spark_engine)

from sparkutils.functions import F, T, col, lit, when, to_date, year, bround, count, median





# from pyspark import SparkContext
# sc = SparkContext._active_spark_context
# SUBL is there still any use for these? uiWebUrl? etc? 80% no use
# for name in (['version', 'applicationId', 'master', 'appName', 'sparkUser',
#              'pythonVer', 'defaultParallelism', 'uiWebUrl', 'startTime']):
#     value = getattr(sc, name)
#     print(f'{name:<20} {value() if callable(value) else value}')

# ── where the data goes ───────────────────────────────────────────────────────
# the only place in this file that knows there is more than one environment.

DATASET_ID = 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'

if ON_DATABRICKS:
    # a Volume is a real POSIX path that BOTH the python process and Spark can see.
    # that matters: on Spark Connect the client and the Spark server are not
    # guaranteed to share a filesystem, so a file written next to the driver is not
    # necessarily a file spark.read.csv() can find. a Volume is the shared ground.
    DATA_ROOT = f'/Volumes/{args.catalog}/{args.schema}/{args.volume}/raw'
    TARGET_TABLE = f'{args.catalog}.{args.schema}.hdb_silver'
    OUT_PARQUET = None
else:
    # relative on purpose - run_pipeline.sh does `cd "$(dirname $0)/../.."` before
    # invoking this, so the working directory is the repo root in both local and
    # docker. same convention as modules/pipe_hdb.
    DATA_ROOT = os.environ.get('HDB_DATA_ROOT', 'hive/t2')
    TARGET_TABLE = None
    OUT_PARQUET = f'{DATA_ROOT}/datagovhdb_spark'

RAW_CSV = Path(DATA_ROOT) / 'raw_datagovhdb.csv'

print(f'[step0] on_databricks={ON_DATABRICKS}')
print(f'[step0] data root: {DATA_ROOT}')
print(f'[step0] target: {TARGET_TABLE or OUT_PARQUET}')


# ── fetch ─────────────────────────────────────────────────────────────────────

def download_hdb_csv(hdb_dataset_id, dest: Path):
    """data.gov.sg hands out a short-lived signed URL, it does not serve the CSV
    directly. Two requests: one to mint the URL, one to pull the bytes.

    Unchanged by the port. Writing to a Volume uses ordinary python file IO - the
    only difference from the local case is the string in `dest`."""
    base_url = 'https://api-open.data.gov.sg/v1/public/api/datasets'

    resp = requests.get(f'{base_url}/{hdb_dataset_id}/initiate-download')
    download_url = resp.json().get('data', {}).get('url', '')
    if not download_url:
        raise RuntimeError('API did not return a download URL.')
    print(f'download url: {download_url}')

    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(dest, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    print(f'saved {dest} ({dest.stat().st_size:,} bytes)')
    return dest


download_hdb_csv(DATASET_ID, RAW_CSV)


# ── read ──────────────────────────────────────────────────────────────────────
# inferSchema=False: everything arrives as string, typed only by the explicit
# casts below. inferSchema=True guesses a type from a SAMPLE of rows, and on this
# file that sample-then-commit approach isn't just slower (reading twice) - it's
# unsafe across engines. MEASURED 2026-09-01 on Sail 0.7.1: it sampled early
# floor_area_sqm values, decided Int64, then hit a real decimal ('60.3', row 667)
# and threw SparkRuntimeException - inside the CSV reader itself, before the
# .cast('double') below ever got a chance to run. Real JVM Spark did not throw on
# the same file, so this was a genuine engine divergence, not a data problem.
# Casting from string sidesteps it on both engines: nothing is typed until these
# explicit casts run, so there's no window for a wrong inferred type to blow up
# mid-file.
# lease_commence_date is included for the same reason - it was riding on
# inferSchema too, with no cast to catch a mismatch.

total_data = (
    spark.read.csv(str(RAW_CSV), header=True, inferSchema=False)
    .withColumn('floor_area_sqm', col('floor_area_sqm').cast('double'))
    .withColumn('resale_price', col('resale_price').cast('double'))
    .withColumn('lease_commence_date', col('lease_commence_date').cast('long'))
)

print(f'loaded {total_data.count():,} rows, {len(total_data.columns)} columns')


# ── helper ────────────────────────────────────────────────────────────────────

def sortcount(df, groupcols, showtop=10):
    """polars' `count / count.sum()` has no direct Spark equivalent: an aggregate
    over the WHOLE result is a window with no partition key, and Spark warns loudly
    about that because it funnels every row through one partition.

    So take the grand total with a plain count() and divide by a literal."""
    if isinstance(groupcols, str):
        groupcols = [groupcols]

    total = df.count()
    matrix = (
        df.groupBy(*groupcols)
        .count()
        .withColumn('pcnt', col('count') / lit(total))
        .orderBy(col('count').desc())
    )
    matrix.show(showtop, truncate=False)
    return matrix


# ── derive ────────────────────────────────────────────────────────────────────
# to_date with an explicit 'yyyy-MM' pattern, not inference. Under ANSI mode a value
# that does not match RAISES instead of silently becoming null - which is what you
# want: a bad month column should stop the run, not quietly produce a table full of
# nulls. VERIFIED on both engines by probe.py: local Spark 4.0.1 and Databricks
# serverless Spark 4.2.0 both have ANSI on and both raise here.

hdbdata = (
    total_data
    .withColumn('tx_monthdate', to_date(col('month'), 'yyyy-MM'))
    .withColumn('tx_year', year(to_date(col('month'), 'yyyy-MM')))
)

hdbdata.printSchema()
hdbdata.show(5, truncate=False)

hdbdata = hdbdata.withColumn(
    'covid',
    when(col('tx_year') >= 2021, lit('after 2021')).otherwise(lit('before 2021')),
)

sortcount(hdbdata, 'town')
sortcount(hdbdata, 'flat_type')
sortcount(hdbdata, 'floor_area_sqm')
sortcount(hdbdata, 'lease_commence_date')
sortcount(hdbdata, 'tx_year')

hdbdata = (
    hdbdata
    .withColumn('age_sold', col('tx_year') - col('lease_commence_date'))
    .withColumn('remaining_lease_sold', 99 - (col('tx_year') - col('lease_commence_date')))
    .withColumn('pretend_top_2025', 2025 - (col('tx_year') - col('lease_commence_date')))
)

print(f'shape: ({hdbdata.count():,}, {len(hdbdata.columns)})')


# ── select ────────────────────────────────────────────────────────────────────

hdb_sel = (
    hdbdata
    .select(
        'tx_year', 'tx_monthdate', 'covid', 'flat_type', 'resale_price',
        'age_sold', 'remaining_lease_sold', 'pretend_top_2025',
        'street_name', 'storey_range', 'town',
    )
    .orderBy(col('tx_monthdate').desc())
)

streetnames = sortcount(hdb_sel, 'street_name', 100)
townnames = sortcount(hdb_sel, 'town', 100)
townstreet = sortcount(hdb_sel, ['town', 'street_name'], 100)
sortcount(hdb_sel, 'flat_type', 100)


# ── write ─────────────────────────────────────────────────────────────────────

if ON_DATABRICKS:
    # a governed table, not a path. that is the whole difference: a name in Unity
    # Catalog carries a schema, access control, lineage and time travel, none of
    # which a directory of parquet files on disk has.
    #
    # overwriteSchema disables Delta's guard against a silently changing schema.
    # justified here only because mode('overwrite') already replaces the table
    # wholesale every run - there is no incremental history for a schema change to
    # corrupt. it would be the wrong setting the moment this becomes a merge.
    (
        hdb_sel.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(TARGET_TABLE)
    )
    print(f'wrote table {TARGET_TABLE}')
else:
    # mode('overwrite') because without it a second run fails with
    # AnalysisException: path already exists. Spark refuses by default rather than
    # clobbering - the opposite of polars' write_parquet.
    #
    # coalesce(1) forces a single part file, matching the polars output shape. Drop
    # it for real data: it funnels every row through one task, which is exactly the
    # bottleneck Spark exists to avoid.
    hdb_sel.coalesce(1).write.mode('overwrite').parquet(OUT_PARQUET)
    print(f'wrote {OUT_PARQUET}/ (a directory of part files, not a single file)')


stop_spark(spark)
