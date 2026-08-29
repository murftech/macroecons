"""Spark port of 0_import_datagov.py.

Same pipeline, same output columns - polars swapped for Spark so the script can
run unchanged on Glue / Dataproc / Databricks later.

Two structural differences from the polars version, both forced by Spark:

1. THE FETCH IS STILL PLAIN PYTHON. Spark has no https:// filesystem - it reads
   file://, s3a://, gs://, abfss:// and friends, but not a web URL. So the CSV
   is downloaded to local disk first, then handed to spark.read.csv(). That is
   also what the cloud version will look like: land the raw file in object
   storage, then point Spark at it.

2. THE PARQUET OUTPUT IS A DIRECTORY, not a file. polars' write_parquet() makes
   one file; Spark writes one part-*.parquet per partition plus a _SUCCESS
   marker. Anything reading it must point at the directory, not a file inside.
"""

import os
import shutil
from pathlib import Path
from inspect import getsource
import requests

# get explicit spark fucntions
# import pyspark.sql.functions as F
# import pyspark.sql.types as T
# from pyspark.sql.functions import col, lit, when, to_date, year


# ── start spark ───────────────────────────────────────────────────────────────

from helper_getspark import get_spark
# from modules.pipe_hdb_spark.src.helper_getspark import get_spark
spark = get_spark()

from helper_sparkutils import F, T, col, lit, when, to_date, year, bround, count, median
# from pyspark.sql.functions import only use this line further if i need something new. Also i would not want to edit the git repoed stuff.


# ── fetch ─────────────────────────────────────────────────────────────────────

DATASET_ID = 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'

# Anchored to THIS FILE, not the working directory. A bare relative path like
# 'hive/t2/...' lands wherever you happened to cd to - run from the repo root and
# it writes there, run from here and it writes here. On Glue / Dataproc /
# Databricks the working directory is the platform's choice, not yours, so a bare
# relative path is effectively undefined.
#
# The env override is the seam for the cloud version: set HDB_DATA_ROOT to
# s3://bucket/hdb or gs://bucket/hdb and nothing in the code below changes.
# Same pattern as flight_prices' FLIGHT_ICEBERG_ROOT.
# __file__ exists when Python runs a file - including IPython's %run - but NOT
# when you paste these lines into a REPL. Fall back to the working directory so
# the same line works both ways.
_HERE = Path(__file__).resolve().parent if '__file__' in globals() else Path.cwd()


def _repo_root(start: Path) -> Path:
    """Walk up until a .git directory turns up - the same trick startup_pip.sh
    used in shell (`while [ ! -d .git ]; do cd ..; done`). Beats counting
    .parent.parent.parent, which breaks the moment the module is moved or
    nested one level deeper."""
    for candidate in [start, *start.parents]:
        if (candidate / '.git').is_dir():
            return candidate
    return start        # not in a repo (a container, a tarball) - stay put


DATA_ROOT = os.environ.get('HDB_DATA_ROOT', str(_repo_root(_HERE) / 'hive/t2'))
RAW_CSV = Path(DATA_ROOT) / 'raw_datagovhdb.csv'
OUT_PARQUET = f'{DATA_ROOT}/datagovhdb_spark'
print(f'data root: {DATA_ROOT}')


def download_hdb_csv(hdb_dataset_id, dest: Path):
    """data.gov.sg hands out a short-lived signed URL, it does not serve the CSV
    directly. Two requests: one to mint the URL, one to pull the bytes."""
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
# inferSchema makes Spark read the file twice - once to work out types, once for
# real. Fine at this size; on a big input you would declare the schema instead.
# The two casts mirror the polars version's schema_overrides: without them these
# arrive as strings or longs depending on what the sample rows looked like.

total_data = (
    spark.read.csv(str(RAW_CSV), header=True, inferSchema=True)
    .withColumn('floor_area_sqm', col('floor_area_sqm').cast('double'))
    .withColumn('resale_price', col('resale_price').cast('double'))
)

print(f'loaded {total_data.count():,} rows, {len(total_data.columns)} columns')


# ── helper ────────────────────────────────────────────────────────────────────

def sortcount(df, groupcols, showtop=10):
    """polars' `count / count.sum()` has no direct Spark equivalent: an
    aggregate over the WHOLE result is a window with no partition key, and
    Spark warns loudly about that ('No Partition Defined for Window operation')
    because it funnels every row through one partition.

    So take the grand total with a plain count() and divide by a literal. One
    extra job, no window, no warning - and the unpartitioned window would have
    collected to a single partition anyway, so nothing is lost.
    """
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
# to_date with an explicit 'yyyy-MM' pattern, not inference. Under Spark 4 ANSI
# mode a value that does not match RAISES instead of silently becoming null -
# which is what you want: a bad month column should stop the run, not quietly
# produce a table full of nulls.

hdbdata = (
    total_data
    .withColumn('tx_monthdate', to_date(col('month'), 'yyyy-MM'))
    .withColumn('tx_year', year(to_date(col('month'), 'yyyy-MM')))
)

hdbdata.printSchema()          # polars .glimpse() split in two: schema...
hdbdata.show(5, truncate=False)  # ...and a peek at the rows

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

# polars' .shape is free; Spark has no row count until something counts, so this
# is a real job over the data.
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
# mode('overwrite') because without it a second run fails with
# AnalysisException: path already exists. Spark refuses by default rather than
# clobbering - the opposite of polars' write_parquet.
#
# coalesce(1) forces a single part file, matching the polars output shape. Drop
# it for real data: it funnels every row through one task, which is exactly the
# bottleneck Spark exists to avoid.

hdb_sel.coalesce(1).write.mode('overwrite').parquet(OUT_PARQUET)
print(f'wrote {OUT_PARQUET}/ (a directory of part files, not a single file)')

spark.stop()
