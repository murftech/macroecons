"""Databricks version of modules/pipe_hdb/0_import_datagov.py.

What changed and why:
  - Same polars fetch + clean + enrich logic as before, byte-for-byte.
  - The only real change is the LAST step: instead of `hdb_sel.write_parquet('hive/t2/datagovhdb')`
    writing to a folder you had to remember the convention for, this writes to a governed
    Delta table (catalog.schema.hdb_silver) in Unity Catalog. That gets you a schema,
    ACLs, lineage, and time travel for free - none of which a parquet file on disk gives you.
  - polars stays the tool for the actual data wrangling (this pipeline was deliberately
    polars-only, not pyspark) - Spark is only used at the very end, as the bridge that
    writes into Unity Catalog. That boundary is the one genuinely new concept here.
  - One Databricks-specific wrinkle: spark_python_task files run through an internal
    exec() wrapper, so `__file__` isn't defined the way it would be for a normal
    `python script.py` run - a known Databricks Asset Bundles gotcha. Fix: the job
    passes this script its own source directory explicitly as --src-dir (see
    resources/hdb_pipeline_job.yml, using the bundle's own ${workspace.root_path})
    instead of the script trying to infer its location from __file__.
"""

import argparse
import sys

from polars import col, lit, when


def sortcount(df, groupcols, showtop=10):
    matrix = (df.group_by(groupcols)
        .len()
        .rename({'len': 'count'})
        .with_columns((col('count') / col('count').sum()).alias('pcnt'))
        .sort('count', descending=True)
    )
    matrix.show(showtop)
    return matrix


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--src-dir', required=True)
    # demo only: nothing downstream uses this. the job declares run_date with a DYNAMIC
    # default, {{job.start_time.iso_date}}, so a scheduled run supplies its own date with
    # nobody typing anything - and an override at Run-now time replaces it for that run.
    parser.add_argument('--run-date', required=True)
    args = parser.parse_args()

    print('demo that job parameter has been digested')
    print(f'  --run-date received: {args.run_date}')

    sys.path.append(args.src_dir)
    from helper_hdb import fetch_hdb_data

    target_table = f"{args.catalog}.{args.schema}.hdb_silver"

    # ── load data ────────────────────────────────────────────────────────────
    total_data = fetch_hdb_data('d_8b84c4ee58e3cfc0ece0d773c8ca6abc')

    hdbdata = total_data.with_columns([
        col('month').str.to_date('%Y-%m').alias('tx_monthdate'),
        col('month').str.to_date('%Y-%m').dt.year().alias('tx_year'),
    ])

    hdbdata.glimpse()

    hdbdata = hdbdata.with_columns(
        when(col('tx_year') >= 2021)
        .then(lit('after 2021'))
        .otherwise(lit('before 2021'))
        .alias('covid')
    )

    sortcount(hdbdata, 'town')
    sortcount(hdbdata, 'flat_type')
    sortcount(hdbdata, 'floor_area_sqm')
    sortcount(hdbdata, 'lease_commence_date')
    sortcount(hdbdata, 'tx_year')

    hdbdata = hdbdata.with_columns([
        (col('tx_year') - col('lease_commence_date')).alias('age_sold'),
        (99 - (col('tx_year') - col('lease_commence_date'))).alias('remaining_lease_sold'),
        (2025 - (col('tx_year') - col('lease_commence_date'))).alias('pretend_top_2025'),
    ])

    print(hdbdata.shape)

    hdb_sel = (
        hdbdata
        .select(['tx_year', 'tx_monthdate', 'covid', 'flat_type', 'resale_price',
                 'age_sold', 'remaining_lease_sold', 'pretend_top_2025',
                 'street_name', 'storey_range', 'town'])
        .sort('tx_monthdate', descending=True)
    )

    sortcount(hdb_sel, 'street_name', 100)
    sortcount(hdb_sel, 'town', 100)
    sortcount(hdb_sel, ['town', 'street_name'], 100)
    sortcount(hdb_sel, 'flat_type', 100)

    # ── the one part that's genuinely new: write into Unity Catalog ────────────
    # spark is auto-available in any job task running on a Databricks cluster/serverless
    # compute - no separate setup needed, unlike on your laptop.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    spark_df = spark.createDataFrame(hdb_sel.to_pandas())
    (
        spark_df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
    )

    print(f'Wrote {hdb_sel.shape[0]:,} rows to {target_table}')


if __name__ == '__main__':
    main()
