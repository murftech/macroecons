"""Local / docker provider
"""

import argparse
import os
from pathlib import Path

# run on terminal, run.sh or make with ENV=production it will read and write to production lakehouse.
# without, default string is dev
ENV = os.environ.get('ENV', 'dev')

CATALOG_NAME = 'macroecons'

def get_lakehouse_root_from_env(env):
    if env not in ('dev', 'production'):
        raise ValueError(f"env must be 'dev' or 'production', got {env!r}")

    resolved_lakehouse = Path(f'/Users/murftech/Root/MasterETL/{env}/lakehouse')
    return resolved_lakehouse


def add_provider_args(parser):
    print('\n\n')
    added = [
        parser.add_argument('--env', default=ENV, choices=('dev', 'production'), help="which lakehouse to target - defaults to the ENV env var (itself defaulting to 'dev')"),
        # parser.add_argument('--something_else', ...),
    ]
    print('args_added:', [a.option_strings[0] for a in added])



def provider_overwrite_spark_engine(requested):
    """Local honours whatever --spark_engine asked for (sail or java) - pass it through.
    NOT a no-op like add_provider_args: the script needs a real engine string back."""
    return requested

def preprovision_local_jvm_spark(env, write_format: str):

    from pyspark.sql import SparkSession

    if write_format not in ('iceberg', 'delta'):
        raise ValueError(f'preprovision_local_jvm_spark: no JVM setup for {write_format!r}')

    packages, extensions = [], []
    builder = SparkSession.builder

    if write_format == 'iceberg':
        from lakehouse_io.helper_pyiceberg_io import ICEBERG_CATALOG_NAME as MY_CATALOG

        iceberg_warehouse = get_lakehouse_root_from_env(env) / 'iceberg' / CATALOG_NAME
        catalog_db_path = iceberg_warehouse / '_icebergcatalog.db'

        # from apache iceberg documentaion page: https://iceberg.apache.org/docs/latest/jdbc/#configurations >>
        # catalog, warehouse (path), type (jdbc), uri
        # spark-sql --packages org.apache.iceberg:              iceberg-spark-runtime-3.5_2.12:1.11.0 \
        #     --conf spark.sql.catalog.my_catalog =             org.apache.iceberg.spark.SparkCatalog \
        #     --conf spark.sql.catalog.my_catalog.warehouse=    s3://my-bucket/my/key/prefix \
        #     --conf spark.sql.catalog.my_catalog.type=         jdbc \
        #     --conf spark.sql.catalog.my_catalog.uri=          jdbc:mysql://test.1234567890.us-west-2.rds.amazonaws.com:3306/default
        # sub, optional test what happens, or ask claude if each of this line goes missing, and why. actually doesnt matter just follow

        # choose packaged jars from internet
        groupId = 'org.apache.iceberg' # fixed string
        artifactId = 'iceberg-spark-runtime-4.0_2.13' # 4.0_2.13; forced by pip pyspark version,
        version='1.10.0' # 1.10.x
        packages.append(f'{groupId}:{artifactId}:{version}')

        groupId = 'org.xerial' # fixed string
        artifactId = 'sqlite-jdbc' # fixed string
        version='3.46.1.0' # how to choose?
        packages.append(f'{groupId}:{artifactId}:{version}')

        builder = (builder
            .config(f'spark.sql.catalog.{MY_CATALOG}',          'org.apache.iceberg.spark.SparkCatalog')
            .config(f'spark.sql.catalog.{MY_CATALOG}.warehouse', f'file://{iceberg_warehouse}')
            .config(f'spark.sql.catalog.{MY_CATALOG}.type',      'jdbc')
            .config(f'spark.sql.catalog.{MY_CATALOG}.uri',       f'jdbc:sqlite:{catalog_db_path}')
            )

    # claude undigested
    if write_format == 'delta':
        # no warehouse/uri config at all: delta tables are addressed by path
        # (delta.`<abs path>`), there is no catalog to point at - see helper_deltalake_io.
        groupId = 'io.delta' # fixed string
        artifactId = 'delta-spark_2.13' # _2.13 = scala build, same as pyspark 4.0.x
        version='4.0.1' # delta 4.0.x <-> spark 4.0.x; delta 4.1+ targets spark 4.1
        packages.append(f'{groupId}:{artifactId}:{version}')
        extensions.append('io.delta.sql.DeltaSparkSessionExtension')
        # DeltaCatalog can ONLY be the session catalog (it wraps spark_catalog), unlike
        # iceberg's which gets its own name - this is what makes delta.`path` resolve.
        # sub where is the documentation for this, and it doesnt require as many things as iceberg???
        builder = builder.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

    builder = builder.config('spark.jars.packages', ','.join(packages))
    if extensions:
        builder = builder.config('spark.sql.extensions', ','.join(extensions))

    print(f'spins up a {write_format!r} embedded spark without assigning to the spark namespace, \
    which will be picked up later by getspark, to add further configs')

    builder.getOrCreate()



def get_landing_dir(args, origin, dataset):
    """Where 0_land_csv.py drops the raw CSVs (and 1_import_to_t1.py reads them).
    """
    return str(get_lakehouse_root_from_env(args.env) / 'landing' / origin / dataset)


def dispatch_write(
    data, *, tier, origin, dataset, write_format: str, 
    overwrite_keys: list, show_partitions=False,
    on_newcols='evolve', on_missingcols='pad_null',
    spark, args):

    '''
    1. Which ONE destination to write to (format x engine) - one format per run
    2. Handling more than one frame
    3. Deciding what order to write dataframes in
    4. Unifying table write addresses from tier, origin, dataset.
    5. Vallidating overwrite key formats according to write_format

    '''

    # unzip provider branched args
    engine = args.spark_engine
    lakehouse_root = get_lakehouse_root_from_env(args.env)   # provider-specific: local only has .env, databricks has .catalog instead

    ##### write a lists of datadataframes OR just one dataframe ###
    dataframes  = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]
    
    # widest schema first - the frame with the most columns establishes the
    # destination's full width on write #1, so every later (narrower) frame just needs on_missingcols='pad_null'.
    dataframes = sorted(dataframes, key=lambda df: -len(df.columns))

    ############ parquet ##############

    if write_format == 'parquet':
        from lakehouse_io import helper_pyarrow_io
        PARQUET_DIR = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')
        
        for df in dataframes:
            helper_pyarrow_io.write_partition_guarded(
                df.toArrow(), PARQUET_DIR,  partition_keys = overwrite_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)

    ############ iceberg ##############
    if write_format == 'iceberg' and engine == 'sail':
            from lakehouse_io import helper_pyiceberg_io
            ICEBERG_WAREHOUSE = lakehouse_root / 'iceberg' / CATALOG_NAME   # the Catalog layer lives IN the warehouse path itself for iceberg
            ICEBERG_NAMESPACE = tier
            ICEBERG_TBL_NAME  = f'{origin}__{dataset}'
            ICEBERG_FQN       = f'{ICEBERG_NAMESPACE}.{ICEBERG_TBL_NAME}'
            catalog = helper_pyiceberg_io.getOrCreate_catalog(ICEBERG_WAREHOUSE)

            for df in dataframes:

                helper_pyiceberg_io.write_partition_guarded(
                df.toArrow(), catalog, ICEBERG_FQN, partition_keys = overwrite_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)

    if write_format == 'iceberg' and engine == 'java':
            from lakehouse_io import helper_sparkiceberg_io
            from lakehouse_io.helper_pyiceberg_io import ICEBERG_CATALOG_NAME
            ICEBERG_FQN = f'{ICEBERG_CATALOG_NAME}.{tier}.{origin}__{dataset}'

            for df in dataframes:

                helper_sparkiceberg_io.write_partition_guarded(
                    df, spark, ICEBERG_FQN, partition_keys = overwrite_keys,
                    on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)

    ############ delta ##############
    if write_format == 'delta' and engine == 'java':
            from lakehouse_io import helper_sparkdelta_io
            # addressed by path, no catalog registration - see helper_deltalake_io docstring
            DELTA_FQN = f"delta.`{lakehouse_root / 'delta' / CATALOG_NAME / tier / f'{origin}__{dataset}'}`"

            for df in dataframes:

                helper_sparkdelta_io.write_span_guarded(
                    df, spark, DELTA_FQN, span_key = overwrite_keys,
                    on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)




def togg_read(spark, args, *, tier, origin, dataset, write_format='parquet', span_col=None, bounds=None):
    """Read a persisted tier back as a Spark DataFrame. `write_format` defaults to 'parquet'
    (the canonical local copy, and the only one Sail can read directly); pass
    write_format='iceberg' to read the pyiceberg copy instead (scan -> arrow -> createDataFrame,
    also Sail-safe). `args.env` selects dev vs production, same as dispatch_write.

    span_col + bounds : delta only, both or neither. Reads just the window
    span_col BETWEEN {bounds[0]}-01 AND {bounds[1]}-01 (the same window dispatch_write
    overwrites), pushed down to delta's file stats. Neither = the whole table.
    """
    if (span_col is None) != (bounds is None):
        raise ValueError(f'togg_read: pass span_col AND bounds together, or neither - got span_col={span_col!r}, bounds={bounds!r}')
    if span_col is not None and write_format != 'delta':
        raise NotImplementedError(f'togg_read: a span_col/bounds window is delta-only for now, got write_format={write_format!r}')

    from lakehouse_io import helper_pyarrow_io
    from lakehouse_io import helper_pyiceberg_io

    lakehouse_root = get_lakehouse_root_from_env(args.env)

    if write_format == 'parquet':
        parquet_dir = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')
        return helper_pyarrow_io.read(spark, parquet_dir)

    if write_format == 'iceberg':
        iceberg_catalog = helper_pyiceberg_io.getOrCreate_catalog(lakehouse_root / 'iceberg' / CATALOG_NAME)
        return helper_pyiceberg_io.sail_read_iceberg(
            spark, iceberg_catalog, table_fqn=f'{tier}.{origin}__{dataset}')

    # claude undigested
    if write_format == 'delta':
        # JVM only (needs DeltaCatalog from preprovision_local_jvm_spark); Sail read not built yet
        from lakehouse_io import helper_sparkdelta_io
        df = helper_sparkdelta_io.read(
            spark, f"delta.`{lakehouse_root / 'delta' / CATALOG_NAME / tier / f'{origin}__{dataset}'}`")
        if span_col is not None:
            from sparkutils.functions import F
            lo, hi = f'{bounds[0]}-01', f'{bounds[1]}-01'
            print(f'[delta] read window {span_col} {lo}..{hi}')
            df = df.filter(F.expr(f"{span_col} >= DATE'{lo}' AND {span_col} <= DATE'{hi}'"))
        return df

    raise SystemExit(f"togg_read write_format must be 'parquet', 'iceberg' or 'delta', got {write_format!r}")
