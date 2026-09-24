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
        parser.add_argument('--env', default=ENV, choices=('dev', 'production'),
                             help="which lakehouse to target - defaults to the ENV env var (itself defaulting to 'dev')"),
        # parser.add_argument('--something_else', ...),
    ]
    print('args_added:', [a.option_strings[0] for a in added])



def provider_overwrite_spark_engine(requested):
    """Local honours whatever --spark_engine asked for (sail or java) - pass it through.
    NOT a no-op like add_provider_args: the script needs a real engine string back."""
    return requested

def preprovision_local_iceberg_spark(env):
    """Kept for 2_stage_to_t2.py's existing call site - iceberg-only case of
    preprovision_local_jvm_spark below."""
    preprovision_local_jvm_spark(env, {'iceberg'})


def preprovision_local_jvm_spark(env, formats):
    """
    Note: arg env is needed as the catalog path differs by environment
    ONE builder for every JVM table format this run needs (iceberg and/or delta).

    WHY ONE, not one function per format: jars/extensions/catalogs only load when the JVM
    boots - the FIRST getOrCreate(). A second getOrCreate() adopts that running session and
    silently ignores its own spark.jars.packages, so a separate delta call after the iceberg
    one would leave delta missing. VERIFIED 2026-09-24: iceberg (own catalog) + delta
    (DeltaCatalog as spark_catalog) coexist in one session.
    """

    from pyspark.sql import SparkSession

    formats = set(formats)
    unknown = formats - {'iceberg', 'delta'}
    if unknown:
        raise ValueError(f'preprovision_local_jvm_spark: no JVM setup for {sorted(unknown)}')

    packages, extensions = [], []
    builder = SparkSession.builder

    if 'iceberg' in formats:
        from helper_pyiceberg_io import ICEBERG_CATALOG_NAME as MY_CATALOG

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
    if 'delta' in formats:
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

    print(f'spins up a {sorted(formats)} embedded spark without assigning to the spark namespace, \
    which will be picked up later by getspark, to add further configs')

    builder.getOrCreate()



def get_landing_dir(args, origin, dataset):
    """Where 0_land_csv.py drops the raw CSVs (and 1_import_to_t1.py reads them).
    """
    return str(get_lakehouse_root_from_env(args.env) / 'landing' / origin / dataset)



_KNOWN_FORMATS = ('parquet', 'iceberg', 'delta')


def _parse_format(write_format):
    """ONE table format per run (decided 2026-09-24) - a comma list is refused, not split.
    Two formats in one run are two separate commits with no rollback between them: if the
    second fails the first has landed, and the copies silently disagree."""
    fmt = write_format.strip()
    if ',' in fmt:
        raise Exception(f"one write format per run, got {write_format!r}. Run once per format instead. No write at all.")
    if fmt not in _KNOWN_FORMATS:
        raise Exception(f"invalid format {write_format!r}. Choose one of {_KNOWN_FORMATS}. No write at all.")
    return fmt


def dispatch_write(data, *, tier, origin, dataset, write_format, partition_keys, span_col=None, show_partitions=False,
               on_newcols='evolve', on_missingcols='pad_null',
               bounds=None, spark, args):

    '''
    1. Which ONE destination to write to (format x engine) - one format per run
    2. Handling more than one frame
    3. Deciding what order to write frames in
    4. Unifying table write addresses from tier, origin, dataset - all under
       get_lakehouse_root_from_env(args.env) now, following the readme's Warehouse ROOT >
       Catalog > Tier > Table convention for BOTH formats, not just iceberg.

    partition_keys : physical layout - used by parquet + iceberg only.
    span_col       : the DATE column the delta replaceWhere window is built on - used by
                     delta only (delta is unpartitioned, see helper_deltalake_io). Required
                     when write_format='delta'. Kept apart from partition_keys on
                     purpose: before, delta took partition_keys[-1] by position.
    '''

    engine = args.spark_engine           # universal flag, same for every provider
    lakehouse_root = get_lakehouse_root_from_env(args.env)   # provider-specific: local only has .env, databricks has .catalog instead

    fmt = _parse_format(write_format)
    if fmt == 'delta' and span_col is None:
        raise Exception("delta needs span_col= (the date column its overwrite window is built on). No write at all.")
    if fmt == 'delta' and engine != 'java':
        raise Exception(f"delta is JVM-only locally for now (helper_sparkdelta_io) - got spark_engine={engine!r}. "
                        f"The Sail path (helper_deltalake_io write) is not built yet. No write at all.")

    frames  = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]

    # widest schema first - the frame with the most columns establishes the
    # destination's full width on write #1, so every later (narrower) frame
    # just needs on_missingcols='pad_null'.
    # len(df.columns) - schema only; the old df.toArrow() collected every era just to count columns.
    frames = sorted(frames, key=lambda df: -len(df.columns))

    # ---------- resolve the ONE destination ----------

    if fmt == 'parquet':
        import helper_pyarrow_io
        PARQUET_DIR = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')

    elif fmt == 'iceberg' and engine == 'sail':
        import helper_pyiceberg_io
        ICEBERG_WAREHOUSE = lakehouse_root / 'iceberg' / CATALOG_NAME   # the Catalog layer lives IN the warehouse path itself for iceberg
        ICEBERG_NAMESPACE = tier
        ICEBERG_TBL_NAME  = f'{origin}__{dataset}'
        ICEBERG_FQN       = f'{ICEBERG_NAMESPACE}.{ICEBERG_TBL_NAME}'
        catalog = helper_pyiceberg_io.getOrCreate_catalog(ICEBERG_WAREHOUSE)

    elif fmt == 'iceberg' and engine == 'java':
        import helper_sparkiceberg_io
        from helper_pyiceberg_io import ICEBERG_CATALOG_NAME
        ICEBERG_FQN = f'{ICEBERG_CATALOG_NAME}.{tier}.{origin}__{dataset}'

    elif fmt == 'delta':          # engine == 'java' guaranteed above
        import helper_sparkdelta_io
        # addressed by path, no catalog registration - see helper_deltalake_io docstring
        DELTA_FQN = f"delta.`{lakehouse_root / 'delta' / CATALOG_NAME / tier / f'{origin}__{dataset}'}`"

    # ---------- write each frame to that ONE destination ----------

    for df in frames:

        if fmt == 'parquet':
            helper_pyarrow_io.write_partition_guarded(
                df.toArrow(), PARQUET_DIR, partition_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
            print(f'DONE:  parquet -> {PARQUET_DIR}')

        elif fmt == 'iceberg' and engine == 'sail':
            helper_pyiceberg_io.write_partition_guarded(
                df.toArrow(), catalog, ICEBERG_FQN, partition_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
            print(f'DONE:  iceberg -> {ICEBERG_FQN}')

        elif fmt == 'iceberg' and engine == 'java':
            helper_sparkiceberg_io.write_partition_guarded(
                df, spark, ICEBERG_FQN, partition_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
            print(f'DONE:  iceberg -> {ICEBERG_FQN}')

        elif fmt == 'delta':
            # the window (bounds, else this frame's own min..max span_col) is built inside
            # helper_sparkdelta_io.build_span, at write time - same call as databricks.py makes
            helper_sparkdelta_io.write_partition_guarded(
                df, spark, DELTA_FQN, span_col, bounds=bounds,
                on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
            # no DONE print here - helper_sparkdelta_io prints its own (with the write mode)

        # if fmt == 'delta' and engine == 'sail':
        #     # defer
        #     helper_deltalake_io.write_partition_guarded(
        #         df.toArrow(), catalog, ICEBERG_FQN, partition_keys,
        #         on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
        #     print(f'DONE:  iceberg -> {ICEBERG_FQN}')



def read_tier(spark, args, *, tier, origin, dataset, fmt='parquet', span_col=None, bounds=None):
    """Read a persisted tier back as a Spark DataFrame. `fmt` defaults to 'parquet'
    (the canonical local copy, and the only one Sail can read directly); pass
    fmt='iceberg' to read the pyiceberg copy instead (scan -> arrow -> createDataFrame,
    also Sail-safe). `args.env` selects dev vs production, same as dispatch_write.

    span_col + bounds : delta only, both or neither. Reads just the window
    span_col BETWEEN {bounds[0]}-01 AND {bounds[1]}-01 (the same window dispatch_write
    overwrites), pushed down to delta's file stats. Neither = the whole table.
    """
    if (span_col is None) != (bounds is None):
        raise ValueError(f'read_tier: pass span_col AND bounds together, or neither - got span_col={span_col!r}, bounds={bounds!r}')
    if span_col is not None and fmt != 'delta':
        raise NotImplementedError(f'read_tier: a span_col/bounds window is delta-only for now, got fmt={fmt!r}')

    import helper_pyarrow_io
    import helper_pyiceberg_io

    lakehouse_root = get_lakehouse_root_from_env(args.env)

    if fmt == 'parquet':
        parquet_dir = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')
        return helper_pyarrow_io.read(spark, parquet_dir)
    if fmt == 'iceberg':
        iceberg_catalog = helper_pyiceberg_io.getOrCreate_catalog(lakehouse_root / 'iceberg' / CATALOG_NAME)
        return helper_pyiceberg_io.sail_read_iceberg(
            spark, iceberg_catalog, table_fqn=f'{tier}.{origin}__{dataset}')

    # claude undigested
    if fmt == 'delta':
        # JVM only (needs DeltaCatalog from preprovision_local_jvm_spark); Sail read not built yet
        import helper_sparkdelta_io
        df = helper_sparkdelta_io.read(
            spark, f"delta.`{lakehouse_root / 'delta' / CATALOG_NAME / tier / f'{origin}__{dataset}'}`")
        if span_col is not None:
            from sparkutils.functions import F
            lo, hi = f'{bounds[0]}-01', f'{bounds[1]}-01'
            print(f'[delta] read window {span_col} {lo}..{hi}')
            df = df.filter(F.expr(f"{span_col} >= DATE'{lo}' AND {span_col} <= DATE'{hi}'"))
        return df

    raise SystemExit(f"read_tier fmt must be 'parquet', 'iceberg' or 'delta', got {fmt!r}")
