"""Databricks provider - managed Unity Catalog tables (Delta + Iceberg) for the tiers,
a UC Volume path for the landing zone. Env args (--catalog/--schema/--volume) come
from the job JSON task parameters.

No import shim needed here: the script's add_src_to_path() has already put src/ on
sys.path, and argv[0] on serverless is the full workspace script path anyway.
"""


def add_provider_args(parser):
    """Supplied by the job JSON task parameters; --catalog/--schema required so a misconfigured
    job fails loudly instead of landing files somewhere unexpected."""
    added = [
        parser.add_argument('--catalog', required=True),
        parser.add_argument('--schema',  required=True),   # UC schema that holds the landing Volume
        parser.add_argument('--volume',  default='landing'),
    ]
    print('args_added:', [f'{a.option_strings[0]}={a.default!r}' for a in added])


def provider_overwrite_spark_engine(requested):
    """Databricks provides the JVM engine; a job must never spin up a pysail server."""
    return 'java'


def prepare_local_iceberg_spark(env):
    """No-op here - Unity Catalog is already the registered catalog on any Databricks
    cluster/serverless session, nothing to pre-configure. Exists only so 1_import_to_t1.py's
    shared call site (prepare_local_iceberg_spark(...), then get_spark(...)) works unmodified against
    either provider - local's version does real SparkSession.builder work before get_spark()
    adopts it; this one has nothing to do. `env` is unused here (Databricks has no --env flag
    at all - the call site passes getattr(args, 'env', None), which is always None here)."""
    return


def get_landing_dir(args, origin, dataset):
    """A UC Volume path - a real POSIX path BOTH this process and Spark can see
    (matters on Spark Connect, where the client and server may not share a disk).
    The Volume itself is the landing zone, so just origin/dataset under it."""
    return f'/Volumes/{args.catalog}/{args.schema}/{args.volume}/{origin}/{dataset}'


_KNOWN_FORMATS   = {'parquet', 'delta', 'iceberg'}
_DBX_SUPPORTED   = {'delta', 'iceberg'}    # parquet is a path format (local-only); UC managed tables are delta/iceberg


def _parse_formats(write_format):
    formats = {f.strip() for f in write_format.split(',')}
    if formats - _KNOWN_FORMATS:
        raise SystemExit(f"unknown --write_format {sorted(formats - _KNOWN_FORMATS)}; known: parquet, delta, iceberg")
    if formats - _DBX_SUPPORTED:
        raise SystemExit(f"databricks writes delta + iceberg managed tables only; {sorted(formats - _DBX_SUPPORTED)} "
                         f"is local-only (parquet = a Hive-partitioned dir, not a catalog table)")
    return formats


def dispatch_write(data, *, tier, origin, dataset, write_format, partition_keys,
               columns_contract=None, show_partitions=False, bounds=None, spark, args):
    """Union the frame(s) and write managed catalog tables, Delta and/or Iceberg.

    # TARGET (Databricks only): managed tables in the tier LAYER SCHEMA (medallion
    # convention = schema-per-layer), NOT alongside the landing Volume (that stays in
    # --schema). source is folded into the table name ({ORIGIN}_) because the layer
    # took the schema slot. delta keeps the bare name; iceberg gets the _iceberg suffix:
    #   macroecons.t1.datagov_resale_flat_prices          (Delta)
    #   macroecons.t1.datagov_resale_flat_prices_iceberg  (Iceberg)
    # a managed table is NOT a Volume path - it lands under {catalog}.{schema} as a
    # Table in UC's managed storage; read it back with spark.read.table(FQN), never by
    # path.
    #
    # 2026-09-22: iceberg now routes to helper_databricks_io.write_partition_guarded
    # (checks 0/1/3/4/5 + on_newcols/on_missingcols + explicit provisioning, never implicit -
    # see that module's own docstring for why: the real UC tables here have no partition or
    # cluster spec at all, verified via DESCRIBE TABLE EXTENDED, so helper_sparkiceberg_io's
    # overwritePartitions()-based mechanism can't be used - this file's .overwrite(span) can).
    # delta STILL calls the old, unmigrated helper_sparkiceberg_io.create_or_overwrite, which
    # does not exist any more - this branch is known-broken, deliberately out of scope for
    # this pass ("iceberg first, then clean up for delta" - the same call made for the local
    # path). Provisioning (create_table) is a separate, deliberate step for iceberg now, same
    # contract as every other guarded write in this codebase - NOT called from here.

    `data`   : one Spark DataFrame, a list of them, or a dict of them (per-era, for
               bronze - keys are ignored, only .values() is used) - unioned here.
    `bounds` : (start_month, end_month) to overwrite an exact window (silver); None to
               derive the window from the data itself (bronze - every era is a
               contiguous month block, so the min..max span is exact).
    `columns_contract` : unused here - unionByName handles column alignment.
    `show_partitions` : unused here - accepted only for call-site parity with
               providers.local.dispatch_write (1_import_to_t1.py/2_stage_to_t2.py
               pass it unconditionally to whichever provider is active; the
               databricks call site TypeError'd without this, found 2026-09-22).
               helper_databricks_io.write_partition_guarded has no
               show_partitions of its own yet.
    `partition_keys` : list; databricks does not physically partition (managed tables get
               Liquid Clustering, or here, verified, no clustering at all). Only
               partition_keys[-1] is used - the column the windowed overwrite span is built on.
    """
    from functools import reduce
    from sparkutils.functions import F

    import helper_sparkiceberg_io
    import helper_databricks_io

    formats = _parse_formats(write_format)
    frames  = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]
    df_all  = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    period_col = partition_keys[-1]      # databricks doesn't physically partition; this is
                                    # the column the replaceWhere overwrite span is built on
    data_months = sorted(str(r[0]) for r in df_all.select(period_col).distinct().collect())
    if bounds is not None:
        lo, hi = f'{bounds[0]}-01', f'{bounds[1]}-01'
    else:
        lo, hi = data_months[0], data_months[-1]
    n_out = df_all.count()
    print(f'[union] {n_out:,} rows, {period_col} {lo}..{hi}')
    span = F.expr(f"{period_col} >= DATE'{lo}' AND {period_col} <= DATE'{hi}'")

    FQN = {'delta':   f'{args.catalog}.{tier}.{origin}_{dataset}',
           'iceberg': f'{args.catalog}.{tier}.{origin}_{dataset}_iceberg'}

    if 'delta' in formats:
        # UNMIGRATED - calls a function that no longer exists (see module docstring above).
        # Deliberately left broken: out of scope for this iceberg-only pass.
        helper_sparkiceberg_io.create_or_overwrite(df_all, fqn=FQN['delta'], fmt='delta', span=span, spark=spark)

    if 'iceberg' in formats:
        # T1's own stated priority is never losing data - same defaults as the other 3 engines'
        # real production writes (helper_pyiceberg_io, helper_pyarrow_io, helper_sparkiceberg_io).
        helper_databricks_io.write_partition_guarded(
            df_all, spark, FQN['iceberg'], span, [period_col],
            on_newcols='evolve', on_missingcols='pad_null')

    return n_out, data_months


def read_tier(spark, args, *, tier, origin, dataset, fmt='delta'):
    """Read a tier table back as a Spark DataFrame. `fmt` defaults to 'delta' (the
    canonical copy, bare name); pass fmt='iceberg' for the _iceberg twin. We still
    write BOTH formats downstream regardless of which one we read here."""
    import helper_sparkiceberg_io
    suffix = '_iceberg' if fmt == 'iceberg' else ''
    return helper_sparkiceberg_io.read(spark, f'{args.catalog}.{tier}.{origin}_{dataset}{suffix}')
