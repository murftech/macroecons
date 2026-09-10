"""Databricks provider - managed Unity Catalog tables (Delta + Iceberg) for the tiers,
a UC Volume path for the landing zone. Env args (--catalog/--schema/--volume) come
from the job JSON task parameters.

No import shim needed here: the script's add_src_to_path() has already put src/ on
sys.path, and argv[0] on serverless is the full workspace script path anyway.
"""


def add_args(parser):
    """Supplied by the job JSON task parameters; --catalog/--schema required so a misconfigured
    job fails loudly instead of landing files somewhere unexpected."""
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema',  required=True)   # UC schema that holds the landing Volume
    parser.add_argument('--volume',  default='landing')


def get_spark_engine(requested):
    """Databricks provides the JVM engine; a job must never spin up a pysail server."""
    return 'java'


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


def write_tier(data, *, tier, origin, dataset, write_format, part_cols,
               columns_contract=None, bounds=None, spark, args):
    """Union the frame(s) and write managed catalog tables, Delta and/or Iceberg.

    # TARGET (Databricks only): managed tables in the tier LAYER SCHEMA (medallion
    # convention = schema-per-layer), NOT alongside the landing Volume (that stays in
    # --schema). source is folded into the table name ({ORIGIN}_) because the layer
    # took the schema slot. delta keeps the bare name; iceberg gets the _iceberg suffix:
    #   macroecons.t1.datagov_resale_flat_prices          (Delta)
    #   macroecons.t1.datagov_resale_flat_prices_iceberg  (Iceberg)
    # a managed table is NOT a Volume path - it lands under {catalog}.{schema} as a
    # Table in UC's managed storage; read it back with spark.read.table(FQN), never by
    # path. The write mechanics (create-first / replaceWhere-overwrite / align-down)
    # live in helper_catalog_io.

    `data`   : one Spark DataFrame, or a list of them (per-era, for bronze) - unioned here.
    `bounds` : (start_month, end_month) to overwrite an exact window (silver); None to
               derive the window from the data itself (bronze - every era is a
               contiguous month block, so the min..max span is exact).
    `columns_contract` : unused here - unionByName handles column alignment.
    `part_cols` : list; databricks does not physically partition (managed tables get
               Liquid Clustering). Only part_cols[-1] is used - the column the
               replaceWhere overwrite span is built on.
    """
    from functools import reduce
    from sparkutils.functions import F

    import helper_catalog_io

    formats = _parse_formats(write_format)
    frames  = data if isinstance(data, list) else [data]
    df_all  = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

    period_col = part_cols[-1]      # databricks doesn't physically partition; this is
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

    # NB: the first-ever write to each table defines its schema. run --eras all (or
    # import_2017_onwards, which carries every column) FIRST - older eras are a strict
    # column subset and only ever align DOWN in helper_catalog_io.
    for fmt in ('delta', 'iceberg'):
        if fmt in formats:
            helper_catalog_io.create_or_overwrite(df_all, fqn=FQN[fmt], fmt=fmt, span=span, spark=spark)

    return n_out, data_months


def read_tier(spark, args, *, tier, origin, dataset, fmt='delta'):
    """Read a tier table back as a Spark DataFrame. `fmt` defaults to 'delta' (the
    canonical copy, bare name); pass fmt='iceberg' for the _iceberg twin. We still
    write BOTH formats downstream regardless of which one we read here."""
    import helper_catalog_io
    suffix = '_iceberg' if fmt == 'iceberg' else ''
    return helper_catalog_io.read(spark, f'{args.catalog}.{tier}.{origin}_{dataset}{suffix}')
