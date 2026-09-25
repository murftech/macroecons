"""Databricks provider - managed Unity Catalog DELTA tables for the tiers, a UC Volume
path for the landing zone. Env args (--catalog/--schema/--volume) come from the job
JSON task parameters.

DELTA ONLY (decided 2026-09-24). On this workspace a `USING iceberg` managed table turned
out to be a Delta table + UniForm underneath (verified via table properties:
delta.enablemanagedicebergtable / universalFormat.enabledFormats=iceberg, format=DELTA),
so "iceberg on databricks" only adds an Iceberg face for OUTSIDE engines - nothing
outside Databricks reads these tables, so plain Delta. If an external Iceberg reader
ever appears, switch UniForm on for that one table (ALTER TABLE ... SET TBLPROPERTIES) -
no rewrite, no code change here.

Tables (medallion = schema-per-layer; origin folded into the name):
    {catalog}.{tier}.{origin}__{dataset}     e.g. macroecons.t1.datagov__resale_flat_prices
    (double underscore - matches providers.local's naming; unified 2026-09-25, was
    single underscore before)
Provisioned explicitly by deploy_databricks/databricks_table_management.sh `create` -
never implicitly by a write (same contract as local's provisioning). Catalog/schema/
volume provisioning is a separate script, databricks_provision.sh (scope split 2026-09-25).

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


def get_landing_dir(args, origin, dataset):
    """A UC Volume path - a real POSIX path BOTH this process and Spark can see
    (matters on Spark Connect, where the client and server may not share a disk).
    The Volume itself is the landing zone, so just origin/dataset under it."""
    return f'/Volumes/{args.catalog}/{args.schema}/{args.volume}/{origin}/{dataset}'


def _require_delta(write_format):
    """argparse already restricts --write_format to ONE of parquet/iceberg/delta; this adds
    what databricks accepts: delta only. parquet is the scripts' DEFAULT, so a job that
    forgets --write_format lands here and is refused loudly rather than half-running."""
    if write_format != 'delta':
        raise SystemExit(f"databricks writes managed DELTA tables only - got --write_format {write_format!r} "
                         f"(parquet is local-only; iceberg is not used on databricks, see module docstring)")


def _fqn(args, tier, origin, dataset):
    # double underscore - matches providers.local's f'{origin}__{dataset}' naming
    # (2026-09-25, was single underscore, unified to stop local/Databricks table
    # names disagreeing). The OLD single-underscore table
    # (e.g. macroecons.t1.datagov_resale_flat_prices) is a separate, now-orphaned
    # table under this rename - not automatically migrated.
    return f'{args.catalog}.{tier}.{origin}__{dataset}'


def dispatch_write(data, *, tier, origin, dataset, write_format, overwrite_keys,
                   show_partitions=False, spark, args):
    """Union the frame(s) and write ONE managed Delta table via helper_sparkdelta_io -
    the same call providers.local makes for delta, only the table name differs.

    `data`           : one Spark DataFrame, a list of them, or a dict of them (per-era,
                 for bronze - keys are ignored, only .values() is used) - unioned here.
    `overwrite_keys` : a single string column name - unified 2026-09-24 (was `span_col`,
                 with an unused `partition_keys` kept only for call-site parity with
                 providers.local). Everything here is delta, so this MUST be exactly one
                 column - delta's IN-list overwrite is single-column only (LOCKED
                 2026-09-24); only dates actually present in the unioned data are ever
                 replaced.
    """
    from functools import reduce
    from lakehouse_io import helper_sparkdelta_io

    _require_delta(write_format)
    if not isinstance(overwrite_keys, str):
        raise Exception(f"constraint delta write allowed to filter IN on only ONE column. "
                        f"Lists or multiple lists are not allowed. Provide a string.")
    span_col = overwrite_keys

    frames = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]
    df_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    print(f'[union] {len(frames)} frame(s) -> {df_all.count():,} rows')

    # T1's own stated priority is never losing data - same defaults as local's writes.
    helper_sparkdelta_io.write_span_guarded(
        df_all, spark, _fqn(args, tier, origin, dataset), [span_col],   # write_span_guarded now takes a list (LOCKED shape: exactly one)
        show_partitions=show_partitions, on_newcols='evolve', on_missingcols='pad_null')


def togg_read(spark, args, *, tier, origin, dataset, write_format='delta', span_col=None, bounds=None):
    """Read a tier's managed Delta table back as a Spark DataFrame. The caller passes
    write_format = its own --write_format (read in the format written) - anything but delta is refused.

    span_col + bounds : both or neither - reads just the window dispatch_write overwrites
    (span_col BETWEEN {bounds[0]}-01 AND {bounds[1]}-01), same contract as providers.local.
    """
    from lakehouse_io import helper_sparkdelta_io

    _require_delta(write_format)
    if (span_col is None) != (bounds is None):
        raise ValueError(f'togg_read: pass span_col AND bounds together, or neither - got span_col={span_col!r}, bounds={bounds!r}')

    df = helper_sparkdelta_io.read(spark, _fqn(args, tier, origin, dataset))
    if span_col is not None:
        from sparkutils.functions import F
        lo, hi = f'{bounds[0]}-01', f'{bounds[1]}-01'
        print(f'[delta] read window {span_col} {lo}..{hi}')
        df = df.filter(F.expr(f"{span_col} >= DATE'{lo}' AND {span_col} <= DATE'{hi}'"))
    return df
