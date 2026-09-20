"""Local / docker provider - MasterETL/{ENV}/lakehouse, plain file IO.

ENV is resolved per-call via args.env (see add_provider_args -> --env), not a
module constant - lets a script override dev/production per-invocation
instead of only via the shell's ENV var, without needing that var set
beforehand.
"""
import os
from pathlib import Path

# seeds --env's default only (see add_provider_args) - ENV=production python
# script.py ... still works with no flag; args.env is the real source of
# truth from here on.
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


def get_landing_dir(args, origin, dataset):
    """Where 0_land_csv.py drops the raw CSVs (and 1_import_to_t1.py reads them).
    """
    return str(get_lakehouse_root_from_env(args.env) / 'landing' / origin / dataset)


_KNOWN_FORMATS     = {'parquet', 'delta', 'iceberg'}
_LOCAL_SUPPORTED   = {'parquet', 'iceberg'}    # delta is databricks-only (managed table, needs JVM + a catalog)


def _parse_formats(write_format):
    formats = {f.strip() for f in write_format.split(',')}
    if formats - _KNOWN_FORMATS:
        raise SystemExit(f"unknown --write_format {sorted(formats - _KNOWN_FORMATS)}; known: parquet, delta, iceberg")
    if formats - _LOCAL_SUPPORTED:
        raise SystemExit(f"local writes parquet + iceberg only; {sorted(formats - _LOCAL_SUPPORTED)} is databricks-only "
                         f"(delta = a managed catalog table)")
    return formats


def dispatch_write(data, *, tier, origin, dataset, write_format, partition_keys,
               on_newcols='evolve', on_missingcols='pad_null',
               bounds=None, spark=None, args=None):

    '''
    1. Which engine(s) to write to at all
    2. Handling more than one frame
    3. Deciding what order to write frames in
    4. Unifying table write addresses from tier, origin, dataset - all under
       get_lakehouse_root_from_env(args.env) now, following the readme's Warehouse ROOT >
       Catalog > Tier > Table convention for BOTH formats, not just iceberg.
    '''

    lakehouse_root = get_lakehouse_root_from_env(args.env)
    formats = _parse_formats(write_format)
    frames  = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]

    # widest schema first - the frame with the most columns establishes the
    # destination's full width on write #1, so every later (narrower) frame
    # just needs on_missingcols='pad_null'.
    frames = sorted(frames, key=lambda df: -len(df.toArrow().schema.names))

    if 'parquet' in formats:
        import helper_pyarrow_io
        PARQUET_DIR = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')

    if 'iceberg' in formats:
        import helper_pyiceberg_io
        ICEBERG_WAREHOUSE = lakehouse_root / 'iceberg' / CATALOG_NAME   # the Catalog layer lives IN the warehouse path itself for iceberg
        ICEBERG_NAMESPACE = tier
        ICEBERG_TBL_NAME  = f'{origin}__{dataset}'
        ICEBERG_FQN       = f'{ICEBERG_NAMESPACE}.{ICEBERG_TBL_NAME}'
        iceberg_catalog = helper_pyiceberg_io.getOrCreate_catalog(ICEBERG_WAREHOUSE)
        iceberg_catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)

    # ---------- parquet dir + pyiceberg, engine-free, per frame ----------
    period_col = partition_keys[-1]      # finest grain = the time axis (month span + all_months)
    total_rows, all_months = 0, set()
    for df in frames:
        arrow_native = df.toArrow()                     # per-frame columns - for iceberg (table evolves)
        months = sorted({str(v) for v in arrow_native.column(period_col).to_pylist()})
        total_rows += arrow_native.num_rows
        all_months.update(months)
        print(f'[{months[0]}..{months[-1]}] {arrow_native.num_rows:,} rows, {len(months)} months')

        if 'parquet' in formats:
            helper_pyarrow_io.write_partition_guarded(
                arrow_native, PARQUET_DIR, partition_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols)
            print(f'DONE:  parquet -> {PARQUET_DIR}')

        if 'iceberg' in formats:
            helper_pyiceberg_io.write_partition_guarded(
                arrow_native, iceberg_catalog, ICEBERG_FQN, partition_keys,
                on_newcols=on_newcols, on_missingcols=on_missingcols)
            print(f'DONE:  iceberg -> {ICEBERG_FQN}')

    return total_rows, sorted(all_months)


def read_tier(spark, args, *, tier, origin, dataset, fmt='parquet'):
    """Read a persisted tier back as a Spark DataFrame. `fmt` defaults to 'parquet'
    (the canonical local copy, and the only one Sail can read directly); pass
    fmt='iceberg' to read the pyiceberg copy instead (scan -> arrow -> createDataFrame,
    also Sail-safe). `args.env` selects dev vs production, same as dispatch_write.
    """
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
    raise SystemExit(f"read_tier fmt must be 'parquet' or 'iceberg', got {fmt!r}")
