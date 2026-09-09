"""Local / docker provider - relative paths under datalake/, plain file IO.

Covers IS_SH (run.sh, docker) and IS_IPYTHON (hand-run in a kernel). Both
write to the same place, so there is no split in here.
"""


def add_args(parser):
    '''stub'''


def get_spark_engine(requested):
    """Local honours whatever --spark_engine asked for (sail or java) - pass it through.
    NOT a no-op like add_args: the script needs a real engine string back."""
    return requested


def get_landing_dir(args, origin, dataset):
    """Where 0_land_csv.py drops the raw CSVs (and 1_import_to_t1.py reads them).

    Relative on purpose - run.sh cd's to the repo root before invoking the script.
    `args` is unused locally; kept for signature parity with the databricks provider.
    """
    return f'datalake/landing/{origin}/{dataset}'


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


def write_tier(data, *, tier, origin, dataset, write_format, part_col,
               columns_contract=None, bounds=None, spark=None, args=None):
    """Write one tier as files under datalake/ - parquet dir and/or pyiceberg table.

    # TARGET (local only): t1/t2 are FILES here. on Databricks they are managed
    # catalog tables instead - NOT a path - see providers/databricks.py::write_tier.

    `data`     : one Spark DataFrame, or a list of them (per-era, for bronze).
    `bounds`   : unused locally - the window is already applied by the caller, and
                 both writers delete+append per partition. kept for signature parity.
    `columns_contract` : parquet only - it has no schema evolution, so every frame is
                 padded up to this fixed column list. iceberg evolves its own schema
                 (that is the one thing it gives you here a bare parquet dir cannot),
                 so it gets the per-frame columns untouched.
    `spark` / `args` : unused locally - kept for signature parity with databricks.
    """
    import pyarrow as pa
    import helper_pyarrow_io
    import helper_iceberg_io

    formats = _parse_formats(write_format)
    frames  = data if isinstance(data, list) else [data]

    PARQUET_DIR  = f'datalake/hive/{tier}/{origin}/{dataset}'
    ICEBERG_NAME = f'{tier}.{origin}__{dataset}'                    # catalog identity: catalog.schema__table
    ICEBERG_PATH = f'datalake/iceberg/{tier}/{origin}/{dataset}'    # where the files physically go

    # ---------- parquet dir + pyiceberg, engine-free, per frame ----------
    total_rows, all_months = 0, set()
    for df in frames:
        arrow_native = df.toArrow()                     # per-frame columns - for iceberg (table evolves)
        months = sorted({str(v) for v in arrow_native.column(part_col).to_pylist()})
        total_rows += arrow_native.num_rows
        all_months.update(months)
        print(f'[{months[0]}..{months[-1]}] {arrow_native.num_rows:,} rows, {len(months)} months')

        if 'parquet' in formats:
            arrow_out = arrow_native
            if columns_contract:
                for c in columns_contract:
                    if c not in arrow_out.column_names:
                        arrow_out = arrow_out.append_column(c, pa.nulls(arrow_out.num_rows, pa.string()))
                arrow_out = arrow_out.select([*columns_contract, part_col])
            helper_pyarrow_io.write_partitioned(arrow_out, PARQUET_DIR, part_col)
            print(f'DONE:  parquet -> {PARQUET_DIR}')

        if 'iceberg' in formats:
            helper_iceberg_io.replace_partitions(
                arrow_native, part_col, table_fqn=ICEBERG_NAME, location=ICEBERG_PATH)
            print(f'DONE:  iceberg -> {ICEBERG_PATH}')

    return total_rows, sorted(all_months)


def read_tier(spark, args, *, tier, origin, dataset, fmt='parquet'):
    """Read a persisted tier back as a Spark DataFrame. `fmt` defaults to 'parquet'
    (the canonical local copy, and the only one Sail can read directly); pass
    fmt='iceberg' to read the pyiceberg copy instead (scan -> arrow -> createDataFrame,
    also Sail-safe). `args` unused locally - kept for signature parity with databricks.
    """
    import helper_pyarrow_io
    import helper_iceberg_io

    if fmt == 'parquet':
        return helper_pyarrow_io.read(spark, f'datalake/hive/{tier}/{origin}/{dataset}')
    if fmt == 'iceberg':
        return helper_iceberg_io.read(
            spark, table_fqn=f'{tier}.{origin}__{dataset}',
            location=f'datalake/iceberg/{tier}/{origin}/{dataset}')
    raise SystemExit(f"read_tier fmt must be 'parquet' or 'iceberg', got {fmt!r}")
