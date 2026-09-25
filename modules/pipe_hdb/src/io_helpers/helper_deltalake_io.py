"""Provision local Delta tables with delta-rs (the `deltalake` package) - no JVM, no Spark.
Same role helper_pyiceberg_io plays for iceberg: provisioning lives here, never
implicitly inside a write. The JVM write/read path is helper_sparkdelta_io.

THE ONE BIG DIFFERENCE FROM ICEBERG - there is no catalog file.
Iceberg needs _icebergcatalog.db to map a name -> metadata location. A Delta table
carries its own metadata in <table>/_delta_log, so the DIRECTORY LAYOUT IS the catalog:

    {full_warehouse_path}/{namespace}/{tbl_name}/_delta_log/...
    e.g. .../lakehouse/delta/macroecons/t1/datagov__resale_flat_prices

Spark addresses the same table by path:  delta.`<absolute table path>`  (see
table_path_fqn below) - nothing to register, nothing forgotten between sessions.

TABLE FEATURES - kept at delta-rs defaults on purpose: protocol (1,2), no features.
VERIFIED 2026-09-24 (deltalake 1.6.5 + delta-spark 4.0.1): a table created here is
read AND written by Spark, and delta-rs reads back what Spark wrote. Turning on
liquid clustering bumps the protocol to (1,7) + clustering/domainMetadata, after
which delta-rs 1.6.5 can still read but can NO LONGER WRITE ('Unsupported table
features required: [ClusteredTable, DomainMetadata]'). Re-probe before enabling
any feature (clustering, deletion vectors, column mapping).

Not here yet (Sail/non-JVM path): write_partition_guarded + a Sail read.
"""

# CLAUDE UNDIGESTED

import shutil
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, Schema


####################################
######## for provisioning ##########
####################################

def table_path(full_warehouse_path: str | Path, namespace: str, tbl_name: str) -> Path:
    """{warehouse}/{namespace}/{tbl_name} - the table's identity on disk."""
    return Path(full_warehouse_path) / namespace / tbl_name


def table_path_fqn(full_warehouse_path: str | Path, namespace: str, tbl_name: str) -> str:
    """The name Spark (DeltaCatalog as spark_catalog) resolves: delta.`<abs path>`."""
    return f'delta.`{table_path(full_warehouse_path, namespace, tbl_name)}`'


def _require_absolute(full_warehouse_path, fn_name):
    if not Path(full_warehouse_path).is_absolute():
        raise ValueError(
            f"{fn_name} requires an ABSOLUTE path, got {str(full_warehouse_path)!r}. "
            f"A relative path resolves against whatever cwd the script ran from - "
            f"a different cwd silently means a different (empty) lakehouse.")


def custom_describe_catalog(full_warehouse_path: str | Path):
    """Walk the warehouse: namespaces = subdirs, tables = subdirs holding a _delta_log."""
    full_warehouse_path = Path(full_warehouse_path)
    print(f'delta warehouse: {full_warehouse_path}')
    namespaces = sorted(p for p in full_warehouse_path.iterdir() if p.is_dir()) if full_warehouse_path.exists() else []
    print(f'namespaces: {[p.name for p in namespaces]}')
    for ns in namespaces:
        for tbl in sorted(p for p in ns.iterdir() if p.is_dir()):
            if not DeltaTable.is_deltatable(str(tbl)):
                print(f'  {ns.name}.{tbl.name}: NOT a delta table (no _delta_log) - stray dir')
                continue
            dt = DeltaTable(str(tbl))
            p = dt.protocol()
            print(f'  {ns.name}.{tbl.name}: version={dt.version()}  files={len(dt.file_uris())}  '
                  f'partition_by={dt.metadata().partition_columns}  '
                  f'protocol=({p.min_reader_version},{p.min_writer_version})  features={p.writer_features}')


def sh_provision_delta_catalog(full_warehouse_path, namespaces: list):
    """Idempotent namespaces - the delta twin of sh_provision_iceberg_catalog.
    A namespace is only a directory here, so 'create' = mkdir and 'drop an
    unspecified one' = rmdir, which only ever succeeds on an EMPTY dir (the same
    'skip if it still has a table' rule as the iceberg version).
    """
    _require_absolute(full_warehouse_path, 'sh_provision_delta_catalog')
    FULL_WAREHOUSE_PATH = Path(full_warehouse_path)
    FULL_WAREHOUSE_PATH.mkdir(parents=True, exist_ok=True)

    ###### create namespaces as required #######
    for ns in namespaces:
        (FULL_WAREHOUSE_PATH / ns).mkdir(exist_ok=True)

    ###### safely delete namespaces not specified #######
    print()
    for ns_dir in sorted(p for p in FULL_WAREHOUSE_PATH.iterdir() if p.is_dir()):
        if ns_dir.name not in namespaces:
            try:
                ns_dir.rmdir()
                print(f'dropped unspecified namespace {ns_dir.name!r}')
            except OSError:
                print(f'{ns_dir.name!r} not in requested list but is not empty - skipped')

    print()
    print('after provision')
    custom_describe_catalog(FULL_WAREHOUSE_PATH)


#########################################
######## for table definition ##########
#########################################

def _pa_type_to_code(t: pa.DataType) -> str:
    """Render a pyarrow DataType as the Python source that constructs it.
    Duplicated from helper_pyiceberg_io on purpose for now - pure pyarrow, no iceberg
    dependency; importing it from there would drag pyiceberg into a delta-only run.
    """
    if pa.types.is_string(t): return 'pa.string()'
    if pa.types.is_int64(t): return 'pa.int64()'
    if pa.types.is_int32(t): return 'pa.int32()'
    if pa.types.is_float64(t): return 'pa.float64()'
    if pa.types.is_float32(t): return 'pa.float32()'
    if pa.types.is_boolean(t): return 'pa.bool_()'
    if pa.types.is_date32(t): return 'pa.date32()'
    if pa.types.is_date64(t): return 'pa.date64()'
    if pa.types.is_timestamp(t): return f'pa.timestamp({t.unit!r})'
    raise NotImplementedError(f'no code-gen mapping for pyarrow type {t!r} - add one to pa_type_to_code()')


def schema_to_code(schema: pa.Schema, var_name: str = 'SCHEMA') -> str:
    """A pa.Schema -> ready-to-paste Python source that reconstructs it exactly."""
    lines = [f'{var_name} = pa.schema([']
    for f in schema:
        lines.append(f'    pa.field({f.name!r}, {_pa_type_to_code(f.type)}, nullable={f.nullable}),')
    lines.append('])')
    return '\n'.join(lines)

    
def createOrEvolve_table(
    schema_source: pa.Schema | pa.Table,
    full_warehouse_path,
    namespace: str,
    tbl_name: str):
    """
    Provisioning only - never called implicitly from a write.
    Evolve here ONLY adds columns, never deletes, never changes a type.

    NO partitioning, NO clustering - decided 2026-09-24, so there is no partition_keys
    parameter at all (unlike helper_pyiceberg_io's). Matches the live Databricks tables;
    Delta's replaceWhere doesn't need a layout to be correct (verified 2026-09-24).
    An existing table found WITH partition columns raises - a Delta table's partition
    columns are fixed at create time, only a purge + re-provision changes them.
    """
    _require_absolute(full_warehouse_path, 'createOrEvolve_table')
    path = table_path(full_warehouse_path, namespace, tbl_name)
    table_fqn = f'{namespace}.{tbl_name}'

    if isinstance(schema_source, pa.Table):
        print('schema provisioning - from dataFrame')
        schema = schema_source.schema
    elif isinstance(schema_source, pa.Schema):
        print('schema provisioned - from hand-rolled pa.Schema')
        schema = schema_source
    else:
        raise TypeError(f'schema_source must be a pa.Schema or pa.Table, got {type(schema_source)}')

    # 0. create if it does not exist yet
    if not DeltaTable.is_deltatable(str(path)):
        print()
        print(f'{table_fqn} does not exist yet -')
        print('create table yes. using DeltaTable.create:')

        #### main ####
        DeltaTable.create(str(path), schema=schema, mode='error')
        #### main ####

        print('check schema and partition of created table')
        custom_describe_catalog(Path(full_warehouse_path))
        print(f'table path: {path}')
        return

    dt = DeltaTable(str(path))
    current = pa.schema(dt.schema().to_arrow())
    evolution = []

    # 1. REFUSE - this codebase's delta tables are never partitioned (see docstring)
    current_partitions = list(dt.metadata().partition_columns)
    if current_partitions:
        raise Exception(
            f'{table_fqn}: found partition columns {current_partitions} - delta tables here are '
            f'unpartitioned by decision. Purge and re-provision to remove them.')

    # 2. REFUSE - type change on an existing column
    type_changes = [f'{f.name}: {current.field(f.name).type} -> {f.type}'
                    for f in schema if f.name in current.names and current.field(f.name).type != f.type]
    if type_changes:
        raise Exception(f'{table_fqn}: type change(s) requested, refusing: {type_changes}')

    # 3. EVOLVE - add columns (always nullable: existing rows have no value for them)
    missing_cols = [f for f in schema if f.name not in current.names]
    if missing_cols:
        print(f'evolving schema - adding {[f.name for f in missing_cols]}')

        #### main ####
        new_fields = pa.schema([f.with_nullable(True) for f in missing_cols])
        dt.alter.add_columns(Schema.from_arrow(new_fields).fields)
        #### main ####

        evolution.append('add columns')

    # 4. No Change
    if not evolution:
        print()
        print(f'{table_fqn} already instantiated - nothing is changed.')

    dt = DeltaTable(str(path))
    print('current schema of table')
    print(pa.schema(dt.schema().to_arrow()))
    print(f'table path: {path}  version={dt.version()}')


####################################
######## for cleanup ##########
####################################

def double_safe_purge(full_warehouse_path, table_fqn):
    """rmtree the table directory with friction on purpose - one-shot, unrecoverable
    (there is no catalog entry to drop: the directory IS the table).
    `table_fqn` is '{namespace}.{tbl_name}'.
    """
    _require_absolute(full_warehouse_path, 'double_safe_purge')
    namespace, tbl_name = table_fqn.split('.', 1)
    path = table_path(full_warehouse_path, namespace, tbl_name)

    print('Executing double-safe. It runs shutil.rmtree on the table dir. standing up twice confirmation...')
    if not path.exists():
        print(f'{table_fqn} does not exist at {path} - nothing to purge.')
        return

    has_data = DeltaTable.is_deltatable(str(path)) and len(DeltaTable(str(path)).file_uris()) > 0
    if not has_data:
        print(f'{table_fqn} has no data files at all - skipping confirmation, purging directly.')
        shutil.rmtree(path)
        print(f'{table_fqn} purged.')
        return

    print(f'About to PERMANENTLY purge {table_fqn!r} at {path} - _delta_log AND data files, unrecoverable.')

    first = input("Type the table name to confirm (1/2): ")
    if first != table_fqn:
        print(f'Mismatch on first confirmation ({first!r} != {table_fqn!r}) - aborted, nothing deleted.')
        return

    second = input("Type it again to confirm (2/2): ")
    if second != table_fqn:
        print(f'Mismatch on second confirmation ({second!r} != {table_fqn!r}) - aborted, nothing deleted.')
        return

    shutil.rmtree(path)
    print(f'{table_fqn} purged.')
