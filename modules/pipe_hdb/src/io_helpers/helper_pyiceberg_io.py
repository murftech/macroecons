
# testers
# import sys
# sys.path.insert(0, '/Users/murftech/Dropbox/Datarepo/macroecons/modules/pipe_hdb/src')

####################################
######## for provisioning ##########
####################################
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog

# The pyiceberg SqlCatalog's .db's target catalog name
ICEBERG_CATALOG_NAME = 'icebergcatalog'


def custom_describe_catalog(catalog):
    print(f'catalog.name:{catalog.name}')
    schemas = catalog.list_namespaces()
    print(f'catalog.list_namespaces():{schemas}')
    for i in schemas:
        print(f'catalog.list_tables():{catalog.list_tables(i)}')

# testers
# full_warehouse_path = '/Users/murftech/Root/MasterETL/dev/lakehouse/iceberg'

def getOrCreate_catalog(full_warehouse_path: str | Path):
    """
    creates or gets a _icebergcatalog.db in the full_warehousepath given
    """
    full_warehouse_path = Path(full_warehouse_path)

    if not full_warehouse_path.is_absolute():
        raise ValueError(
            f"getOrCreate_catalog requires an ABSOLUTE path, got {str(full_warehouse_path)!r}. "
            f"A relative path gets baked into every table's metadata_location and silently "
        )
    full_warehouse_path.mkdir(parents=True, exist_ok=True)
    catalog_db_path=Path(full_warehouse_path, '_icebergcatalog.db')

    if Path(catalog_db_path).exists():
        print()
        print(f'{catalog_db_path} already instantiated -')
        print('SqlCatalog will [get], not [Create] >>')
    else:
        print()
        print(f'{catalog_db_path} has not been created -')
        print('SqlCatalog will [Create], not [get] >> ')

    #### main ####
    catalog = SqlCatalog(
        warehouse=f'file://{full_warehouse_path}',     # Tells for THIS instance of catalog, if i CREATE a new table using this catalog, which path create it under.
        uri=f'sqlite:///{catalog_db_path}?timeout=30',  # point to this existing file with all its stored pointers (catalog_name, namespace, table_name, metadata_location, previous_metadata_location, table_type). if none yet, create empty.
        # added timeout so conrurrent scripts running functions off here will not fail.
        name = ICEBERG_CATALOG_NAME, # this should never vary else i will have many useless catalog names inside.
    )

    # LP:
    # sqlite:/// tells what sql engine to use. Choices are: sqlite, postgresql, mysql
    # file:// tells what filesystem to expect/build. Choice are: file://, s3://, gs://, abfss://, hdfs://

    # The catalog definition row only stores: 
    # catalog_name, namespace, table_name, metadata_location, previous_metadata_location, table_type
    # sub: do you mean one row per table????

    # So warehouse= really is exactly one thing: the default address generator 
    # for brand-new tables that don't specify their own location.

    ##############
    
    print()
    print('inspect chosen warehouse dir')
    print(*full_warehouse_path.iterdir(), sep='\n')

    return catalog


# tester
# full_warehouse_path = '/Users/murftech/Root/MasterETL/dev/lakehouse/iceberg'
# sh_provision_iceberg_catalog(full_warehouse_path, ['t1', 't2', 't3'])


def sh_provision_iceberg_catalog(full_warehouse_path, namespaces: list = None):
    """
    creates or gets a _icebergcatalog.db in the full_warehousepath given
    AND idempotent namespaces
    """
    
    #### identify datalake dir ######
    from pathlib import Path
    if not Path(full_warehouse_path).is_absolute():
        raise ValueError(
            f"sh_provision_iceberg_catalog requires an ABSOLUTE path, got "
            f"{str(full_warehouse_path)!r}. Pass Path(...).resolve() instead - "
            f"see getOrCreate_catalog's own docstring for why."
        )
    FULL_WAREHOUSE_PATH = Path(full_warehouse_path)

    ###### create and/or load it's catalog.db #######


    # getOrCreate_catalog
    catalog = getOrCreate_catalog(FULL_WAREHOUSE_PATH)
    print()
    print('get create catalog')
    custom_describe_catalog(catalog)

    # then check that there is only icebergcatalog name
    _sqlite_list_catalog_names(FULL_WAREHOUSE_PATH)
    _drop_catalog_name(FULL_WAREHOUSE_PATH, 'default') # else drop the ones i dont want

    ###### create namespaces as required #######
    for ns in namespaces:
        catalog.create_namespace_if_not_exists(ns)

    print()
    print('after create_namespace')
    custom_describe_catalog(catalog)    

    ###### safely delete namespaces not specified #######

    from pyiceberg.exceptions import NamespaceNotEmptyError

    print()
    for ns in catalog.list_namespaces():
        ns_name = '.'.join(ns)   # namespaces come back as tuples, e.g. ('t1',)
        if ns_name not in namespaces:
            try:
                catalog.drop_namespace(ns)
                print(f'dropped unspecified namespace {ns_name!r}')
                custom_describe_catalog(catalog) 
            except NamespaceNotEmptyError:
                print(f'{ns_name!r} not in requested list but still has a table - skipped')
 



#########################################
######## for table definition ##########
#########################################

import pyarrow as pa


def _pa_type_to_code(t: pa.DataType) -> str:
    """Render a pyarrow DataType as the Python source that constructs it.
    on anything else, so a gap is loud, not a silently wrong schema.
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
    """A pa.Schema -> ready-to-paste Python source that reconstructs it exactly
    Use this instead of hand-transcribing a printed schema 
    """
    lines = [f'{var_name} = pa.schema([']
    for f in schema:
        lines.append(f'    pa.field({f.name!r}, {_pa_type_to_code(f.type)}, nullable={f.nullable}),')
    lines.append('])')
    return '\n'.join(lines)


def createOrEvolve_table(
    schema_source: pa.Schema | pa.Table,
    partition_keys: list,
    catalog,
    namespace: str,
    tbl_name: str):

    """
    Provisioning only - never called implicitly from write_partition().
    Evolve here ONLY adds columns or changes parition, never delete.
    """
    table_fqn = f'{namespace}.{tbl_name}'

    if isinstance(schema_source, pa.Table):
        print('schema provisioning - from dataFrame')
        schema = schema_source.schema
    elif isinstance(schema_source, pa.Schema):
        print('schema provisioned - from hand-rolled pa.Schema')
        schema = schema_source

    from pyiceberg.transforms import IdentityTransform

    # 0. create fqn specification in catalog if it did not exist yet
    if not catalog.table_exists(table_fqn):
        print()
        print(f'{table_fqn} does not exist yet created -')
        print('create table yes. using catalog.create_table:')

        #### main ####
        catalog.create_table(table_fqn, schema=schema)

        tbl = catalog.load_table(table_fqn)

        with tbl.update_spec() as spec:
            for col in partition_keys:
                spec.add_field(col, IdentityTransform())
                # IdentityTransform means "partition by the column's raw value, unchanged", 
                # so each distinct value of tx_monthdate gets its own partition. 
                # Other Transforms.
                # Time transforms (Day, Month, Hour) are by far the most common. 
                # Event, log and transaction tables usually have a timestamp column, and partitioning by day(event_ts) is the standard setup. 
                # Why: Queries filter on the timestamp, and users never see or manage a separate date column. We can switch partition easier
        #### main ####

        print('check schema and partition of created fqn')
        print(tbl)
        print(f'table_fqn: {table_fqn}')        
        return

    tbl = catalog.load_table(table_fqn)

    evolution=[]

    # 1. EVOLVE - add columns
    missing_cols = set(schema.names) - set(tbl.schema().as_arrow().names)

    if missing_cols:
        # _evolve_schema_additive
        print(f'evolving schema - adding {sorted(missing_cols)}')

        #### main ####
        # take the whole final schema, for each new col, set nullable =True
        schema_with_new_and_nullable = pa.schema([
            f.with_nullable(True) if f.name in missing_cols else f
            for f in schema
        ])

        with tbl.update_schema() as upd:
            # EXPLAIN: tbl has current schema, union with argument's schema
            upd.union_by_name(schema_with_new_and_nullable)
        #### main ####
        
        tbl = tbl.refresh()
        # EXPLAIN: reloads the table's metadata from the catalog, so your tbl object reflects what's really stored now.
        print('schema and partition of fqn after add column')
        print(tbl)
        evolution.append('add columns')

    # 2. EVOLVE partition keys
    current_spec_cols = {f.name for f in tbl.spec().fields}
    missing_partitions = set(partition_keys) - current_spec_cols
    extra_partitions = current_spec_cols - set(partition_keys)

    if missing_partitions or extra_partitions:
        #### main ####
        with tbl.update_spec() as spec:
            for col in missing_partitions:
                spec.add_field(col, IdentityTransform())
            for col in extra_partitions:
                spec.remove_field(col)
        #### main ####
        tbl = tbl.refresh()
  
        if missing_partitions:
            print(f'evolving partition spec - added {sorted(missing_partitions)}')
        if extra_partitions:
            print(f'evolving partition spec - removed {sorted(extra_partitions)}')                
        print('schema and partition of fqn after evolving partition')
        print(tbl)
        evolution.append('evolve partition keys')
    
    # 3. No Change
    if len(evolution) == 0:
        print()
        print('current schema and partition of fqn')
        print(tbl)
        print()
        print(f'{table_fqn} already instantiated -')
        print(f'Nothing is changed. Proceed to use catalog.load_table({table_fqn})')
        
    print(f'table_fqn: {table_fqn}')
    print(f'latest metadata json: {tbl.metadata_location}')

    return

def safe_drop_column(catalog, table_fqn: str, column_name: str):
    """
    Remove a column from the table's current schema, after a typed confirmation.
    Metadata only: parquet bytes stay, and old snapshots can still read it. Future snapshots will never as-
    Re-adding the name makes a NEW empty column.
    """
    tbl = catalog.load_table(table_fqn)
    if column_name not in tbl.schema().as_arrow().names:
        print(f'{table_fqn}: {column_name!r} not present - nothing to drop.')
        return

    print(f'About to drop column {column_name!r} from {table_fqn!r} - every era loses it. '
          f'Re-adding the name later creates a NEW empty column, the old values do not come back.')

    confirm = input(f"Type the column name to confirm: ")
    if confirm != column_name:
        print(f'Mismatch on confirmation ({confirm!r} != {column_name!r}) - aborted, nothing dropped.')
        return

    ### main ###
    with tbl.update_schema() as upd:
        upd.delete_column(column_name)
    ### main ###        

    tbl = tbl.refresh()
    print(f'{table_fqn}: dropped column {column_name!r}.')
    print(tbl)


####################################
######## for write ##########
####################################

from typing import Literal
from . import shared_schema_guards as guards
import math
from functools import reduce
from pyiceberg.expressions import In, EqualTo, And, Or


def write_partition(
    arrow_table: pa.Table,
    catalog, table_fqn: str,
    partition_keys: list, show_partitions=False
    ):
    """
    Lazy Iceberg write - zero schema checks, for POC / quick-and-dirty use only.
    """
    guards.summarize_partitions(arrow_table, partition_keys, 'iceberg', show_partitions)

    ### main ###
    tbl = catalog.load_table(table_fqn)
    print(f'RUN: using pyiceberg.catalog.sql, tbl.dynamic_partition_overwrite, into path: {tbl.location()}')
    tbl.dynamic_partition_overwrite(arrow_table)
    ### main ###
    print(f'DONE:  iceberg -> {table_fqn}  (dynamic_partition_overwrite)')

    return tbl


def write_partition_full_refresh(
    arrow_table: pa.Table, catalog, table_fqn: str, partition_keys: list, show_partitions=False):

    print(f'[iceberg] full refresh on {table_fqn} - dropping ALL existing rows, replacing with {arrow_table.num_rows} incoming rows')
    tbl = catalog.load_table(table_fqn)
    tbl.overwrite(arrow_table)
    guards.summarize_partitions(arrow_table, partition_keys, 'iceberg', show_partitions)

    return tbl

def write_partition_guarded(
    arrow_table: pa.Table,
    catalog, table_fqn: str,
    partition_keys: list, show_partitions=False,
    *,
    on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
    on_missingcols: Literal['pad_null', 'error'] = 'error',
    full_refresh=False):

    """a schema guard against the table's CURRENT schema, before calling write_partition()
    """
    
    guards.literal_is_valid(on_newcols, on_missingcols)
    guards.assert_keys_is_list(partition_keys)

    ### load the table and label the schema for diffs ###
    print(f"[guard] running schema guards for {table_fqn}")

    tbl = catalog.load_table(table_fqn)
    current_schema = tbl.schema().as_arrow()
    current_names = set(current_schema.names)
    new_schema = arrow_table.schema
    new_names = set(new_schema.names)

    ##########

    problems_silent = []   # iceberg would NOT complain - we raise
    problems_loud = []     # iceberg would complain on its own when write_partition called - we only warn

    ###################################
    ### schema sync enforce 0: partition key columns must exist in the first place ###
    ###################################
    guards.assert_overwrite_keys_exist(arrow_table, partition_keys)

    ###################################
    ### schema sync enforce 1: no partition key must not contain null ###
    ###################################
    arrow_table, problem_collect = guards.check_overwrite_keys_nullable_false(arrow_table, partition_keys)
    problems_silent += problem_collect

    ###################################
    ### schema sync enforce 2: partition keys must match destination ###
    ###################################
    # dynamic_partition_overwrite uses the table's own spec and ignores partition_keys - a mismatch is silent
    current_partition_cols = {f.name for f in tbl.spec().fields}
    problems_silent += guards.check_partition_keys_match_current(current_partition_cols, partition_keys)

    ###################################
    ### schema sync enforce 3: Behaviour: on_missingcols (error or pad_null) ###
    ###################################
    arrow_table, new_names, problem_collect = (
        guards.reconcile_missing_columns(
        arrow_table, current_schema, new_names,
        on_missingcols)
    )
    new_schema = arrow_table.schema
    problems_silent += problem_collect

    ###################################
    ### schema sync enforce 4: Behaviour: on_newcols (error, drop, or evolve additive ###
    ###################################
    newcols_results = guards.reconcile_new_columns(arrow_table, current_names, new_names, on_newcols)

    arrow_table = newcols_results.df
    new_names = newcols_results.new_names
    extra_cols = newcols_results.extra_cols
    problems_silent += newcols_results.problem_collect
    new_schema = arrow_table.schema

    ###################################
    ### schema sync enforce 5: persist the exact type case of each column. ###
    ###################################
    # dynamic_partition_overwrite fails loudly on a type mismatch (verified)
    problems_loud += guards.check_types_match(current_schema, new_schema)

    ##### updated all schemas ####
    finalized_arrow_table = arrow_table

    ##### collect all problems ####
    guards.raise_or_warn(
        problems_silent, problems_loud,
        engine_note=" iceberg will catch on its own - proceeding anyway, letting "
                    "dynamic_partition_overwrite() raise its own error")

    # AFTER raise_or_warn - We dont want to
    # change the table's schema until we know the write isn't going to be refused.
    # iceberg specific
    if newcols_results.needs_evolve:
        print(f"[guard] {table_fqn}: evolving schema - adding {sorted(extra_cols)}")

        #### main ####
        schema_with_new_and_nullable = pa.schema([
            f.with_nullable(True) if f.name in extra_cols else f
            for f in new_schema
        ])

        with tbl.update_schema() as upd:
            upd.union_by_name(schema_with_new_and_nullable)
    ### RUN WRITE ###
    if full_refresh:
        return write_partition_full_refresh(finalized_arrow_table, catalog, table_fqn, partition_keys, show_partitions)
    else:
        return write_partition(finalized_arrow_table, catalog, table_fqn, partition_keys, show_partitions)



####################################
######## for read ##########
####################################


def sail_read_iceberg(spark, catalog, table_fqn: str):
    """Read an Iceberg table back as a Spark DataFrame - pyiceberg scan -> arrow -> """
    tbl = catalog.load_table(table_fqn)
    df_load = spark.createDataFrame(tbl.scan().to_arrow())
    print('tbl.scan().to_arrow() >> spark.createDataFrame')
    return df_load



####################################
######## future iceberg applications: rollback, tagging, backfill files to new partition etc ##########
####################################


####################################
######## for cleanup ##########
####################################


#### appendix ####


#### for removing of data completely, without using filesystem, making sure catalog addresses syncs ####

def double_safe_purge(catalog, table_fqn):
    """purge_table() with friction on purpose - it's a one-shot, unrecoverable
    """

    print('Executing double-safe. It runs catalog.purge_table(). standing up twice confirmation...')
    tbl = catalog.load_table(table_fqn)
    has_data = len(list(tbl.scan().plan_files())) > 0

    if not has_data:
        print(f'{table_fqn} has no data files at all - skipping confirmation, purging directly.')
        catalog.purge_table(table_fqn)
        print(f'{table_fqn} purged.')
        return

    print(f'About to PERMANENTLY purge {table_fqn!r} - metadata AND data files, unrecoverable.')

    first = input(f"Type the table name to confirm (1/2): ")
    if first != table_fqn:
        print(f'Mismatch on first confirmation ({first!r} != {table_fqn!r}) - aborted, nothing deleted.')
        return

    second = input(f"Type it again to confirm (2/2): ")
    if second != table_fqn:
        print(f'Mismatch on second confirmation ({second!r} != {table_fqn!r}) - aborted, nothing deleted.')
        return

    catalog.purge_table(table_fqn)
    print(f'{table_fqn} purged.')

### for view and cleanup of the .db file in case of funny injections. 


def _sqlite_list_catalog_names(full_warehouse_path: str | Path):

    catalog_db_path=Path(full_warehouse_path, '_icebergcatalog.db')
    print(f'catalog file inspected: {catalog_db_path}')

    import sqlite3
    con = sqlite3.connect(str(catalog_db_path))
    names = [row[0] for row in con.execute('''
        SELECT DISTINCT catalog_name FROM iceberg_tables
        UNION
        SELECT DISTINCT catalog_name FROM iceberg_namespace_properties
    ''')]
    con.close()
    return names

def _drop_catalog_name(full_warehouse_path: str | Path, catalog_name):
    """
    SUB
    CLAUDE UNGIGESTED ITS Iokay la i dont need this in prod
    Remove every namespace + table registered under `catalog_name`, in the
    warehouse's shared _icebergcatalog.db.

    Row-only (drop_table + drop_namespace) - NEVER purge_table here, since the
    physical files this catalog_name's rows point at may still be referenced
    by another catalog_name sharing the same file (e.g. after wholesale-copying
    registrations to a new name). Use this to clean up a stray/leftover name,
    not to delete real data.
    """
    catalog_db_path = Path(full_warehouse_path, '_icebergcatalog.db')

    if not catalog_db_path.exists():
        print(f'{catalog_db_path} does not exist - nothing to drop.')
        return

    cat = SqlCatalog(
        warehouse=f'file://{full_warehouse_path}',
        uri=f'sqlite:///{catalog_db_path}',
        name=catalog_name,
    )

    for ns in cat.list_namespaces():
        for fqn in cat.list_tables(ns):
            cat.drop_table(fqn)   # row only - never purge_table here
            print(f'dropped {fqn} from {catalog_name!r} (row only, files untouched)')
        cat.drop_namespace(ns)
        print(f'dropped namespace {ns} from {catalog_name!r}')

    print()
    print(f'{catalog_name!r} now sees:', cat.list_namespaces())

### for migration of catalog in case named wrongly

### tester:
# clone_catalog_registrations(FULL_WAREHOUSE_PATH, catalog_db_path, 'macroecons', 'icebergcatalog')

def clone_catalog_registrations(warehouse_path, catalog_db_path, from_name, to_name):
    """Wholesale-copy every namespace + table registration from one catalog_name
    to another IN THE SAME FILE - same metadata_location, zero data duplication.
    """
    old_cat = SqlCatalog(from_name, warehouse=f'file://{warehouse_path}', uri=f'sqlite:///{catalog_db_path}')
    new_cat = SqlCatalog(to_name,   warehouse=f'file://{warehouse_path}', uri=f'sqlite:///{catalog_db_path}')

    for ns in old_cat.list_namespaces():
        new_cat.create_namespace_if_not_exists(ns)
        for fqn in old_cat.list_tables(ns):
            new_cat.register_table(fqn, old_cat.load_table(fqn).metadata_location)

    return new_cat



#### explicit-filter version of write_partition, superseded by tbl.dynamic_partition_overwrite ####
#### not sure when do we really need this form ###

def _write_partition_overwrite_filter(
    arrow_table: pa.Table,
    catalog,
    table_fqn: str,
    partition_keys: list,
    show_partitions=False):
    """Lazy Iceberg write with a hand-built overwrite filter - the version write_partition used before
    it switched to tbl.dynamic_partition_overwrite. Kept for reference, and for tables whose spec
    dynamic_partition_overwrite refuses (non-identity transforms).
    """
    import math
    from functools import reduce
    from pyiceberg.expressions import In, EqualTo, And, Or

    combos, values_per_col = guards.summarize_partitions(arrow_table, partition_keys, 'iceberg', show_partitions)

    ### main ###
    tbl = catalog.load_table(table_fqn)   # NoSuchTableError if it doesn't exist - not this function's job to create it

    ### BUILD overwriteFilter ###
    ### Clause undigested how does it match the original open field syntax
    n_leaves = len(combos)
    if n_leaves == 0:
        # The iceberg block is there because of the filter.
        print('[iceberg] incoming batch has 0 rows - nothing to replace, nothing written.')
        return tbl

    n_cross = math.prod(len(vals) for vals in values_per_col.values())
    if n_leaves == n_cross:
        # every combination of the per-column values is present, so per-column In() is exact
        overwrite_filter = reduce(And, [In(col, vals) for col, vals in values_per_col.items()])
    else:
        # sparse combinations: In() AND In() would also match combinations NOT in this batch and delete them
        overwrite_filter = reduce(Or, [
            reduce(And, [EqualTo(col, row[col]) for col in partition_keys]) for row in combos
        ])
    ### main ###

    print(f'RUN: using pyarrow.dataset, tbl.overwrite, into path: {tbl.location()}')
    ### main ###
    tbl.overwrite(arrow_table, overwrite_filter=overwrite_filter)
    ### main ###

    return tbl
