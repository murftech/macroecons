

####################################
######## for provisioning ##########
####################################
import os
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog


# sub should i place this together with helper_pyiceberg.io, maybe i should. 60%
# subL then i should use full_warehouse_path too
# because i will be provisioning somewhere in DB MAter probably


def custom_describe_catalog(catalog):
    print(f'catalog.name:{catalog.name}')
    schemas = catalog.list_namespaces()
    print(f'catalog.list_namespaces():{schemas}')
    for i in schemas:
        print(f'catalog.list_tables():{catalog.list_tables(i)}')

# metadata.json = what the table is (schema, partitions, snapshots, physical location). The catalog = what the table is called.
def getOrCreate_catalog(full_warehouse_path: str | Path):
    """
    To loading pyiceberg instantiated catalog with pyiceberg.SqlCatalog())
    db is always named: _icebergcatalog.db and always in root of the iceberg warehouse directory.
    catalog inside is always named icebergcatalog. Should never have more than one.
    """
    Path(full_warehouse_path).mkdir(parents=True, exist_ok=True)
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
        uri=f'sqlite:///{catalog_db_path}',  # point to this existing file with all its stored pointers (catalog_name, namespace, table_name, metadata_location, previous_metadata_location, table_type). if none yet, create empty.
        name = 'icebergcatalog', # this should never vary else i will have many useless catalog names inside.
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
    print(f'listdir: {os.listdir(full_warehouse_path)}')

    return catalog


# tester
# relative_warehouse_path = 'datalake/iceberg2'
# sh_provision_iceberg_catalog(relative_warehouse_path, ['t1', 't2', 't3'])


def sh_provision_iceberg_catalog(relative_warehouse_path, namespaces: list = None):

    #### identify datalake dir ######
    from pathlib import Path
    FULL_WAREHOUSE_PATH = Path(relative_warehouse_path).resolve()

    ###### create and/or load it's catalog.db #######


    # getOrCreate_catalog?
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

def createOrEvolve_table(
    schema_source: pa.Schema | pa.Table,
    partition_keys: list,
    catalog,
    namespace: str,
    tbl_name: str):
    """
    Provisioning only - never called implicitly from write_partition().

    ADD only - never deletes columns. A column missing from THIS CALL's schema
    just means this era predates that column (normal, expected - not a delete
    request: comparing per-era schemas was the wrong signal for delete-intent).
    Deletion is its own explicit function, drop_column() - call it only when
    you genuinely mean to remove a column from the table forever.
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

        catalog.create_table(table_fqn, schema=schema)

        # CLAUDE UNDIGESTED what is identity transform, why col called two times?
        tbl = catalog.load_table(table_fqn)
        with tbl.update_spec() as spec:
            for col in partition_keys:
                spec.add_field(col, IdentityTransform(), col)
        
        print('check schema and partition of created fqn')
        print(tbl)
        print(f'table_fqn: {table_fqn}')        
        return

    tbl = catalog.load_table(table_fqn)

    evolution=[]
    # 1. schema ADD first - a new partition field (step 2) may need the column to already exist
    missing_cols = set(schema.names) - set(tbl.schema().as_arrow().names)
    # CLAUDE UNGIGESTED
    if missing_cols:
        # _evolve_schema_additive
        print(f'evolving schema - adding {sorted(missing_cols)}')

        # ---- SPECIAL NEW ADDITION (2026-09-14) ----
        # Iceberg refuses to add a NON NULLABLE column to a table that already has rows
        # (no value to backfill into pre-existing rows). Spark's lit(x) marks the
        # resulting column non-nullable (it's a correct-but-narrow)
        # hence we must always have a idempotent force new addition columns to be NULLABLE. NONSENSE BUGG
        schema_for_add = pa.schema([
            f.with_nullable(True) if f.name in missing_cols else f
            for f in schema
        ])
        with tbl.update_schema() as upd:
            upd.union_by_name(schema_for_add)
        # ---- END SPECIAL NEW ADDITION ----

        tbl = tbl.refresh()
        print('schema and partition of fqn after add column')
        print(tbl)
        evolution.append('add columns')

    # 2. partition spec add/remove - must land BEFORE schema delete (step 3), since Iceberg
    #    refuses to delete a column still referenced by a partition field
    # CLAUDE UNGIGESTED
    current_spec_cols = {f.name for f in tbl.spec().fields}
    missing_partitions = set(partition_keys) - current_spec_cols
    extra_partitions = current_spec_cols - set(partition_keys)
    if missing_partitions or extra_partitions:
        if missing_partitions:
            print(f'evolving partition spec - adding {sorted(missing_partitions)}')
        if extra_partitions:
            print(f'evolving partition spec - removing {sorted(extra_partitions)}')
        with tbl.update_spec() as spec:
            for col in missing_partitions:
                spec.add_field(col, IdentityTransform(), col)
            for col in extra_partitions:
                spec.remove_field(col)
        tbl = tbl.refresh()
        print('schema and partition of fqn after evolving partition')
        print(tbl)
        evolution.append('evolve partition keys')

    # ---- SPECIAL NEW ADDITION (2026-09-14): step 3 (schema DELETE) removed entirely ----
    # It used to compare THIS CALL's schema against the table and auto-delete whatever
    # the table had that this call didn't - but "this era is narrower than the table"
    # is the NORMAL case for any older era once a later one has evolved the schema, not
    # a delete request. That made the opt-in flag from earlier today still fire on every
    # ordinary older-era write. Deletion is now its own explicit function, drop_column()
    # below - call it only when you actually mean to remove a column from the table.
    # ---- END SPECIAL NEW ADDITION ----

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


# ---- SPECIAL NEW ADDITION (2026-09-14) ----
def drop_column(table_fqn: str, catalog, column_name: str):
    """Explicitly remove a column from the table's schema, forever - every
    era's rows lose it, not just one call's. Never called automatically
    (createOrEvolve_table no longer infers delete-intent from a per-era
    schema comparison - a narrower era is normal, not a delete request).
    Call this only when you genuinely mean to drop a column.
    """
    tbl = catalog.load_table(table_fqn)
    if column_name not in tbl.schema().as_arrow().names:
        print(f'{table_fqn}: {column_name!r} not present - nothing to drop.')
        return
    with tbl.update_schema() as upd:
        upd.delete_column(column_name)
    tbl = tbl.refresh()
    print(f'{table_fqn}: dropped column {column_name!r}.')
    print(tbl)
# ---- END SPECIAL NEW ADDITION ----


####################################
######## for write ##########
####################################

def write_partition(
    arrow_table: pa.Table, 
    catalog, 
    table_fqn: str, 
    partition_keys: list, 
    show_partitions=False):
    """Lazy Iceberg write - zero checks, for POC / quick-and-dirty use only."""

    from pyiceberg.expressions import In, And

    tbl = catalog.load_table(table_fqn)   # NoSuchTableError if it doesn't exist - not this function's job to create it
    
    import pyarrow.compute as pc

    # CLAUDE UNGIGESTED: how does this tie in to the shape i already knew explicitly. and how to print to make the command explicitly displayed maybe
    values_per_col = {col: pc.unique(arrow_table.column(col)).to_pylist() for col in partition_keys}
    filters = [In(col, vals) for col, vals in values_per_col.items()]
    overwrite_filter = filters[0] if len(filters) == 1 else And(*filters)

    # CLAUDE UNGIGESTED: what is this message and how is this aligned with pyarrow's nice message
    print()
    print(f'[iceberg] replacing partitions on {partition_keys}:')
    for col, vals in values_per_col.items():
        print(f'    {col:<14} {len(vals):>4} values')

    tbl.overwrite(arrow_table, overwrite_filter=overwrite_filter)

    # CLAUDE UNGIGESTED: wtf is this in pyarrow?
    # if show_partitions:
    #     custom_describe_catalog(catalog)

    return tbl


def write_partition_full_refresh(
    arrow_table: pa.Table, catalog, table_fqn: str, show_partitions=False):
    
    print(f'[iceberg] full refresh on {table_fqn} - dropping ALL existing rows, replacing with {arrow_table.num_rows} incoming rows')
    tbl = catalog.load_table(table_fqn)
    tbl.overwrite(arrow_table)

    return tbl



def write_partition_guarded(
    arrow_table: pa.Table,
    catalog,
    table_fqn: str,
    partition_keys: list,
    show_partitions=False,
    *,
    allow_new_columns=False,
    pad_missing_columns=False,
    full_refresh=False):

    """a schema guard against the table's CURRENT schema, before calling write_partition()

    `pad_missing_columns` : toggle for check 3. False (default) = raise, no push - a
        missing column could be legitimate (this era predates it) OR a bug upstream
        (a refactor silently dropped a column) - the guard can't tell those apart, so
        it stops you here where you can still investigate, rather than silently writing
        nulls into what should have been real data. True = null-pad (typed from the
        table's own schema) and push anyway - opt into this only once you've actually
        confirmed the missing column is expected, not accidental.
    """
    tbl = catalog.load_table(table_fqn)

    old_schema = tbl.schema().as_arrow()
    incoming_schema = arrow_table.schema

    ## reference old column names vs new column names
    old_names, new_names = set(old_schema.names), set(incoming_schema.names)

    problems_silent = []
    problems_loud = []


    print('schema sync enforce 1: partition_keys must match the table\'s current spec')
    # tbl.overwrite fails silently
    current_spec_cols = {f.name for f in tbl.spec().fields}
    if set(partition_keys) != current_spec_cols:
        problems_silent.append(
            f"partition_keys {partition_keys} doesn't match the table's current spec "
            f"{sorted(current_spec_cols)} - call createOrEvolve_table() first"
        )

    print('schema sync enforce 2: partition_keys columns must exist in the incoming data')
    # tbl.overwrite fails loudly
    missing_partition_cols = set(partition_keys) - new_names
    if missing_partition_cols:
        problems_loud.append(
            f"partition_keys column(s) {sorted(missing_partition_cols)} not found in the "
            f"incoming data (has: {sorted(new_names)}) - iceberg's own tbl.overwrite() would "
            f"fail on this too, just with a much less clear KeyError"
        )

    print('schema sync enforce 3: push data must have ALL columns of destination schema')
    # ---- SPECIAL NEW ADDITION (2026-09-14): pad_missing_columns toggle, default raise ----
    # A missing column could be legitimate (this era predates it) or a bug upstream -
    # the guard can't tell those apart, so it defaults to raising (mirrors the parquet
    # guard's pad_missing_columns toggle - same reasoning, same default). Only pads
    # when explicitly told to.
    missing_cols = old_names - new_names
    if missing_cols:
        if not pad_missing_columns:
            problems_silent.append(
                f"incoming batch is MISSING columns {sorted(missing_cols)} "
                f"(pass pad_missing_columns=True to null-pad and push instead)"
            )
        else:
            print(f"[guard] {table_fqn}: auto-padding {sorted(missing_cols)} as null "
                  f"(present in the table, absent from this batch)")
            for col in missing_cols:
                arrow_table = arrow_table.append_column(
                    col, pa.nulls(arrow_table.num_rows, old_schema.field(col).type)
                )
            incoming_schema = arrow_table.schema
            new_names = set(incoming_schema.names)
    # ---- END SPECIAL NEW ADDITION ----

    print('schema sync enforce 4: push data is advisable not to have extra columns')
    # tbl.overwrite fails loudly
    if (new_names - old_names) and not allow_new_columns:
        problems_loud.append(
            f"incoming batch has NEW columns {sorted(new_names - old_names)} "
            f"(pass allow_new_columns=True, or call createOrEvolve_table() first to evolve "
            f"the schema)"
        )

    print('schema sync enforce 5: persist the exact type case of each column.')
    # tbl.overwrite fails loudly
    # for each matching column name in old_names and new_names
    for n in sorted(old_names & new_names):
        old_type, new_type = old_schema.field(n).type, incoming_schema.field(n).type
        if old_type != new_type and not _helper_softer_match(old_type, new_type):
            problems_loud.append(f"type change on '{n}': {old_type} -> {new_type}")

    if problems_silent:
        all_problems = problems_silent + problems_loud
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(all_problems, 1))
        raise Exception(f"[guard] {table_fqn}: {len(all_problems)} problem(s) found:\n{numbered}\nno push")

    elif problems_loud:
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(problems_loud, 1))
        print(f"[guard] {table_fqn}: {len(problems_loud)} problem(s) iceberg will catch on its own:")
        print(numbered)
        print('proceeding anyway - letting tbl.overwrite() raise its own error')

    print('RUN')
    if full_refresh:
        return write_partition_full_refresh(arrow_table, catalog, table_fqn, show_partitions)
    else:
        return write_partition(arrow_table, catalog, table_fqn, partition_keys, show_partitions)



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

# def _evolve_schema(tbl, incoming: pa.Schema):
#     """Add any columns in `incoming` the table doesn't have yet (union by name); returns the reloaded table."""
#     if set(incoming.names) - set(tbl.schema().as_arrow().names):
#         with tbl.update_schema() as upd:
#             upd.union_by_name(incoming)
#         tbl = tbl.refresh()
#     return tbl


# def _align_to_table(arrow_table: pa.Table, tbl) -> pa.Table:
#     """Reindex `arrow_table` to the table's schema: reorder, cast, fill absent columns with null.

#     Replaces a positional `.cast()` - which failed on any column-set or order
#     difference. Spark's arrow output can also differ in string/timestamp encoding
#     from pyiceberg's, which the per-field `.cast` here absorbs.
#     """
#     target = tbl.schema().as_arrow()
#     cols = [
#         arrow_table.column(f.name).cast(f.type) if f.name in arrow_table.column_names
#         else pa.nulls(arrow_table.num_rows, f.type)
#         for f in target
#     ]
#     return pa.table(cols, schema=target)




####################################
######## for cleanup ##########
####################################


#### appendix ####


def _helper_softer_match(type_a: pa.DataType, type_b: pa.DataType) -> bool:
    # for specifically use in write_partition_guarded: 'schema sync enforce 5: persist the exact type case of each column.'

    def is_string_like(t):
        return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)
    string_match = (is_string_like(type_a) and is_string_like(type_b))

    def is_binary_like(t):
        return pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)
    binary_match = (is_binary_like(type_a) and is_binary_like(type_b))

    def is_list_like(t):
        return pa.types.is_list(t) or pa.types.is_large_list(t)
    list_match = (is_list_like(type_a) and is_list_like(type_b))


    types_align = (string_match or binary_match or list_match)
    
    return types_align



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
    CLAUDE UNGIGESTED
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
