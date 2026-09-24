import pyarrow as pa
import pyarrow.dataset as ds
from typing import Literal
import shared_schema_guards as guards

####################################
######## for write ##########
####################################

def write_partition(arrow_table: pa.Table, path_to_table: str, partition_keys: list, show_partitions=False):

    guards.summarize_partitions(arrow_table, partition_keys, 'hive', show_partitions)

    print(f'RUN: using pyarrow.dataset, ds.write_dataset, into path: {path_to_table}')
    ### main ###
    ds.write_dataset(
        data = arrow_table,
        base_dir = path_to_table,
        partitioning = partition_keys,                  # partitions always date or strings so this will never footgun me
        partitioning_flavor='hive',                     # REQUIRED to change default
        existing_data_behavior = 'delete_matching',     # Replace paritions REQUIRED to change default
        format = 'parquet'                                  # required to be sepcified for PA Table (can be parquet, orc, csv)
    )
    ### main ###



def write_partition_full_refresh(arrow_table: pa.Table, path_to_table: str, partition_keys: list, show_partitions=False):
    import os, shutil
    print(f'[hive] full refresh on {path_to_table} - dropping ALL existing partitions, replacing with {arrow_table.num_rows} incoming rows')
    if os.path.isdir(path_to_table):
        shutil.rmtree(path_to_table)
    write_partition(arrow_table, path_to_table, partition_keys, show_partitions)


def write_partition_guarded(
    arrow_table: pa.Table,
    path_to_table: str,
    partition_keys: list,
    show_partitions=False,
    *,
    on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
    on_missingcols: Literal['pad_null', 'error'] = 'error',
    full_refresh=False):
    """write_partition + a schema guard against the dataset already at path_to_table.
    """

    guards.literal_is_valid(on_newcols, on_missingcols)

    ### load the filepath and label the schema for diffs ###
    print(f"[guard] running schema guards for {path_to_table}")

    # digest
    current_partition_cols, current_schema = _existing_dataset_schema(path_to_table)   # (None, None) when nothing on disk yet

    ##########

    problems_silent = []   # ds.write_dataset would NOT complain - we raise

    ###################################
    ### schema sync enforce 0: partition key columns must exist in the first place ###
    ###################################
    guards.assert_partition_keys_exist(arrow_table, partition_keys)

    ###################################
    ### schema sync enforce 1: no partition key must not contain null ###
    ###################################
    # ds.write_dataset fails silently (delete_matching would overwrite the wrong bucket)
    arrow_table, problem_collect = guards.check_partition_keys_nullable_false(arrow_table, partition_keys)
    problems_silent += problem_collect

    if current_schema is None:
        print('nothing on disk, nothing to check - write straight through.')

    else:
        current_names = set(current_schema.names)

        # partition columns live in the folder names, not inside the parquet files - so they are left out of the diff.
        new_schema = arrow_table.drop_columns(partition_keys).schema
        new_names = set(new_schema.names)

        ###################################
        ### schema sync enforce 2: partition keys must match destination ###
        ###################################
        # ds.write_dataset fails silently (writes under the wrong partitioning, no error)
        problems_silent += guards.check_partition_keys_match_current(current_partition_cols, partition_keys, ordered=True)

        ###################################
        ### schema sync enforce 3: Behaviour: on_missingcols (error or pad_null) ###
        ###################################
        # ds.write_dataset fails silently (writes a file physically lacking the column)
        arrow_table, new_names, problem_collect = (
            guards.reconcile_missing_columns(
            arrow_table, current_schema, new_names,
            on_missingcols)
        )
        new_schema = arrow_table.drop_columns(partition_keys).schema
        problems_silent += problem_collect

        ###################################
        ### schema sync enforce 4: Behaviour: on_newcols (error, drop, or evolve additive ###
        ###################################
        # ds.write_dataset fails silently (writes the extra column - files already on disk just won't have it)
        newcols_results = guards.reconcile_new_columns(arrow_table, current_names, new_names, on_newcols)

        arrow_table = newcols_results.arrow_table
        new_names = newcols_results.new_names
        extra_cols = newcols_results.extra_cols
        problems_silent += newcols_results.problem_collect
        new_schema = arrow_table.drop_columns(partition_keys).schema

        # parquet specific
        if extra_cols and newcols_results.needs_evolve:
            print(
                f"[guard] {path_to_table}: incoming batch has NEW columns {sorted(extra_cols)} "
                f"- on_newcols='evolve', writing them through anyway. parquet dir has no schema "
                f"evolution: files already on disk will NOT have {sorted(extra_cols)} until backfilled."
            )

        ##### updated all schemas ####
        finalized_schema = current_schema       # nothing to evolve on disk: the files keep the schema they were written with

        ###################################
        ### schema sync enforce 5: persist the exact type case of each column. ###
        ###################################
        # ds.write_dataset fails silently (no native type check at all - verified empirically), so this raises too
        problems_silent += guards.check_types_match(finalized_schema, new_schema)

    ##### updated all tables ####
    finalized_arrow_table = arrow_table

    ##### collect all problems ####
    guards.raise_or_warn(problems_silent, [])

    print('RUN')

    if full_refresh:
        write_partition_full_refresh(finalized_arrow_table, path_to_table, partition_keys, show_partitions)
    else:
        write_partition(finalized_arrow_table, path_to_table, partition_keys, show_partitions)


####################################
######## for read ##########
####################################


# sub: currently useless i should always use the native function if by itself.
def read(spark, root: str):
    spark_dataframe = spark.read.parquet(root)
    return spark_dataframe

# why not this name. but reading in pyaroows cannot be symmetrical to reading iceberg i think. check that both read are ddifferent.abs

def sail_read_pyarrow(spark, root: str):
    spark_dataframe = spark.read.parquet(root)
    return spark_dataframe


def pandas_read_pyarrow(root: str):
    # pandas twin of sail_read_pyarrow: hive dirs -> arrow table -> pandas. partition columns come back from the folder names.
    pandas_dataframe = ds.dataset(root, format='parquet', partitioning='hive').to_table().to_pandas()
    return pandas_dataframe


####################################
#### appendix ####
####################################


####
# testers
# path_to_table = '/Users/murftech/Root/MasterETL/dev/lakehouse/hive/macroecons/t1/datagov__resale_flat_prices/'
# partition_keys = ['tx_monthdate']

def _existing_dataset_schema(path_to_table: str):
    """
    Union of all the schemas of ALL .parquet in dataset folder
    """

    import os
    if not os.path.isdir(path_to_table) or not os.listdir(path_to_table):
        # EXPLAIN: # if folder does not exist, or folder is empty
        print('dataset not yet exists, return None, not Error')
        return None, None

    ### main ###
    d = ds.dataset(path_to_table, format='parquet', partitioning='hive')

    if d.partitioning is not None:
        partition_keys = list(d.partitioning.schema.names)
    else: 
        partition_keys =[]

    list_schemas = [parquet.physical_schema for parquet in d.get_fragments()]
    schema_superset = pa.unify_schemas(list_schemas)
    ### main ###

    # list_schemas = []
    ## for use inline understanding
    # # testers
    # for fragment in d.get_fragments():
    #     # print(f'reading: {fragment.path}')  
    #     pa_schema = fragment.physical_schema
    #     # print(f'schema is: {pa_schema}')
    #     list_schemas.append(pa_schema)

    return partition_keys, schema_superset


def _write_partition_footgun(arrow_table: pa.Table, path_to_table: str, partition_keys: list, show_partitions=False):

     # use this version instead instead if ever footgun for forgotten reason, but

    part_schema = pa.schema([arrow_table.schema.field(c) for c in partition_keys]) # use if ever footgun
    print(part_schema)

    print(f'RUN: using pyarrow.dataset, ds.write_dataset, into path: {path_to_table}')


    ### main ###
    ds.write_dataset(
        data = arrow_table,
        base_dir = path_to_table,
        partitioning=ds.partitioning(part_schema, flavor='hive'),
        existing_data_behavior = 'delete_matching',     # Replace paritions REQUIRED to change default
        format = 'parquet'                                  # required to be sepcified for PA Table (can be parquet, orc, csv)
    )
    #########

    guards.summarize_partitions(arrow_table, partition_keys, 'hive', show_partitions)



# sub
# any use for this?

# def list_partitions(root: str, partition_col: str):
#     """The `partition_col=` directory names currently on disk - for eyeballing
#     that a write replaced only what it should have."""
#     import os
#     if not os.path.isdir(root):
#         return []
#     return sorted(d for d in os.listdir(root) if d.startswith(f'{partition_col}='))

# def list_partitions(path_to_table: str, partition_keys: list):
#     """Leaf partition directories (relative to path_to_table) currently on disk."""
#     import os
#     if not os.path.isdir(path_to_table):
#         return []
#     depth = len(partition_keys)
#     return sorted(
#         os.path.relpath(dp, path_to_table)
#         for dp, subdirs, _ in os.walk(path_to_table)
#         if dp != path_to_table and os.path.relpath(dp, path_to_table).count(os.sep) + 1 == depth and not subdirs
#     )

def list_partitions(path_to_table: str, partition_keys: list):
    """Leaf partition directories currently on disk, relative to path_to_table.

    partition_keys=['tx_monthdate']            -> ['tx_monthdate=1990-01-01', ...]
    partition_keys=['town', 'tx_monthdate']    -> ['town=BEDOK/tx_monthdate=1990-01-01', ...]

    Set-diff the result before vs after a write to confirm delete_matching
    replaced only the intended partitions. Reads the filesystem directly (not
    via pyarrow) so it's an independent check of what physically landed.
    """
    import os
    if not os.path.isdir(path_to_table):
        return []

    depth = len(partition_keys)
    found = []
    for dirpath, _, _ in os.walk(path_to_table):
        rel = os.path.relpath(dirpath, path_to_table)
        if rel == '.':
            continue
        segs = rel.split(os.sep)
        if len(segs) == depth and all(
            s.startswith(f'{col}=') for s, col in zip(segs, partition_keys)
        ):
            found.append(rel)
    return sorted(found)


