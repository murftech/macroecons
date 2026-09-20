import pyarrow as pa
import pyarrow.dataset as ds

def write_partition(arrow_table: pa.Table, path_to_table: str, partition_keys: list, show_partitions=False):

    # part_schema = pa.schema([arrow_table.schema.field(c) for c in partition_keys]) # use if ever footgun
    # print(part_schema)


    print(f'RUN: using pyarrow.dataset, ds.write_dataset, into path: {path_to_table}')
    ### main ###
    ds.write_dataset(
        data = arrow_table,
        base_dir = path_to_table,
        # partitioning=ds.partitioning(part_schema, flavor='hive'), # use instead if ever footgun
        partitioning = partition_keys,                  # partitions always date or strings so this will never footgun me
        partitioning_flavor='hive',                     # REQUIRED to change default
        existing_data_behavior = 'delete_matching',     # Replace paritions REQUIRED to change default
        format = 'parquet'                                  # required to be sepcified for PA Table (can be parquet, orc, csv)
    )
    #########

    print('summaries:')

    n_leaves = arrow_table.select(partition_keys).group_by(partition_keys).aggregate([]).num_rows
    print(f"[hive] replacing {n_leaves} leaf partitions on {'/'.join(partition_keys)}:")
    for c in partition_keys:
        vals = sorted(set(arrow_table.column(c).to_pylist()))
        print(f"    {c:<14} {len(vals):>4} values   {vals[0]} … {vals[-1]}")

    if show_partitions == True:
        print('show partitions')
        _arrow_listvalues(arrow_table, partition_keys, 1000)



def write_partition_guarded(arrow_table: pa.Table, path_to_table: str, partition_keys: list,
                            show_partitions=False, *, allow_new_columns=False,
                            pad_missing_columns=False):
    """write_partition + a schema guard against the dataset already at path_to_table.

    Rejects (Exception) BEFORE writing if, versus what's on disk:
      1. the partition key list changed
      2. the incoming batch is MISSING a column the table has  (unless pad_missing_columns=True)
      3. the incoming batch has a NEW column   (unless allow_new_columns=True)
      4. a shared column changed type
    First write to a fresh dir: nothing on disk, nothing to check - writes straight through.

    `pad_missing_columns` : toggle for check 2. False (default) = raise, no push - forces
        you to notice and decide. True = null-pad the missing column(s) (typed from the
        on-disk schema, i.e. what's already there) and push anyway - this is the only
        sane way to "allow" a missing column for parquet, since unlike iceberg there's no
        format-level schema evolution: pushing WITHOUT padding would write a file physically
        lacking the column, recreating the exact cross-file mismatch this guard exists to
        prevent. There is no "skip the check, push as-is" option here on purpose.

    Use this for the pipeline tier writes. Plain write_partition stays for ad-hoc /
    """

    old_schema, old_keys = _existing_dataset_schema(path_to_table, partition_keys)

    if old_keys is None:
        print('nothing on disk, nothing to check - write straight through.')

    if old_keys is not None:
        print("schema sync enforce 1: partition keys requested mismatch destination's keys")
        if old_keys != list(partition_keys):
            raise Exception(
                f"[guard] partition scheme change in {path_to_table}: on disk {old_keys}, "
                f"writing {list(partition_keys)}. wipe the dir to re-partition.\n"
                "no push"
            )

        incoming = pa.schema([f for f in arrow_table.schema if f.name not in partition_keys])
        old_n, new_n = set(old_schema.names), set(incoming.names)

        print('schema sync enforce 2: push data must have ALL columns of destination schema')

        missing_cols = old_n - new_n
        if missing_cols:
            if not pad_missing_columns:
                raise Exception(
                    f"[guard] {path_to_table}: incoming batch is MISSING columns {sorted(missing_cols)} "
                    f"(pass pad_missing_columns=True to null-pad and push instead) \n"
                    "no push"
                    )
            print(f"[guard] {path_to_table}: auto-padding {sorted(missing_cols)} as null "
                  f"(present on disk, absent from this batch)")
            for col in missing_cols:
                arrow_table = arrow_table.append_column(
                    col, pa.nulls(arrow_table.num_rows, old_schema.field(col).type)
                )
            incoming = pa.schema([f for f in arrow_table.schema if f.name not in partition_keys])
            old_n, new_n = set(old_schema.names), set(incoming.names)

        print('schema sync enforce 3: Push data is advisable not to have extra columns')
        if new_n - old_n:
            if not allow_new_columns:
                raise Exception(
                    f"[guard] {path_to_table}: incoming batch has NEW columns {sorted(new_n - old_n)} "
                    f"(parquet dir has no schema evolution; wipe + rebuild, or pass allow_new_columns=True) \n"
                    "no push"
                )
            print(
                f"[guard] {path_to_table}: incoming batch has NEW columns {sorted(new_n - old_n)} "
                f"- allow_new_columns=True, proceeding anyway. parquet dir has no schema evolution: "
                f"files already on disk will NOT have {sorted(new_n - old_n)} until backfilled."
            )
        
        print('schema sync enforce 4: persist the exact type case of each column.')
        for n in old_n & new_n:
            if old_schema.field(n).type != incoming.field(n).type:
                raise Exception(
                    f"[guard] {path_to_table}: type change on '{n}': "
                    f"{old_schema.field(n).type} -> {incoming.field(n).type} \n"
                    "no push"
                )

    print('RUN')
    write_partition(arrow_table, path_to_table, partition_keys, show_partitions)



def _existing_dataset_schema(path_to_table: str, partition_keys: list):
    """(data_schema, partition_keys) of the parquet dataset already at path_to_table, or
    (None, None) if the dir is absent / empty. Reads footers only, via pyarrow.
    Raises if the dir won't open as one dataset (e.g. a mixed partition layout).

    # ---- SPECIAL NEW ADDITION (2026-09-14) ----
    # data_schema is the UNION of every physical file's own schema, not
    # ds.dataset()'s auto-discovered one. Without an explicit schema=,
    # ds.dataset() silently INTERSECTS columns across mismatched files -
    # a column only some files have just vanishes from the discovered schema,
    # with no error. That made this "random": which columns "exist" depended
    # on whatever mix of files happened to be on disk at call time, which
    # changes as allow_new_columns=True writes accumulate over separate runs.
    # Unioning each fragment's physical_schema directly is deterministic: the
    # destination's schema is always the true union of every column ever
    # written here, independent of file-listing order or current file mix.
    # ---- END SPECIAL NEW ADDITION ----
    """
    import os
    if not os.path.isdir(path_to_table) or not os.listdir(path_to_table):
        return None, None
    d = ds.dataset(path_to_table, format='parquet', partitioning='hive')
    keys = list(d.partitioning.schema.names) if d.partitioning is not None else []
    file_schemas = [f.physical_schema for f in d.get_fragments()]
    data = pa.unify_schemas(file_schemas)
    return data, keys



def _arrow_listvalues(arrow_table, columns, CAP = 30):

    distinct = (
        arrow_table
        .select(columns)
        .group_by(columns)
        .aggregate([])
        .sort_by([(c, "ascending") for c in columns])
        )

    rows = distinct.to_pylist()
    print(f"[hive] replacing {len(rows)} partitions on {'/'.join(columns)}:")
    for r in rows[:CAP]:
        print("  " + " / ".join(str(r[c]) for c in columns))
    if len(rows) > CAP:
        print(f"  … and {len(rows) - CAP} more")






# currently useless i should always use the native function if by itself.
def read(spark, root: str):
    spark_dataframe = spark.read.parquet(root)
    return spark_dataframe


# sub
# what is the bottom really for? 

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