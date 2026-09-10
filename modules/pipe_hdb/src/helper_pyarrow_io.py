import pyarrow as pa
import pyarrow.dataset as ds

def write_partition(arrow_table: pa.Table, base_dir: str, partitioning: list, show_partitions=False):

    # part_schema = pa.schema([arrow_table.schema.field(c) for c in partitioning]) # use if ever footgun
    # print(part_schema)

    ds.write_dataset(
        data = arrow_table,
        base_dir = base_dir,
        # partitioning=ds.partitioning(part_schema, flavor='hive'), # use instead if ever footgun
        partitioning = partitioning,                    # partitions always date or strings so this will never footgun me
        partitioning_flavor='hive',                     # REQUIRED to change default
        existing_data_behavior = 'delete_matching',     # Replace paritions REQUIRED to change default
        format = 'parquet'                                  # required to be sepcified for PA Table (can be parquet, orc, csv)
    )

    print('summaries:')

    n_leaves = arrow_table.select(partitioning).group_by(partitioning).aggregate([]).num_rows
    print(f"[hive] replacing {n_leaves} leaf partitions on {'/'.join(partitioning)}:")
    for c in partitioning:
        vals = sorted(set(arrow_table.column(c).to_pylist()))
        print(f"    {c:<14} {len(vals):>4} values   {vals[0]} … {vals[-1]}")
    
    if show_partitions == True:
        print('show partitions')
        _arrow_listvalues(arrow_table, partitioning, 1000)



def write_partition_guarded(arrow_table: pa.Table, base_dir: str, partitioning: list,
                            show_partitions=False, *, allow_new_columns=False):
    """write_partition + a schema guard against the dataset already at base_dir.

    Rejects (Exception) BEFORE writing if, versus what's on disk:
      1. the partition key list changed
      2. the incoming batch is MISSING a column the table has
      3. the incoming batch has a NEW column   (unless allow_new_columns=True)
      4. a shared column changed type
    First write to a fresh dir: nothing on disk, nothing to check - writes straight through.

    Use this for the pipeline tier writes. Plain write_partition stays for ad-hoc /
    """

    old_schema, old_keys = _existing_dataset_schema(base_dir, partitioning)

    if old_keys is None:
        print('nothing on disk, nothing to check - write straight through.')

    if old_keys is not None:
        print("schema sync enforce 1: partition keys requested mismatch destination's keys") 
        if old_keys != list(partitioning):
            raise Exception(
                f"[guard] partition scheme change in {base_dir}: on disk {old_keys}, "
                f"writing {list(partitioning)}. wipe the dir to re-partition.\n"
                "no push"
            )

        incoming = pa.schema([f for f in arrow_table.schema if f.name not in partitioning])
        old_n, new_n = set(old_schema.names), set(incoming.names)

        print('schema sync enforce 2: push data must have ALL columns of destination schema')

        if old_n - new_n:
            raise Exception(
                f"[guard] {base_dir}: incoming batch is MISSING columns {sorted(old_n - new_n)}\n"
                "no push"
                )
        
        print('schema sync enforce 3: Push data is advisable not to have extra columns')
        if (new_n - old_n) and not allow_new_columns:
            raise Exception(
                f"[guard] {base_dir}: incoming batch has NEW columns {sorted(new_n - old_n)} "
                f"(parquet dir has no schema evolution; wipe + rebuild, or pass allow_new_columns=True) \n"
                "no push"
            )
        
        print('schema sync enforce 4: persist the exact type case of each column.')
        for n in old_n & new_n:
            if old_schema.field(n).type != incoming.field(n).type:
                raise Exception(
                    f"[guard] {base_dir}: type change on '{n}': "
                    f"{old_schema.field(n).type} -> {incoming.field(n).type} \n"
                    "no push"
                )

    print('RUN')
    write_partition(arrow_table, base_dir, partitioning, show_partitions)



def _existing_dataset_schema(base_dir: str, partitioning: list):
    """(data_schema, partition_keys) of the parquet dataset already at base_dir, or
    (None, None) if the dir is absent / empty. Reads footers only, via pyarrow.
    Raises if the dir won't open as one dataset (e.g. a mixed partition layout).
    """
    import os
    if not os.path.isdir(base_dir) or not os.listdir(base_dir):
        return None, None
    d = ds.dataset(base_dir, format='parquet', partitioning='hive')
    keys = list(d.partitioning.schema.names) if d.partitioning is not None else []
    data = pa.schema([f for f in d.schema if f.name not in keys])   # strip the partition cols
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






# currently useless
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

# def list_partitions(base_dir: str, partitioning: list):
#     """Leaf partition directories (relative to base_dir) currently on disk."""
#     import os
#     if not os.path.isdir(base_dir):
#         return []
#     depth = len(partitioning)
#     return sorted(
#         os.path.relpath(dp, base_dir)
#         for dp, subdirs, _ in os.walk(base_dir)
#         if dp != base_dir and os.path.relpath(dp, base_dir).count(os.sep) + 1 == depth and not subdirs
#     )

def list_partitions(base_dir: str, partitioning: list):
    """Leaf partition directories currently on disk, relative to base_dir.

    partitioning=['tx_monthdate']            -> ['tx_monthdate=1990-01-01', ...]
    partitioning=['town', 'tx_monthdate']    -> ['town=BEDOK/tx_monthdate=1990-01-01', ...]

    Set-diff the result before vs after a write to confirm delete_matching
    replaced only the intended partitions. Reads the filesystem directly (not
    via pyarrow) so it's an independent check of what physically landed.
    """
    import os
    if not os.path.isdir(base_dir):
        return []

    depth = len(partitioning)
    found = []
    for dirpath, _, _ in os.walk(base_dir):
        rel = os.path.relpath(dirpath, base_dir)
        if rel == '.':
            continue
        segs = rel.split(os.sep)
        if len(segs) == depth and all(
            s.startswith(f'{col}=') for s, col in zip(segs, partitioning)
        ):
            found.append(rel)
    return sorted(found)