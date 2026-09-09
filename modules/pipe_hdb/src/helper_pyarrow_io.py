"""Engine-free partitioned parquet write with dynamic-overwrite semantics.

Takes a pyarrow Table (from a Spark DataFrame's .toArrow()) and writes it
Hive-partitioned. Only the partitions PRESENT in the incoming table are replaced;
every other partition already on disk is left untouched.

This is pyarrow's `existing_data_behavior='delete_matching'` - the equivalent of
Spark's `partitionOverwriteMode=dynamic`, which Sail 0.7.x silently ignores.

No Spark, no JVM. pyarrow only.
"""

import pyarrow as pa
import pyarrow.dataset as ds


def write_partitioned(table: pa.Table, root: str, partition_col: str):
    """Write `table` to `root`, Hive-partitioned by `partition_col`.

    delete_matching: for each distinct `partition_col` value in `table`, delete
    that partition directory first, then write fresh. Partitions not in `table`
    are not touched, so re-running one month leaves the other months intact.
    """
    months = sorted({str(v) for v in table.column(partition_col).to_pylist()})
    print(f'[hive] replacing {partition_col} partitions: {months}')

    part_schema = pa.schema([table.schema.field(partition_col)])

    ds.write_dataset(
        table,
        base_dir=root,
        format='parquet',
        partitioning=ds.partitioning(part_schema, flavor='hive'),
        existing_data_behavior='delete_matching',
    )


def read(spark, root: str):
    """Read a Hive-partitioned parquet dir back as a Spark DataFrame. Sail and the
    JVM both read a plain parquet dir directly, so this needs no engine-free path."""
    return spark.read.parquet(root)


def list_partitions(root: str, partition_col: str):
    """The `partition_col=` directory names currently on disk - for eyeballing
    that a write replaced only what it should have."""
    import os
    if not os.path.isdir(root):
        return []
    return sorted(d for d in os.listdir(root) if d.startswith(f'{partition_col}='))
