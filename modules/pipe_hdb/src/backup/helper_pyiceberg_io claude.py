"""Engine-free incremental Iceberg write for a partitioned HDB table.

Takes a pyarrow Table (from a Spark DataFrame's .toArrow()) and replaces exactly
the `partition_col` values it covers - via pyiceberg's `Table.overwrite(df,
overwrite_filter=...)`, a delete-matching + append committed as ONE snapshot.
Same intent as flight_prices/modules/helper_iceberg_io.py (clear the slice
once, append the new rows), now atomic: a reader sees either the old months
or the new months, never a gap between them.

The table's schema EVOLVES: a batch carrying a column the table lacks grows the
table (union by name), and older data reads that column as null. This is the one
thing iceberg gives you here that a bare partitioned-parquet dir cannot.

The caller MUST pass `partition_col`, `table_fqn` and `location` - there is no
default table, so t1 and t2 each name their own.

No Spark, no JVM. Requires `pyiceberg` (`uv add "pyiceberg[sql-sqlite]"`).

Layout (format-high, matching the parquet/delta siblings):
    datalake/iceberg/_catalog.db                    one sqlite catalog, all tables
    datalake/iceberg/<tier>/<schema>/<dataset>/     each table's data + metadata

On Databricks, swap get_catalog() for the Iceberg REST catalog (Unity Catalog's
endpoint); write_partition() is unchanged.
"""

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In
from pyiceberg.transforms import IdentityTransform

WAREHOUSE = Path(os.environ.get('ICEBERG_WAREHOUSE_DIR', 'datalake/iceberg'))
CATALOG_DB = WAREHOUSE / '_catalog.db'


def get_catalog():
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        'hdb',
        uri=f'sqlite:///{CATALOG_DB}',
        warehouse=f'file://{WAREHOUSE.resolve()}',
    )


def get_table(arrow_schema: pa.Schema, partition_col: str,
              table_fqn: str, location: str):
    """Load the table, creating it (partitioned by `partition_col`, identity
    transform) from the arrow schema on first use.

    `table_fqn` is `namespace.table` (namespace may be dotted, e.g. `t1.datagov`);
    `location` is where the data + metadata files physically go."""
    namespace = table_fqn.rsplit('.', 1)[0]
    cat = get_catalog()
    try:
        cat.create_namespace(namespace)
    except Exception:
        pass  # already exists

    try:
        return cat.load_table(table_fqn)
    except NoSuchTableError:
        pass  # genuinely not registered yet - create it below
    except Exception as e:
        # catalog row exists but the table is unreadable (e.g. its metadata file
        # was deleted by an interrupted run). drop the dangling entry so the
        # create below can proceed, rather than masking this as "already exists".
        print(f'[iceberg] {table_fqn} unreadable ({type(e).__name__}) - dropping stale entry')
        try:
            cat.drop_table(table_fqn)  # default: does not purge data files
        except Exception:
            pass

    tbl = cat.create_table(table_fqn, schema=arrow_schema,
                           location=str(Path(location).resolve()))
    with tbl.update_spec() as spec:
        spec.add_field(partition_col, IdentityTransform(), partition_col)
    return tbl


def write_partition(arrow_table: pa.Table, partition_col: str,
                       *, table_fqn: str, location: str):
    """Replace exactly the `partition_col` values present in `arrow_table`,
    leaving all other partitions in the table untouched.

    `Table.overwrite(df, overwrite_filter=...)` deletes the rows/files matching
    the filter AND appends the new ones as ONE atomic commit (one snapshot) -
    not two separate delete-then-append commits. A reader sees either the old
    months or the new months, never a gap where the partition is emptied but
    not yet refilled.
    """
    tbl = get_table(arrow_table.schema, partition_col, table_fqn, location)

    tbl = _evolve_schema(tbl, arrow_table.schema)     # grow the table if this batch has new columns
    arrow_table = _align_to_table(arrow_table, tbl)   # match column set / order / type to the table

    months = pc.unique(arrow_table.column(partition_col)).to_pylist()
    print(f'[iceberg] replacing {partition_col} partitions: {sorted(str(m) for m in months)}')

    tbl.overwrite(arrow_table, overwrite_filter=In(partition_col, months))
    return tbl


def read(spark, *, table_fqn: str, location: str = None):
    """Read an Iceberg table back as a Spark DataFrame - pyiceberg scan -> arrow ->
    spark.createDataFrame. Engine-free / Sail-safe: the JVM Iceberg reader is not
    available on Sail. `location` is unused (the catalog resolves it) - kept for
    call-site symmetry with write_partition().
    """
    tbl = get_catalog().load_table(table_fqn)
    return spark.createDataFrame(tbl.scan().to_arrow())


def _evolve_schema(tbl, incoming: pa.Schema):
    """Add any columns in `incoming` the table doesn't have yet (union by name); returns the reloaded table."""
    if set(incoming.names) - set(tbl.schema().as_arrow().names):
        with tbl.update_schema() as upd:
            upd.union_by_name(incoming)
        tbl = tbl.refresh()
    return tbl


def _align_to_table(arrow_table: pa.Table, tbl) -> pa.Table:
    """Reindex `arrow_table` to the table's schema: reorder, cast, fill absent columns with null.

    Replaces a positional `.cast()` - which failed on any column-set or order
    difference. Spark's arrow output can also differ in string/timestamp encoding
    from pyiceberg's, which the per-field `.cast` here absorbs.
    """
    target = tbl.schema().as_arrow()
    cols = [
        arrow_table.column(f.name).cast(f.type) if f.name in arrow_table.column_names
        else pa.nulls(arrow_table.num_rows, f.type)
        for f in target
    ]
    return pa.table(cols, schema=target)
