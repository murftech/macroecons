"""Write / read a catalog-registered table (Delta or Iceberg) via Spark's V2 writer.

`df.writeTo(fqn)` for any JVM-Spark platform that ships the format catalogs:
Databricks (Unity Catalog), Glue (Glue Data Catalog), Dataproc (Dataproc Metastore /
BigLake), EMR. The catalog wiring is get_spark()'s job; this module assumes `fqn`
already resolves to a real catalog.

Does NOT run on Sail (Rust, no JVM, no Delta/Iceberg JARs) - that is why the
path-based helpers (helper_pyarrow_io, helper_pyiceberg_io) stay separate.

writeTo(...) semantics:
  .using(fmt).create()   first run - CREATE the table. no partitionBy: Databricks
                         gives managed Delta tables Liquid Clustering by default,
                         and high-cardinality date partitioning is an anti-pattern.
  .overwrite(<cond>)     later runs - delete the rows matching <cond>, append this
                         batch. NOT .overwritePartitions() - dynamic partition
                         overwrite is rejected on a Liquid-clustered Delta table.
                         a replaceWhere-style predicate works on clustered Delta
                         AND Iceberg, no partitioning needed.
"""


def align_down(df, target_schema):
    """Reindex `df` to `target_schema`: add columns the table has but `df` lacks as
    typed nulls, then select in the table's column order. Same idea as
    helper_pyiceberg_io._align_to_table, for Spark schemas.

    Alignment only ever goes DOWN - the first write to a fresh table defines its
    schema, and an incoming batch that carries FEWER columns (e.g. 1990-2014 HDB
    eras lack remaining_lease) is padded up to match. A batch with a genuinely NEW
    column would have it dropped here; there is no schema-grow path.
    """
    from sparkutils.functions import lit
    aligned = df
    for field in target_schema:
        if field.name not in aligned.columns:
            aligned = aligned.withColumn(field.name, lit(None).cast(field.dataType))
    return aligned.select([f.name for f in target_schema])


def create_or_overwrite(df, *, fqn, fmt, span, spark):
    """First run: CREATE `fqn` as a `fmt` ('delta'|'iceberg') table from `df`.
    Later runs: replace exactly the rows matching `span` (a replaceWhere-style
    Column predicate) with `df`, aligned down to the existing table schema."""
    if spark.catalog.tableExists(fqn):
        aligned = align_down(df, spark.table(fqn).schema)
        aligned.writeTo(fqn).overwrite(span)
        print(f'DONE:  {fmt:7} -> {fqn}  (overwrite)')
    else:
        df.writeTo(fqn).using(fmt).create()
        print(f'DONE:  {fmt:7} -> {fqn}  (created)')


def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
