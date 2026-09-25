"""Write / read a catalog-registered Iceberg table via Spark's V2 writer, using a
"""

from typing import Literal, NamedTuple

from .shared_schema_guards import (
    literal_is_valid,
    assert_keys_is_list,
    check_partition_keys_match_current,
    raise_or_warn,
    assert_overwrite_keys_exist,
    check_overwrite_keys_nullable_false,
    reconcile_missing_columns,
    NewColsResult,
    reconcile_new_columns,
    spark_evolve_schema,
    check_types_match,
    summarize_partitions,
)


def _current_partition_columns(spark, fqn):
    """Fetch an existing Iceberg table's current partition columns, by parsing
    DESCRIBE TABLE's "# Partition Information" section."""
    rows = spark.sql(f"DESCRIBE TABLE {fqn}").collect()
    cols, in_partition_section = [], False
    for r in rows:
        name = (r['col_name'] or '').strip()
        if name == '# Partition Information':
            in_partition_section = True
            continue
        if in_partition_section:
            if name == '' or name.startswith('#'):
                continue  # blank separator row, or the "# col_name" sub-header
            cols.append(name)
    return cols


# ── the write path itself  ───────────
# summarize_partitions moved to shared_schema_guards_spark.py (2026-09-23) - imported above,
# alongside the checks, for the same reason: pure incoming-DataFrame logic, zero destination
# dependency. helper_databricks_io.py can pull it from there directly when it needs one too.

def write_partition(df, spark, fqn, partition_keys: list, show_partitions=False):
    summarize_partitions(df, partition_keys, 'iceberg', show_partitions)

    print(f'RUN: using pyspark DataFrameWriterV2, df.writeTo.overwritePartitions, into table: {fqn}')

    ### main ###
    df.writeTo(fqn).overwritePartitions()
    ### main ###
    
    print(f'DONE:  iceberg -> {fqn}  (overwritePartitions)')
    # why is there done here
# looks good

def write_partition_full_refresh(df, spark, fqn, partition_keys: list, show_partitions=False):

    from sparkutils.functions import lit
    print(f'[iceberg] full refresh on {fqn} - dropping ALL existing rows, replacing with {df.count()} incoming rows')
    df.writeTo(fqn).overwrite(lit(True))
    print(f'DONE:  iceberg -> {fqn}  (overwrite, full refresh)')
    summarize_partitions(df, partition_keys, 'iceberg', show_partitions)
# looks good


def write_partition_guarded(df, spark, fqn, partition_keys: list, show_partitions=False, *,
                            on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                            on_missingcols: Literal['pad_null', 'error'] = 'error',
                            full_refresh=False):
    """A schema guard against the table's CURRENT schema, before calling
    write_partition()
    """

    literal_is_valid(on_newcols, on_missingcols)
    assert_keys_is_list(partition_keys)

    ### load the table and label the schema for diffs ###
    print(f"[guard] running schema guards for {fqn}")

    current_schema = spark.table(fqn).schema
    current_names = {f.name for f in current_schema}
    new_schema = df.schema
    new_names = set(df.columns)

    ##########

    # Native behaviour of writeTo(...).overwritePartitions(), verified 2026-09-21 against a local JVM Iceberg catalog:
    #   silent: null partition key; different partition_keys (ignored)      -> problems_silent (we raise)
    #   loud:   unsafe type change (string into double) - safe upcasts (long into double) are accepted -> problems_loud (we warn)
    #   loud as well, but we raise first with a clearer message: missing column, new column
    problems_silent = []   # the writer would NOT complain - we raise
    problems_loud = []     # the writer would complain on its own when write_partition is called - we only warn

    ###################################
    ### schema sync enforce 0: partition key columns must exist in the first place ###
    ###################################
    assert_overwrite_keys_exist(df, partition_keys)

    ###################################
    ### schema sync enforce 1: no partition key must not contain null ###
    ###################################
    df, problem_collect = check_overwrite_keys_nullable_false(df, partition_keys)
    problems_silent += problem_collect

    ###################################
    ### schema sync enforce 2: partition keys must match destination ###
    ###################################
    # overwritePartitions() uses the table's own spec and ignores partition_keys - a mismatch is silent
    current_partition_cols = _current_partition_columns(spark, fqn)
    problems_silent += check_partition_keys_match_current(current_partition_cols, partition_keys)

    ###################################
    ### schema sync enforce 3: Behaviour: on_missingcols (error or pad_null) ###
    ###################################
    df, new_names, problem_collect = (
        reconcile_missing_columns(
        df, current_schema, new_names,
        on_missingcols)
    )
    new_schema = df.schema
    problems_silent += problem_collect

    ###################################
    ### schema sync enforce 4: Behaviour: on_newcols (error, drop, or evolve additive ###
    ###################################
    newcols_results = reconcile_new_columns(df, current_names, new_names, on_newcols)

    df = newcols_results.df
    new_names = newcols_results.new_names
    extra_cols = newcols_results.extra_cols
    problems_silent += newcols_results.problem_collect
    new_schema = df.schema

    ###################################
    ### schema sync enforce 5: persist the exact type case of each column. ###
    ###################################
    problems_loud += check_types_match(current_schema, new_schema)

    ##### updated all schemas ####
    finalized_df = df

    ##### collect all problems ####
    raise_or_warn(
        problems_silent, problems_loud,
        engine_note=" iceberg will catch on its own - proceeding anyway, letting "
                    "overwritePartitions() raise its own error")

    # AFTER raise_or_warn - We dont want to
    # change the table's schema until we know the write isn't going to be refused.
    if newcols_results.needs_evolve:
        spark_evolve_schema(spark, fqn, extra_cols, new_schema)

    ### RUN WRITE ###
    if full_refresh:
        write_partition_full_refresh(finalized_df, spark, fqn, partition_keys, show_partitions)
    else:
        write_partition(finalized_df, spark, fqn, partition_keys, show_partitions)


def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
