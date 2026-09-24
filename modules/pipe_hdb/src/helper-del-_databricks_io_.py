"""Write / read a catalog-registered Iceberg table on DATABRICKS via Spark's V2 writer,
"""

from typing import Literal

from shared_schema_guards import literal_is_valid, raise_or_warn
from shared_schema_guards_spark import (
    assert_partition_keys_exist,
    check_partition_keys_not_null,
    reconcile_missing_columns,
    reconcile_new_columns,
    check_types_match,
)


# ── provisioning - minimal, explicit, never implicit from the write path ──────

# def create_table(df, spark, fqn):
#     df.writeTo(fqn).using('iceberg').create()
#     print(f'DONE:  iceberg -> {fqn}  (created, no partition/cluster spec)')
# # sub do we need to delete this and allow provision in databricks. or we need this becasue
# # I need to make sure my provision all in one place.
# # can i use cli to create this table or better not


# ── the write path itself ──────────────────────────────────────────────────

def write_partition(df, spark, fqn, span, span_keys: list):
    print(f'[iceberg] windowed overwrite on {span_keys}')
    df.writeTo(fqn).overwrite(span)
    print(f'DONE:  iceberg -> {fqn}  (overwrite, span-windowed)')
# sub: the span must be determined in write time, not pre given?


def write_partition_full_refresh(df, spark, fqn):
    from sparkutils.functions import lit
    print(f'[iceberg] full refresh on {fqn} - dropping ALL existing rows, replacing with {df.count()} incoming rows')
    df.writeTo(fqn).overwrite(lit(True))
    print(f'DONE:  iceberg -> {fqn}  (overwrite, full refresh)')
# sub: explain this


def write_partition_guarded(df, spark, fqn, span, span_keys: list, *,
                            on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                            on_missingcols: Literal['pad_null', 'error'] = 'error',
                            full_refresh=False):
    """
    `span`      : a boolean Column predicate (e.g. a date-range window), built by
                  the caller - this function does not derive it from the data.
    `span_keys` : the column(s) `span` is built from (e.g. the period column) -
                  checked for existence (check 0) and nulls (check 1) the same
                  way partition_keys are elsewhere, even though there is no
                  partition spec involved on this destination shape.
    """
    literal_is_valid(on_newcols, on_missingcols)

    print(f"[guard] running schema guards for {fqn}")

    current_schema = spark.table(fqn).schema     # no pre-check - native raise if fqn doesn't exist
    current_names = {f.name for f in current_schema}
    new_schema = df.schema
    new_names = set(df.columns)

    ##########

    # The write mechanism (.overwrite(span)) is VERIFIED 2026-09-10 against real UC tables.
    # The checks below (0/1/3/4/5) are new here and have NOT themselves been run for real yet.
    problems_silent = []   # the writer would NOT complain - we raise
    problems_loud = []     # the writer would complain on its own when write_partition is called - we only warn
    # sub need to check these

    ###################################
    ### schema sync enforce 0: span key columns must exist in the first place ###
    ###################################
    assert_partition_keys_exist(df, span_keys)
    # sub: since parition is not there, does these still make sense?

    ###################################
    ### schema sync enforce 1: no span key may contain null ###
    ###################################
    problem_collect = check_partition_keys_not_null(df, span_keys)
    problems_silent += problem_collect
    # sub: since parition is not there, does these still make sense?
    ###################################
    ### schema sync enforce 2: SKIPPED - this destination has no partition/cluster scheme ###
    ###################################

    ###################################
    ### schema sync enforce 3: Behaviour: on_missingcols (error or pad_null) ###
    ###################################
    df, new_names, problem_collect = reconcile_missing_columns(df, current_schema, new_names, on_missingcols)
    new_schema = df.schema
    problems_silent += problem_collect

    ###################################
    ### schema sync enforce 4: Behaviour: on_newcols (error, drop, or evolve additive) ###
    ###################################
    newcols_results = reconcile_new_columns(df, current_names, new_names, on_newcols)

    df = newcols_results.df
    new_names = newcols_results.new_names
    extra_cols = newcols_results.extra_cols
    problems_silent += newcols_results.problem_collect
    new_schema = df.schema

    # spark specific - an ALTER TABLE, no shared mechanism
    if extra_cols and newcols_results.needs_evolve:
        print(f"[guard] {fqn}: evolving schema - adding {sorted(extra_cols)}")
        col_defs = ', '.join(f'{c} {new_schema[c].dataType.simpleString()}' for c in sorted(extra_cols))
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")

    ##### updated all schemas ####
    finalized_df = df
    finalized_schema = spark.table(fqn).schema      # re-read: picks up the ALTER TABLE when evolve ran

    ###################################
    ### schema sync enforce 5: persist the exact type case of each column. ###
    ###################################
    problems_loud += check_types_match(finalized_schema, new_schema)

    ##### collect all problems ####
    raise_or_warn(
        problems_silent, problems_loud,
        engine_note=" iceberg will catch on its own - proceeding anyway, letting "
                    "overwrite() raise its own error")

    print('RUN')

    if full_refresh:
        write_partition_full_refresh(finalized_df, spark, fqn)
    else:
        write_partition(finalized_df, spark, fqn, span, span_keys)


def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
