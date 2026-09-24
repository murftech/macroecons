"""Write / read a Delta table via Spark's V2 writer (JVM only) - predicate overwrite,
no partition spec needed.

Scope (the file's real contract): any destination doing ROW-LEVEL overwrite by predicate:
  - Delta on any JVM Spark - local (delta-spark 4.0.1, DeltaCatalog as spark_catalog,
    table addressed by path: delta.`<abs path>`) and Databricks. VERIFIED 2026-09-24
    locally: overwrite(span) on a file only PARTLY inside span replaces just the
    matching rows.
  - Databricks-managed Iceberg (VERIFIED 2026-09-10, .overwrite(span) selective).
NOT for OSS Iceberg on an unpartitioned table: overwrite(span) raises
ValidationException "Cannot delete file where some, but not all, rows match filter"
(VERIFIED 2026-09-24, iceberg-spark-runtime 1.10.0) - use helper_sparkiceberg_io there.

Delta-only extras, both enforced natively (verified 2026-09-24), NOT by the guard below:
  - a row OUTSIDE span is rejected (DELTA_REPLACE_WHERE_MISMATCH) - no duplicates outside the window.
  - a null in a NOT NULL column is rejected (DELTA_NOT_NULL_CONSTRAINT_VIOLATED).
A Databricks-managed Iceberg table passed here gets neither (not verified either way).

Provisioning is NOT here - helper_deltalake_io.createOrEvolve_table (delta-rs) owns it.
"""

from typing import Literal

from shared_schema_guards import literal_is_valid, raise_or_warn
from shared_schema_guards_spark import (
    assert_partition_keys_exist,
    check_partition_keys_not_null,
    reconcile_missing_columns,
    reconcile_new_columns,
    check_types_match,
    summarize_span,
)


# ── provisioning - minimal, explicit, never implicit from the write path ──────

# def create_table(df, spark, fqn):
#     df.writeTo(fqn).using('iceberg').create()
#     print(f'DONE:  iceberg -> {fqn}  (created, no partition/cluster spec)')
# # sub do we need to delete this and allow provision in databricks. or we need this becasue
# # I need to make sure my provision all in one place.
# # can i use cli to create this table or better not


# ── the write path itself ──────────────────────────────────────────────────

def build_span(df, span_col: str, bounds=None):
    """The overwrite window, decided at WRITE time from the batch itself - returns
    (span, lo, hi), span being a boolean Column predicate on `span_col`.

    `bounds=None`         : lo..hi = this batch's own min..max span_col (bronze - each
                            era is one contiguous month block, so min..max is exact).
    `bounds=(start, end)` : exactly {start}-01..{end}-01 (silver - the REQUESTED window;
                            months in it absent from the batch get emptied, which is the point).
    """
    from sparkutils.functions import F
    if bounds is not None:
        lo, hi = f'{bounds[0]}-01', f'{bounds[1]}-01'
    else:
        lo, hi = [str(v) for v in df.select(F.min(span_col), F.max(span_col)).first()]
    span = F.expr(f"{span_col} >= DATE'{lo}' AND {span_col} <= DATE'{hi}'")
    return span, lo, hi


def write_partition(df, spark, fqn, span_col: str, bounds=None):
    """Lazy write - zero checks, for POC / quick-and-dirty use only."""
    span, lo, hi = build_span(df, span_col, bounds)
    print(f'[delta] windowed overwrite on {span_col} {lo}..{hi} (every row in this window is replaced)')
    df.writeTo(fqn).overwrite(span)
    print(f'DONE:  delta -> {fqn}  (overwrite, span-windowed)')
# sub: the span must be determined in write time, not pre given?


def write_partition_full_refresh(df, spark, fqn):
    from sparkutils.functions import lit
    print(f'[delta] full refresh on {fqn} - dropping ALL existing rows, replacing with {df.count()} incoming rows')
    df.writeTo(fqn).overwrite(lit(True))
    print(f'DONE:  delta -> {fqn}  (overwrite, full refresh)')
# sub: explain this


def write_partition_guarded(df, spark, fqn, span_col: str, bounds=None, show_partitions=False, *,
                            on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                            on_missingcols: Literal['pad_null', 'error'] = 'error',
                            full_refresh=False):
    """
    `span_col` : the DATE column the overwrite window is built on (e.g. tx_monthdate) -
                 checked for existence (check 0) and nulls (check 1) the same way
                 partition_keys are elsewhere, even though there is no partition spec
                 on this destination shape.
    `bounds`   : None = window from the batch's own min..max span_col; (start, end) =
                 exactly that month window. See build_span - the window is decided here,
                 at write time, not handed in pre-built (changed 2026-09-24).
    `show_partitions` : list every distinct span_col value being replaced.
    """
    span_keys = [span_col]      # checks 0/1 take a list
    literal_is_valid(on_newcols, on_missingcols)

    print(f"[guard] running schema guards for {fqn}")

    current_schema = spark.table(fqn).schema     # no pre-check - native raise if fqn doesn't exist
    current_names = {f.name for f in current_schema}
    new_schema = df.schema
    new_names = set(df.columns)

    ##########

    # The write mechanism (.overwrite(span)) is VERIFIED 2026-09-10 against real UC tables
    # and 2026-09-24 against local delta.
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

    finalized_df = df

    ###################################
    ### schema sync enforce 5: persist the exact type case of each column. ###
    ###################################
    # against the PRE-evolve schema: evolve only adds columns, which aren't shared
    # columns yet, so the compared set is identical to comparing after the ALTER.
    problems_loud += check_types_match(current_schema, new_schema)

    ##### collect all problems ####
    raise_or_warn(
        problems_silent, problems_loud,
        engine_note=" delta will catch on its own - proceeding anyway, letting "
                    "overwrite() raise its own error")

    # spark specific - an ALTER TABLE, no shared mechanism.
    # AFTER raise_or_warn on purpose (moved 2026-09-24): the destination's schema is
    # only ever changed once the guard's verdict is "push" - before, a batch could
    # add columns to the table and then still be refused with "no push".
    if extra_cols and newcols_results.needs_evolve:
        print(f"[guard] {fqn}: evolving schema - adding {sorted(extra_cols)}")
        col_defs = ', '.join(f'{c} {new_schema[c].dataType.simpleString()}' for c in sorted(extra_cols))
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")

    print('RUN')

    if full_refresh:
        write_partition_full_refresh(finalized_df, spark, fqn)
    else:
        # checks 0/1 passed above, so min/max below can't be skewed by a missing col or nulls
        summarize_span(finalized_df, span_col, label='delta', show_partitions=show_partitions)
        write_partition(finalized_df, spark, fqn, span_col, bounds)


def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
