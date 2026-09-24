"""Write / read a catalog-registered Iceberg table on DATABRICKS via Spark's V2 writer,
for Unity-Catalog MANAGED tables - no real partition spec, no clustering either.

VERIFIED 2026-09-22 via DESCRIBE TABLE EXTENDED against the real macroecons.t1 tables:
neither the iceberg table nor its Delta twin declares a "# Partition Information" or a
"Clustering Information" section at all - they are plain, unpartitioned, unclustered
managed tables. That's the whole reason this file exists separately from
helper_sparkiceberg_io.py: that file's write mechanism, .writeTo(fqn).overwritePartitions(),
REQUIRES a real partition spec and would be rejected outright here - there's nothing on
this destination for it to derive partitions from.

Recovers the write mechanism from src/backup/helper_catalog_io_og.py's create_or_overwrite
(VERIFIED 2026-09-10 against real UC tables - "all 4 UC tables = 986,355 = local exactly",
".overwrite(span) confirmed selective"), which providers/databricks.py has kept calling
under that old name ever since - unmigrated when helper_sparkcatalog_io.py (renamed
helper_sparkiceberg_io.py 2026-09-22) was pivoted to the iceberg-only/real-partition-spec
MVP on 2026-09-16. This file is that migration: the SAME .overwrite(span) mechanism,
wrapped in the newer schema-guard framework (checks 0/1/3/4/5, on_newcols/on_missingcols,
"provisioning is separate, never implicit").

MVP SCOPE (2026-09-22): iceberg only, matching this session's defense-pass scope. Delta
support (fmt='delta') is deliberately deferred - same "iceberg first" call as
helper_sparkiceberg_io.py's own history.

Schema guard: checks 0/1/3/4/5 are IMPORTED from shared_schema_guards_spark.py, shared
VERBATIM with helper_sparkiceberg_io.py - pure column/schema logic, no partitioning
concept in any of them. check_partition_keys_match_current (check 2 elsewhere) is NOT
used here: there is no partition/cluster scheme on this destination to compare a
requested one against (verified above) - the old create_or_overwrite never had a check 2
equivalent either.

Requires `fqn` to already exist as a table - provisioning (create_table below) is a
separate, deliberate step, never implicit here, same contract as
helper_sparkiceberg_io.write_partition_guarded. No pre-check: relies on spark.table(fqn)
raising its own native AnalysisException when missing.

NOT YET VERIFIED end-to-end against a real Databricks job run. The write mechanism
(.overwrite(span)) is proven (2026-09-10); the schema-guard wrapper built around it here
(checks 0/1/3/4/5, on_newcols/on_missingcols, explicit create_table) is new and has not
itself been run for real yet - that's what the first deploy attempt is for.
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

def create_table(df, spark, fqn):
    """Minimal, one-shot CREATE - no partitioning, no clustering (matches the real
    macroecons.t1 tables' own shape, verified 2026-09-22). Does NOT evolve an
    existing table's schema - that's write_partition_guarded's on_newcols='evolve'
    job, via ALTER TABLE.

    Stays Spark-native here, unlike helper_sparkiceberg_io (which dropped its own
    create_table 2026-09-22 in favor of helper_pyiceberg_io.createOrEvolve_table) -
    pyiceberg's SqlCatalog has no bridge into Unity Catalog, so there is no
    equivalent provisioning path to defer to for a Databricks-managed table.
    """
    df.writeTo(fqn).using('iceberg').create()
    print(f'DONE:  iceberg -> {fqn}  (created, no partition/cluster spec)')


# ── the write path itself ──────────────────────────────────────────────────

def write_partition(df, spark, fqn, span, span_keys: list):
    """Lazy write - zero checks, for POC / quick-and-dirty use only. The proven
    mechanism (VERIFIED 2026-09-10 against real UC tables - ".overwrite(span)
    confirmed selective") recovered from the old create_or_overwrite: replace
    exactly the rows matching `span` (a boolean Column predicate, built by the
    caller - e.g. a date-range window), leave everything outside it untouched.
    Requires the table to already exist - Spark's own native error otherwise,
    not this function's job to create it.
    """
    print(f'[iceberg] windowed overwrite on {span_keys}')
    df.writeTo(fqn).overwrite(span)
    print(f'DONE:  iceberg -> {fqn}  (overwrite, span-windowed)')


def write_partition_full_refresh(df, spark, fqn):
    """Drop ALL existing rows, replace with `df` - same .overwrite(lit(True))
    mechanism as helper_sparkiceberg_io's, verified 2026-09-22: wipes every row,
    keeps the table's identity/schema, still schema-checked (an incompatible
    batch raises natively). NOT the same as .writeTo(fqn).replace(), which also
    lets the schema itself change - see helper_sparkiceberg_io's own docstring
    on this for the tested reasoning.
    """
    from sparkutils.functions import lit
    print(f'[iceberg] full refresh on {fqn} - dropping ALL existing rows, replacing with {df.count()} incoming rows')
    df.writeTo(fqn).overwrite(lit(True))
    print(f'DONE:  iceberg -> {fqn}  (overwrite, full refresh)')


def write_partition_guarded(df, spark, fqn, span, span_keys: list, *,
                            on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                            on_missingcols: Literal['pad_null', 'error'] = 'error',
                            full_refresh=False):
    """A schema guard against the table's CURRENT schema, before calling write_partition().
    Same checks 0/1/3/4/5, same on_newcols/on_missingcols vocabulary as
    helper_sparkiceberg_io.write_partition_guarded - NO check 2: this destination
    carries no partition/cluster scheme to compare a requested one against
    (verified 2026-09-22 via DESCRIBE TABLE EXTENDED - no "# Partition Information"
    section at all on the real table).

    Requires `fqn` to already exist - provisioning (create_table above) is a
    separate, deliberate step, never implicit here. No pre-check: relies on
    spark.table(fqn) raising its own native AnalysisException when missing,
    same contract as helper_sparkiceberg_io.write_partition_guarded.

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

    ###################################
    ### schema sync enforce 0: span key columns must exist in the first place ###
    ###################################
    assert_partition_keys_exist(df, span_keys)

    ###################################
    ### schema sync enforce 1: no span key may contain null ###
    ###################################
    problem_collect = check_partition_keys_not_null(df, span_keys)
    problems_silent += problem_collect

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
