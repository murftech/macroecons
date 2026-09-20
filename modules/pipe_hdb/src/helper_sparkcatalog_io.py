"""Write / read a catalog-registered Iceberg table via Spark's V2 writer, using a
REAL partition spec - the local-JVM / AWS Glue / GCP BigLake shape, not the
Databricks-managed-table (Liquid Clustering) shape. See PROVIDERS_DEFENSE.md's
"4 write configurations" discussion for the full locked architecture; this file
covers only the "real partitioning" rows of that table for now.

MVP SCOPE (2026-09-16): iceberg only, not clustered, not delta. Built to mirror
helper_pyiceberg_io.py's write_partition / write_partition_guarded split as
closely as possible - same function names, same partition_keys-first calling
convention, same on_newcols/on_missingcols vocabulary, same "table must already
exist, provisioning is a separate explicit step, never implicit here" contract.
Delta support and the Databricks-clustered path are deliberately deferred -
"iceberg first, then clean up for delta" was the explicit call.

Does NOT run on Sail (Rust, no JVM, no Iceberg JARs) - that is why the
path-based helper (helper_pyiceberg_io) stays separate. Needs a JVM Spark
session with the Iceberg Spark extension + a catalog configured (see
`_iceberg_local_session_config()` in the test file for how to do that entirely
from calling code, with zero edits to the separate sparkutils repo).

writeTo(...) semantics used here:
  .overwritePartitions()   dynamic partition overwrite - Spark derives which
                           partitions to replace from partition_keys' actual
                           values in the incoming DataFrame, same idea as
                           helper_pyiceberg_io.write_partition's In(col, vals)
                           filter, just computed by Spark internally instead
                           of built explicitly in Python. REQUIRES a real
                           partition spec on the destination table - this is
                           exactly why it's rejected outright on a
                           Liquid-Clustered (Databricks-managed) table, and
                           exactly why that case needs a different function
                           entirely, not a flag on this one.

Schema guard - checks 0-5, same shape as helper_pyiceberg_io.write_partition_guarded:
  `validate_guard_args`, `check_partition_keys_match` (check 2), and
  `raise_or_warn` are imported directly from shared_schema_guards.py - they're
  pure Python with no pyarrow dependency, so there's nothing to reimplement.
  Checks 0/1 (partition key existence/nullability) and checks 3/4/5 (schema
  reconciliation) are natively reimplemented against Spark's DataFrame/
  StructType - deliberately not a pyarrow<->Spark type conversion, per the
  session's own explicit call on that tradeoff.

VERIFIED 2026-09-16 against a real local JVM Iceberg catalog (booted from a
  standalone script, zero edits to sparkutils - see the "load the JAR without
  touching sparkutils" pattern in this session's own history): write_partition_
  guarded's checks 0/1/2/3/4 and the "table must already exist" contract all
  confirmed correct on the first real run, except `_current_partition_columns`'s
  DESCRIBE TABLE parsing, which was wrong (guessed a transform-expression format
  that isn't what Iceberg actually emits) and has since been corrected against
  the real output. See its own docstring for the confirmed format.
"""
from typing import Literal

from shared_schema_guards import validate_guard_args, check_partition_keys_match, raise_or_warn


# ── checks 0/1 - native Spark, no pyarrow equivalent to reuse ─────────────────

def assert_partition_keys_exist(df, partition_keys: list, dest_label: str):
    """Check 0 - immediate, gated raise, no toggle. Same role as
    shared_schema_guards.assert_partition_keys_exist, reimplemented against a
    Spark DataFrame's .columns instead of a pa.Table's .column_names.
    """
    missing = set(partition_keys) - set(df.columns)
    if missing:
        raise Exception(
            f"[guard] {dest_label}: partition_keys column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(df.columns)}) - refusing to write.\n"
            "no push"
        )


def check_partition_keys_not_null(df, partition_keys: list) -> list:
    """Check 1 - collects null-partition-key problems (does NOT raise - caller
    decides silent vs loud). No non-nullable cast step like pyarrow's twin -
    Spark doesn't enforce schema nullability the same way at write time, so
    the VALUE check is the whole load-bearing part here.
    """
    from sparkutils.functions import col
    null_partition_cols = {}
    for c in partition_keys:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            null_partition_cols[c] = null_count
    problems = []
    if null_partition_cols:
        problems.append(
            f"partition column(s) {null_partition_cols} have null value(s) - a null "
            f"partition key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        )
    return problems


# ── checks 3/4/5 - native Spark reimplementation of the pyarrow-typed checks ──

def align_down(df, target_schema):
    """Reindex `df` to `target_schema`: add columns the table has but `df` lacks
    as typed nulls, then select in the table's column order. Called after
    reconcile_missing_columns/reconcile_new_columns have already run (their
    padding/dropping makes this a no-op reorder by then) - same relationship
    as helper_pyiceberg_io's own internal alignment step before write_partition.
    """
    from sparkutils.functions import lit
    aligned = df
    for field in target_schema:
        if field.name not in aligned.columns:
            aligned = aligned.withColumn(field.name, lit(None).cast(field.dataType))
    return aligned.select([f.name for f in target_schema])


def reconcile_missing_columns(df, old_schema, new_names: set, on_missingcols: str, dest_label: str):
    """Check 3 (native Spark). Returns (df, new_names, problems). Same shape as
    shared_schema_guards.reconcile_missing_columns, reimplemented for StructType.
    """
    from sparkutils.functions import lit
    old_names = {f.name for f in old_schema}
    missing_cols = old_names - new_names
    problems = []
    if missing_cols:
        if on_missingcols == 'error':
            problems.append(
                f"incoming batch is MISSING columns {sorted(missing_cols)} "
                f"(pass on_missingcols='pad_null' to null-pad and push instead)"
            )
        else:  # 'pad_null'
            print(f"[guard] {dest_label}: auto-padding {sorted(missing_cols)} as null "
                  f"(present on destination, absent from this batch)")
            for field in old_schema:
                if field.name in missing_cols:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
            new_names = new_names | missing_cols
    return df, new_names, problems


def reconcile_new_columns_drop_or_error(df, old_names: set, new_names: set, on_newcols: str, dest_label: str):
    """The 'error'/'drop' halves of check 4 only - 'evolve' stays caller-owned
    (needs an ALTER TABLE, genuinely engine-specific - same split as
    shared_schema_guards.reconcile_new_columns_drop_or_error).
    Returns (df, new_names, extra_cols, problems, handled).
    """
    extra_cols = new_names - old_names
    problems = []
    if not extra_cols:
        return df, new_names, extra_cols, problems, True

    if on_newcols == 'error':
        problems.append(
            f"incoming batch has NEW columns {sorted(extra_cols)} "
            f"(pass on_newcols='evolve' to write them through, or on_newcols='drop' "
            f"to discard them)"
        )
        return df, new_names, extra_cols, problems, True
    elif on_newcols == 'drop':
        print(f"[guard] {dest_label}: dropping NEW columns {sorted(extra_cols)} "
              f"- on_newcols='drop', not writing them.")
        df = df.drop(*sorted(extra_cols))
        new_names = new_names - extra_cols
        return df, new_names, extra_cols, problems, True
    else:  # 'evolve' - not handled here, caller does its own ALTER TABLE
        return df, new_names, extra_cols, problems, False


def check_types_match(old_schema, new_schema, common_names) -> list:
    """Check 5 (native Spark) - loud only, never blocks on its own. Iceberg's
    own writer may still catch a mismatch natively - same relationship as the
    pyarrow/pyiceberg guard has to its own engines' native checks.
    """
    problems = []
    old_types = {f.name: f.dataType for f in old_schema}
    new_types = {f.name: f.dataType for f in new_schema}
    for n in sorted(common_names):
        if old_types[n] != new_types[n]:
            problems.append(f"type change on '{n}': {old_types[n]} -> {new_types[n]}")
    return problems


def _current_partition_columns(spark, fqn):
    """Fetch an existing Iceberg table's current partition columns, by parsing
    DESCRIBE TABLE's "# Partition Information" section.

    VERIFIED empirically 2026-09-16 against a real local Iceberg catalog - the
    actual format (confirmed by inspection, not the guess this originally
    shipped with) is:
        Row(col_name='# Partition Information', data_type='', comment='')
        Row(col_name='# col_name', data_type='data_type', comment='comment')
        Row(col_name='tx_monthdate', data_type='string', comment=None)
    i.e. identity-partitioned columns are listed by plain name with their data
    type - NOT as a `identity(col)`-style transform expression, which is what
    this function originally (wrongly) assumed before being run for real.
    """
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


# ── provisioning - minimal, explicit, never implicit from the write path ──────

def create_table(df, spark, fqn, partition_keys: list):
    """Minimal, one-shot CREATE - the real equivalent of
    helper_pyiceberg_io.createOrEvolve_table's *first-creation* branch only.
    Does NOT evolve an existing table's schema/partition spec (createOrEvolve_
    table's other job) - that's deliberately out of scope for this MVP pass,
    not yet built. Provisioning stays separate and explicit on purpose, same
    as pyiceberg - write_partition_guarded below never creates a table itself.
    """
    from sparkutils.functions import col as _col
    df.writeTo(fqn).using('iceberg').partitionedBy(*[_col(c) for c in partition_keys]).create()
    print(f'DONE:  iceberg -> {fqn}  (created, partitioned by {partition_keys})')


# ── the write path itself - mirrors helper_pyiceberg_io.py exactly ───────────

def write_partition(df, spark, fqn, partition_keys: list):
    """Lazy write - zero checks, for POC / quick-and-dirty use only. Mirrors
    helper_pyiceberg_io.write_partition's role exactly: the raw mechanism
    write_partition_guarded calls into once checks pass. Requires the table to
    already exist (NoSuchTableError-equivalent otherwise, via Spark's own
    native error - not this function's job to create it).
    """
    print(f'[iceberg] dynamic partition overwrite on {partition_keys}')
    df.writeTo(fqn).overwritePartitions()
    print(f'DONE:  iceberg -> {fqn}  (overwritePartitions)')


def write_partition_guarded(df, spark, fqn, partition_keys: list, *,
                            on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                            on_missingcols: Literal['pad_null', 'error'] = 'error'):
    """A schema guard against the table's CURRENT schema, before calling
    write_partition() - same role, same checks 0-5, same on_newcols/
    on_missingcols vocabulary as helper_pyiceberg_io.write_partition_guarded.

    Requires `fqn` to already exist as a table - provisioning (create_table
    above, or the fuller createOrEvolve_table equivalent, not yet built) is a
    separate, deliberate step, never implicit here. Mirrors pyiceberg's own
    catalog.load_table() raising when the table doesn't exist yet.
    """
    validate_guard_args(on_newcols, on_missingcols)

    if not spark.catalog.tableExists(fqn):
        raise Exception(
            f"[guard] {fqn}: table does not exist - provisioning is a separate, "
            f"explicit step (create_table), never implicit inside write_partition_guarded."
        )

    old_schema = spark.table(fqn).schema
    old_names, new_names = {f.name for f in old_schema}, set(df.columns)

    print('schema sync enforce 0: partition key columns must exist in the incoming data')
    assert_partition_keys_exist(df, partition_keys, fqn)

    print('schema sync enforce 1: no partition key column may contain a null value')
    problems_silent = check_partition_keys_not_null(df, partition_keys)

    print("schema sync enforce 2: partition_keys must match the table's current spec")
    current_keys = _current_partition_columns(spark, fqn)
    problems_silent += check_partition_keys_match(partition_keys, current_keys)

    print('schema sync enforce 3: push data must have ALL columns of destination schema')
    df, new_names, problems = reconcile_missing_columns(df, old_schema, new_names, on_missingcols, fqn)
    problems_silent += problems

    print('schema sync enforce 4: push data must not have new columns unless told to')
    df, new_names, extra_cols, problems, handled = reconcile_new_columns_drop_or_error(
        df, old_names, new_names, on_newcols, fqn)
    problems_silent += problems

    if extra_cols and not handled:  # 'evolve' - genuinely engine-specific, no shared mechanism
        print(f"[guard] {fqn}: evolving schema - adding {sorted(extra_cols)}")
        col_defs = ', '.join(f'{c} {df.schema[c].dataType.simpleString()}' for c in sorted(extra_cols))
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")
        old_schema = spark.table(fqn).schema
        old_names = {f.name for f in old_schema}

    print('schema sync enforce 5: persist the exact type case of each column.')
    problems_loud = check_types_match(old_schema, df.schema, old_names & new_names)

    raise_or_warn(problems_silent, problems_loud, fqn,
                 engine_note=" iceberg will catch on its own - proceeding anyway, "
                             "letting overwritePartitions() raise its own error")

    aligned = align_down(df, spark.table(fqn).schema)
    write_partition(aligned, spark, fqn, partition_keys)


def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
