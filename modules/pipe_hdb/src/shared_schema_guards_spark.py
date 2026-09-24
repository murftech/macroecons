"""Shared, Spark-native pieces of write_partition_guarded, imported by both
helper_sparkiceberg_io.py (real-partition-spec, local-JVM/Glue/BigLake catalogs)
and helper_databricks_io.py (Databricks-managed, no partition/cluster
spec on the destination). Pulled out 2026-09-22, extracted verbatim from
helper_sparkiceberg_io.py - the split exists because these checks are pure
column/schema logic with ZERO dependency on how the destination physically
overwrites data (real partition spec vs a windowed replaceWhere-style
overwrite): checks 0/1/3/4/5 never once reference a partition or a cluster
key, only column names, null counts, and types. Only check 2 (does the
destination's current partition/cluster SCHEME match what's being written)
and the actual write mechanism are genuinely destination-specific, and each
of those two files keeps its own version of those.

This is the Spark-side twin of shared_schema_guards.py (which is pyarrow-typed
and can't be reused here - a pa.Table has no .filter()/.withColumn(), a
Spark DataFrame has no .cast(pa.schema(...))). `literal_is_valid`,
`check_partition_keys_match_current`, and `raise_or_warn` are pure Python with
no pyarrow dependency either way, so both Spark files still import those
three directly from shared_schema_guards.py - only the four that ARE
DataFrame-typed live here.

`summarize_partitions` (2026-09-23) and its predicate-window twin `summarize_span`
(2026-09-24) are here for the same reason as the checks
above, just on partition VALUES instead of schema: it only ever looks at the
incoming DataFrame's partition_keys columns, never fqn or the write mechanism
(overwritePartitions() vs a windowed overwrite(span)), so it's exactly as
destination-agnostic as checks 0/1/3/4/5. Mirrors shared_schema_guards.
summarize_partitions (the pyarrow twin) in spirit, not signature - that one
returns (combos, values_per_col) for the old explicit-filter iceberg write;
nothing here needs the combinations back, so this one returns nothing.
"""
from typing import NamedTuple


# ── checks 0/1 - native Spark, no pyarrow equivalent to reuse ─────────────────

def assert_partition_keys_exist(df, partition_keys: list):
    """Check 0 - immediate, gated raise, no toggle. Same role as
    shared_schema_guards.assert_partition_keys_exist, reimplemented against a
    Spark DataFrame's .columns instead of a pa.Table's .column_names.
    """
    print("schema sync enforce 0: partition key columns must exist in the incoming data")
    missing = set(partition_keys) - set(df.columns)
    if missing:
        raise Exception(
            f"[guard] partition_keys column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(df.columns)}) - refusing to write.\n"
            "no push"
        )


def check_partition_keys_not_null(df, partition_keys: list) -> list:
    """Check 1 - collects null-partition-key problems (does NOT raise - caller
    decides silent vs loud). No non-nullable cast step like pyarrow's twin -
    Spark doesn't enforce schema nullability the same way at write time, so
    the VALUE check is the whole load-bearing part here.
    """
    print("schema sync enforce 1: no partition key column may contain a null value")
    from sparkutils.functions import col
    null_partition_cols = {}
    for c in partition_keys:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            null_partition_cols[c] = null_count

    problem_collect = []
    if null_partition_cols:
        problem_collect += [
            f"partition column(s) {null_partition_cols} have null value(s) - a null "
            f"partition key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        ]
    return problem_collect


# ── checks 3/4/5 - native Spark reimplementation of the pyarrow-typed checks ──

def reconcile_missing_columns(df, current_schema, new_names: set, on_missingcols: str):
    """Check 3 (native Spark). Returns (df, new_names, problems). Same shape as
    shared_schema_guards.reconcile_missing_columns, reimplemented for StructType.
    """
    from sparkutils.functions import lit
    print("schema sync enforce 3: push data must have ALL columns of destination schema. Behaviour: on_missingcols")
    current_names = {f.name for f in current_schema}
    missing_cols = current_names - new_names

    problem_collect = []

    if missing_cols:

        if on_missingcols == 'error':
            problem_collect += [
                f"incoming batch is MISSING columns {sorted(missing_cols)} "
                f"(pass on_missingcols='pad_null' to null-pad and push instead)"
            ]

        elif on_missingcols == 'pad_null':
            print(f"[guard] auto-padding {sorted(missing_cols)} as null "
                  f"(present on destination, absent from this batch)")
            for field in current_schema:
                if field.name in missing_cols:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
            new_names = new_names | missing_cols

        else:
            raise Exception('invalid on_missingcols')

    return df, new_names, problem_collect


class NewColsResult(NamedTuple):
    """Same fields, same order as shared_schema_guards.NewColsResult - `df` in place of `arrow_table`."""
    df: object
    new_names: set
    problem_collect: list
    extra_cols: set
    needs_evolve: bool      # True only for 'evolve' with new columns: the caller still has to do it


def reconcile_new_columns(df, current_names: set, new_names: set, on_newcols: str) -> NewColsResult:
    """The 'error'/'drop' halves of check 4 only - 'evolve' stays caller-owned
    (needs an ALTER TABLE, genuinely engine-specific - same split as
    shared_schema_guards.reconcile_new_columns).
    """
    print("schema sync enforce 4: push data must not have new columns unless told to. Behaviour: on_newcols")
    extra_cols = new_names - current_names
    problem_collect = []
    needs_evolve = False

    if extra_cols:
        if on_newcols == 'error':
            problem_collect += [
                f"incoming batch has NEW columns {sorted(extra_cols)} "
                f"(pass on_newcols='evolve' to write them through, or on_newcols='drop' "
                f"to discard them)"
            ]

        elif on_newcols == 'drop':
            print(f"[guard] dropping NEW columns {sorted(extra_cols)} "
                  f"- on_newcols='drop', not writing them.")
            df = df.drop(*sorted(extra_cols))
            new_names = new_names - extra_cols

        else:  # 'evolve' - not done here, caller does its own ALTER TABLE
            needs_evolve = True

    return NewColsResult(df, new_names, problem_collect, extra_cols, needs_evolve)


def check_types_match(current_schema, new_schema) -> list:
    """Check 5 (native Spark). The shared column names are worked out here, from the two schemas."""
    print("schema sync enforce 5: persist the exact type case of each column.")
    problem_collect = []

    current_types = {f.name: f.dataType for f in current_schema}
    new_types = {f.name: f.dataType for f in new_schema}
    common_names = set(current_types) & set(new_types)

    for columnname in sorted(common_names):
        if current_types[columnname] != new_types[columnname]:
            problem_collect += [
                f"type change found on '{columnname}': {current_types[columnname]} -> {new_types[columnname]}"
            ]

    return problem_collect


# ── partition-value summary - not a check, but same destination-agnostic shape ──

def summarize_partitions(df, partition_keys: list, label: str, show_partitions=False, CAP=30):
    """Print what a write is about to replace - Spark-native twin of
    shared_schema_guards.summarize_partitions (that one is pyarrow-typed,
    .group_by()/.aggregate() on a pa.Table, so a Spark DataFrame can't reuse
    it). df.select(keys).distinct().collect() is the Spark equivalent of
    arrow_table.select(keys).group_by(keys).aggregate([]).to_pylist() -
    verified 2026-09-22, same output format, same cap behaviour. No return
    value: unlike pyiceberg's old explicit-filter write, nothing here needs
    the combinations back - overwritePartitions()/overwrite(lit(True))/
    overwrite(span) never consume a filter built from them.
    """
    combos = [r.asDict() for r in df.select(partition_keys).distinct().collect()]
    values_per_col = {c: sorted({r[c] for r in combos}) for c in partition_keys}

    n_leaves = len(combos)
    if n_leaves == 0:
        print(f"[{label}] incoming batch has 0 rows - no partitions to replace.")
        return

    print(f"[{label}] replacing {n_leaves} leaf partitions on {'/'.join(partition_keys)}:")
    for c, vals in values_per_col.items():
        print(f"    {c:<14} {len(vals):>4} values   {vals[0]} … {vals[-1]}")

    if show_partitions:
        print('show partitions')
        rows = sorted(combos, key=lambda r: [r[c] for c in partition_keys])
        for r in rows[:CAP]:
            print("    " + "/".join(f"{c}={r[c]}" for c in partition_keys))
        if len(rows) > CAP:
            print(f"    … and {len(rows) - CAP} more")


def summarize_span(df, span_col: str, label: str, show_partitions=False, CAP=30):
    """Print what the incoming batch holds on span_col - the twin of summarize_partitions
    for PREDICATE-WINDOW destinations (delta, Databricks-managed iceberg): no partition
    folders there, so "leaf partitions" would be wrong - rows inside a window are replaced.

    Counts only the values PRESENT in the batch; the overwrite itself replaces the whole
    window (bounds), which can include months with no incoming rows - the writer's own
    "windowed overwrite ... lo..hi" print is the authoritative one.
    Same destination-agnostic shape as summarize_partitions: only ever reads the
    incoming DataFrame, never fqn or the write mechanism.
    """
    values = sorted(r[0] for r in df.select(span_col).distinct().collect())
    if not values:
        print(f'[{label}] incoming batch has 0 rows')
        return
    print(f'[{label}] incoming batch: {len(values)} distinct {span_col} values {values[0]} … {values[-1]}')
    if show_partitions:
        for v in values[:CAP]:
            print(f'    {span_col}={v}')
        if len(values) > CAP:
            print(f'    … and {len(values) - CAP} more')
