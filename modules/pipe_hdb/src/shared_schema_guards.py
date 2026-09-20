"""Shared, engine-agnostic pieces of write_partition_guarded, imported by
helper_pyarrow_io.py, helper_pyiceberg_io.py, and (2026-09-16) helper_sparkcatalog_io.py.

Most of this operates on pyarrow Tables/Schemas - no pyarrow.dataset path
logic, no pyiceberg catalog/table logic, no Spark DataFrame logic either.
That split is not arbitrary: reviewing all three guards found that checks
0/1/3/5, plus the comparison halves of checks 2/4, never actually touch an
engine-specific object - only fetching "what's already there" (a filesystem
footer union, catalog.load_table, or spark.table(fqn).schema) and the
'evolve' remediation (a no-op-ish write-through, a real tbl.update_schema()
mutation, or an ALTER TABLE ... ADD COLUMNS) are genuinely engine-specific.

Three functions here are pure Python with NO pyarrow dependency at all -
`validate_guard_args`, `check_partition_keys_match`, `raise_or_warn` - and are
imported directly by helper_sparkcatalog_io.py without any reimplementation;
they were already engine-agnostic before Spark ever needed them, not
retrofitted. Everything else here IS pyarrow-typed (checks 3/4/5, plus checks
0/1's arrow_table versions) - helper_sparkcatalog_io.py reimplements THOSE
natively against Spark's own StructType/DataFrame rather than converting
types, a deliberate choice (see PROVIDERS_DEFENSE.md) to avoid pyarrow<->Spark
type-conversion risk on already-production-verified code for no real benefit.
"""
import pyarrow as pa


_VALID_NEWCOLS = ('evolve', 'drop', 'error')
_VALID_MISSINGCOLS = ('pad_null', 'error')


def validate_guard_args(on_newcols, on_missingcols):
    """Raise ValueError if either toggle isn't one of its known values."""
    if on_newcols not in _VALID_NEWCOLS:
        raise ValueError(f"on_newcols must be one of {_VALID_NEWCOLS}, got {on_newcols!r}")
    if on_missingcols not in _VALID_MISSINGCOLS:
        raise ValueError(f"on_missingcols must be one of {_VALID_MISSINGCOLS}, got {on_missingcols!r}")


def assert_partition_keys_exist(arrow_table: pa.Table, partition_keys: list, dest_label: str):
    """Check 0 - immediate, gated raise, no toggle. Has to run before anything
    else calls arrow_table.column(col) for a partition key, or a native pyarrow
    error fires first with a much less clear message (verified empirically -
    this is the exact bug that motivated pulling this check out on its own).
    """
    missing = set(partition_keys) - set(arrow_table.column_names)
    if missing:
        raise Exception(
            f"[guard] {dest_label}: partition_keys column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(arrow_table.column_names)}) - refusing "
            f"to write.\n"
            "no push"
        )


def check_and_cast_partition_keys_not_null(arrow_table: pa.Table, partition_keys: list):
    """Check 1 - collects null-partition-key problems (does NOT raise - caller
    decides silent vs loud) and casts every OTHER partition column to
    nullable=False (skips any column with a recorded problem - casting one that
    genuinely has nulls would itself raise, a different failure in front of the
    one actually being reported). Returns (arrow_table, problems: list[str]).
    """
    null_partition_cols = {}
    for col in partition_keys:
        null_count = arrow_table.column(col).null_count
        if null_count > 0:
            null_partition_cols[col] = null_count

    problems = []
    if null_partition_cols:
        problems.append(
            f"partition column(s) {null_partition_cols} have null value(s) - a null "
            f"partition key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        )

    arrow_table = arrow_table.cast(pa.schema([
        f.with_nullable(False) if (f.name in partition_keys and f.name not in null_partition_cols) else f
        for f in arrow_table.schema
    ]))
    return arrow_table, problems


def check_partition_keys_match(partition_keys: list, current_keys) -> list:
    """Check 2's comparison only - caller computes current_keys however its
    engine represents "the destination's current partition scheme" (a plain
    list for pyarrow's on-disk footer union, tbl.spec().fields for iceberg).
    Returns a list of 0 or 1 problem strings.
    """
    if set(partition_keys) != set(current_keys):
        return [f"partition scheme change: destination has {sorted(current_keys)}, "
                f"writing {list(partition_keys)}. wipe/re-provision to re-partition."]
    return []


def reconcile_missing_columns(arrow_table: pa.Table, old_schema: pa.Schema, new_names: set,
                              on_missingcols: str, dest_label: str):
    """Check 3. Returns (arrow_table, new_names, problems)."""
    old_names = set(old_schema.names)
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
            for col in missing_cols:
                arrow_table = arrow_table.append_column(
                    col, pa.nulls(arrow_table.num_rows, old_schema.field(col).type)
                )
            new_names = new_names | missing_cols
    return arrow_table, new_names, problems


def reconcile_new_columns_drop_or_error(arrow_table: pa.Table, old_names: set, new_names: set,
                                        on_newcols: str, dest_label: str):
    """The 'error'/'drop' halves of check 4 only - 'evolve' stays engine-owned
    on purpose (pyarrow just writes the column through with a warning; iceberg
    genuinely mutates the catalog schema via tbl.update_schema() - no shared
    mechanism exists for that, and trying to force one would be fake symmetry).

    Returns (arrow_table, new_names, extra_cols, problems, handled).
    handled=False means on_newcols=='evolve' and extra_cols is non-empty - the
    caller still owns doing its own engine-specific thing with it.
    """
    extra_cols = new_names - old_names
    problems = []
    if not extra_cols:
        return arrow_table, new_names, extra_cols, problems, True

    if on_newcols == 'error':
        problems.append(
            f"incoming batch has NEW columns {sorted(extra_cols)} "
            f"(pass on_newcols='evolve' to write them through, or on_newcols='drop' "
            f"to discard them)"
        )
        return arrow_table, new_names, extra_cols, problems, True
    elif on_newcols == 'drop':
        print(f"[guard] {dest_label}: dropping NEW columns {sorted(extra_cols)} "
              f"- on_newcols='drop', not writing them.")
        arrow_table = arrow_table.drop_columns(sorted(extra_cols))
        new_names = new_names - extra_cols
        return arrow_table, new_names, extra_cols, problems, True
    else:  # 'evolve' - not handled here, caller does its own engine-specific thing
        return arrow_table, new_names, extra_cols, problems, False


def softer_type_match(type_a: pa.DataType, type_b: pa.DataType) -> bool:
    """Leniency for check 5: string/large_string/string_view count as matching,
    same for binary variants and list/large_list."""
    def is_string_like(t):
        return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)
    def is_binary_like(t):
        return pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_binary_view(t)
    def is_list_like(t):
        return pa.types.is_list(t) or pa.types.is_large_list(t)
    return (
        (is_string_like(type_a) and is_string_like(type_b)) or
        (is_binary_like(type_a) and is_binary_like(type_b)) or
        (is_list_like(type_a) and is_list_like(type_b))
    )


def check_types_match(old_schema: pa.Schema, incoming_schema: pa.Schema, common_names) -> list:
    """Check 5."""
    problems = []
    for n in sorted(common_names):
        old_type, new_type = old_schema.field(n).type, incoming_schema.field(n).type
        if old_type != new_type and not softer_type_match(old_type, new_type):
            problems.append(f"type change on '{n}': {old_type} -> {new_type}")
    return problems


def raise_or_warn(problems_silent: list, problems_loud: list, dest_label: str, engine_note: str = ""):
    """Final step, shared by both engines: raise once if anything's silent
    (combining silent + loud into one exception), else warn-and-proceed if
    only loud problems were found.
    """
    if problems_silent:
        all_problems = problems_silent + problems_loud
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(all_problems, 1))
        raise Exception(f"[guard] {dest_label}: {len(all_problems)} problem(s) found:\n{numbered}\nno push")
    elif problems_loud:
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(problems_loud, 1))
        print(f"[guard] {dest_label}: {len(problems_loud)} problem(s) found{engine_note}:")
        print(numbered)
        print('proceeding anyway')
