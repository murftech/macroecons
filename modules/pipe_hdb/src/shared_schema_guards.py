"""Shared, engine-agnostic pieces of write_partition_guarded, imported by
helper_pyarrow_io.py, helper_pyiceberg_io.py, and helper_sparkiceberg_io.py.
"""

from typing import NamedTuple

import pyarrow as pa

def literal_is_valid(on_newcols, on_missingcols):

    _VALID_NEWCOLS = ('evolve', 'drop', 'error')
    _VALID_MISSINGCOLS = ('pad_null', 'error')

    """Raise ValueError if either toggle isn't one of its known values.
    you may add more if needed.
    """
    if on_newcols not in _VALID_NEWCOLS:
        raise ValueError(f"on_newcols must be one of {_VALID_NEWCOLS}, got {on_newcols!r}")
    if on_missingcols not in _VALID_MISSINGCOLS:
        raise ValueError(f"on_missingcols must be one of {_VALID_MISSINGCOLS}, got {on_missingcols!r}")


def assert_partition_keys_exist(arrow_table: pa.Table, partition_keys: list):
    """Check 0 - immediate, gated raise, no toggle. Has to run before anything or a native pyarrow
    error fires first with a less clear message.
    """
    print("schema sync enforce 0: partition key columns must exist in the incoming data")
    missing = set(partition_keys) - set(arrow_table.column_names)
    if missing:
        raise Exception(
            f"[guard] partition_keys column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(arrow_table.column_names)}) - refusing "
            f"to write.\n"
            "no push"
        )


def check_partition_keys_nullable_false(arrow_table: pa.Table, partition_keys: list):
    """Check 1 - collects null-partition-key problems, and casts every partition column to nullable=False
    """

    print("schema sync enforce 1: no partition key column may contain a null value")
    partition_cols_with_null_values = {}
    for col in partition_keys:
        null_count = arrow_table.column(col).null_count
        if null_count > 0:
            partition_cols_with_null_values[col] = null_count

    problem_collect = []

    if partition_cols_with_null_values:
        problem_collect.append(
            f"partition column(s) {partition_cols_with_null_values} have null value(s) - a null "
            f"partition key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        )
        return arrow_table, problem_collect
    else:

        schema_with_nullable_false = pa.schema([
            f.with_nullable(False) if f.name in partition_keys else f
            for f in arrow_table.schema
        ])

        arrow_table_recast = arrow_table.cast(schema_with_nullable_false)
        return arrow_table_recast, problem_collect



def check_partition_keys_match_current(current_partition_cols, partition_keys: list, ordered: bool = False) -> list:
    """Returns a list of 0 or 1 problem strings."""
    
    print("schema sync enforce 2: partition_keys must match the destination's current partition scheme")
    if ordered:
    # ordered=True: compare as lists - right for hive parquet, where the key order IS the folder nesting
        mismatch = list(partition_keys) != list(current_partition_cols)
        shown_current = list(current_partition_cols)
    else:
    # ordered=False (default): compare as sets - right for iceberg, where the key order is only metadata.
        mismatch = set(partition_keys) != set(current_partition_cols)
        shown_current = sorted(current_partition_cols)

    if mismatch:
        return [f"partition scheme change: destination has {shown_current}, "
                f"writing {list(partition_keys)}. wipe/re-provision to re-partition."]
    return []


def reconcile_missing_columns(
    arrow_table: pa.Table,
    current_schema: pa.Schema, 
    new_names: set,
    on_missingcols: str):
    """Check 3. Returns (arrow_table, new_names, problems)."""

    print("schema sync enforce 3: push data must have ALL columns of destination schema. Behaviour: on_missingcols")
    current_names = set(current_schema.names)
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
            _full_row_count = arrow_table.num_rows

            # pad with typed nulls
            for columnname in missing_cols:
                col_type = current_schema.field(columnname).type
                pa_null_array = pa.nulls(size = _full_row_count, type = col_type)
                arrow_table = arrow_table.append_column(columnname, pa_null_array)
                 
            # add padded new cols to new_names    
            new_names = new_names | missing_cols

        else:
            raise Exception('invalid on_missingcols')                

    return arrow_table, new_names, problem_collect


class NewColsResult(NamedTuple):
    arrow_table: pa.Table
    new_names: set
    problem_collect: list
    extra_cols: set
    needs_evolve: bool      # True only for 'evolve' with new columns: the caller still has to do it


def reconcile_new_columns(
    arrow_table: pa.Table,
    current_names: set,
    new_names: set,
    on_newcols: str) -> NewColsResult:

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
            arrow_table = arrow_table.drop_columns(sorted(extra_cols))
            new_names = new_names - extra_cols

        else:  # 'evolve' - not done here, caller does its own engine-specific thing
            needs_evolve = True

    return NewColsResult(arrow_table, new_names, problem_collect, extra_cols, needs_evolve)


def _softer_type_match(type_a: pa.DataType, type_b: pa.DataType) -> bool:
    """Leniecy for check 5: string/large_string/string_view count as matching, also binary variants and list/large_list."""

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


def check_types_match(current_schema: pa.Schema, new_schema: pa.Schema) -> list:
    """Check 5."""
    print("schema sync enforce 5: persist the exact type case of each column.")
    problem_collect = []

    common_names = set(current_schema.names) & set(new_schema.names)

    for columnname in sorted(common_names):
        # EXPLAIN
        # pa.Schema is an ordered collection of fields. 
        # .field(n) finds the one called n and returns it as a pa.Field.
        # .type takes only the pa.Datatype from that field
        current_typing = current_schema.field(columnname).type
        new_typing = new_schema.field(columnname).type

        if current_typing != new_typing and not _softer_type_match(current_typing, new_typing):
            problem_collect += [f"type change found on '{columnname}': {current_typing} -> {new_typing}"]

    return problem_collect


def raise_or_warn(problems_silent: list, problems_loud: list, engine_note: str = ""):
    """Final step: raise once at end of guard if anything's silent
    (Prints combining silent + loud into one exception),
    warn-and-proceed if only loud problems were found. Letting write_partition error there with engine's messages.
    """
    if problems_silent:
        all_problems = problems_silent + problems_loud
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(all_problems, 1))
        raise Exception(f"[guard] {len(all_problems)} problem(s) found:\n{numbered}\nno push")

    elif problems_loud:
        numbered = "\n".join(f"  {i}. {p}" for i, p in enumerate(problems_loud, 1))
        print(f"[guard] {len(problems_loud)} problem(s) found{engine_note}:")
        print(numbered)
        print('proceeding anyway')
    
    else:
        print('[guard] enforced guards passed')


# ---- partition summary: the "what is this write about to replace" print, shared by the
# pyarrow and iceberg writes. Takes the incoming batch, not the destination, so it is
# engine-neutral. ----

def summarize_partitions(arrow_table: pa.Table, partition_keys: list, label: str, show_partitions=False, CAP=30):
    """Print what a write is about to replace, and return the distinct partitions it found.
    """
    # claude undigested
    combos = arrow_table.select(partition_keys).group_by(partition_keys).aggregate([]).to_pylist()
    values_per_col = {c: sorted({r[c] for r in combos}) for c in partition_keys}

    print('Summaries:')
    n_leaves = len(combos)
    if n_leaves == 0:
        print(f"[{label}] incoming batch has 0 rows - no partitions to replace.")
        return combos, values_per_col

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

    return combos, values_per_col
