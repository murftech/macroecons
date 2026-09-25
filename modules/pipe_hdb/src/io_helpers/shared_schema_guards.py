"""Shared, engine-agnostic pieces of write_partition_guarded, imported by
helper_pyarrow_io.py, helper_pyiceberg_io.py, helper_sparkiceberg_io.py, and
helper_sparkdelta_io.py.

Propagated 2026-09-25: all four now import from here, verified end-to-end
(pytest suite + real local writes through run.sh, all four engines/formats -
Databricks not included in that round). shared_schema_guards_arrow.py /
_spark.py are now dead - nothing imports from them any more.
"""


import pyarrow as pa
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

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


def assert_keys_is_list(keys):
    if not isinstance(keys, list):
        raise Exception(f"Please pass list for partition keys, even for single column.")


def resolve_single_key(keys) -> str:
    assert_keys_is_list(keys)
    if len(keys) != 1:
        raise Exception(f"Please pass a single column only in a list.")
    return keys[0]


def assert_overwrite_keys_exist(df: DataFrame | pa.Table, keys: list):
    """Check 0 - immediate, gated raise, no toggle. Has to run before anything or native checks on it raise less clear message
    """

    print("schema sync enforce 0: overwrite key columns must exist in the incoming data")
    # arrow, spark split
    if isinstance(df, DataFrame):
        names = df.columns
    elif isinstance(df, pa.Table):
        names = df.column_names

    missing = set(keys) - set(names)
    if missing:
        raise Exception(
            f"[guard] overwrite key column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(names)}) - refusing "
            f"to write.\n"
            "no push"
        )


def check_overwrite_keys_nullable_false(df: DataFrame | pa.Table, keys: list):
    """Check 1 - collects null-overwrite-key problems, and (pyarrow only - pyiceberg
    needs it, spark does not) casts every overwrite key column to nullable=False.
    """

    print("schema sync enforce 1: no overwrite key column may contain a null value")
    key_cols_with_null_values = {}

    if isinstance(df, DataFrame):
        from sparkutils.functions import col

    for columnname in keys:
        # arrow, spark split
        if isinstance(df, DataFrame):
            null_count = df.filter(col(columnname).isNull()).count()
        elif isinstance(df, pa.Table):
            null_count = df.column(columnname).null_count

        if null_count > 0:
            key_cols_with_null_values[columnname] = null_count

    problem_collect = []

    if key_cols_with_null_values:
        problem_collect.append(
            f"overwrite key column(s) {key_cols_with_null_values} have null value(s) - a null "
            f"overwrite key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        )
        return df, problem_collect

    # arrow, spark split
    if isinstance(df, DataFrame):
        return df, problem_collect
    elif isinstance(df, pa.Table):
        # pyiceberg needs this, spark java does not
        schema_with_nullable_false = pa.schema([
            f.with_nullable(False) if f.name in keys else f
            for f in df.schema
        ])

        df_recast = df.cast(schema_with_nullable_false)
        return df_recast, problem_collect



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
    df: DataFrame | pa.Table,
    current_schema: StructType | pa.Schema,
    new_names: set,
    on_missingcols: str):
    """Check 3. Returns (df, new_names, problem_collect)."""

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

            ### main ###
            # arrow, spark split
            # pad with typed nulls
            if isinstance(df, DataFrame):
                from sparkutils.functions import lit
                for columnname in missing_cols:
                    col_type = current_schema[columnname].dataType
                    df = df.withColumn(columnname, lit(None).cast(col_type))
            elif isinstance(df, pa.Table):
                _full_row_count = df.num_rows
                for columnname in missing_cols:
                    col_type = current_schema.field(columnname).type
                    pa_null_array = pa.nulls(size = _full_row_count, type = col_type)
                    df = df.append_column(columnname, pa_null_array)
            ### main ###

            # add padded new cols to new_names
            new_names = new_names | missing_cols

        else:
            raise Exception('invalid on_missingcols')

    return df, new_names, problem_collect


from typing import NamedTuple
class NewColsResult(NamedTuple):
    df: DataFrame | pa.Table
    new_names: set
    problem_collect: list
    extra_cols: set
    needs_evolve: bool      # True only for 'evolve' with new columns: the caller still has to do it


def reconcile_new_columns(
    df: DataFrame | pa.Table,
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
            ### main ###
            # arrow, spark split
            if isinstance(df, DataFrame):
                df = df.drop(*sorted(extra_cols))
            elif isinstance(df, pa.Table):
                df = df.drop_columns(sorted(extra_cols))
            ### main ###
            new_names = new_names - extra_cols
            
        else:  # 'evolve' - not done here, caller does its own engine-specific thing
            needs_evolve = True

    return NewColsResult(df, new_names, problem_collect, extra_cols, needs_evolve)


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


def check_types_match(current_schema: StructType | pa.Schema, new_schema: StructType | pa.Schema) -> list:
    """Check 5.
    """
    print("schema sync enforce 5: persist the exact type case of each column.")
    problem_collect = []

    common_names = set(current_schema.names) & set(new_schema.names)

    for columnname in sorted(common_names):
        # arrow, spark split
        ### main ###
        if isinstance(current_schema, StructType):
            # EXPLAIN
            # StructType is an ordered collection of StructFields.
            # schema[n] finds the one called n and returns it as a StructField.
            # .dataType takes only the DataType from that field
            current_typing = current_schema[columnname].dataType
            new_typing = new_schema[columnname].dataType
            types_differ = current_typing != new_typing
        elif isinstance(current_schema, pa.Schema):
            # EXPLAIN
            # pa.Schema is an ordered collection of fields.
            # .field(n) finds the one called n and returns it as a pa.Field.
            # .type takes only the pa.Datatype from that field
            current_typing = current_schema.field(columnname).type
            new_typing = new_schema.field(columnname).type
            types_differ = current_typing != new_typing and not _softer_type_match(current_typing, new_typing)
        ### main ###

        if types_differ:
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

def summarize_partitions(df: DataFrame | pa.Table, partition_keys: list, label: str, show_partitions=False, CAP=30):
    """Print what a write is about to replace - list distinct values
    """
    ### main ###
    # arrow, spark split
    if isinstance(df, DataFrame):
        df_sel_distinct = df.select(partition_keys).distinct().collect() # pyspark still has this dinosaur pyspark.sql.types.Row object bullshit
        combination_list = []
        for row in df_sel_distinct:
            combination_list.append(row.asDict())

    elif isinstance(df, pa.Table):
        df_sel_distinct = df.select(partition_keys).group_by(partition_keys).aggregate([])
        combination_list = df_sel_distinct.to_pylist() # its a pyarrow method arrow to list, easy

    ### main ###

    values_per_col = {c: sorted({r[c] for r in combination_list}) for c in partition_keys}

    print('Summaries:')
    n_leaves = len(combination_list)
    if n_leaves == 0:
        print(f"[{label}] incoming batch has 0 rows - no partitions to replace.")
        return

    print(f"[{label}] replacing {n_leaves} leaf partitions on {'/'.join(partition_keys)}:")
    for c, vals in values_per_col.items():
        print(f"    {c:<14} {len(vals):>4} values   {vals[0]} … {vals[-1]}")

    if show_partitions:
        print('show partitions')
        rows = sorted(combination_list, key=lambda r: [r[c] for c in partition_keys])
        for r in rows[:CAP]:
            print("    " + "/".join(f"{c}={r[c]}" for c in partition_keys))
        if len(rows) > CAP:
            print(f"    … and {len(rows) - CAP} more")


# ── spark-only: no arrow/pyiceberg counterpart in this file ──────────────────

def spark_evolve_schema(spark, fqn, extra_cols: set, new_schema):
    """'evolve' execution in reconcile_new_columns
    evolve add columns
    [spark SQL based]
    """
    sql_columns_syntax = ', '.join(f'{c} {new_schema[c].dataType.simpleString()}' for c in sorted(extra_cols))
    # eg resolution: sql_columns_syntax >> 'flat_type string, remaining_lease string'

    print(f"[guard] {fqn}: evolving schema - adding {sorted(extra_cols)}")

    ### main ###
    # EXPLAIN: Sends the exact same add column SQL to the engine to decipher according to what their catalog does with it.
    # Same code, engine-portable
    # no matter iceberg, delta, local or cloud catalog, adds 1 commit and adds n columns to the schema in this commit
    spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({sql_columns_syntax})")
    # eg resolution: spark.sql("ALTER TABLE macroecons.t1.tbl ADD COLUMNS (flat_type string, remaining_lease string)")

    ### main ###

def summarize_span(df: DataFrame, span_col: str, label: str, show_partitions=False, CAP=30):
    """Print what a write is about to replace - list distinct values
    [pyspark based]
    """

    ### main ###
    values = sorted(r[0] for r in df.select(span_col).distinct().collect())
    ### main ###

    print('Summaries:')

    if not values:
        print(f'[{label}] incoming batch has 0 rows')
        return []

    print(f'[delta] IN-overwrite {len(values)} distinct value(s) actually present in this batch')
    print(f"    {span_col:<14} {len(values):>4} values   {values[0]} … {values[-1]}")

    if show_partitions:
        print('show partitions')
        for v in values[:CAP]:
            print(f'    {span_col}={v}')
        if len(values) > CAP:
            print(f'    … and {len(values) - CAP} more')

    return values

