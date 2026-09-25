"""Shared, Spark-native pieces of write_partition_guarded, imported by both
helper_sparkiceberg_io.py (real-partition-spec, local-JVM/Glue/BigLake catalogs)
and helper_databricks_io.py (Databricks-managed, no partition/cluster
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


# ── checks 0/1 - native Spark, no pyarrow equivalent to reuse ─────────────────

# could combine
def assert_overwrite_keys_exist(df: DataFrame, keys: list):
    """Check 0 - immediate, gated raise, no toggle. Same role as
    [pyspark based]
    """
    print("schema sync enforce 0: overwrite key columns must exist in the incoming data")
    
    missing = set(keys) - set(df.columns)
        missing = set(keys) - set(df.columns)

    if missing:
        raise Exception(
            f"[guard] overwrite key column(s) {sorted(missing)} not found "
            f"in the incoming data (has: {sorted(df.columns)}) - refusing to write.\n"
            "no push"
        )

def check_overwrite_keys_nullable_false(df: DataFrame, keys: list) -> list:
    """Check 1 - collects null-overwrite-key problems
    [pyspark based]
    """
    print("schema sync enforce 1: no overwrite key column may contain a null value")
    from sparkutils.functions import col
    key_cols_with_null_values = {}
    
    for c in keys:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            key_cols_with_null_values[c] = null_count

    problem_collect = []
    
    if key_cols_with_null_values:
        problem_collect += [
            f"overwrite key column(s) {key_cols_with_null_values} have null value(s) - a null "
            f"overwrite key isn't just unqueryable, it's destructive on overwrite - "
            f"refusing to write"
        ]
    return problem_collect


# ── checks 3/4/5 - native Spark reimplementation of the pyarrow-typed checks ──
# ok fine i dont think i should combine them they would have code not needed on production in cloud.

def reconcile_missing_columns(
    df: DataFrame,
    current_schema: StructType,
    new_names: set,
    on_missingcols: str):
    """Check 3. Returns (df, new_names, problem_collect).
    [pyspark based]
    """

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

            ### main ###
            # pad with typed nulls
            from sparkutils.functions import lit
            for field in current_schema:
                if field.name in missing_cols:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
            ### main ###

            # add padded new cols to new_names    
            new_names = new_names | missing_cols

        else:
            raise Exception('invalid on_missingcols')

    return df, new_names, problem_collect


from typing import NamedTuple
class NewColsResult(NamedTuple):
    """Same fields, same order as shared_schema_guards.NewColsResult - `df` in place of `arrow_table`."""
    df: DataFrame
    new_names: set
    problem_collect: list
    extra_cols: set
    needs_evolve: bool      # True only for 'evolve' with new columns: the caller still has to do it


def reconcile_new_columns(df: DataFrame, current_names: set, new_names: set, on_newcols: str) -> NewColsResult:
    """
    [pyspark based]
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
            ### main ###                  
            df = df.drop(*sorted(extra_cols))
            ### main ###
            new_names = new_names - extra_cols

        else:  # 'evolve' - not done here, caller does its own ALTER TABLE
            needs_evolve = True

    return NewColsResult(df, new_names, problem_collect, extra_cols, needs_evolve)


def spark_evolve_schema(spark, fqn, extra_cols: set, new_schema):
    """'evolve' execution in reconcile_new_columns
    evolve add columns
    [spark SQL based]
    # cluade undigested
    """
    col_defs = ', '.join(f'{c} {new_schema[c].dataType.simpleString()}' for c in sorted(extra_cols))
    print(f"[guard] {fqn}: evolving schema - adding {sorted(extra_cols)}")
    spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")



def check_types_match(current_schema: StructType, new_schema: StructType) -> list:
    """Check 5
    [pyspark based]
    undigested
    """

    print("schema sync enforce 5: persist the exact type case of each column.")
    problem_collect = []

    common_names = set(current_schema.names) & set(new_schema.names)

    for columnname in sorted(common_names):
        # EXPLAIN
        # StructType is an ordered collection of StructFields.
        # schema[n] finds the one called n and returns it as a StructField.
        # .dataType takes only the DataType from that field

        ### main ###
        current_typing = current_schema[columnname].dataType
        new_typing = new_schema[columnname].dataType
        ### main ###

        if current_typing != new_typing:
            problem_collect += [f"type change found on '{columnname}': {current_typing} -> {new_typing}"]

    return problem_collect


# ── partition-value summary - not a check, but same destination-agnostic shape ──

def summarize_partitions(df: DataFrame, partition_keys: list, label: str, show_partitions=False, CAP=30):
    """Print what a write is about to replace - list distinct values
    [pyspark based]
    """
    ### main ###
    combos = [r.asDict() for r in df.select(partition_keys).distinct().collect()]
    ### main ###

    values_per_col = {c: sorted({r[c] for r in combos}) for c in partition_keys}

    print('Summaries:')
    n_leaves = len(combos)
    if n_leaves == 0:
        print(f"[{label}] incoming batch has 0 rows - no partitions to replace.")

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

