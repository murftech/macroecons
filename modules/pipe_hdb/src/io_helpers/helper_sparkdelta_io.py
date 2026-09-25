"""Write / read a Delta table via Spark's V2 writer (JVM only) - predicate overwrite,
no partition spec needed.
"""

from typing import Literal

from .shared_schema_guards import (
    literal_is_valid,
    resolve_single_key,
    raise_or_warn,
    assert_overwrite_keys_exist,
    check_overwrite_keys_nullable_false,
    reconcile_missing_columns,
    reconcile_new_columns,
    spark_evolve_schema,
    check_types_match,
    summarize_span,
)


####################################
######## for provisioning ##########
####################################
# Spark-native twin of helper_pyiceberg_io.createOrEvolve_table / helper_deltalake_io.
# createOrEvolve_table - real ALTER TABLE DDL against a live catalog (works on
# Databricks/UC and local JVM Delta identically), NOT delta-rs. Verified 2026-09-25:
# delta-rs's write path breaks under column mapping (ADD COLUMN raises, plain append
# silently nulls data) - this file exists to sidestep that entirely by going through
# real Spark, which does not have that limitation (verified: ALTER TABLE ADD COLUMNS
# works fine on a column-mapped table with Row Tracking + Deletion Vectors on, matching
# the real macroecons.t1's own feature set).
# NO catalog/namespace/schema provisioning here on purpose - Databricks/UC catalogs and
# schemas are provisioned separately (deploy_databricks/databricks_provision.sh); this
# file is TABLE-level only, same scope split as helper_pyiceberg_io vs
# sh_provision_iceberg_catalog.

def createOrEvolve_table(spark, fqn, schema, column_mapping: bool = True):
    """
    Provisioning only - never called implicitly from write_span_guarded.
    Evolve here ONLY adds columns, never deletes, never changes a type - same
    contract as helper_pyiceberg_io.createOrEvolve_table / helper_deltalake_io.

    `schema` : a pyspark.sql.types.StructType.
    NO partitioning, NO clustering - matches providers/databricks.py's own decision
    (UC managed tables here are unpartitioned by design; Delta's replaceWhere/IN-list
    overwrite doesn't need a physical layout to be correct).

    `column_mapping=True` sets delta.columnMapping.mode='name' at CREATE time - free
    here (no data exists yet, no protocol-upgrade-on-populated-table risk), and is
    what makes safe_drop_column below usable on any table created through this
    function without a separate ALTER TABLE SET TBLPROPERTIES step later.
    """
    # spark.catalog.tableExists() only resolves catalog-registered names - it raises
    # on the local delta.`<path>` syntax rather than returning False. spark.table(fqn)
    # resolves BOTH forms (same reason write_span_guarded reads the current schema
    # this way), so use that + AnalysisException instead.
    from pyspark.sql.utils import AnalysisException
    try:
        spark.table(fqn)
        exists = True
    except AnalysisException:
        exists = False

    if not exists:
        print(f'{fqn} does not exist yet -')
        print('create table yes. using CREATE TABLE DDL:')

        col_defs = ', '.join(f'{f.name} {f.dataType.simpleString()}' for f in schema)
        tblproperties = ''
        if column_mapping:
            tblproperties = (
                " TBLPROPERTIES ("
                "'delta.columnMapping.mode'='name', "
                "'delta.minReaderVersion'='2', "
                "'delta.minWriterVersion'='5')"
            )

        ### main ###
        spark.sql(f"CREATE TABLE {fqn} ({col_defs}) USING DELTA{tblproperties}")
        ### main ###

        print('check schema and properties of created table')
        spark.sql(f"DESCRIBE TABLE {fqn}").show(truncate=False)
        spark.sql(f"SHOW TBLPROPERTIES {fqn}").show(truncate=False)
        return

    current_schema = spark.table(fqn).schema
    current_names = {f.name for f in current_schema}

    # REFUSE - type change on an existing column
    type_changes = [
        f'{f.name}: {current_schema[f.name].dataType} -> {f.dataType}'
        for f in schema if f.name in current_names and current_schema[f.name].dataType != f.dataType
    ]
    if type_changes:
        raise Exception(f'{fqn}: type change(s) requested, refusing: {type_changes}')

    # EVOLVE - add columns (always nullable: existing rows have no value for them)
    missing_cols = [f for f in schema if f.name not in current_names]
    if missing_cols:
        print(f'evolving schema - adding {[f.name for f in missing_cols]}')
        col_defs = ', '.join(f'{f.name} {f.dataType.simpleString()}' for f in missing_cols)

        ### main ###
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({col_defs})")
        ### main ###
    else:
        print(f'{fqn} already instantiated - nothing is changed.')

    print('current schema + properties of table')
    spark.sql(f"DESCRIBE TABLE {fqn}").show(truncate=False)
    spark.sql(f"SHOW TBLPROPERTIES {fqn}").show(truncate=False)


def safe_drop_column(spark, fqn, column_name):
    """
    Remove a column from the table's current schema, after a typed confirmation.
    Metadata only: parquet bytes stay, and old snapshots/time-travel can still read
    it (until a real VACUUM). Re-adding the name later makes a NEW empty column -
    same contract as helper_pyiceberg_io.safe_drop_column.

    Databricks/Delta-specific requirement, verified empirically 2026-09-25: DROP
    COLUMN raises AnalysisException [DELTA_UNSUPPORTED_DROP_COLUMN] unless
    delta.columnMapping.mode is already 'name' or 'id' on the table. This function
    REFUSES rather than silently enabling column mapping as a side effect of a drop -
    that's a real, somewhat one-way protocol upgrade and deserves its own explicit
    step (createOrEvolve_table(column_mapping=True) for a new table, or a deliberate
    ALTER TABLE ... SET TBLPROPERTIES for an existing one).
    """
    current_schema = spark.table(fqn).schema
    if column_name not in current_schema.names:
        print(f'{fqn}: {column_name!r} not present - nothing to drop.')
        return

    props = {r['key']: r['value'] for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()}
    mapping_mode = props.get('delta.columnMapping.mode')
    if mapping_mode not in ('name', 'id'):
        print(f"{fqn}: delta.columnMapping.mode is {mapping_mode!r} - DROP COLUMN needs "
              f"'name' or 'id'. Not enabling it automatically as a side effect of a drop - "
              f"enable it explicitly first, then re-run.")
        return

    print(f'About to drop column {column_name!r} from {fqn!r} - metadata only, old parquet '
          f'files keep the data (still reachable via time travel until VACUUM). '
          f'Re-adding the name later creates a NEW empty column, the old values do not come back.')

    confirm = input(f"Type the column name to confirm: ")
    if confirm != column_name:
        print(f'Mismatch on confirmation ({confirm!r} != {column_name!r}) - aborted, nothing dropped.')
        return

    ### main ###
    spark.sql(f"ALTER TABLE {fqn} DROP COLUMN {column_name}")
    ### main ###

    print(f'{fqn}: dropped column {column_name!r}.')
    spark.sql(f"DESCRIBE TABLE {fqn}").show(truncate=False)


# ── the write path itself ──────────────────────────────────────────────────

def write_span(df, spark, fqn, span_col: str, show_partitions=False):
    """Lazy write - zero checks, for POC / quick-and-dirty use only."""

    span_distinct_values = summarize_span(df, span_col, label='delta', show_partitions=show_partitions)

    # build overwrite condition
    import sparkutils.functions as F
    condition = F.col(span_col).isin(span_distinct_values)

    print(f'RUN: using pyspark DataFrameWriterV2, df.writeTo.overwrite, into table: {fqn}')

    # trip Spark's too many distinct values WARN - raise the threshold just for this call
    prior = spark.conf.get('spark.sql.debug.maxToStringFields')
    spark.conf.set('spark.sql.debug.maxToStringFields', '200')

    ### main ###
    # a wide IN-list condition makes the write plan's string repr wide enough to
    df.writeTo(fqn).overwrite(condition)
    ### main ###

    spark.conf.set('spark.sql.debug.maxToStringFields', prior)

    print(f'DONE:  delta -> {fqn}  (overwrite, IN-list)')
    
# done

def write_full_refresh(df, spark, fqn):
    from sparkutils.functions import lit
    print(f'[delta] full refresh on {fqn} - dropping ALL existing rows, replacing with {df.count()} incoming rows')
    df.writeTo(fqn).overwrite(lit(True))
    print(f'DONE:  delta -> {fqn}  (overwrite, full refresh)')
# done 

def write_span_guarded(df, spark, fqn, span_key: list, show_partitions=False, *,
                       on_newcols: Literal['evolve', 'drop', 'error'] = 'error',
                       on_missingcols: Literal['pad_null', 'error'] = 'error',
                       full_refresh=False):

    literal_is_valid(on_newcols, on_missingcols)
    span_col = resolve_single_key(span_key)

    print(f"[guard] running schema guards for {fqn}")

    current_schema = spark.table(fqn).schema     # no pre-check - native raise if fqn doesn't exist
    current_names = {f.name for f in current_schema}
    new_schema = df.schema
    new_names = set(df.columns)

    ##########

    problems_silent = []   # the writer would NOT complain - we raise
    problems_loud = []     # the writer would complain on its own when write_span is called - we only warn

    ###################################
    ### schema sync enforce 0: span key column must exist in the first place ###
    ###################################
    assert_overwrite_keys_exist(df, span_key)
    # done

    ###################################
    ### schema sync enforce 1: no span key may contain null ###
    ###################################
    df, problem_collect = check_overwrite_keys_nullable_false(df, span_key)
    problems_silent += problem_collect
    # done

    ###################################
    ### schema sync enforce 2: SKIPPED - this destination has no partition/cluster scheme ###
    ###################################
    # done

    ###################################
    ### schema sync enforce 3: Behaviour: on_missingcols (error or pad_null) ###
    ###################################
    df, new_names, problem_collect = reconcile_missing_columns(df, current_schema, new_names, on_missingcols)
    new_schema = df.schema
    problems_silent += problem_collect
    # done

    ###################################
    ### schema sync enforce 4: Behaviour: on_newcols (error, drop, or evolve additive) ###
    ###################################
    newcols_results = reconcile_new_columns(df, current_names, new_names, on_newcols)

    df = newcols_results.df
    new_names = newcols_results.new_names
    extra_cols = newcols_results.extra_cols
    # loud, not silent - verified 2026-09-25: Delta's own writeTo(...).overwrite() raises
    # a clean, named error on a schema mismatch (AnalysisException:
    # [_LEGACY_ERROR_TEMP_DELTA_0007]) - same enforcement character as check 5's type match.
    problems_loud += newcols_results.problem_collect
    new_schema = df.schema
    # done

    ###################################
    ### schema sync enforce 5: persist the exact type case of each column. ###
    ###################################
    problems_loud += check_types_match(current_schema, new_schema)
    # done

    ####################################
    finalized_df = df

    ##### collect all problems ####
    raise_or_warn(
        problems_silent, problems_loud,
        engine_note=" delta will catch on its own - proceeding anyway, letting "
                    "overwrite() raise its own error")
    # done

    # AFTER raise_or_warn - We dont want to
    # change the table's schema until we know the write isn't going to be refused.
    if newcols_results.needs_evolve:
        spark_evolve_schema(spark, fqn, extra_cols, new_schema)
    # done

    ### RUN WRITE ###
    if full_refresh:
        write_full_refresh(finalized_df, spark, fqn)
    else:
        write_span(finalized_df, spark, fqn, span_col, show_partitions=show_partitions)

def read(spark, fqn):
    """Read a catalog table back as a Spark DataFrame."""
    return spark.read.table(fqn)
