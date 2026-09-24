"""pytest suite for helper_sparkiceberg_io's schema guard (checks 3/4/5, native
Spark reimplementation of shared_schema_guards.py's shape/vocabulary).

Deliberately scoped to the check FUNCTIONS themselves (reconcile_missing_columns,
reconcile_new_columns, check_types_match, raise_or_warn, summarize_partitions) -
none of these touch a catalog at all, so they're testable with a plain local Spark
session and no real Iceberg/Delta table anywhere. `create_or_overwrite`'s full
first-create / overwrite flow needs a REAL catalog (spark.catalog.tableExists,
spark.table, df.writeTo) - that's Phase 2 (the local JVM Iceberg catalog wiring in
get_spark(), not built yet) and gets its own integration test once that exists.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from conftest import PARTITION_KEYS  # unused here, just confirms conftest loads cleanly
from helper_sparkiceberg_io import (
    literal_is_valid,
    reconcile_missing_columns,
    reconcile_new_columns,
    check_types_match,
    raise_or_warn,
    summarize_partitions,
)


@pytest.fixture(scope='module')   # module, not session: a session-long plain session would be handed to the live Iceberg tests
def spark():
    s = SparkSession.builder.appName('test_helper_sparkiceberg_io').master('local[1]').getOrCreate()
    yield s
    s.stop()


@pytest.fixture
def data_narrow(spark):
    """Same conceptual shape as conftest.py's pyarrow data_narrow - a Spark
    DataFrame instead, since this module's checks are Spark-native."""
    return spark.createDataFrame(
        [('2020-01', 'ANG MO KIO', 300000.0), ('2020-01', 'BEDOK', 350000.0)],
        ['tx_monthdate', 'town', 'resale_price'])


@pytest.fixture
def data_wide(spark):
    return spark.createDataFrame(
        [('2020-02', 'ANG MO KIO', 310000.0, '3 ROOM'), ('2020-02', 'BEDOK', 360000.0, '4 ROOM')],
        ['tx_monthdate', 'town', 'resale_price', 'flat_type'])


def _schema(*names_and_types):
    """Build a bare StructType from (name, DataType) pairs - a fabricated
    'current_schema', same idea as conftest.py's pa.table(...) fixtures standing in
    for what a real destination would report, no real catalog involved."""
    return StructType([StructField(n, t, True) for n, t in names_and_types])


# ── literal_is_valid ────────────────────────────────────────────────────────

def test_literal_is_valid_rejects_unknown_values():
    with pytest.raises(ValueError, match='on_newcols'):
        literal_is_valid('push', 'pad_null')
    with pytest.raises(ValueError, match='on_missingcols'):
        literal_is_valid('evolve', 'ignore')


def test_literal_is_valid_accepts_known_values():
    for newcols in ('evolve', 'drop', 'error'):
        for missingcols in ('pad_null', 'error'):
            literal_is_valid(newcols, missingcols)  # must not raise


# ── reconcile_missing_columns (check 3) ────────────────────────────────────────

def test_missing_columns_pad_null_adds_typed_null_column(data_narrow):
    current_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()), ('flat_type', StringType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, current_schema, new_names, 'pad_null')
    assert problems == []
    assert 'flat_type' in df.columns
    assert updated_names == {'tx_monthdate', 'town', 'resale_price', 'flat_type'}
    assert df.select('flat_type').distinct().collect()[0][0] is None


def test_missing_columns_error_reports_without_mutating(data_narrow):
    current_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()), ('flat_type', StringType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, current_schema, new_names, 'error')
    assert len(problems) == 1
    assert 'MISSING' in problems[0] and 'flat_type' in problems[0]
    assert 'flat_type' not in df.columns  # error path never mutates


def test_no_missing_columns_is_a_no_op(data_narrow):
    current_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, current_schema, new_names, 'error')
    assert problems == []
    assert updated_names == new_names


# ── reconcile_new_columns (check 4, 'error'/'drop' halves) ──────

def test_new_columns_drop_removes_column(data_wide):
    current_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, problems, extra_cols, needs_evolve = reconcile_new_columns(
        data_wide, current_names, new_names, 'drop')
    assert needs_evolve is False
    assert extra_cols == {'flat_type'}
    assert problems == []
    assert 'flat_type' not in df.columns
    assert updated_names == current_names


def test_new_columns_error_reports_without_mutating(data_wide):
    current_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, problems, extra_cols, needs_evolve = reconcile_new_columns(
        data_wide, current_names, new_names, 'error')
    assert needs_evolve is False
    assert len(problems) == 1 and 'NEW' in problems[0]
    assert 'flat_type' in df.columns  # error path never mutates


def test_new_columns_evolve_is_not_handled_here(data_wide):
    current_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, problems, extra_cols, needs_evolve = reconcile_new_columns(
        data_wide, current_names, new_names, 'evolve')
    assert needs_evolve is True  # caller (create_or_overwrite) owns the ALTER TABLE
    assert extra_cols == {'flat_type'}
    assert problems == []


def test_no_new_columns_is_a_no_op(data_narrow):
    current_names = set(data_narrow.columns)
    new_names = set(data_narrow.columns)
    df, updated_names, problems, extra_cols, needs_evolve = reconcile_new_columns(
        data_narrow, current_names, new_names, 'error')
    assert needs_evolve is False
    assert extra_cols == set()
    assert problems == []


# ── check_types_match (check 5, loud only) ─────────────────────────────────────

def test_check5_type_mismatch_detected():
    current_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    new_schema = _schema(('resale_price', LongType()), ('town', StringType()))
    problems = check_types_match(current_schema, new_schema)
    assert len(problems) == 1
    assert "'resale_price'" in problems[0]


def test_check5_matching_types_reports_nothing():
    current_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    new_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    problems = check_types_match(current_schema, new_schema)
    assert problems == []


# ── raise_or_warn ───────────────────────────────────────────────────────────────

def test_raise_or_warn_raises_on_silent_problems():
    with pytest.raises(Exception, match='no push'):
        raise_or_warn(['bad thing'], [])


def test_raise_or_warn_proceeds_on_loud_only(capsys):
    raise_or_warn([], ['minor thing'])  # must not raise
    assert 'proceeding anyway' in capsys.readouterr().out


def test_raise_or_warn_no_problems_says_so(capsys):
    raise_or_warn([], [])
    assert capsys.readouterr().out.strip() == '[guard] enforced guards passed'


# ── summarize_partitions ─────────────────────────────────────────────────────

def test_summarize_partitions_reports_leaf_count_and_values(spark, capsys):
    df = spark.createDataFrame(
        [('e1', '2020-01'), ('e1', '2020-02'), ('e2', '2020-02')], ['era', 'tx_monthdate'])
    summarize_partitions(df, ['era', 'tx_monthdate'], 'spark')
    out = capsys.readouterr().out
    assert '[spark] replacing 3 leaf partitions on era/tx_monthdate:' in out
    assert 'era' in out and '2 values' in out
    assert 'tx_monthdate' in out and '2020-01' in out and '2020-02' in out
    assert 'show partitions' not in out   # show_partitions defaults False


def test_summarize_partitions_show_partitions_lists_each_combo(spark, capsys):
    df = spark.createDataFrame([('e1', '2020-01'), ('e2', '2020-02')], ['era', 'tx_monthdate'])
    summarize_partitions(df, ['era', 'tx_monthdate'], 'spark', show_partitions=True)
    out = capsys.readouterr().out
    assert 'show partitions' in out
    assert 'era=e1/tx_monthdate=2020-01' in out
    assert 'era=e2/tx_monthdate=2020-02' in out


def test_summarize_partitions_caps_the_listing(spark, capsys):
    df = spark.createDataFrame([('e1', f'm{i:02d}') for i in range(40)], ['era', 'tx_monthdate'])
    summarize_partitions(df, ['era', 'tx_monthdate'], 'spark', show_partitions=True)
    out = capsys.readouterr().out
    assert '… and 10 more' in out


def test_summarize_partitions_empty_batch_says_so(spark, capsys):
    empty = spark.createDataFrame([], 'era string, tx_monthdate string')
    summarize_partitions(empty, ['era', 'tx_monthdate'], 'spark')
    out = capsys.readouterr().out
    assert out.strip() == '[spark] incoming batch has 0 rows - no partitions to replace.'

