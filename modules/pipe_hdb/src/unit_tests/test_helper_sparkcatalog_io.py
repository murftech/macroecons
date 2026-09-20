"""pytest suite for helper_sparkcatalog_io's schema guard (checks 3/4/5, native
Spark reimplementation of shared_schema_guards.py's shape/vocabulary).

Deliberately scoped to the check FUNCTIONS themselves (reconcile_missing_columns,
reconcile_new_columns_drop_or_error, check_types_match, raise_or_warn, align_down) -
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
from helper_sparkcatalog_io import (
    validate_guard_args,
    align_down,
    reconcile_missing_columns,
    reconcile_new_columns_drop_or_error,
    check_types_match,
    raise_or_warn,
)


@pytest.fixture(scope='session')
def spark():
    s = SparkSession.builder.appName('test_helper_sparkcatalog_io').master('local[1]').getOrCreate()
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
    'old_schema', same idea as conftest.py's pa.table(...) fixtures standing in
    for what a real destination would report, no real catalog involved."""
    return StructType([StructField(n, t, True) for n, t in names_and_types])


# ── validate_guard_args ────────────────────────────────────────────────────────

def test_validate_guard_args_rejects_unknown_values():
    with pytest.raises(ValueError, match='on_newcols'):
        validate_guard_args('push', 'pad_null')
    with pytest.raises(ValueError, match='on_missingcols'):
        validate_guard_args('evolve', 'ignore')


def test_validate_guard_args_accepts_known_values():
    for newcols in ('evolve', 'drop', 'error'):
        for missingcols in ('pad_null', 'error'):
            validate_guard_args(newcols, missingcols)  # must not raise


# ── reconcile_missing_columns (check 3) ────────────────────────────────────────

def test_missing_columns_pad_null_adds_typed_null_column(data_narrow):
    old_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()), ('flat_type', StringType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, old_schema, new_names, 'pad_null', 'test_table')
    assert problems == []
    assert 'flat_type' in df.columns
    assert updated_names == {'tx_monthdate', 'town', 'resale_price', 'flat_type'}
    assert df.select('flat_type').distinct().collect()[0][0] is None


def test_missing_columns_error_reports_without_mutating(data_narrow):
    old_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()), ('flat_type', StringType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, old_schema, new_names, 'error', 'test_table')
    assert len(problems) == 1
    assert 'MISSING' in problems[0] and 'flat_type' in problems[0]
    assert 'flat_type' not in df.columns  # error path never mutates


def test_no_missing_columns_is_a_no_op(data_narrow):
    old_schema = _schema(('tx_monthdate', StringType()), ('town', StringType()),
                          ('resale_price', DoubleType()))
    new_names = set(data_narrow.columns)
    df, updated_names, problems = reconcile_missing_columns(
        data_narrow, old_schema, new_names, 'error', 'test_table')
    assert problems == []
    assert updated_names == new_names


# ── reconcile_new_columns_drop_or_error (check 4, 'error'/'drop' halves) ──────

def test_new_columns_drop_removes_column(data_wide):
    old_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, extra_cols, problems, handled = reconcile_new_columns_drop_or_error(
        data_wide, old_names, new_names, 'drop', 'test_table')
    assert handled is True
    assert extra_cols == {'flat_type'}
    assert problems == []
    assert 'flat_type' not in df.columns
    assert updated_names == old_names


def test_new_columns_error_reports_without_mutating(data_wide):
    old_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, extra_cols, problems, handled = reconcile_new_columns_drop_or_error(
        data_wide, old_names, new_names, 'error', 'test_table')
    assert handled is True
    assert len(problems) == 1 and 'NEW' in problems[0]
    assert 'flat_type' in df.columns  # error path never mutates


def test_new_columns_evolve_is_not_handled_here(data_wide):
    old_names = {'tx_monthdate', 'town', 'resale_price'}
    new_names = set(data_wide.columns)
    df, updated_names, extra_cols, problems, handled = reconcile_new_columns_drop_or_error(
        data_wide, old_names, new_names, 'evolve', 'test_table')
    assert handled is False  # caller (create_or_overwrite) owns the ALTER TABLE
    assert extra_cols == {'flat_type'}
    assert problems == []


def test_no_new_columns_is_a_no_op(data_narrow):
    old_names = set(data_narrow.columns)
    new_names = set(data_narrow.columns)
    df, updated_names, extra_cols, problems, handled = reconcile_new_columns_drop_or_error(
        data_narrow, old_names, new_names, 'error', 'test_table')
    assert handled is True
    assert extra_cols == set()
    assert problems == []


# ── check_types_match (check 5, loud only) ─────────────────────────────────────

def test_check5_type_mismatch_detected():
    old_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    new_schema = _schema(('resale_price', LongType()), ('town', StringType()))
    problems = check_types_match(old_schema, new_schema, {'resale_price', 'town'})
    assert len(problems) == 1
    assert "'resale_price'" in problems[0]


def test_check5_matching_types_reports_nothing():
    old_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    new_schema = _schema(('resale_price', DoubleType()), ('town', StringType()))
    problems = check_types_match(old_schema, new_schema, {'resale_price', 'town'})
    assert problems == []


# ── raise_or_warn ───────────────────────────────────────────────────────────────

def test_raise_or_warn_raises_on_silent_problems():
    with pytest.raises(Exception, match='no push'):
        raise_or_warn(['bad thing'], [], 'test_table')


def test_raise_or_warn_proceeds_on_loud_only(capsys):
    raise_or_warn([], ['minor thing'], 'test_table')  # must not raise
    assert 'proceeding anyway' in capsys.readouterr().out


def test_raise_or_warn_no_problems_is_silent(capsys):
    raise_or_warn([], [], 'test_table')
    assert capsys.readouterr().out == ''


# ── align_down ──────────────────────────────────────────────────────────────────

def test_align_down_pads_and_reorders(spark, data_narrow):
    target_schema = _schema(('flat_type', StringType()), ('tx_monthdate', StringType()),
                            ('town', StringType()), ('resale_price', DoubleType()))
    aligned = align_down(data_narrow, target_schema)
    assert aligned.columns == ['flat_type', 'tx_monthdate', 'town', 'resale_price']
    assert aligned.select('flat_type').distinct().collect()[0][0] is None
