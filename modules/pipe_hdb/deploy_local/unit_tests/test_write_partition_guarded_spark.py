"""pytest suite for helper_sparkiceberg_io.write_partition_guarded - the FLOW, not the check functions.

test_helper_sparkiceberg_io.py covers each check function alone. This file runs the whole
guarded function against a stand-in `spark` (FakeSpark below): real local Spark DataFrames,
but the catalog calls the guard makes (tableExists, table().schema, sql ALTER TABLE) and the
final write are faked, so no Iceberg JAR / JVM catalog is needed. It proves the guard's
decisions (raise / pad / drop / evolve / warn) - what the real Iceberg writer then does natively
is the job of test_helper_sparkiceberg_io_iceberg_live.py.
"""
import os
import sys

os.environ.setdefault('PYSPARK_PYTHON', sys.executable)

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from lakehouse_io import helper_sparkiceberg_io as hs

TABLE_SCHEMA = StructType([
    StructField('tx_monthdate', StringType()),
    StructField('town', StringType()),
    StructField('resale_price', DoubleType()),
])
COLS = ['tx_monthdate', 'town', 'resale_price']
_TYPES = {'string': StringType(), 'double': DoubleType(), 'bigint': LongType()}


class _FakeTable:
    def __init__(self, schema):
        self.schema = schema


class FakeSpark:
    """Only the calls write_partition_guarded makes. ALTER TABLE ... ADD COLUMNS really changes
    the fake table's schema, so the guard's re-read after evolving sees the new column.
    No tableExists pre-check in the guard any more - table() itself raises when `exists=False`,
    the same failure mode as the real spark.table(fqn) on a missing table (verified message below)."""

    def __init__(self, schema=TABLE_SCHEMA, exists=True):
        self._schema = schema
        self._exists = exists
        self.alters = []

    def table(self, fqn):
        if not self._exists:
            raise Exception(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{fqn}` cannot be found.")
        return _FakeTable(self._schema)

    def sql(self, query):
        assert query.startswith('ALTER TABLE'), query
        self.alters.append(query)
        for col_def in query.split('ADD COLUMNS (')[1].rstrip(')').split(', '):
            name, type_name = col_def.split(' ', 1)
            self._schema = StructType(list(self._schema.fields) + [StructField(name, _TYPES[type_name])])


@pytest.fixture(scope='module')
def spark_session():
    # stopped at the end of this module, so the live Iceberg tests never inherit a plain session
    s = (SparkSession.builder.appName('test_write_partition_guarded_spark')
         .master('local[1]').config('spark.ui.enabled', 'false').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


@pytest.fixture
def run(spark_session, monkeypatch):
    """run(df, **toggles) -> (fake_spark, written_columns). Raises whatever the guard raises.
    written_columns comes from whichever of write_partition / write_partition_full_refresh the
    guard actually called (full_refresh=True routes to the latter) - fake.write_kind records which."""
    def _run(df, table_keys=('tx_monthdate',), fake=None, keys=('tx_monthdate',), **toggles):
        fake = fake or FakeSpark()
        written = []
        fake.write_kind = None

        def _fake_write_partition(d, spark, fqn, k, show_partitions=False):
            written.append(sorted(d.columns))
            fake.write_kind = 'write_partition'

        def _fake_full_refresh(d, spark, fqn, k, show_partitions=False):
            written.append(sorted(d.columns))
            fake.write_kind = 'write_partition_full_refresh'

        monkeypatch.setattr(hs, '_current_partition_columns', lambda spark, fqn: list(table_keys))
        monkeypatch.setattr(hs, 'write_partition', _fake_write_partition)
        monkeypatch.setattr(hs, 'write_partition_full_refresh', _fake_full_refresh)
        hs.write_partition_guarded(df, fake, 't1.x', list(keys), **toggles)
        return fake, (written[0] if written else None)
    return _run


def _df(spark_session, rows, cols):
    return spark_session.createDataFrame(rows, cols)


@pytest.fixture
def clean(spark_session):
    return _df(spark_session, [('2020-01', 'ANG MO KIO', 300000.0)], COLS)


@pytest.fixture
def missing_town(spark_session):
    return _df(spark_session, [('2020-01', 1.0)], ['tx_monthdate', 'resale_price'])


@pytest.fixture
def with_new_col(spark_session):
    return _df(spark_session, [('2020-01', 'A', 1.0, '3 ROOM')], COLS + ['flat_type'])


# ── shape validation - overwrite keys must always be a list, even for one column ──
# (shared_schema_guards_arrow.assert_keys_is_list, reused by every engine's write_*_guarded).
# Called directly, NOT through the `run` fixture - that fixture does list(keys)
# unconditionally, which would explode a bare string into characters before this
# test ever got to exercise the real guard.

def test_partition_keys_as_bare_string_raises(spark_session, clean):
    with pytest.raises(Exception, match='Please pass list'):
        hs.write_partition_guarded(clean, FakeSpark(), 't1.x', 'tx_monthdate')


# ── the always-on checks ──────────────────────────────────────────────────────

def test_clean_batch_is_written_as_is(run, clean):
    fake, written = run(clean)
    assert written == sorted(COLS)
    assert fake.alters == []


def test_table_that_does_not_exist_raises(run, clean):
    # no guard pre-check any more - this is spark.table(fqn)'s own native failure bubbling up
    with pytest.raises(Exception, match='TABLE_OR_VIEW_NOT_FOUND'):
        run(clean, fake=FakeSpark(exists=False))


def test_invalid_toggle_raises_before_anything_else(run, clean):
    with pytest.raises(ValueError, match='on_newcols'):
        run(clean, on_newcols='evolv')


def test_check0_partition_key_column_missing_raises(run, spark_session):
    bad = _df(spark_session, [('A', 1.0)], ['town', 'resale_price'])
    with pytest.raises(Exception, match='overwrite key column'):
        run(bad)


def test_check1_null_partition_key_raises(run, spark_session):
    bad = spark_session.createDataFrame([(None, 'A', 1.0)], 'tx_monthdate string, town string, resale_price double')
    with pytest.raises(Exception, match='null value'):
        run(bad)


def test_check2_table_partitioned_differently_raises(run, clean):
    with pytest.raises(Exception, match='partition scheme change'):
        run(clean, table_keys=('town',))


def test_check2_key_order_is_not_a_mismatch_for_iceberg(run, spark_session):
    # iceberg keeps the key order as metadata only, so the same keys in another order are fine here
    df = _df(spark_session, [('2020-01', 'A', 1.0)], COLS)
    fake, written = run(df, table_keys=('town', 'tx_monthdate'), keys=('tx_monthdate', 'town'))
    assert written == sorted(COLS)


# ── check 3 - missing column ──────────────────────────────────────────────────

def test_check3_missing_column_error_raises(run, missing_town):
    with pytest.raises(Exception, match='MISSING'):
        run(missing_town, on_missingcols='error')


def test_check3_missing_column_pad_null_pads_and_writes(run, missing_town):
    fake, written = run(missing_town, on_missingcols='pad_null')
    assert written == sorted(COLS)


# ── check 4 - new column ──────────────────────────────────────────────────────

def test_check4_new_column_error_raises(run, with_new_col):
    with pytest.raises(Exception, match='NEW'):
        run(with_new_col, on_newcols='error')


def test_check4_new_column_drop_writes_without_it(run, with_new_col):
    fake, written = run(with_new_col, on_newcols='drop')
    assert written == sorted(COLS)
    assert fake.alters == []


def test_check4_new_column_evolve_alters_table_once_and_writes_it(run, with_new_col):
    fake, written = run(with_new_col, on_newcols='evolve')
    assert len(fake.alters) == 1 and 'flat_type' in fake.alters[0]
    assert written == sorted(COLS + ['flat_type'])


# ── check 5 - type change is 'loud': the guard only warns, the writer decides ─

def test_check5_type_change_warns_and_still_reaches_the_writer(run, spark_session, capsys):
    df = _df(spark_session, [('2020-01', 'A', 1)], COLS)          # resale_price arrives as long, table has double
    fake, written = run(df)
    assert written == sorted(COLS)
    assert "type change found on 'resale_price'" in capsys.readouterr().out


# ── full_refresh - routes to write_partition_full_refresh instead of write_partition ─

def test_full_refresh_false_by_default_uses_write_partition(run, clean):
    fake, written = run(clean)
    assert fake.write_kind == 'write_partition'
    assert written == sorted(COLS)


def test_full_refresh_true_routes_to_write_partition_full_refresh(run, clean):
    fake, written = run(clean, full_refresh=True)
    assert fake.write_kind == 'write_partition_full_refresh'
    assert written == sorted(COLS)


def test_full_refresh_still_runs_all_checks_first(run, with_new_col):
    # full_refresh only changes the FINAL write call - checks 0-5 are unconditional, same as
    # helper_pyiceberg_io's write_partition_guarded
    with pytest.raises(Exception, match='NEW'):
        run(with_new_col, on_newcols='error', full_refresh=True)
