"""LIVE integration tests for helper_sparkcatalog_io's iceberg write path -
against a REAL local JVM Iceberg catalog, not fixtures. Genuinely slow (boots a
real JVM Spark session, downloads the iceberg-spark-runtime JAR from Maven the
first time) and needs network access - deliberately kept separate from the fast,
network-free suite in test_helper_sparkcatalog_io.py, which only covers the
individual check functions in isolation.

This is what actually proved write_partition_guarded correct end-to-end
(2026-09-16) - and caught a real bug in the process: _current_partition_columns'
DESCRIBE TABLE parsing was wrong on the first attempt (guessed a
`identity(col)`-transform-expression format; the real format lists partition
columns by plain name). Fixed after inspecting real output, not by guessing
again - see that function's docstring in helper_sparkcatalog_io.py.

Zero edits to the separate sparkutils repo - the JAR/catalog config is set up
entirely here, via SparkSession.builder BEFORE get_spark() would ever be
called, exploiting get_spark()'s own "adopt an existing session" behavior.
"""
import sys, os, tempfile

import pytest

os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)

from conftest import PARTITION_KEYS as _  # noqa: F401 - confirms conftest loads cleanly

FQN = 'local_iceberg.t1.test_table'
PARTITION_KEYS = ['tx_monthdate']


@pytest.fixture(scope='module')
def spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip('pyspark not installed')

    warehouse = tempfile.mkdtemp(prefix='iceberg_warehouse_')
    try:
        s = (
            SparkSession.builder
            .appName('test_helper_sparkcatalog_io_iceberg_live')
            .master('local[1]')
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0')
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .config('spark.sql.catalog.local_iceberg', 'org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.local_iceberg.type', 'hadoop')
            .config('spark.sql.catalog.local_iceberg.warehouse', warehouse)
            .getOrCreate()
        )
        s.sql('CREATE NAMESPACE IF NOT EXISTS local_iceberg.t1')
    except Exception as e:
        pytest.skip(f'could not boot a local JVM Iceberg session (network/JAR issue?): {e}')
    yield s
    s.stop()


@pytest.fixture
def data_narrow(spark):
    return spark.createDataFrame(
        [('2020-01', 'ANG MO KIO', 300000.0), ('2020-01', 'BEDOK', 350000.0)],
        ['tx_monthdate', 'town', 'resale_price'])


@pytest.fixture
def data_wide(spark):
    return spark.createDataFrame(
        [('2020-02', 'ANG MO KIO', 310000.0, '3 ROOM'), ('2020-02', 'BEDOK', 360000.0, '4 ROOM')],
        ['tx_monthdate', 'town', 'resale_price', 'flat_type'])


@pytest.fixture
def data_null_partition(spark):
    return spark.createDataFrame(
        [('2020-01', 'ANG MO KIO', 300000.0), (None, 'BEDOK', 350000.0)],
        ['tx_monthdate', 'town', 'resale_price'])


@pytest.fixture(autouse=True)
def _reset_table(spark, data_narrow):
    from helper_sparkcatalog_io import create_table
    spark.sql(f'DROP TABLE IF EXISTS {FQN}')
    create_table(data_narrow, spark, FQN, PARTITION_KEYS)


def test_basic_round_trip(spark, data_narrow):
    from helper_sparkcatalog_io import write_partition_guarded
    write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS,
                            on_newcols='error', on_missingcols='error')
    assert spark.table(FQN).count() == 2


def test_evolve_pad_null_matches_pyiceberg_T1_default(spark, data_wide, data_narrow):
    from helper_sparkcatalog_io import write_partition_guarded
    write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                            on_newcols='evolve', on_missingcols='pad_null')
    write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS,
                            on_newcols='evolve', on_missingcols='pad_null')
    df = spark.table(FQN)
    assert 'flat_type' in df.columns
    assert df.count() == 4


def test_check0_missing_partition_key_raises(spark, data_narrow):
    from helper_sparkcatalog_io import write_partition_guarded
    bad = data_narrow.drop('tx_monthdate')
    with pytest.raises(Exception, match='partition_keys column'):
        write_partition_guarded(bad, spark, FQN, PARTITION_KEYS)


def test_check1_null_partition_key_raises(spark, data_null_partition):
    from helper_sparkcatalog_io import write_partition_guarded
    with pytest.raises(Exception, match='null value'):
        write_partition_guarded(data_null_partition, spark, FQN, PARTITION_KEYS,
                                on_newcols='evolve', on_missingcols='pad_null')


def test_check4_error_blocks_unexpected_new_col(spark, data_wide):
    from helper_sparkcatalog_io import write_partition_guarded
    with pytest.raises(Exception, match='NEW'):
        write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                                on_newcols='error', on_missingcols='pad_null')


def test_guard_requires_table_already_provisioned(spark, data_narrow):
    from helper_sparkcatalog_io import write_partition_guarded
    spark.sql(f'DROP TABLE IF EXISTS {FQN}')
    with pytest.raises(Exception, match='does not exist'):
        write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS)
