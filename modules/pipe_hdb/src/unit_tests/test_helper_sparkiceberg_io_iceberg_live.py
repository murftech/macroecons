"""LIVE integration tests for helper_sparkiceberg_io's iceberg write path -
against a REAL local JVM Iceberg catalog, not fixtures. Genuinely slow (boots a
real JVM Spark session, downloads the iceberg-spark-runtime JAR from Maven the
first time) and needs network access - deliberately kept separate from the fast,
network-free suite in test_helper_sparkiceberg_io.py, which only covers the
individual check functions in isolation.

This is what actually proved write_partition_guarded correct end-to-end
(2026-09-16) - and caught a real bug in the process: _current_partition_columns'
DESCRIBE TABLE parsing was wrong on the first attempt (guessed a
`identity(col)`-transform-expression format; the real format lists partition
columns by plain name). Fixed after inspecting real output, not by guessing
again - see that function's docstring in helper_sparkiceberg_io.py.

Zero edits to the separate sparkutils repo - the JAR/catalog config is set up
entirely here, via SparkSession.builder BEFORE get_spark() would ever be
called, exploiting get_spark()'s own "adopt an existing session" behavior.

PROVISIONING (2026-09-22): helper_sparkiceberg_io.py no longer has its own
create_table - the real contract is helper_pyiceberg_io.createOrEvolve_table
only (see that removal's comment in helper_sparkiceberg_io.py). The shared
fixtures below (module-scoped Spark session + Hadoop catalog, reset via inline
Spark DDL) do NOT go through pyiceberg at all - that inline DDL is test-only
scaffolding for fast, repeated per-test resets, not a stand-in for the real
provisioning contract. The real cross-engine claim (a table pyiceberg
provisions is fully readable/writable by this file's write_partition_guarded)
is proven separately, in TestPyicebergProvisionsSparkWrites below, using its
own fully isolated warehouse/catalog/session - NOT the shared fixtures here.

Why isolated: verified empirically that a table provisioned via pyiceberg's
SqlCatalog (sqlite-backed) IS correctly read/written by Spark's
writeTo(fqn).overwritePartitions(), PROVIDED Spark's catalog config is also a
JDBC catalog (catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog) pointed at the
SAME sqlite db + warehouse, with the SAME catalog name (the JDBC catalog spec
scopes every row by a `catalog_name` column keyed off that name - a mismatch
there sees zero tables even against the identical db file). BUT repeating that
handoff many times against one shared, long-lived Spark session - as this
file's per-test _reset_table used to do via the old create_table - genuinely
deadlocks: confirmed directly that pyiceberg alone (no Spark) survives
back-to-back drop/create loops fine, so the fault is a live Spark JDBC-catalog
session holding the sqlite file open for its own lifetime, not a timeout
tuning issue (raising sqlite's busy_timeout to 30s made no difference - the
lock is held, not momentarily contended). So: provision via pyiceberg BEFORE
a Spark session touching that catalog exists, never interleaved with one.
"""
import sys, os, tempfile
from pathlib import Path

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
            .appName('test_helper_sparkiceberg_io_iceberg_live')
            .master('local[1]')
            .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0')
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .config('spark.sql.catalog.local_iceberg', 'org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.local_iceberg.type', 'hadoop')
            .config('spark.sql.catalog.local_iceberg.warehouse', warehouse)
            .getOrCreate()
        )
        if s.conf.get('spark.sql.catalog.local_iceberg', None) is None:
            pytest.fail('getOrCreate() handed back a Spark session built WITHOUT the Iceberg config - an earlier '
                        'fixture leaked its session. Make that fixture module-scoped and stop() it in teardown.')
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
    """Test-only scaffolding, NOT the production provisioning contract (that's
    helper_pyiceberg_io.createOrEvolve_table - see this file's module docstring for why
    it can't be reused here, per-test, against a shared live Spark session). Inlined
    directly rather than imported, since helper_sparkiceberg_io deliberately has no
    create_table of its own any more."""
    from sparkutils.functions import col as _col
    spark.sql(f'DROP TABLE IF EXISTS {FQN}')
    data_narrow.writeTo(FQN).using('iceberg').partitionedBy(*[_col(c) for c in PARTITION_KEYS]).create()


def test_basic_round_trip(spark, data_narrow):
    from helper_sparkiceberg_io import write_partition_guarded
    write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS,
                            on_newcols='error', on_missingcols='error')
    assert spark.table(FQN).count() == 2


def test_evolve_pad_null_matches_pyiceberg_T1_default(spark, data_wide, data_narrow):
    from helper_sparkiceberg_io import write_partition_guarded
    write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                            on_newcols='evolve', on_missingcols='pad_null')
    write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS,
                            on_newcols='evolve', on_missingcols='pad_null')
    df = spark.table(FQN)
    assert 'flat_type' in df.columns
    assert df.count() == 4


def test_check0_missing_partition_key_raises(spark, data_narrow):
    from helper_sparkiceberg_io import write_partition_guarded
    bad = data_narrow.drop('tx_monthdate')
    with pytest.raises(Exception, match='partition_keys column'):
        write_partition_guarded(bad, spark, FQN, PARTITION_KEYS)


def test_check1_null_partition_key_raises(spark, data_null_partition):
    from helper_sparkiceberg_io import write_partition_guarded
    with pytest.raises(Exception, match='null value'):
        write_partition_guarded(data_null_partition, spark, FQN, PARTITION_KEYS,
                                on_newcols='evolve', on_missingcols='pad_null')


def test_check4_error_blocks_unexpected_new_col(spark, data_wide):
    from helper_sparkiceberg_io import write_partition_guarded
    with pytest.raises(Exception, match='NEW'):
        write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                                on_newcols='error', on_missingcols='pad_null')


def test_guard_requires_table_already_provisioned(spark, data_narrow):
    # no pre-check in the guard any more - this is spark.table(fqn)'s own native
    # AnalysisException bubbling up, same failure mode as pyiceberg's NoSuchTableError
    from helper_sparkiceberg_io import write_partition_guarded
    spark.sql(f'DROP TABLE IF EXISTS {FQN}')
    with pytest.raises(Exception, match='TABLE_OR_VIEW_NOT_FOUND'):
        write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS)


def test_full_refresh_wipes_unrelated_existing_rows(spark, data_narrow, data_wide):
    # data_narrow (2020-01) is already in the table via _reset_table. A full_refresh write of
    # data_wide (2020-02, a different month) must leave ONLY data_wide's rows - proves this
    # isn't just another overwritePartitions() (which would leave 2020-01 untouched).
    from helper_sparkiceberg_io import write_partition_guarded
    write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                            on_newcols='evolve', on_missingcols='pad_null', full_refresh=True)
    rows = spark.table(FQN).select('tx_monthdate').distinct().collect()
    assert sorted(r['tx_monthdate'] for r in rows) == ['2020-02']


def test_full_refresh_still_enforces_schema(spark, data_narrow, data_wide):
    # .overwrite(lit(True)), not .replace() - the table's schema is NOT allowed to change here,
    # same as any other guarded write. An unexpected new column must still be blocked.
    from helper_sparkiceberg_io import write_partition_guarded
    with pytest.raises(Exception, match='NEW'):
        write_partition_guarded(data_wide, spark, FQN, PARTITION_KEYS,
                                on_newcols='error', on_missingcols='error', full_refresh=True)


def test_show_partitions_prints_the_real_summary(spark, data_narrow, capsys):
    # end-to-end proof that summarize_partitions is actually wired into the real write path,
    # not just tested standalone against a fabricated DataFrame
    from helper_sparkiceberg_io import write_partition_guarded
    write_partition_guarded(data_narrow, spark, FQN, PARTITION_KEYS,
                            on_newcols='error', on_missingcols='error', show_partitions=True)
    out = capsys.readouterr().out
    assert '[iceberg] replacing 1 leaf partitions on tx_monthdate:' in out
    assert 'show partitions' in out
    assert 'tx_monthdate=2020-01' in out


class TestPyicebergProvisionsSparkWrites:
    """The real provisioning-policy proof, fully isolated from every fixture above: its own
    temp warehouse, its own pyiceberg SqlCatalog, its own short-lived Spark session - started
    and stopped within this one test, never shared - so pyiceberg's writes and Spark's JDBC
    catalog connection never coexist against a live session the way the deadlock above needed.

    Proves: createOrEvolve_table (pyiceberg, JDBC/sqlite catalog) provisions a table that
    write_partition_guarded (Spark, JDBC catalog pointed at the SAME sqlite db + warehouse,
    SAME catalog name) can both write AND that pyiceberg can still read correctly afterward -
    the actual claim behind removing helper_sparkiceberg_io.create_table.
    """

    def test_provision_via_pyiceberg_then_write_via_spark(self):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            pytest.skip('pyspark not installed')

        # Spark allows exactly one SparkSession per JVM - getOrCreate() silently REUSES the
        # module `spark` fixture's session (still alive - its own teardown only runs at the
        # very end of this file) and ignores any new .config(...) calls ("Using an existing
        # Spark session; only runtime SQL configurations will take effect", verified). This
        # class runs last in the file (nothing after it needs the shared session), so it's
        # safe to stop it here first, forcing a genuinely fresh session with this test's own
        # catalog config.
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()

        import glob
        import pyarrow as pa
        from helper_pyiceberg_io import getOrCreate_catalog, createOrEvolve_table

        warehouse = Path(tempfile.mkdtemp(prefix='cross_engine_iceberg_'))
        fqn_ns, fqn_tbl = 't1', 'test_table'
        partition_keys = ['tx_monthdate']

        # ── provision via pyiceberg, BEFORE any Spark session touches this catalog ──
        catalog = getOrCreate_catalog(warehouse)
        catalog.create_namespace_if_not_exists(fqn_ns)
        schema = pa.schema([
            pa.field('tx_monthdate', pa.string(), nullable=False),
            pa.field('town', pa.string(), nullable=True),
            pa.field('resale_price', pa.float64(), nullable=True),
        ])
        createOrEvolve_table(schema, partition_keys, catalog, fqn_ns, fqn_tbl)
        catalog_db_path = warehouse / '_icebergcatalog.db'

        iceberg_jars = glob.glob(os.path.expanduser(
            '~/.ivy2*/cache/org.apache.iceberg/iceberg-spark-runtime-4.0_2.13/jars/*.jar'))
        sqlite_jars = glob.glob(os.path.expanduser('~/.ivy2*/cache/org.xerial/sqlite-jdbc/jars/*.jar'))
        jars_conf = (
            {'spark.jars': f'{iceberg_jars[0]},{sqlite_jars[0]}'} if iceberg_jars and sqlite_jars
            else {'spark.jars.packages': 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,'
                                         'org.xerial:sqlite-jdbc:3.46.1.0'}
        )

        # ── a dedicated, short-lived Spark session - never shared with the module fixture above ──
        builder = (
            SparkSession.builder
            .appName('test_provision_via_pyiceberg_then_write_via_spark')
            .master('local[1]')
            .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
            .config('spark.sql.catalog.icebergcatalog', 'org.apache.iceberg.spark.SparkCatalog')
            .config('spark.sql.catalog.icebergcatalog.catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog')
            .config('spark.sql.catalog.icebergcatalog.uri', f'jdbc:sqlite:{catalog_db_path}')
            .config('spark.sql.catalog.icebergcatalog.warehouse', f'file://{warehouse}')
        )
        for k, v in jars_conf.items():
            builder = builder.config(k, v)

        try:
            spark2 = builder.getOrCreate()
        except Exception as e:
            pytest.skip(f'could not boot an isolated JVM Iceberg session (network/JAR issue?): {e}')

        try:
            spark_fqn = f'icebergcatalog.{fqn_ns}.{fqn_tbl}'

            from helper_sparkiceberg_io import write_partition_guarded, _current_partition_columns
            assert _current_partition_columns(spark2, spark_fqn) == partition_keys

            df = spark2.createDataFrame(
                [('2020-01', 'ANG MO KIO', 300000.0), ('2020-01', 'BEDOK', 350000.0)],
                ['tx_monthdate', 'town', 'resale_price'])
            write_partition_guarded(df, spark2, spark_fqn, partition_keys,
                                    on_newcols='error', on_missingcols='error')

            assert spark2.table(spark_fqn).count() == 2

            # pyiceberg still sees the SAME table, updated by Spark's write
            tbl = catalog.load_table(f'{fqn_ns}.{fqn_tbl}').refresh()
            assert tbl.scan().to_arrow().num_rows == 2
        finally:
            spark2.stop()
