"""Shared fixtures for the write_partition_guarded pytest suite (both engines).

No package structure exists under src/ (everything imports by plain module
name, sys.path-style, same as every pipeline script here) - so this conftest's
only job besides fixtures is putting src/ on sys.path once, for every test
file in this directory.
"""
import glob
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Spark's JVM is launched ONCE per process, and --jars / --packages only take effect at launch. Whichever
# test module starts a Spark session first launches it - if that is a plain one, the live Iceberg tests
# later find no Iceberg classes ("Cannot find catalog plugin class") and skip. So every JVM in this run is
# launched with the Iceberg runtime on its classpath, using the copy Spark already cached (no network).
# sqlite-jdbc is included too (2026-09-22) - needed by any JVM Iceberg catalog config using
# catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog against a sqlite-backed pyiceberg SqlCatalog
# (see TestPyicebergProvisionsSparkWrites in test_helper_sparkiceberg_io_iceberg_live.py).
# Stopping a SparkSession and building a new one CAN change session-level config (like which
# catalogs are registered) but can NEVER add a jar the JVM didn't boot with - verified: omitting
# this here produces "Failed to connect: jdbc:sqlite:..." (driver class not on the classpath) even
# in a freshly-recreated session, because it's the same underlying JVM process.
_ICEBERG_JAR = next(iter(sorted(glob.glob(os.path.expanduser(
    '~/.ivy2*/cache/org.apache.iceberg/iceberg-spark-runtime-4.0_2.13/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar')))), None)
_SQLITE_JAR = next(iter(sorted(glob.glob(os.path.expanduser(
    '~/.ivy2*/cache/org.xerial/sqlite-jdbc/jars/sqlite-jdbc-*.jar')))), None)
_JARS = ','.join(j for j in (_ICEBERG_JAR, _SQLITE_JAR) if j)
if _JARS:
    os.environ.setdefault('PYSPARK_SUBMIT_ARGS', f'--jars {_JARS} pyspark-shell')

import pyarrow as pa
import pytest

PARTITION_KEYS = ['tx_monthdate']


@pytest.fixture
def partition_keys():
    return list(PARTITION_KEYS)


@pytest.fixture
def data_narrow():
    """Stand-in for an older HDB era before a column existed yet - e.g. pre-2015
    resale data before 'flat_type' style additions. No flat_type column."""
    return pa.table({
        'tx_monthdate': ['2020-01', '2020-01'],
        'town': ['ANG MO KIO', 'BEDOK'],
        'resale_price': [300000.0, 350000.0],
    })


@pytest.fixture
def data_wide():
    """Same shape as data_narrow, plus one extra column - stand-in for a later
    era that gained a column. Different partition values from data_narrow so a
    real write actually adds new partitions rather than overwriting the same
    ones."""
    return pa.table({
        'tx_monthdate': ['2020-02', '2020-02'],
        'town': ['ANG MO KIO', 'BEDOK'],
        'resale_price': [310000.0, 360000.0],
        'flat_type': ['3 ROOM', '4 ROOM'],
    })


@pytest.fixture
def data_null_partition():
    return pa.table({
        'tx_monthdate': ['2020-01', None],
        'town': ['ANG MO KIO', 'BEDOK'],
        'resale_price': [300000.0, 350000.0],
    })


@pytest.fixture
def data_type_mismatch():
    """Same columns/partition values as data_narrow, but resale_price is int64
    instead of float64 - exercises check 5 (type match): iceberg's own writer
    raises on it (loud, the guard only warns), parquet has no check so the guard raises."""
    return pa.table({
        'tx_monthdate': ['2020-01'],
        'town': ['ANG MO KIO'],
        'resale_price': [300000],
    })
