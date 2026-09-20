"""Shared fixtures for the write_partition_guarded pytest suite (both engines).

No package structure exists under src/ (everything imports by plain module
name, sys.path-style, same as every pipeline script here) - so this conftest's
only job besides fixtures is putting src/ on sys.path once, for every test
file in this directory.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
    instead of float64 - exercises check 5 (type match), which is the one
    'loud' (warn, don't block) check, not silent like the others."""
    return pa.table({
        'tx_monthdate': ['2020-01'],
        'town': ['ANG MO KIO'],
        'resale_price': [300000],
    })
