"""pytest suite for helper_sparkdelta_io.write_span_guarded - shape validation only.

No full guard-combo suite here yet (unlike test_write_partition_guarded_pyarrow.py /
_iceberg.py / _spark.py) - this covers exactly what was manually verified: span_key
must always be a list, and must contain exactly one column, even though delta's
overwrite predicate is genuinely single-column (LOCKED 2026-09-24, see module
docstring). Both checks run in resolve_single_key (shared_schema_guards_arrow.py) before
spark/fqn are ever touched, so a plain None stand-in for both is enough here - no
FakeSpark needed for these two cases.
"""
import pytest

from lakehouse_io.helper_sparkdelta_io import write_span_guarded


def test_span_key_as_bare_string_raises():
    with pytest.raises(Exception, match='Please pass list'):
        write_span_guarded(None, None, 'dummy_fqn', 'tx_monthdate')


def test_span_key_as_multi_column_list_raises():
    # the one constraint iceberg/parquet DON'T have: delta's IN-list overwrite is
    # single-column only - a real list, just the wrong length, still gets rejected.
    with pytest.raises(Exception, match='single column'):
        write_span_guarded(None, None, 'dummy_fqn', ['era', 'tx_monthdate'])
