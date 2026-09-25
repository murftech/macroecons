"""pytest suite for helper_pyiceberg_io.write_partition_guarded.

Converted from the REPL demo `1_import_to_t1 unit_test_for_write_partition_guarded.py`
(kept as-is alongside this file - it's the eyeball tool, this is the proof).
Every test gets its own pytest tmp_path-backed SqlCatalog, so there's no
shared table to reset between tests the way the REPL demo needed
double_safe_purge()/createOrEvolve_table().
"""
import pytest

from lakehouse_io.helper_pyiceberg_io import getOrCreate_catalog, createOrEvolve_table, write_partition_guarded
from conftest import PARTITION_KEYS

TBL_NAME = 'test_table'
TBL_FQN = f't1.{TBL_NAME}'


@pytest.fixture
def catalog(tmp_path):
    cat = getOrCreate_catalog(tmp_path / 'iceberg')
    cat.create_namespace_if_not_exists('t1')
    return cat


def _seed(catalog, data, partition_keys=PARTITION_KEYS):
    """Provision TBL_FQN with `data`'s schema, the real production path
    (createOrEvolve_table) - unlike pyarrow, write_partition_guarded itself
    always requires the table to already exist (see
    test_guard_requires_table_already_provisioned below), so seeding can't be
    done through the guarded function's own first-write behavior."""
    createOrEvolve_table(data, partition_keys, catalog, 't1', TBL_NAME)


# ── the pyarrow/iceberg asymmetry: no "nothing on disk yet" case here ─────────

def test_guard_requires_table_already_provisioned(catalog, data_narrow):
    # write_partition_guarded unconditionally does catalog.load_table(table_fqn)
    # - unlike pyarrow's guard, which treats an absent/empty dir as a legitimate
    # first write. Provisioning (createOrEvolve_table) is a separate, deliberate
    # step here, never implicit.
    with pytest.raises(Exception):
        write_partition_guarded(data_narrow, catalog, TBL_FQN, PARTITION_KEYS,
                                 on_newcols='error', on_missingcols='error')


# ── shape validation - overwrite keys must always be a list, even for one column ──
# (shared_schema_guards_arrow.assert_keys_is_list, reused by every engine's write_*_guarded)

def test_partition_keys_as_bare_string_raises(catalog, data_narrow):
    _seed(catalog, data_narrow)
    with pytest.raises(Exception, match='Please pass list'):
        write_partition_guarded(data_narrow, catalog, TBL_FQN, 'tx_monthdate')


# ── checks 0/1/2 - always-on, no toggle affects them ──────────────────────────

def test_first_real_write_after_provisioning_skips_checks_3_4_5(catalog, data_narrow):
    # table just created from data_narrow's own schema - writing that same
    # shape back should have nothing to report even under the strictest
    # settings.
    _seed(catalog, data_narrow)
    write_partition_guarded(data_narrow, catalog, TBL_FQN, PARTITION_KEYS,
                             on_newcols='error', on_missingcols='error')


def test_check0_missing_partition_key_column_raises(catalog, data_narrow):
    _seed(catalog, data_narrow)
    bad = data_narrow.drop_columns(['tx_monthdate'])
    with pytest.raises(Exception, match='overwrite key column'):
        write_partition_guarded(bad, catalog, TBL_FQN, PARTITION_KEYS)


def test_check1_null_partition_key_always_raises(catalog, data_narrow, data_null_partition):
    _seed(catalog, data_narrow)
    with pytest.raises(Exception, match='null value'):
        write_partition_guarded(data_null_partition, catalog, TBL_FQN, PARTITION_KEYS,
                                 on_newcols='evolve', on_missingcols='pad_null')


def test_check2_partition_scheme_change_raises(catalog, data_narrow):
    _seed(catalog, data_narrow)
    with pytest.raises(Exception, match='partition scheme change'):
        write_partition_guarded(data_narrow, catalog, TBL_FQN, ['town'],
                                 on_newcols='error', on_missingcols='error')


# ── checks 3/4 - the (on_newcols, on_missingcols) combo matrix ────────────────
# Same combos/rationale as test_write_partition_guarded_pyarrow.py's COMBOS -
# the DRY pass (shared_schema_guards_arrow.py) unified vocabulary and check numbering
# across both engines, so the expected pass/fail shape per combo is identical.

COMBOS = [
    pytest.param('evolve', 'pad_null', 'data_narrow', 'data_wide', False, None,
                 id='evolve-pad_null__new_col_evolves_through'),
    pytest.param('evolve', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='evolve-pad_null__missing_col_gets_padded'),

    pytest.param('evolve', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='evolve-error__older_era_after_evolution_raises'),

    pytest.param('drop', 'pad_null', 'data_narrow', 'data_wide', False, None,
                 id='drop-pad_null__new_col_silently_dropped'),
    pytest.param('drop', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='drop-pad_null__missing_col_gets_padded'),

    pytest.param('drop', 'error', 'data_narrow', 'data_wide', False, None,
                 id='drop-error__new_col_silently_dropped'),
    pytest.param('drop', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='drop-error__narrow_T1_shaped_batch_genuinely_fails'),

    pytest.param('error', 'pad_null', 'data_narrow', 'data_wide', True, 'NEW',
                 id='error-pad_null__unexpected_new_col_still_blocked'),
    pytest.param('error', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='error-pad_null__known_missing_col_gets_padded'),

    pytest.param('error', 'error', 'data_narrow', 'data_wide', True, 'NEW',
                 id='error-error__new_col_blocked'),
    pytest.param('error', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='error-error__missing_col_blocked'),
]


@pytest.mark.parametrize('on_newcols,on_missingcols,seed_fixture,write_fixture,expect_raise,match', COMBOS)
def test_guard_combo(request, catalog, on_newcols, on_missingcols, seed_fixture, write_fixture,
                      expect_raise, match):
    seed_data = request.getfixturevalue(seed_fixture)
    write_data = request.getfixturevalue(write_fixture)
    _seed(catalog, seed_data)

    call = lambda: write_partition_guarded(
        write_data, catalog, TBL_FQN, PARTITION_KEYS,
        on_newcols=on_newcols, on_missingcols=on_missingcols)

    if expect_raise:
        with pytest.raises(Exception, match=match):
            call()
    else:
        call()


# ── check 5 - type mismatch is 'loud' only for the guard itself ───────────────

def test_check5_type_mismatch_reaches_icebergs_own_native_check(catalog, data_narrow, data_type_mismatch):
    # same columns/partition value as data_narrow, only resale_price's type
    # differs - no new/missing column, so checks 3/4 report nothing and only
    # check 5 (loud - warn, don't block) fires from the guard itself. Verified
    # empirically (not just from the docstring's claim): the guard lets this
    # through, but tbl.overwrite() then rejects it with its own native
    # ValueError - unlike pyarrow, where the equivalent call (see
    # test_write_partition_guarded_pyarrow.test_check5_type_mismatch_alone_warns_but_does_not_raise)
    # succeeds outright, because ds.write_dataset has no such native check.
    _seed(catalog, data_narrow)
    with pytest.raises(ValueError, match='resale_price'):
        write_partition_guarded(data_type_mismatch, catalog, TBL_FQN, PARTITION_KEYS,
                                 on_newcols='error', on_missingcols='error')
