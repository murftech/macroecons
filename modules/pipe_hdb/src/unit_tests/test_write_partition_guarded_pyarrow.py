"""pytest suite for helper_pyarrow_io.write_partition_guarded.

Converted from the REPL demo `1_import_to_t1 unit_test_for_write_partition_guarded.py`
(kept as-is alongside this file - it's the eyeball tool, this is the proof).
Every test gets a fresh pytest tmp_path, so there's no shared table to reset
between tests the way the REPL demo needed _reset_demo_table()/shutil.rmtree.
"""
from pathlib import Path

import pytest

from helper_pyarrow_io import write_partition_guarded
from conftest import PARTITION_KEYS


def _seed(path: Path, data, partition_keys=PARTITION_KEYS):
    """Establish a destination directory with `data` already written. First
    write to an empty/absent dir always skips checks 2-5 regardless of
    on_newcols/on_missingcols, so the strictest settings are fine here."""
    write_partition_guarded(data, str(path), partition_keys,
                             on_newcols='error', on_missingcols='error')


# ── checks 0/1/2 - always-on, no toggle affects them ──────────────────────────

def test_first_write_to_empty_dir_skips_all_checks(tmp_path, data_narrow):
    # nothing on disk yet - even the strictest settings must not raise
    write_partition_guarded(data_narrow, str(tmp_path / 't1'), PARTITION_KEYS,
                             on_newcols='error', on_missingcols='error')


def test_check0_missing_partition_key_column_raises(tmp_path, data_narrow):
    bad = data_narrow.drop_columns(['tx_monthdate'])
    with pytest.raises(Exception, match='partition_keys column'):
        write_partition_guarded(bad, str(tmp_path / 't1'), PARTITION_KEYS)


def test_check1_null_partition_key_always_raises(tmp_path, data_null_partition):
    # ALWAYS on - no on_newcols/on_missingcols setting can let a null partition
    # key through, since delete_matching would silently destroy the previous
    # null-partitioned rows on write.
    with pytest.raises(Exception, match='null value'):
        write_partition_guarded(data_null_partition, str(tmp_path / 't1'), PARTITION_KEYS,
                                 on_newcols='evolve', on_missingcols='pad_null')


def test_check2_partition_scheme_change_raises(tmp_path, data_narrow):
    path = tmp_path / 't1'
    _seed(path, data_narrow, partition_keys=['tx_monthdate'])
    with pytest.raises(Exception, match='partition scheme change'):
        write_partition_guarded(data_narrow, str(path), ['town'],
                                 on_newcols='error', on_missingcols='error')


# ── checks 3/4 - the (on_newcols, on_missingcols) combo matrix ────────────────
# Source: the REPL demo's Appendix comment block, both engines used the same
# 6 combos once the DRY pass unified vocabulary. Each entry is
# (on_newcols, on_missingcols, seed_data, write_data, expect_raise, match).

COMBOS = [
    # ('evolve', 'pad_null') - T1's real production default: never lose data,
    # evolve immediately for a new column, pad immediately for a missing one.
    pytest.param('evolve', 'pad_null', 'data_narrow', 'data_wide', False, None,
                 id='evolve-pad_null__new_col_evolves_through'),
    pytest.param('evolve', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='evolve-pad_null__missing_col_gets_padded'),

    # ('evolve', 'error') - flagged in the REPL Appendix as contradictory:
    # evolving forward means an older/narrower era arriving later WILL be
    # missing the evolved column, and on_missingcols='error' has no opt-out -
    # so this documents the real (if awkward) consequence, it isn't a bug.
    pytest.param('evolve', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='evolve-error__older_era_after_evolution_raises'),

    # ('drop', 'pad_null') - T2 fixed-contract shape: discard unexpected extra
    # columns silently, but don't stop the job over a merely-missing one.
    pytest.param('drop', 'pad_null', 'data_narrow', 'data_wide', False, None,
                 id='drop-pad_null__new_col_silently_dropped'),
    pytest.param('drop', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='drop-pad_null__missing_col_gets_padded'),

    # ('drop', 'error') - T2 strict: discard extras, but stop immediately (no
    # push at all) if the batch is missing a contract column. Also the
    # "keep the boundary honest" case from the memory: a T1-shaped (narrower)
    # batch against a T2-fixed-contract destination must genuinely fail here,
    # not silently pass through.
    pytest.param('drop', 'error', 'data_narrow', 'data_wide', False, None,
                 id='drop-error__new_col_silently_dropped'),
    pytest.param('drop', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='drop-error__narrow_T1_shaped_batch_genuinely_fails'),

    # ('error', 'pad_null') - looks contradictory at first read ("error one,
    # allow the other?") but is real for T1: guard against an unexpected/
    # accidental new column, while still tolerating a KNOWN-missing one from
    # an older era that predates it.
    pytest.param('error', 'pad_null', 'data_narrow', 'data_wide', True, 'NEW',
                 id='error-pad_null__unexpected_new_col_still_blocked'),
    pytest.param('error', 'pad_null', 'data_wide', 'data_narrow', False, None,
                 id='error-pad_null__known_missing_col_gets_padded'),

    # ('error', 'error') - T2 strict fixed schema, refreshable from t1: neither
    # direction of drift is tolerated.
    pytest.param('error', 'error', 'data_narrow', 'data_wide', True, 'NEW',
                 id='error-error__new_col_blocked'),
    pytest.param('error', 'error', 'data_wide', 'data_narrow', True, 'MISSING',
                 id='error-error__missing_col_blocked'),
]


@pytest.mark.parametrize('on_newcols,on_missingcols,seed_fixture,write_fixture,expect_raise,match', COMBOS)
def test_guard_combo(request, tmp_path, on_newcols, on_missingcols, seed_fixture, write_fixture,
                      expect_raise, match):
    seed_data = request.getfixturevalue(seed_fixture)
    write_data = request.getfixturevalue(write_fixture)
    path = tmp_path / 't1'
    _seed(path, seed_data)

    call = lambda: write_partition_guarded(
        write_data, str(path), PARTITION_KEYS,
        on_newcols=on_newcols, on_missingcols=on_missingcols)

    if expect_raise:
        with pytest.raises(Exception, match=match):
            call()
    else:
        call()


# ── check 5 - type mismatch is 'loud' only, never blocks on its own ───────────

def test_check5_type_mismatch_alone_warns_but_does_not_raise(tmp_path, data_narrow, data_type_mismatch):
    path = tmp_path / 't1'
    _seed(path, data_narrow)
    # same columns, same partition value, only resale_price's type differs -
    # no new/missing column, so checks 3/4 have nothing to report and only
    # check 5 (loud) fires. Must not raise.
    write_partition_guarded(data_type_mismatch, str(path), PARTITION_KEYS,
                             on_newcols='error', on_missingcols='error')
