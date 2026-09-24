"""Local provider - pandas + pyarrow only (mirror of pipe_hdb/src/providers/local.py)
"""

import argparse
import os
import sys
from pathlib import Path

# ── REACH INTO THE ORIGINAL REPO — the one place the mirror does it ──────────
# only the pandas-specific files live in this mirror; shared helpers (helper_pyarrow_io,
# shared_schema_guards, helper_transit) are imported from pipe_hdb/src. APPENDED, not
# inserted: the mirror's own src stays first, so a same-named mirror file always wins.
# __file__ = modules/pipe_hdb_pandas_mirror/src/providers/local_pandas.py -> parents[3] = modules/
ORIGINAL_SRC = Path(__file__).resolve().parents[3] / 'pipe_hdb' / 'src'
if str(ORIGINAL_SRC) not in sys.path:
    sys.path.append(str(ORIGINAL_SRC))

# run on terminal, run.sh or make with ENV=production it will read and write to production lakehouse.
# without, default string is dev
ENV = os.environ.get('ENV', 'dev')

CATALOG_NAME = 'macroecons'

def get_lakehouse_root_from_env(env):
    if env not in ('dev', 'production'):
        raise ValueError(f"env must be 'dev' or 'production', got {env!r}")

    resolved_lakehouse = Path(f'/Users/murftech/Root/MasterETL/{env}/lakehouse')
    return resolved_lakehouse


def add_provider_args(parser):
    print('\n\n')
    added = [
        parser.add_argument('--env', default=ENV, choices=('dev', 'production'),
                             help="which lakehouse to target - defaults to the ENV env var (itself defaulting to 'dev')"),
    ]
    print('args_added:', [a.option_strings[0] for a in added])



def get_landing_dir(args, origin, dataset):
    """Where 0_land_csv.py drops the raw CSVs (and 1_import_to_t1_pandas.py reads them).
    """
    return str(get_lakehouse_root_from_env(args.env) / 'landing' / origin / dataset)



def dispatch_write(data, *, tier, origin, dataset, partition_keys, show_partitions=False,
               on_newcols='evolve', on_missingcols='pad_null', args):

    '''
    1. Handling more than one frame
    2. Deciding what order to write frames in
    3. Unifying the table write address from tier, origin, dataset - under
       get_lakehouse_root_from_env(args.env), Warehouse ROOT > Catalog > Tier > Table.
    Parquet only - there is no format fork here.
    '''

    import pyarrow as pa
    import helper_pyarrow_io

    lakehouse_root = get_lakehouse_root_from_env(args.env)

    frames  = list(data.values()) if isinstance(data, dict) else data if isinstance(data, list) else [data]

    # widest schema first - the frame with the most columns establishes the
    # destination's full width on write #1, so every later (narrower) frame
    # just needs on_missingcols='pad_null'.
    frames = sorted(frames, key=lambda df: -len(df.columns))

    PARQUET_DIR = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')

    for df in frames:
        # preserve_index=False: otherwise the pandas RangeIndex rides along as a '__index_level_0__' column
        helper_pyarrow_io.write_partition_guarded(
            pa.Table.from_pandas(df, preserve_index=False), PARQUET_DIR, partition_keys,
            on_newcols=on_newcols, on_missingcols=on_missingcols, show_partitions=show_partitions)
        print(f'DONE:  parquet -> {PARQUET_DIR}')



def read_tier(args, *, tier, origin, dataset):
    """Read a persisted tier back as a pandas DataFrame. `args.env` selects dev vs production, same as dispatch_write.
    """
    import helper_pyarrow_io

    lakehouse_root = get_lakehouse_root_from_env(args.env)
    parquet_dir = str(lakehouse_root / 'hive' / CATALOG_NAME / tier / f'{origin}__{dataset}')
    return helper_pyarrow_io.pandas_read_pyarrow(parquet_dir)
