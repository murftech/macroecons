import os
import sys
import argparse
from pathlib import Path

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

# ── PICK THE PROVIDER — the only environment branch in this file ─────────────
if IS_DATABRICKS:
    from providers.databricks import add_provider_args, get_landing_dir
elif IS_LOCAL:
    from providers.local import add_provider_args, get_landing_dir, ENV

from helper_datagov import fetch_datagov_csv


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['update', 'backfill'], default='update')
add_provider_args(parser)                # databricks: --catalog/--schema/--volume ; local: --env
args = parser.parse_args()
print(args)

# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    # args.mode = 'backfill'
    args.mode = 'update'


# ── WHERE THE CSVs LAND ───────────────────────────────────────────────────────
ORIGIN, DATASET = 'datagov', 'resale_flat_prices'
LANDING_DIR = get_landing_dir(args, ORIGIN, DATASET)
print(f'[land] landing dir : {LANDING_DIR}')


# ── HDB DATASETS INFO ───────────────────────────────────────────────────────

'''
Information from >> https://data.gov.sg/collections/189/view
# Resale Flat Prices (Based on Approval Date), 1990 - 1999
# dataset_id:
# d_ebc5ab87086db484f88045b47411ebc5

# Resale Flat Prices (Based on Approval Date), 2000 - Feb 2012
# dataset_id:
# d_43f493c6c50d54243cc1eab0df142d6a

# Resale Flat Prices (Based on Registration Date), From Mar 2012 to Dec 2014
# dataset_id:
# d_2d5ff9ea31397b66239f245f57751537

# Resale Flat Prices (Based on Registration Date), From Jan 2015 to Dec 2016
# dataset_id:
# d_ea9ed51da2787afaf8e51f827c304208

# Resale flat prices based on registration date from Jan-2017 onwards
# dataset_id:
# d_8b84c4ee58e3cfc0ece0d773c8ca6abc
'''

dataset_keys = {
    '1990_1999': 'd_ebc5ab87086db484f88045b47411ebc5',
    '2000_2012Feb': 'd_43f493c6c50d54243cc1eab0df142d6a',
    '2012Mar_2014': 'd_2d5ff9ea31397b66239f245f57751537',
    '2015_2016': 'd_ea9ed51da2787afaf8e51f827c304208',
    '2017_onwards': 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc',
}


############### RUN ###############

def land_hdb_table(era):
    resolved_dest_path = Path(LANDING_DIR) / f'{era}_{DATASET}_{dataset_keys[era]}.csv'
    print(f'resolved_dest_path = {resolved_dest_path}')
    fetch_datagov_csv(dataset_keys[era], resolved_dest_path, max_attempts=5)


if args.mode == 'backfill':
    land_hdb_table('1990_1999')
    land_hdb_table('2000_2012Feb')
    land_hdb_table('2012Mar_2014')
    land_hdb_table('2015_2016')

if args.mode == 'update':
    land_hdb_table('2017_onwards')

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
print(args)
