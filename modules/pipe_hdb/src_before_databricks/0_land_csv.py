import os
import sys
from pathlib import Path
import argparse

# sub learn and digest this later
parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['update', 'backfill'], default = 'update')
args = parser.parse_args()
print(args)

###### INLINE OVERWRITES  ######
# args.mode='backfill'
# args.mode='update'

# needs the repo root on sys.path, so it only works when cwd is the repo root:
import sys; sys.path.append('.')
from modules.pipe_hdb.src.helper_datagov import fetch_datagov_csv


# EXPLAIN: use whatever the HDB_DATALAKE_DIR environment variable says; if nobody set it, use 'datalake'.
LANDING_DIR = os.environ.get('HDB_LANDING_DIR', 'datalake/landing/datagov/resale_flat_prices/')
print(LANDING_DIR)

###################################
# HDB DATASETS INFO
###################################

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
    '2017_onwards': 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'
}


def land_hdb_table(era):
    resolved_dest_path = Path(LANDING_DIR) / f'{era}_resale_flat_prices_{dataset_keys[era]}.csv'
    print(f'resolved_dest_path = {resolved_dest_path}')
    fetch_datagov_csv(dataset_keys[era], resolved_dest_path, max_attempts=5)


if args.mode=='backfill':
    land_hdb_table('1990_1999')
    land_hdb_table('2000_2012Feb')
    land_hdb_table('2012Mar_2014')
    land_hdb_table('2015_2016')

if args.mode=='update':
    land_hdb_table('2017_onwards')

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
