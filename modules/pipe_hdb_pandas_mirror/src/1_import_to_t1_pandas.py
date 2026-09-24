###### IMPORT  ######
from pathlib import Path
import sys
import argparse

import pandas as pd

from runtime_env import IS_IPYTHON, add_src_to_path
add_src_to_path('modules/pipe_hdb_pandas_mirror/src')

# ── THE PROVIDER — local only, pandas + pyarrow ─────────────────────────────
from providers.local_pandas import add_provider_args, get_landing_dir, dispatch_write


# ── HANDWRITTEN CONTRACT ─────────────────────────────────────────────────────────
ERA_CONTRACTS = {
    '1990_1999':    {'id': 'd_ebc5ab87086db484f88045b47411ebc5', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2000_2012Feb': {'id': 'd_43f493c6c50d54243cc1eab0df142d6a', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2012Mar_2014': {'id': 'd_2d5ff9ea31397b66239f245f57751537', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2015_2016':    {'id': 'd_ea9ed51da2787afaf8e51f827c304208', 'month_col': 'month', 'src_format': 'yyyy-mm'},
    '2017_onwards': {'id': 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc', 'month_col': 'month', 'src_format': 'yyyy-mm'},
}


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────

print('show sys.argv')
print(sys.argv)
parser = argparse.ArgumentParser()
parser.add_argument('--eras', default='2017_onwards',
                    help="comma-separated era keys or 'all'. currently available eras: " + ','.join(ERA_CONTRACTS))

add_provider_args(parser)                # local: --env


print('\n\n')
args = parser.parse_args()

print(f'resolved args: {args}')


# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    args.eras = '1990_1999'
    # args.eras = 'all'
    print(f'overwritten final args: {args}')



print('\n\n\n ######## BEGIN LOGIC ###########')

# ── VALIDATE ERA PARAMETER ─────────────────────────────────────────────────────────

if args.eras == 'all':
    eras = list(ERA_CONTRACTS)
else:
    eras = args.eras.split(',')

print('era python list to be parsed:')
print(eras)

unknown = [e for e in eras if e not in ERA_CONTRACTS]

if unknown:
    raise SystemExit(f"unknown era(s): {unknown}. keys: {', '.join(ERA_CONTRACTS)}")


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────────────

ORIGIN, DATASET, TIER = 'datagov', 'resale_flat_prices', 't1'

# SOURCE: where the landed CSVs are read from
LANDING_DIR = get_landing_dir(args, ORIGIN, DATASET)
print(f'[read] landing dir : {LANDING_DIR}')


# ── REQUIRED FUNCTIONS ─────────────────────────────────────────────
COMPUTED_PARTITION = 'tx_monthdate'     # the derived partition column

from helper_transit import pandas_add_monthdate

def build_csv_path(era):
    # a tiny helper that builds the path to one landed CSV file, given an era key.
    return Path(LANDING_DIR) / f'{era}_{DATASET}_{ERA_CONTRACTS[era]["id"]}.csv'

############### RUN ###############

era_dfs = {}
for era in eras:
    contract = ERA_CONTRACTS[era]                      # {id, month_col, src_format} for this file
    csv = build_csv_path(era)
    if not csv.exists():
        print(f'[skip] {era}: not landed - {csv}')
        continue

    # dtype=str: source columns stay string (spark's inferSchema=False).
    # keep_default_na=False + na_values=['']: only an empty cell is null, like spark's csv reader -
    # pandas' default would also null out literal 'NA', 'null', 'N/A', 'nan', ...
    df = pd.read_csv(csv, dtype=str, keep_default_na=False, na_values=[''])
    df.info()
    print('sample pandas dataframe right after read_csv')
    print(df.head(3))
    df = pandas_add_monthdate(df, contract['month_col'], contract['src_format'], COMPUTED_PARTITION)
    df['era'] = era   # pretend factor column - known, fixed, 5-value set (ERA_CONTRACTS keys)
    n = len(df)
    if n == 0:
        print(f'[skip] {era}: 0 rows')
        continue
    print(f'[{era}] {n:,} rows')
    era_dfs[era] = df

if not era_dfs:
    raise SystemExit('nothing written - every requested era was missing or empty')


# ── real production write - on_newcols/on_missingcols default to ('evolve', 'pad_null')
#    inside dispatch_write - T1's own stated priority is never losing data. ──

dispatch_write(era_dfs, tier=TIER, origin=ORIGIN, dataset=DATASET,
           partition_keys=['era', COMPUTED_PARTITION], show_partitions=False, args=args)


# ── EXIT ─────────────────────────────────────────────

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
