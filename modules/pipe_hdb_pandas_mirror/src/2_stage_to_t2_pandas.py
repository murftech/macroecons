###### STAGE  ######
from pathlib import Path
import sys
import argparse
from datetime import date

import numpy as np
import pandas as pd

from runtime_env import IS_IPYTHON, add_src_to_path
add_src_to_path('modules/pipe_hdb_pandas_mirror/src')

# ── THE PROVIDER — local only, pandas + pyarrow ─────────────────────────────
from providers.local_pandas import add_provider_args, read_tier, dispatch_write

from helper_transit import pandas_sortcount


# ── RUNTIME TOGGLES ───────────────────────────────────────────────────────────

thisMonth = date.today().strftime('%Y-%m')
print(f'this month is: {thisMonth}')

print('show sys.argv')
print(sys.argv)
parser = argparse.ArgumentParser()
parser.add_argument('--startMonth', default=thisMonth)
parser.add_argument('--endMonth',   default=thisMonth)
add_provider_args(parser)                # local: --env
args = parser.parse_args()

# ── INLINE OVERWRITES (laptop dev only) ──────────────────────────────────────
if IS_IPYTHON:
    args.startMonth = '2022-09'
    args.endMonth = '2022-10'
    # print(args)


print(args)


# ── DATALAKE LAYOUT ───────────────────────────────────────────────────────────
ORIGIN, DATASET = 'datagov', 'resale_flat_prices'
SOURCE_TIER, TIER = 't1', 't2'


############### READ t1 + WINDOW ###############
# t1 is keyed by `tx_monthdate` (a DATE, always the 1st of the month). because every
# value is day-01, an inclusive between on {month}-01 bounds is exact - the end
# month's own value is {endMonth}-01, which the upper bound includes.
lo = date.fromisoformat(f'{args.startMonth}-01')
hi = date.fromisoformat(f'{args.endMonth}-01')

t1_src = read_tier(args, tier=SOURCE_TIER, origin=ORIGIN, dataset=DATASET)

# pyarrow reads the hive partition back as string; force DATE first so the window compares dates, not text.
# .dt.date is REQUIRED on the way out - date32 keeps the t2 folders as tx_monthdate=YYYY-MM-DD (see pandas_add_monthdate).
t1_src['tx_monthdate'] = pd.to_datetime(t1_src['tx_monthdate'], format='%Y-%m-%d').dt.date
t1 = t1_src[t1_src['tx_monthdate'].between(lo, hi)].copy()

n_in = len(t1)
print(f'[read] {n_in:,} rows from t1 in {args.startMonth}..{args.endMonth}')
if n_in == 0:
    raise SystemExit(f'no rows in {args.startMonth}..{args.endMonth}')


############### CAST TYPE ###############
# t1 keeps every column string; casting to real types is silver's job.
# pd.to_numeric raises on a bad value, like spark 4 ANSI cast.
typed = t1.assign(
    floor_area_sqm      = pd.to_numeric(t1['floor_area_sqm']).astype('float64'),
    resale_price        = pd.to_numeric(t1['resale_price']).astype('float64'),
    lease_commence_date = pd.to_numeric(t1['lease_commence_date']).astype('int64'),
)


############### DERIVE NEW COLUMNS ###############
# tx_year is int32 on purpose: spark's year() returns int, and guard 5 checks it against t2 on disk.
# the rest land int64 because int32 - int64 promotes, same as spark's int - long.
derived = typed.assign(
    tx_year = pd.to_datetime(typed['tx_monthdate']).dt.year.astype('int32'),
)
derived = derived.assign(
    covid                = np.where(derived['tx_year'] >= 2021, 'after 2021', 'before 2021'),
    age_sold             = derived['tx_year'] - derived['lease_commence_date'],
    remaining_lease_sold = 99 - (derived['tx_year'] - derived['lease_commence_date']),
    pretend_top_2025     = 2025 - (derived['tx_year'] - derived['lease_commence_date']),
)
derived.info()

# pandas_sortcount(derived, 'town')
# pandas_sortcount(derived, 'flat_type')
# pandas_sortcount(derived, 'tx_year')
# pandas_sortcount(derived, ['town', 'street_name'], 100)

############### SELECT ###############
t2 = (
    derived[[
        'tx_year', 'tx_monthdate', 'covid', 'flat_type', 'resale_price',
        'age_sold', 'remaining_lease_sold', 'pretend_top_2025',
        'street_name', 'storey_range', 'town',
    ]]
    .sort_values('tx_monthdate', ascending=False, ignore_index=True)
)
t2.info()


############### WRITE ###############
# delete_matching replaces exactly the tx_monthdate partitions present in this run's t2 slice.
dispatch_write(
    t2, tier=TIER, origin=ORIGIN, dataset=DATASET,
    partition_keys=['tx_monthdate'], args=args)


# ── STATUS  ───────────────────────────────────────────────────────────────
print('=' * 60)
print(f'  ran: startMonth={args.startMonth}  endMonth={args.endMonth}  engine=pandas  format=parquet')
print('=' * 60)

print('RUNTIME SUMMARY: runned with settings [sys.argv]:')
# argv: argument vector
print(args)
