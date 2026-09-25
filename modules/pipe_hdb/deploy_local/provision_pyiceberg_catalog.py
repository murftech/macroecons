import argparse
import sys
from pathlib import Path

"""
It is meant to be declarative, ie ideompotent run, whatever is here should be whatever is the final state for:
t1,t2,t3 and the parititon list and schema of tables
"""

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL

# ── 1. SETUP SYS.PATH FOR IMPORTS ──────────────────────────────────────────────
# Dynamically locate project root containing pyproject.toml + src/
# Falls back safely when running in IPython/REPL where __file__ doesn't exist
if IS_SH:
    start_path = Path(__file__).resolve()
if IS_IPYTHON:
    start_path = Path.cwd().resolve()

for _a in [start_path] + list(start_path.parents):
    if (_a / 'pyproject.toml').is_file() and (_a / 'src').is_dir():
        sys.path.insert(0, str(_a / 'src'))
        break
    # Optional fallback check for your explicit module path
    elif (_a / 'modules/pipe_hdb/src').is_dir():
        sys.path.insert(0, str(_a / 'modules/pipe_hdb/src'))
        break
    
# for _a in Path(__file__).resolve().parents:
#     if (_a / 'pyproject.toml').is_file() and (_a / 'src').is_dir():
#         sys.path.insert(0, str(_a / 'src')); break
# else:
#     raise RuntimeError(f"no ancestor with pyproject.toml + src/ found above {__file__}")


from providers.local import add_provider_args, get_lakehouse_root_from_env, CATALOG_NAME

from lakehouse_io.helper_pyiceberg_io import (
    sh_provision_iceberg_catalog,
    getOrCreate_catalog,
    createOrEvolve_table,
    custom_describe_catalog,
    double_safe_purge,
    write_partition_guarded,
    sail_read_iceberg,
    schema_to_code
)

parser = argparse.ArgumentParser()
add_provider_args(parser)                # adds >> local: --env
args = parser.parse_args()

# ── INLINE OVERWRITES ──────────────────────────────────────

if IS_IPYTHON:
    args.env = 'dev'
    # args.env = 'production'


#### initiate catalog and namspaces #####


ABSOLUTE_WAREHOUSE_PATH = get_lakehouse_root_from_env(args.env) / 'iceberg' / CATALOG_NAME
print(f'[provision] env={args.env!r} -> {ABSOLUTE_WAREHOUSE_PATH}')

sh_provision_iceberg_catalog(ABSOLUTE_WAREHOUSE_PATH, ['t1', 't2', 't3'])


#### enforcement / evolution of table schemas - restricted to THIS script.
#    any other schema change happens inflight inside write_partition_guarded.

z_catalog = getOrCreate_catalog(ABSOLUTE_WAREHOUSE_PATH)


# #### PROVISION: t1.datagov__resale_flat_prices ####

# use these 
# double_safe_purge(z_catalog, 't1.datagov__resale_flat_prices')

# if files were delete out from folder without iceberg functions. everything will be out of sync
# sub what if i only deleted say one month data... How to like registers... etc. 
# z_catalog.drop_table('t1.datagov__resale_flat_prices')

import pyarrow as pa

z_partition_keys = ['tx_monthdate']
# z_partition_keys= ['era', 'tx_monthdate']
# z_tier = 't1'
z_tier = 't1'
z_origin = 'datagov'
z_dataframe_name = 'resale_flat_prices'
z_tbl_name = z_origin + "__" + z_dataframe_name

z_schema = pa.schema([
        pa.field('month', pa.string(), nullable=True),
        pa.field('town', pa.string(), nullable=True),
        pa.field('flat_type', pa.string(), nullable=True),
        pa.field('block', pa.string(), nullable=True),
        pa.field('street_name', pa.string(), nullable=True),
        pa.field('storey_range', pa.string(), nullable=True),
        pa.field('floor_area_sqm', pa.string(), nullable=True),
        pa.field('flat_model', pa.string(), nullable=True),
        pa.field('lease_commence_date', pa.string(), nullable=True),
        pa.field('resale_price', pa.string(), nullable=True),
        pa.field('tx_monthdate', pa.date32(), nullable=False),
        pa.field('era', pa.string(), nullable=False),
    ])

createOrEvolve_table(
    schema_source=  z_schema, 
    partition_keys= z_partition_keys,
    catalog =       z_catalog, 
    namespace =     z_tier, 
    tbl_name =      z_tbl_name
    )

# #### PROVISION: t2.datagov__resale_flat_prices ####

# double_safe_purge(z_catalog, 't2.datagov__resale_flat_prices')


import pyarrow as pa

z_partition_keys = ['tx_monthdate']
# partition_keys= ['era', 'tx_monthdate'], 
z_tier = 't2'
z_origin = 'datagov'
z_dataframe_name = 'resale_flat_prices'
z_tbl_name = z_origin + "__" + z_dataframe_name


# print(schema_to_code(t2.toArrow().schema))

z_schema = pa.schema([
    pa.field('tx_year', pa.int32(), nullable=True),
    pa.field('tx_monthdate', pa.date32(), nullable=True),
    pa.field('covid', pa.string(), nullable=True),
    pa.field('flat_type', pa.string(), nullable=True),
    pa.field('resale_price', pa.float64(), nullable=True),
    pa.field('age_sold', pa.int64(), nullable=True),
    pa.field('remaining_lease_sold', pa.int64(), nullable=True),
    pa.field('pretend_top_2025', pa.int64(), nullable=True),
    pa.field('street_name', pa.string(), nullable=True),
    pa.field('storey_range', pa.string(), nullable=True),
    pa.field('town', pa.string(), nullable=True),
])


# testers
# schema_source=  z_schema
# partition_keys= z_partition_keys
# catalog =       z_catalog
# namespace =     z_tier
# tbl_name =      z_tbl_name


createOrEvolve_table(
    schema_source=  z_schema, 
    partition_keys= z_partition_keys,
    catalog =       z_catalog, 
    namespace =     z_tier, 
    tbl_name =      z_tbl_name
    )



