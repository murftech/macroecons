import argparse

from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL, add_src_to_path
add_src_to_path('modules/pipe_hdb/src')

from providers.local import add_provider_args, get_lakehouse_root_from_env, CATALOG_NAME

import importlib, helper_pyiceberg_io
importlib.reload(helper_pyiceberg_io)
from helper_pyiceberg_io import (
    sh_provision_iceberg_catalog,
    getOrCreate_catalog,
    createOrEvolve_table,
    custom_describe_catalog,
    double_safe_purge,
    write_partition_guarded,
    sail_read_iceberg,
    schema_to_code
)

# double_safe_purge(catalog, 't1.datagov__resale_flat_prices')

# same --env flag 1_import_to_t1.py exposes via add_provider_args - defaults to the
parser = argparse.ArgumentParser()
add_provider_args(parser)                # local: --env
args = parser.parse_args()

if IS_IPYTHON:
    args.env = 'dev'
    args.env = 'production'


ABSOLUTE_WAREHOUSE_PATH = get_lakehouse_root_from_env(args.env) / 'iceberg' / CATALOG_NAME
print(f'[provision] env={args.env!r} -> {ABSOLUTE_WAREHOUSE_PATH}')

sh_provision_iceberg_catalog(ABSOLUTE_WAREHOUSE_PATH, ['t1', 't2', 't3'])


#### enforcement / evolution of table schemas - restricted to THIS script.
#    any other schema change happens inflight inside write_partition_guarded.

z_catalog = getOrCreate_catalog(ABSOLUTE_WAREHOUSE_PATH)


# #### PROVISION: t1.datagov__resale_flat_prices ####

import pyarrow as pa

z_partition_keys = ['tx_monthdate']
# partition_keys= ['era', 'tx_monthdate'], 
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

import pyarrow as pa

z_partition_keys = ['tx_monthdate']
# partition_keys= ['era', 'tx_monthdate'], 
z_tier = 't2'
z_origin = 'datagov'
z_dataframe_name = 'resale_flat_prices'
z_tbl_name = z_origin + "__" + z_dataframe_name

# pending: to get the printout later during dev.
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



