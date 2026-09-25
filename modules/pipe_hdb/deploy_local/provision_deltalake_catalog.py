import argparse
import sys
from pathlib import Path


"""
It is meant to be declarative, ie ideompotent run, whatever is here should be whatever is the final state for:
t1,t2,t3 and the parititon list and schema of tables
"""


from runtime_env import IS_DATABRICKS, IS_IPYTHON, IS_SH, IS_LOCAL

# runtime_env.add_src_to_path requires the running script itself to sit inside a
# directory literally named 'src' (it walks up from sys.argv[0] looking for one) -
# true for every pipeline/ script, no longer true once this script moved to
# deploy_local/ (a sibling of src/, not a descendant). Same fix as
# unit_tests/conftest.py: walk up from THIS file looking for the module root
# (pyproject.toml + a sibling src/) instead of assuming a fixed relationship.
for _a in Path(__file__).resolve().parents:
    if (_a / 'pyproject.toml').is_file() and (_a / 'src').is_dir():
        sys.path.insert(0, str(_a / 'src')); break
else:
    raise RuntimeError(f"no ancestor with pyproject.toml + src/ found above {__file__}")

from providers.local import add_provider_args, get_lakehouse_root_from_env, CATALOG_NAME

from lakehouse_io.helper_deltalake_io import (
    sh_provision_delta_catalog,
    createOrEvolve_table,
    custom_describe_catalog,
    double_safe_purge,
    schema_to_code
)

parser = argparse.ArgumentParser()
add_provider_args(parser)                # adds >> local: --env
args = parser.parse_args()

# ── INLINE OVERWRITES ──────────────────────────────────────

if IS_IPYTHON:
    args.env = 'dev'
    # args.env = 'production'


#### initiate warehouse and namespaces #####
# no catalog file for delta - the directory layout IS the catalog (see helper_deltalake_io docstring)

ABSOLUTE_WAREHOUSE_PATH = get_lakehouse_root_from_env(args.env) / 'delta' / CATALOG_NAME
print(f'[provision] env={args.env!r} -> {ABSOLUTE_WAREHOUSE_PATH}')

sh_provision_delta_catalog(ABSOLUTE_WAREHOUSE_PATH, ['t1', 't2', 't3'])


#### enforcement / evolution of table schemas - restricted to THIS script.
#    any other schema change happens inflight inside helper_sparkdelta_io.write_partition_guarded.

import pyarrow as pa


# #### PROVISION: t1.datagov__resale_flat_prices ####

# double_safe_purge(ABSOLUTE_WAREHOUSE_PATH, 't1.datagov__resale_flat_prices')

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
    schema_source=       z_schema,
    full_warehouse_path= ABSOLUTE_WAREHOUSE_PATH,
    namespace =          z_tier,
    tbl_name =           z_tbl_name
    )


# #### PROVISION: t2.datagov__resale_flat_prices ####

# double_safe_purge(ABSOLUTE_WAREHOUSE_PATH, 't2.datagov__resale_flat_prices')

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

createOrEvolve_table(
    schema_source=       z_schema,
    full_warehouse_path= ABSOLUTE_WAREHOUSE_PATH,
    namespace =          z_tier,
    tbl_name =           z_tbl_name
    )


custom_describe_catalog(ABSOLUTE_WAREHOUSE_PATH)
