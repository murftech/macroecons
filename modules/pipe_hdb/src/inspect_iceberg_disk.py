"""Ad-hoc inspection: disk size, partition count, valid-vs-expired file weight
for a known iceberg table. Nothing here is pipeline code - throwaway diagnostic.

Run from the repo root (metadata_location inside the catalog is stored relative
to whatever cwd was at write time, so load_table() only resolves from there):

    cd /Users/murftech/Dropbox/Datarepo/macroecons
    uv run --project modules/pipe_hdb python modules/pipe_hdb/src/inspect_iceberg_disk.py
"""
import os
from pyiceberg.catalog.sql import SqlCatalog

REPO_ROOT = '/Users/murftech/Dropbox/Datarepo/macroecons'
TABLE_DIR = os.path.join(REPO_ROOT, 'datalake/iceberg2/t1/resale_flat_prices')
CATALOG_DB = os.path.join(REPO_ROOT, 'datalake/iceberg2/_icebergcatalog.db')
FQN = 't1.resale_flat_prices'

# ---------- 1. total disk size right now (every byte under the table dir) ----------
total_bytes = 0
file_count = 0
for dirpath, _, filenames in os.walk(TABLE_DIR):
    for f in filenames:
        total_bytes += os.path.getsize(os.path.join(dirpath, f))
        file_count += 1

print(f"[disk] {TABLE_DIR}")
print(f"    total size   : {total_bytes / 1e6:.2f} MB  ({file_count} files, all subfolders incl. metadata/)")

data_bytes = sum(
    os.path.getsize(os.path.join(TABLE_DIR, 'data', f))
    for f in os.listdir(os.path.join(TABLE_DIR, 'data'))
)
print(f"    data/ only   : {data_bytes / 1e6:.2f} MB  (parquet files, live + expired)")

# ---------- 2. how many partitions right now (per the CURRENT snapshot) ----------
catalog = SqlCatalog('macroecons', uri=f'sqlite:///{CATALOG_DB}', warehouse='file://' + os.path.abspath(TABLE_DIR))
tbl = catalog.load_table(FQN)

# every data file entry in the current snapshot carries its own partition struct -
# group by that to get today's live partition count (no directory listing involved,
# since this table is hidden-partitioned - confirmed data/ has no part=value folders)
live_files = list(tbl.scan().plan_files())
partitions = {tuple(f.file.partition) for f in live_files}
print(f"\n[partitions] current snapshot: {len(partitions)} partitions, {len(live_files)} live data files")
for p in sorted(partitions):
    print(f"    {p}")

# ---------- 3. MB the current metadata.json says is valid ----------
# file_path in the manifest is stored relative to wherever the catalog was created
# (cwd was the repo root) - normalize to abspath so set comparisons below actually match.
valid_paths = {os.path.abspath(f.file.file_path.replace('file://', '')) for f in live_files}
valid_bytes = sum(os.path.getsize(p) for p in valid_paths)
print(f"\n[valid]   {valid_bytes / 1e6:.2f} MB  ({len(valid_paths)} files) - referenced by current snapshot")

# ---------- 4. MB sitting in data/ that is NOT referenced (expired / orphaned) ----------
all_data_paths = {os.path.abspath(os.path.join(TABLE_DIR, 'data', f)) for f in os.listdir(os.path.join(TABLE_DIR, 'data'))}
expired_paths = all_data_paths - valid_paths
expired_bytes = sum(os.path.getsize(p) for p in expired_paths)
print(f"[expired] {expired_bytes / 1e6:.2f} MB  ({len(expired_paths)} files) - on disk, not referenced by current snapshot")

print(f"\n[check] valid + expired == data/ total: {valid_bytes + expired_bytes == data_bytes}")
