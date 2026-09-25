-- t2 (silver): derived/staged view of t1 - typed columns, one row per transaction.
-- Unpartitioned, unclustered, no UniForm - see providers/databricks.py's own docstring.
CREATE TABLE IF NOT EXISTS macroecons.t2.datagov__resale_flat_prices (
    tx_year              INT,
    tx_monthdate         DATE,
    covid                STRING,
    flat_type            STRING,
    resale_price         DOUBLE,
    age_sold             BIGINT,
    remaining_lease_sold BIGINT,
    pretend_top_2025     BIGINT,
    street_name          STRING,
    storey_range         STRING,
    town                 STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'   = 'name',
    'delta.minReaderVersion'     = '2',
    'delta.minWriterVersion'     = '5'
)
