
CREATE TABLE IF NOT EXISTS macroecons.t1.datagov__resale_flat_prices (
    month               STRING,
    town                STRING,
    flat_type           STRING,
    block               STRING,
    street_name         STRING,
    storey_range        STRING,
    floor_area_sqm      STRING,
    flat_model          STRING,
    lease_commence_date STRING,
    resale_price        STRING,
    tx_monthdate        DATE NOT NULL,
    era                 STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'   = 'name',
    'delta.minReaderVersion'     = '2',
    'delta.minWriterVersion'     = '5'
)
