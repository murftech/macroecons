#!/bin/bash
# cd "$(dirname "$0")"

# ./run.sh import --spark_engine sail --write_format parquet --eras 1990_1999
# ./run.sh import --spark_engine sail --write_format iceberg --eras 1990_1999
# ./run.sh import --spark_engine sail --write_format parquet,iceberg --eras 1990_1999
# ./run.sh import --spark_engine java --write_format parquet --eras 1990_1999
# ./run.sh import --spark_engine java --write_format iceberg --eras 1990_1999
# ./run.sh import --spark_engine java --write_format parquet,iceberg --eras 1990_1999

# ./run.sh stage --spark_engine sail --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
# ./run.sh stage --spark_engine sail --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
# ./run.sh stage --spark_engine sail --write_format parquet,iceberg --startMonth 1990-01 --endMonth 1999-12
# ./run.sh stage --spark_engine java --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
# ./run.sh stage --spark_engine java --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
# ./run.sh stage --spark_engine java --write_format parquet,iceberg --startMonth 1990-01 --endMonth 1999-12


modules/pipe_hdb/deploy_local/run.sh import --spark_engine sail --write_format parquet --eras 1990_1999
# RUN
# Summaries:
# [hive] replacing 120 leaf partitions on tx_monthdate:
#     tx_monthdate    120 values   1990-01-01 … 1999-12-01
# RUN: using pyarrow.dataset, ds.write_dataset, into path: /Users/murftech/Root/MasterETL/dev/lakehouse/hive/macroecons/t1/datagov__resale_flat_prices
# DONE:  parquet -> /Users/murftech/Root/MasterETL/dev/lakehouse/hive/macroecons/t1/datagov__resale_flat_prices  (delete_matching)


modules/pipe_hdb/deploy_local/run.sh import --spark_engine sail --write_format iceberg --eras 1990_1999
# RUN
# Summaries:
# [iceberg] replacing 120 leaf partitions on tx_monthdate:
#     tx_monthdate    120 values   1990-01-01 … 1999-12-01
# RUN: using pyiceberg.catalog.sql, tbl.dynamic_partition_overwrite, into path: file:///Users/murftech/Root/MasterETL/dev/lakehouse/iceberg/macroecons/t1/datagov__resale_flat_prices
# DONE:  iceberg -> t1.datagov__resale_flat_prices  (dynamic_partition_overwrite)


modules/pipe_hdb/deploy_local/run.sh import --spark_engine java --write_format parquet --eras 1990_1999
# RUN
# Summaries:
# [hive] replacing 120 leaf partitions on tx_monthdate:
#     tx_monthdate    120 values   1990-01-01 … 1999-12-01
# RUN: using pyarrow.dataset, ds.write_dataset, into path: /Users/murftech/Root/MasterETL/dev/lakehouse/hive/macroecons/t1/datagov__resale_flat_prices
# DONE:  parquet -> /Users/murftech/Root/MasterETL/dev/lakehouse/hive/macroecons/t1/datagov__resale_flat_prices  (delete_matching)

modules/pipe_hdb/deploy_local/run.sh import --spark_engine java --write_format iceberg --eras 1990_1999
# RUN
# Summaries:
# [iceberg] replacing 120 leaf partitions on tx_monthdate:
#     tx_monthdate    120 values   1990-01-01 … 1999-12-01
# RUN: using pyspark DataFrameWriterV2, df.writeTo.overwritePartitions, into table: icebergcatalog.t1.datagov__resale_flat_prices
# DONE:  iceberg -> icebergcatalog.t1.datagov__resale_flat_prices  (overwritePartitions)
# what will it look like in AWS?

modules/pipe_hdb/deploy_local/run.sh import --spark_engine java --write_format delta --eras 1990_1999
# RUN
# Summaries:
# [delta] incoming batch: 120 distinct tx_monthdate values 1990-01-01 … 1999-12-01
# [delta] IN-overwrite on tx_monthdate - 120 distinct value(s) actually present in this batch
# RUN: using pyspark DataFrameWriterV2, df.writeTo.overwrite, into table: delta.`/Users/murftech/Root/MasterETL/dev/lakehouse/delta/macroecons/t1/datagov__resale_flat_prices`
# 26/09/25 10:23:51 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
# DONE:  delta -> delta.`/Users/murftech/Root/MasterETL/dev/lakehouse/delta/macroecons/t1/datagov__resale_flat_prices`  (overwrite, IN-list)
# what will it look like in databricks
# showpartitions what will it look like All same

modules/pipe_hdb/deploy_local/run.sh stage --spark_engine sail --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
modules/pipe_hdb/deploy_local/run.sh stage --spark_engine sail --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12

modules/pipe_hdb/deploy_local/run.sh stage --spark_engine java --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
modules/pipe_hdb/deploy_local/run.sh stage --spark_engine java --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
modules/pipe_hdb/deploy_local/run.sh stage --spark_engine java --write_format delta           --startMonth 1990-01 --endMonth 1999-12


