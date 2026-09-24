#!/bin/bash
cd "$(dirname "$0")"

./run.sh import --spark_engine sail --write_format parquet --eras 1990_1999
./run.sh import --spark_engine sail --write_format iceberg --eras 1990_1999
./run.sh import --spark_engine sail --write_format parquet,iceberg --eras 1990_1999
./run.sh import --spark_engine java --write_format parquet --eras 1990_1999
./run.sh import --spark_engine java --write_format iceberg --eras 1990_1999
./run.sh import --spark_engine java --write_format parquet,iceberg --eras 1990_1999

./run.sh stage --spark_engine sail --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
./run.sh stage --spark_engine sail --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
./run.sh stage --spark_engine sail --write_format parquet,iceberg --startMonth 1990-01 --endMonth 1999-12
./run.sh stage --spark_engine java --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
./run.sh stage --spark_engine java --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
./run.sh stage --spark_engine java --write_format parquet,iceberg --startMonth 1990-01 --endMonth 1999-12



# modules/pipe_hdb/run.sh import --spark_engine sail --write_format parquet --eras 1990_1999
# modules/pipe_hdb/run.sh import --spark_engine sail --write_format iceberg --eras 1990_1999


# modules/pipe_hdb/run.sh import --spark_engine java --write_format parquet --eras 1990_1999
# modules/pipe_hdb/run.sh import --spark_engine java --write_format iceberg --eras 1990_1999
modules/pipe_hdb/run.sh import --spark_engine java --write_format delta --eras 1990_1999

# modules/pipe_hdb/run.sh stage --spark_engine sail --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
# modules/pipe_hdb/run.sh stage --spark_engine sail --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12

# modules/pipe_hdb/run.sh stage --spark_engine java --write_format parquet         --startMonth 1990-01 --endMonth 1999-12
# modules/pipe_hdb/run.sh stage --spark_engine java --write_format iceberg         --startMonth 1990-01 --endMonth 1999-12
# modules/pipe_hdb/run.sh stage --spark_engine java --write_format delta           --startMonth 1990-01 --endMonth 1999-12

