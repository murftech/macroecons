#!/bin/bash
# pandas mirror of test_all_combinations_bare.sh. the engine x format axes collapse to one
# combination each: pandas is the only engine, parquet the only format.
cd "$(dirname "$0")"


./run_pandas.sh import --eras 1990_1999
./run_pandas.sh stage --startMonth 1990-01 --endMonth 1999-12
# why does this hand?
