

ROOT   := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
$(info ROOT = $(ROOT))

MODULE := modules/pipe_hdb_pandas_mirror
RUN    := cd $(ROOT) && $(MODULE)/run_pandas.sh
$(info RUN = $(RUN))

# to run in production:
# make land ENV=production

.DEFAULT_GOAL := help

.PHONY: setup sync clean clean-t1 clean-t2 land backfill import import-full stage stage-full run fresh help



# ── prepare ─────────────────────────────────────────────────────────────────


sync:                 ## install / refresh the module venv
	cd $(ROOT) && uv sync --project $(MODULE)

setup: sync           ## first-time prep (the writers create their own dirs)

clean:                ## delete ALL generated outputs — t1, t2, t3 (keeps landed CSVs)
	cd $(ROOT) && rm -rf datalake/hive

clean-t1:             ## wipe just t1
	cd $(ROOT) && rm -rf datalake/hive/t1

clean-t2:             ## wipe just t2
	cd $(ROOT) && rm -rf datalake/hive/t2

# ── run buttons — each is ONE fixed command ────────────────────────────────

## will go into help

land:                 ## fetch the current dataset (2017 onwards) - runs pipe_hdb's 0_land_csv.py
	$(RUN) land --mode update

backfill:             ## fetch the historical archives (1990-2016), one-time
	$(RUN) land --mode backfill

import:               ## csv -> t1 (bronze), default era 2017_onwards, hive parquet (pandas)
	$(RUN) import

import-full:          ## csv -> t1 for every era (1990 onwards)
	$(RUN) import --eras all

stage:               ## t1 -> t2 (silver), current month only, hive parquet (pandas)
	$(RUN) stage

stage-full:          ## t1 -> t2 for every month (1990-01 onwards)
	$(RUN) stage --startMonth 1990-01

run: land import stage           ## routine run: land + import + stage

fresh: clean backfill import-full stage-full   ## wipe outputs, then a full run

help:                 ## this list
	@grep -hE '^[a-z][a-z0-9-]*:.*##' $(MAKEFILE_LIST) | sed -E 's/:.*## / - /'
