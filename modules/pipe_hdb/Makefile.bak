
ROOT   := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
$(info ROOT = $(ROOT))

MODULE := modules/pipe_hdb
RUN    := cd $(ROOT) && $(MODULE)/run.sh
$(info RUN = $(RUN))

.DEFAULT_GOAL := help

.PHONY: setup sync clean clean-t1 clean-t2 land backfill import import-full import-java stage stage-full stage-java report run fresh help



# ── prepare ─────────────────────────────────────────────────────────────────


sync:                 ## install / refresh the module venv (incl. iceberg group)
	cd $(ROOT) && uv sync --project $(MODULE) --group iceberg

setup: sync           ## first-time prep (the writers create their own dirs)

clean:                ## delete ALL generated outputs — t1, t2, t3, iceberg catalog (keeps landed CSVs)
	cd $(ROOT) && rm -rf datalake/hive datalake/iceberg datalake/delta

clean-t1:             ## wipe just t1 (next run's self-heal fixes the iceberg catalog row)
	cd $(ROOT) && rm -rf datalake/hive/t1 datalake/iceberg/t1

clean-t2:             ## wipe just t2
	cd $(ROOT) && rm -rf datalake/hive/t2 datalake/iceberg/t2

# ── run buttons — each is ONE fixed command ────────────────────────────────

## will go into help

land:                 ## fetch the current dataset (2017 onwards)
	$(RUN) land --mode update

backfill:             ## fetch the historical archives (1990-2016), one-time
	$(RUN) land --mode backfill

import:               ## csv -> t1 (bronze), default era 2017_onwards, hive + iceberg (sail)
	$(RUN) import

import-full:          ## csv -> t1 for every era (1990 onwards)
	$(RUN) import --eras all

import-java:          ## same as import, on the JVM spark engine
	$(RUN) import --spark_engine java

stage:               ## t1 -> t2 (silver), current month only, hive + iceberg (sail)
	$(RUN) stage

stage-full:          ## t1 -> t2 for every month (1990-01 onwards)
	$(RUN) stage --startMonth 1990-01

stage-java:          ## same as stage, on the JVM spark engine
	$(RUN) stage --spark_engine java

report:               ## build the plotly report
	$(RUN) report

run: land import stage           ## routine run: land + import + stage

fresh: clean backfill import-full stage-full   ## wipe outputs, then a full run

help:                 ## this list
	@grep -hE '^[a-z][a-z0-9-]*:.*##' $(MAKEFILE_LIST) | sed -E 's/:.*## / - /'
