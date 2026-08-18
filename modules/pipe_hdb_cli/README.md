# pipe_hdb on Databricks — the imperative version

The same three-task HDB pipeline as `../pipe_hdb_databricks`, provisioned with plain
`databricks` CLI calls instead of an Asset Bundle. Both can be deployed at once — this
module uses schema `macroecons_hdb_cli` and job `hdb-pipeline-daily-cli`, so nothing
collides.

The python is **not duplicated**. `SRC_LOCAL` points at `../pipe_hdb_databricks/src`.
There is no "bundle root" constraining where source may live.

## Run it

```bash
./deploy_cli.sh bootstrap   # one-time: catalog + schema + volume
./deploy_cli.sh deploy      # recurring: upload src, create/overwrite the job
./deploy_cli.sh run
./deploy_cli.sh status
./deploy_cli.sh destroy
```

`bootstrap`'s catalog step will report a failure on Free Edition — the same
`Metastore storage root URL does not exist` limitation documented in the bundle's
`resources/unity_catalog.yml`. Create the catalog by hand once in Catalog Explorer; after
that the step reports "already exists". It is left in the script deliberately so the
sequence is documented even where one link is manual.

## What needs JSON, and what doesn't

Only `configs/job.json` — the task graph. `databricks jobs create` has no flags for
"three tasks with dependencies and a shared serverless environment", so that body has to
be JSON.

Everything else is a flag-based CLI call with no JSON at all:

| Step | Command |
|---|---|
| catalog | `databricks api post /api/2.0/sql/statements` — `catalogs create` is rejected on Free Edition; SQL works |
| schema | `databricks schemas create NAME CATALOG` |
| volume | `databricks volumes create CATALOG SCHEMA NAME MANAGED` |
| upload | `databricks sync --full SRC DST` |
| trigger | `databricks jobs run-now JOB_ID` |
| teardown | `databricks jobs delete` / `volumes delete` / `schemas delete` |

`sync` rather than `workspace import-dir` on purpose: `import-dir` treats `.py` files as
notebooks and strips their extensions, which breaks `spark_python_task`. `sync` is the
same file-transfer primitive the bundle uses for its own `files/` directory.

## Side by side

| | `pipe_hdb_databricks` (DAB) | `pipe_hdb_cli` (this) |
|---|---|---|
| Deploy | `bundle deploy` | `./deploy_cli.sh deploy` |
| Dry run | `bundle plan` → *"0 to add, 1 to change, 2 unchanged"* | **none** — read the script |
| Idempotency | state diff skips unchanged resources | `run_idempotent` swallows "already exists" per resource |
| Identity | server ids in `.databricks/.../resources.json` | `lookup_job_id` searches **by name** |
| Paths | `TranslatePaths` mutator | `workspace_src_path` + `sed` |
| Ordering | derived from `${resources.*}` references | hand-sequenced in `main()` |
| Deletions | detected via state snapshot | **not detected** |
| dev/prod | `targets:` + `mode: development` | none — would need parameterising |
| Config | ~90 lines YAML across 3 files | ~200 lines bash + 60 lines JSON |

## The point

Line 1 of that table is a wash. Everything below it is what the bundle was doing for you.

The one that bites hardest in practice is **identity**. With no state, "the job I made last
time" can only be found by searching for its name — so two jobs sharing a name makes the
lookup ambiguous, and nothing prevents that. `resources.json` exists precisely to make that
question unambiguous.

The one that bites *quietest* is **deletions**. Remove a task from `configs/job.json` and
`reset` will drop it, because reset overwrites all settings. But remove a whole resource —
stop creating the volume, say — and nothing anywhere notices. It sits in the workspace
forever, and `destroy` only removes what this script happens to hardcode.

Compare `../pipe_hdb/deploy_aws.sh` (5,710 bytes) against the bundle it replaced. Same
trade, one cloud over.
