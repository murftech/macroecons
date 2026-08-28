# pipe_hdb_databricks — handoff context

## What this is
A Databricks Asset Bundle (current docs call it "Declarative Automation Bundle" —
same thing, renamed) that ports `modules/pipe_hdb` — an HDB Singapore housing
resale-price ELT pipeline, originally deployed via Docker on AWS ECS Fargate +
EventBridge Scheduler and GCP Cloud Run + Cloud Scheduler — onto Databricks.
Original project lives at `/Users/murftech/Dropbox/Datarepo/macroecons/modules/pipe_hdb`
on the owner's Mac; this bundle sits alongside it as `pipe_hdb_databricks/`.

Owner is a data engineer learning Databricks hands-on through this real project.
They deploy everything themselves via the Databricks CLI — no live credentials
were ever handed to any AI session working on this.

## Current state: WORKING, confirmed end-to-end
The `hdb_pipeline` job has run successfully on Databricks Free Edition (serverless
compute), all 3 tasks green, ~1m47s total. Confirmed via the owner's own UI
screenshots (Catalog Explorer, Jobs run graph, Lineage panel).

- Writes `macroecons.dev_murftech7_macroecons_hdb.hdb_silver` (managed Delta table)
- Writes 3 Plotly HTML reports to the `reports` Unity Catalog volume
  (dated subfolder + a `latest/` copy)
- Target is `dev` (`mode: development`), which is why the schema name above got
  auto-prefixed with `dev_murftech7_` — expected behavior, not a bug
- Schedule exists (`quartz_cron_expression: "0 0 1 * * ?"`, Asia/Singapore) but is
  **still `pause_status: PAUSED`** — the job has only ever been triggered manually
  via `bundle run`, the daily 1am schedule has never actually fired

## File inventory
```
databricks.yml                    bundle root: name, engine: direct, variables, dev target
resources/unity_catalog.yml       schema + volume definitions (catalog itself NOT
                                   created by the bundle — see gotcha #1 below)
resources/hdb_pipeline_job.yml    the job: 3 tasks, schedule, serverless environment spec
src/01_ingest_bronze.py           polars fetch/clean -> Spark bridge -> Delta table write
src/02_report_plotly.py           Spark read -> polars -> 3 unchanged Plotly charts -> HTML
src/03_publish_outputs.py         verifies volume writes, optional GCS/S3 mirror (unused)
src/helper_hdb.py                 unchanged from original, pure polars/requests
src/helper_transform_for_plotly.py unchanged from original, pure polars
deploy_databricks.sh              validate/deploy/run/all/status/destroy wrapper,
                                   modeled on the original deploy_aws.sh
README.md                         full deploy walkthrough
```

## Gotchas already solved — do not rediscover these
1. **Free Edition can't create catalogs via the bundle.** `resources.catalogs` was
   removed from `unity_catalog.yml` entirely — Free Edition's Default Storage has no
   exposed root URL, and the catalogs REST API rejects creation without an explicit
   `MANAGED LOCATION` (known limitation, databricks/cli#4513, closed "not planned").
   **The `macroecons` catalog must exist already, created by hand once via Catalog
   Explorer → Create Catalog → Default Storage**, before any `bundle deploy`.
2. **`client: "1"` fails on fresh Free Edition workspaces** ("Invalid platform channel
   Client-1") — it's Databricks' oldest serverless environment version, no longer
   supported. Fixed to `client: "4"` (current stable as of writing; "5" exists but
   was still Beta).
3. **`spark_python_task` has no `__file__`.** Its internal `exec()` wrapper never
   defines it, so scripts can't self-locate sibling helper modules the normal way.
   Fixed by adding a `--src-dir` argparse param, passed by the job YAML as
   `${workspace.root_path}/files/src`, with local imports moved inside `main()`
   after `sys.path.append(args.src_dir)`.
4. **`mode: development` silently prefixes resource names.** Task parameters must
   reference `${resources.schemas.hdb_schema.name}` / `${resources.volumes.hdb_reports_volume.name}`,
   NOT the raw `${var.schema}` / `${var.volume_name}` — otherwise tasks look for
   `macroecons.macroecons_hdb` (`[SCHEMA_NOT_FOUND]`) instead of the actual deployed
   `dev_murftech7_macroecons_hdb`.

## Deliberately deferred ("later" list) — owner's explicit choice, not forgotten
- **Connect owner's own AWS account** for classic (non-serverless) compute. Real
  cost implications discussed (~$50-150+/month). Needed for job clusters, BYO-S3,
  or anything beyond Free Edition's serverless-only ceiling.
- **BYO-S3 / customer-managed storage** for Unity Catalog, instead of Databricks'
  Default Storage. Relevant because Free Edition's Acceptable Use Policy explicitly
  prohibits storing financial account numbers, SSN/government IDs, health data, etc.
  — a contractual restriction independent of storage location, so BYO-S3 alone
  would NOT make Free Edition suitable for real financial/PII data; a paid plan
  would still be required for that. Owner has AWS already; had the Databricks
  "Create a new credential" dialog open at last check but hadn't yet created the
  IAM role on the AWS side.
- **`pyproject.toml` + `uv.lock`** instead of the current inline YAML dependency
  list in `hdb_pipeline_job.yml`'s `environments.spec.dependencies`. Agreed as the
  more current/adopted pattern generally (not Databricks-specific), just not done yet.

## Not yet done, no explicit ask yet either
- Flip `pause_status: PAUSED` → `UNPAUSED` in `hdb_pipeline_job.yml`, redeploy —
  activates the real unattended daily 1am SGT run. Purely a one-line edit + deploy,
  intentionally left as a deliberate manual step rather than scripted.

## Deploy workflow
```
./deploy_databricks.sh validate   # local structural check, no deploy
./deploy_databricks.sh deploy     # push bundle -> creates/updates schema+volume+job
./deploy_databricks.sh run        # manually trigger hdb_pipeline once
./deploy_databricks.sh all        # validate + deploy + run in sequence
./deploy_databricks.sh status     # print workspace links to deployed resources
./deploy_databricks.sh destroy    # tear down (CLI prompts to confirm)
```
Prereq: `databricks auth login --host <workspace-url>` once, already done.

## Design decisions worth preserving
- Kept the exact 3 Plotly HTML charts as-is (owner's explicit choice over switching
  to native Databricks AI/BI dashboards) — `02_report_plotly.py`'s chart-building
  code is untouched from the original `2_report_firstbq.py`.
- Pipeline is deliberately polars-only for the actual data wrangling; PySpark is
  used strictly as the write/read bridge at the Unity Catalog boundary
  (`spark.createDataFrame(...).write.format('delta')...` /
  `spark.table(...).toPandas()`), not adopted throughout.
