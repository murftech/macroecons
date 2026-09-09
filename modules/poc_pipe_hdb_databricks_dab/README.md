# pipe_hdb on Databricks

This is `modules/pipe_hdb` re-provisioned as a **Databricks Asset Bundle** (DAB) —
Databricks' "infrastructure as code" format. One `databricks bundle deploy` creates
everything; nothing is clicked together by hand in the UI.

## What a "Databricks workspace" is, in one paragraph

A workspace is your account's actual Databricks environment — the thing you log
into, where notebooks, jobs, and data catalogs live, similar to how an AWS account
or a GCP project is the "place" your ECS/Cloud Run resources lived before. You
don't have one yet, so step 1 below is creating one; it's free and takes about two
minutes.

## What changed vs. the original pipeline

| Original (AWS/GCP) | Databricks equivalent | Why |
|---|---|---|
| `hive/t2/datagovhdb` parquet file | `catalog.schema.hdb_silver` Delta table (Unity Catalog) | Real governance/ACLs/lineage instead of a folder-naming convention |
| `output/*.html` or your OneDrive path | Unity Catalog **Volume** (`/Volumes/catalog/schema/reports/...`) | A "folder" that isn't tied to one machine, browsable in the workspace UI |
| Dockerfile + `deploy_docker.sh` | *(nothing — deleted)* | Databricks jobs run your `.py` files directly on managed compute, no image to build |
| `deploy_aws.sh` / `deploy_gcp.sh` + IAM JSON configs | *(nothing — deleted)* | No ECS task definitions or Cloud Build to hand-maintain |
| `schedules.sh` + EventBridge/Cloud Scheduler | `schedule:` block inside `resources/hdb_pipeline_job.yml` | The job carries its own schedule |
| `run_pipeline.py` subprocess orchestration | 3 chained tasks in one Lakeflow Job | Real dependency graph + retry/alerting instead of `subprocess.run` |
| `helper_hdb.py`, `helper_transform_for_plotly.py` | **unchanged, copied as-is** | Pure polars, no filesystem/cloud dependency — nothing about them was AWS/GCP-specific to begin with |

The three Plotly charts (overlay / facet / fixed-axis) are pixel-for-pixel the same
— per what you asked for, this is a lift of the *infrastructure*, not a rebuild of
the report.

## Step 1 — Create a Databricks workspace (skip if you already did this)

1. Go to https://www.databricks.com/learn/free-edition and sign up (email + password, no credit card).
2. It provisions a free workspace for you automatically — you'll land inside it in your browser.
3. Note the workspace URL in your address bar, something like `https://xxxxx.cloud.databricks.com` — you'll need it in Step 2.

## Step 2 — Point the Databricks CLI at your workspace

On your Mac, in a terminal:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login --host https://<your-workspace-url>
```

This opens a browser to log in and stores a token locally — nothing to paste into
any chat.

## Step 3 — Check the catalog name

In the workspace UI, click **Catalog** in the left sidebar and note the catalog
name available to you (Free Edition usually gives you one called `workspace`).
If yours is different, either edit `catalog: workspace` at the top of
`databricks.yml`, or override it at deploy time (Step 4).

## Step 4 — Deploy

```bash
cd pipe_hdb_databricks
databricks bundle validate                    # sanity-checks the YAML against your real workspace
databricks bundle deploy -t dev                # creates the schema, volume, and job
databricks bundle run hdb_pipeline -t dev      # triggers one manual run so you can watch it work
```

If your catalog isn't `workspace`:

```bash
databricks bundle deploy -t dev --var="catalog=main"
```

## Step 5 — Watch it run

`databricks bundle run` prints a link to the run in the Databricks Jobs UI — open
it and watch the three tasks (`ingest_bronze` → `report_plotly` → `publish_outputs`)
execute in order. Click into `catalog.schema.hdb_silver` under **Catalog** afterward
to see the table Unity Catalog now governs, and into the `reports` Volume to
download the generated HTML.

## Step 6 — Turn on the schedule

The job deploys **paused** on purpose, so you can check one manual run before it's
live. Once you're happy:

```bash
databricks bundle deploy -t dev  # after flipping pause_status: UNPAUSED in resources/hdb_pipeline_job.yml
```

or just flip the toggle in the Jobs UI directly.

## Things worth knowing before you rely on this

- **Compute is serverless** — no cluster to size or forget to shut down. Free
  Edition includes a serverless compute allowance; if you outgrow it, that's the
  one place a real cost could show up.
- **GCS/S3 publish is optional and untested from here** — `03_publish_outputs.py`
  keeps the old upload logic behind `--gcs-bucket`/`--s3-bucket`, but wiring
  credentials via Databricks Secrets (rather than local `~/.aws`/`gcloud` config,
  which won't exist on Databricks' compute) is a TODO left in that file's docstring.
  The pipeline works completely without it — the Volume is now the "landing" spot.
- **This bundle only touches Databricks** — it doesn't remove your AWS/GCP
  deployment. Run both in parallel as long as you want, or tear down
  `deploy_aws.sh`/`deploy_gcp.sh`'s resources yourself once you trust this one.
