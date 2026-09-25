# Worktable — decisions to apply at the lakehouse migration / packaging step

Captured 2026-09-24, ahead of [[project_lakehouse_rollout_three_repos]] (planning-only today,
no edits yet). Recorded now so the reasoning survives to when that work actually starts.

---

## DECIDED — leave `shared_schema_guards.py` as-is, do not extract `literal_is_valid`/`raise_or_warn`

**Context:** `shared_schema_guards.py` does `import pyarrow as pa` at module level (needed for
its own pyarrow-typed functions' type hints and bodies - `reconcile_missing_columns`,
`check_types_match`, etc., which genuinely can't run without pyarrow). The two Spark helper
files (`helper_sparkdelta_io.py`, `helper_sparkiceberg_io.py`) only import `literal_is_valid`
and `raise_or_warn` from that file - both are pure Python, neither touches `pa` anywhere - so
a pure-Spark caller transitively pulls in pyarrow just to reach two functions that don't need
it.

**The fix considered:** pull `literal_is_valid` + `raise_or_warn` into a third, genuinely
dependency-free module (e.g. `shared_schema_guards_core.py`), so a spark-only install/extra
never touches pyarrow at all.

**Decision: don't do it.** Verified, not assumed, before deciding:
- pyarrow is cheap relative to the thing that actually matters (pyspark + JVM) - no system
  deps, a single wheel, installs in seconds.
- pyarrow is already preinstalled in Databricks serverless environment version 5's standard
  base environment, with Arrow-based Python UDF optimization on by default there - so on the
  one real cloud target this code runs on today, the question is moot regardless of how the
  package declares its dependencies.
- The wheel itself isn't tiny (~35-65MB depending on version/platform - notable enough that
  PyPI's own infra team gave pyarrow a size-limit exception), so the "zero-pyarrow spark
  install" this extraction would unlock only matters for genuinely size-constrained serverless
  packaging (AWS Lambda's zip/unzip limits, a minimal container image) - relevant to
  [[project_pipe_hdb_emr_serverless_plan]] / [[project_pipe_hdb_spark_serverless_containers]]
  specifically, not to local dev or Databricks.

**How to apply:** don't extract at the lakehouse-migration/packaging step either, unless the
actual target at that point is a size-constrained serverless deploy (Lambda, tight container
image) rather than Databricks/local/EMR-on-EC2. If that's ever the real target, revisit this
decision with that constraint named explicitly, rather than doing it speculatively now.

## DECIDED — package as ONE repo with pip extras, not 3 separate git packages

**Context raised:** whether to package `helper_*_io.py` + the two `shared_schema_guards*.py`
files as one git package, or split by engine into 3 separate repos.

**Decision:** one package, with optional extras (e.g. `helper_io[pyarrow]`, `helper_io[spark]`)
gating the heavy per-engine dependencies - not 3 separate git repos.

**Why:** this session was spent making `shared_schema_guards.py` and `shared_schema_guards_spark.py`
mirror each other exactly - same check numbering (0/1/2/3/4/5), same reasoning for where
`spark_evolve_schema`/schema-evolution runs relative to `raise_or_warn`, ported deliberately
across both files each time something changed. Splitting into separate repos removes the single
PR / single diff that currently keeps that mirroring honest - drift between them would have to
be caught by memory or discipline instead of by the change being physically in one commit.

**If a split ever happens, the real fault line is JVM vs non-JVM** (pyarrow+pyiceberg vs
spark-iceberg+spark-delta), not "3 packages" - a 3-way split by engine leaves
`shared_schema_guards.py`'s pure logic with no natural single home, since it's consumed by two
of the three would-be packages.

**How to apply:** at the lakehouse-migration/packaging step, start with one package + extras.
Only split further if a concrete, felt cost shows up (e.g. a consumer project that genuinely
can't tolerate pyspark/pyiceberg as even an optional dependency) - not preemptively.
