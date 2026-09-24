# Provider branches — defense notes & task backlog

Captured 2026-09-10 from the walkthrough of `src/providers/local.py` + `src/providers/databricks.py`.
This is the agenda for shaping the providers into "my own product".

Status legend:
- **DECIDED** — agreed, do it when we pick up this thread
- **DISCUSS** — open, but I have a lean noted
- **PARK** — later, needs a trigger or more understanding
- **Q** — comprehension question; short answer noted, expand when we get there

---

## Worktable

Ordered as originally raised. Status vocab: DONE = implemented + verified · OPEN = agreed,
not started · TO DISCUSS = needs a conversation first · PARKED = deferred on purpose ·
PENDING REVIEW = Claude drafted an answer/discussion, user has NOT read or discussed it in full yet.

| # | Thread | Your call | § | Status |
|---|---|---|---|---|
| 1 | `read_bronze` → rename | `read_tier` | 1a | DONE |
| 2 | stubs / "cost of no base class" → alternatives + what a base class is | discuss when raised | 4b | PARKED |
| 3 | `local.add_args` — make the no-op explicit to readers | do | 2c | DONE |
| 4 | `databricks.add_args` maybe useless; if real → `add_catalog_args` | discuss / park | 1b | TO DISCUSS |
| 5 | no-throwaway-args rule; script 2 only needs `--catalog` | discuss & work through | 3a | TO DISCUSS |
| 6 | resolve engine in series | do | 2a | DONE |
| 7 | "origin/dataset are parameters" — what does it mean | question | 5a | PENDING REVIEW |
| 8 | robust the local relative path; test from 3 cwd's | discuss | 3f | TO DISCUSS |
| 9 | loop-per-era vs `unionByName` — why the difference | question | 5b | PENDING REVIEW |
| 10 | understand the Databricks align-down / schema mechanism | park (learning) | 4e | PARKED |
| 11 | `read_tier` — 2 read paths vs 4 | discuss later | 3e | DONE (via #20 ph4 — 4 paths: local parquet/iceberg, dbx delta/iceberg) |
| 12 | why `write_window` is separate — is that the idea | question | 5c | PENDING REVIEW (code now merged → #20 ph5, but the *why* is unread) |
| 13 | collapse `write_table` + `write_window` into one | discuss | 3b | DONE (#20 ph5 → `write_tier`) |
| 14 | is all duplication in the write stage? then dedup | park | 4a | DONE (#20 ph2 → `helper_catalog_io`) |
| 15 | stale `databricks.py` L1–5 module docstring | clear — do | 2b | DONE 2026-09-10 — now "managed UC tables (Delta+Iceberg) for tiers, Volume for landing; `add_src_to_path()` (not `__init__`) puts src/ on path" |
| 16 | `columns_contract` default mismatch (local vs databricks) | discuss | 3c | DONE (#20 ph5 — `=None` default both sides) |
| 17 | `DONE:` log-prefix inconsistency between the two writers | discuss | 3d | DONE (#20 ph2 — helper owns the `DONE:` line) |
| 18 | product forks A / B / C | park, explain later | 4c / 4d | DONE — #20 delivered fork B |
| 19 | move the "don't boot a 2nd engine on an active session" guard into `sparkutils.get_spark`; then delete both `get_spark_engine()` providers | agreed, do later | 4f | PARKED |
| 20 | helper families + symmetrical write/read: extract `helper_catalog_io.py`, format matrix `{parquet,delta,iceberg}`, providers = hdb orchestration only | design agreed, phased plan | 6 | **DONE 2026-09-10 — local + Databricks fully verified (A–D9 all green)** — all 4 UC tables = 986,355 = local exactly; probe 0c closed (709,050); `.overwrite(span)` confirmed selective (D8) |
| — | (blocker) `local.get_spark_engine` was stubbed to `'''stub'''` → returned `None` → broke every local run | fix | 6.0 | DONE — restored `return requested` |

Landed alongside #6 (not originally listed): `landing_dir` → `get_landing_dir` + a `get_*` /
verb naming scheme — §2a-bis.

---

## 1. Renames

### 1a. `read_bronze` → `read_tier`  · DONE 2026-09-10
Decision: **`read_tier`** — keep the `tier=` param, provider is now a general "read any
layer" addressing surface (t3 report stage is planned, so the generality will pay off).
`SOURCE_TIER='t1'` constant kept (consistent with keeping `tier=`). Renamed at 5 sites
(2 defs + 2 imports + 1 call in `2_stage_to_t2.py`); docstrings already tier-generic, no
wording changed. Local run re-verified.

--- discussion record ---
Evolution of my thinking:
- first: `read_t1`
- then: `read_tier` makes more sense *because `tier` is already an argument* — the function isn't hard-tied to t1
- then: why not `read_table`? it will always read a UC/Delta table or a hive table
- also: I may want **4 read paths, not 2** — see 3e (now PARKED).

**Discussion so far:**
- The name and the `tier=` param disagree today (`read_bronze` = a role name, `tier=` = generic).
  Picking the name picks the signature:
  - `read_t1` → **drop `tier=`**, hardcode `t1`; also kills the `SOURCE_TIER='t1'` constant
    (used only in this one call — an always-`t1` constant is the same speculative smell as a
    throwaway arg). Cost: `read_t2` later is a ~1-char copy × 2 providers.
  - `read_tier` → keep `tier=`, just name it honestly; one function forever; the provider
    becomes a general "read any layer" adapter.
  - `read_table` → **entangled with §3b**: `write_table` already means the script-1
    bronze-batch writer, so `read_table` next to it falsely implies a pair. Only viable if
    the write side is renamed too. Park the `read_table` option until §3b.
- Neither env reads a **Volume** here — local reads a parquet *directory*, databricks reads a
  *managed UC table*. The Volume is CSV-landing only (`landing_dir`), consumed by script 1.
- Root cause: the provider has **no naming scheme** (`add_args`/`spark_engine`/`landing_dir`
  = named by action; `write_table` = by target shape; `read_bronze` = by medallion role).
  Pick a scheme and this resolves.
- Claude's lean: `read_t1` + drop `tier=`. Go `read_tier` only if t3 report stage is likely
  *and* you want a general addressing surface.

**OPEN — deciding question:** is the parked t3 report stage ([[project_pipe_hdb_t3_report_plan]])
likely enough to want one `read_tier` now, or stay concrete with `read_t1` (+ add `read_t2`
when t3 is real)?

### 1b. `add_args` → `add_catalog_args` / `togg_catalog_args`  · DISCUSS / PARK
Caveat: this whole `add_args` block might be **throwaway** — it exists mostly because we were
experimenting with Databricks *job parameters*. If it turns out to be a real need, rename to
something like `add_catalog_args` or `togg_catalog_args` (intent-revealing). If not, it may
shrink or vanish — see 3a.

---

## 2. DECIDED — do when we pick up

### 2a. Resolve `spark_engine` in series, explicitly  · DONE 2026-09-10
Rationale: engine *gotten to* once, in serial, not silently forced inside a `get_spark(...)`
arg. The STATUS print now shows the real engine (`java` on Databricks), not what was passed.
- placement: **(b)** in the `# ── START SPARK` section, above `get_spark` — replaces the
  invisible nested call `get_spark(app, spark_engine(args.spark_engine))` with a visible line.
  (Not "right after parse" — the IPYTHON block also sets `args.spark_engine`, so the resolve
  has to sit downstream of it.)
- new line (scripts 1 & 2):
  ```python
  # resolve the engine ONCE, here: get_spark_engine() returns 'java' on databricks, passes
  # --spark_engine through locally. below this line args.spark_engine IS the live engine.
  args.spark_engine = get_spark_engine(args.spark_engine)
  spark = get_spark('<app>', args.spark_engine)
  ```
- **rename `spark_engine()` → `get_spark_engine()`** (provider function; the `--spark_engine`
  flag and `args.spark_engine` attr are untouched). Scratch notes in script 1 (old lines
  90–96) deleted.
- all 3 scripts re-verified locally (land / import / stage green).

### 2a-bis. `landing_dir()` → `get_landing_dir()`  · DONE 2026-09-10
Same `get_` convention. Renamed at defs (both providers), imports + calls (scripts 0 & 1),
and the `__init__.py` docstring examples.

**Naming scheme now forming** (fixes the "provider has no scheme" root cause from §1a):
- `get_*` = pure getter, returns a value: `get_spark_engine`, `get_landing_dir` (and `read_tier` is arguably one too — leave as `read_` since it's an IO verb)
- `togg_*` = a toggle / on-off registration (user's note in `1_import_to_t1.py` line ~114:
  *"need to settle to prefix with get_ and togg_ its important"*) — candidate for `add_args`
  → `togg_catalog_args` (thread #4).
- verb = does IO / mutates: `read_tier`, `write_table`, `write_window`
Apply this lens to any future provider function. Settling `get_`/`togg_` fully = threads #4 + the
"defend everything below" note in script 1.

### 2b. Fix stale docstring, `databricks.py` lines 3–4  · DECIDED ("clear")
"providers/__init__.py already put src/ on sys.path" is no longer true — each script's
`add_src_to_path('modules/pipe_hdb/src')` does that now, and `__init__.py` is a bare
namespace. Reword or delete those two lines.

### 2c. Make the `add_args` no-op explicit to readers  · DONE 2026-09-10
- `local.add_args` body: user settled on a bare `'''stub'''` docstring, no `return parser`.
  (Claude proposed a longer "defined only so every script can call it unconditionally"
  docstring; user trimmed it to `stub`.)
- (a) dropped `return parser` from `databricks.add_args` too — argparse mutates in place,
  return was ignored at all 3 call sites.
- (d) fixed the stale docstring clause: `providers/__init__ picks this module` →
  `the script's if IS_DATABRICKS picks this module`. (The *module* docstring at
  databricks.py L1–5 still has the same rot — that's thread #15, still OPEN.)
- rest of the thread-#3 discussion (rename to `add_catalog_args`, Protocol, by-absence)
  parked → threads #1b / #2 / #4.
- stage run re-verified.

---

## 3. DISCUSS — open threads (with a lean)

### 3a. NO throwaway values per script  · DISCUSS  *(my rule)*
**Rule:** a script must never be passed an arg it ignores. Keeps each script portable and
non-confusing to reverse-engineer. Today script 2's Databricks path inherits the shared
`add_args` and the deploy passes throwaway `--schema` / `--volume`. That bothers me.
Mechanisms to work through: parametrised `add_args(parser, volume=True)`, split into
`add_args_uc` / `add_args_volume`, per-script arg sets, or drop `add_args` entirely (1b).
Work it through when we're at this task.

### 3b. Collapse `write_table` + `write_window` into ONE function?  · DISCUSS
The 4 asymmetries: list vs single DF · span-from-data vs span-from-args · align-down vs not ·
returns-a-tuple vs returns-nothing. Unifying shape would be: always take a list, always take
an explicit `months=` / span, always return stats. Want to try it and see if the one
function stays readable.

### 3c. `columns_contract` default mismatch  · DISCUSS
`write_table` local: keyword-required (no default). Databricks: `=None`. Pick one convention.

### 3d. `DONE:` log-prefix inconsistency  · DISCUSS
`write_window` prints `DONE:  hive    -> …` (both providers). `write_table` prints
`  hive    -> …` with no prefix. Standardise the log lines across all provider writes.

### 3e. `read_bronze` — 2 read paths vs 4  · PARK (2026-09-10)
Now: local reads hive parquet, Databricks reads Delta — the *canonical* copy on each side.
2 was an MVP (see 5d). The 4-path version: local read iceberg **and** hive; databricks read
iceberg **and** delta.
**Parked.** The question to answer when unparked: script 2 can only build t2 from *one* read
of t1, so a 2nd read only earns its keep if you're (a) cross-checking the hive & iceberg
copies agree, or (b) planning to flip which copy is source-of-truth. If neither, the iceberg
copy of t1 stays a schema-evolution demo and 2 paths is correct. `tier` (which layer) and
format (which copy) are different axes — settle the 1a name first.

### 3f. Robust the local relative path  · DISCUSS
`landing_dir` local returns `datalake/landing/...` — only safe because `run.sh` cd's to the
repo root first. Breaks if a script is launched from elsewhere.
- Idea: anchor on `runtime_env`'s `_src` (already computed) + `os.getcwd()`?
- Feasible / worth it? Discuss.
- **Test matrix once decided:** run `run.sh` from `modules/pipe_hdb/`, from `modules/pipe_hdb/src/`,
  and from the repo root — all three should behave identically.

---

## 4. PARK — later / needs understanding

### 4a. All the duplication is in the WRITE stage  · PARK
Confirm that framing, then dedup: the ~4 near-identical path builders, the `targets = []`
+ `for fmt, fqn in targets` loop (copied between the two Databricks writers), and the
`tableExists → overwrite(span) else create()` block. Park for consideration alongside 3b.

### 4b. "Stubs kept in lockstep" — what's the alternative? What is a base class?  · PARK (discussed 2026-09-10, parked as a future learning item)
Original: *both modules must keep all six names in lockstep even where one is a stub — the cost
of no base class.*

**Discussion outcome — options laid out, no decision, revisit when comfortable with the OOP bits:**
- The real question isn't "OOP or not", it's *how to stop two impls of one contract from drifting*.
- **base class (`abc.ABC` + `@abstractmethod`)**: Python refuses to instantiate a provider missing
  a required method (real enforcement, vs today's runtime-ImportError). Base can also carry a
  *default* body → kills the no-op stubs (`add_args` no-op lives once on the base, only
  `DatabricksProvider` overrides). Cost: classes + `self` + `provider.foo()` calls; the visible
  `if IS_DATABRICKS: from providers.X import …` block shrinks to one `= DatabricksProvider()` line.
  Terms: `abc` = "Abstract Base Classes" module; `ABC` = the base to inherit; `abstractmethod` =
  decorator forcing subclasses to implement.
- **`typing.Protocol`** (= thread 4d / fork C): keep the two modules as-is; a type checker
  (Pyright/mypy) verifies both *structurally match* the Protocol. Enforcement only if you run the
  checker; does NOT remove stubs (still need a body to satisfy it). ~zero runtime change.
- **by-absence** (= thread 4 direction): no shared interface — each script imports only what its
  env needs, `local.py` simply has no `add_catalog_args`. Fully kills the stub. Cost: every
  script's import block is bespoke; can't hold "the provider interface" as one thing.
- **single-module dict-dispatch** (`P['get_landing_dir'](...)`): rejected — loses autocomplete +
  type safety, still an entry per env, strictly worse than status quo.
- **Claude's read:** cost is real but cheap now (exactly 1 true no-op stub, `local.add_args`; 6
  fns; 2 impls). Lockstep is arguably a feature. Do nothing until stub count grows → then ABC
  with base defaults; or add a Protocol + Pyright for a safety net that keeps the modules.

### 4c. Fork B — provider returns *addressing*, shared `io.py` does the *writing*  · PARK
`provider.t1_target(args)` → `Target(kind='files', hive_path=…, iceberg_path=…)` or
`Target(kind='uc', delta_fqn=…, iceberg_fqn=…)`; one `io.write(dfs, target, write_format, span)`
dispatches on `kind`. Cleanest split, scales to a 3rd environment (Glue/EMR) without a 3rd
fat module. Park until a 2nd dataset or 3rd environment actually forces it.

### 4d. Fork C — `typing.Protocol` for a checkable interface  · PARK
Don't understand it yet. Explain later; pairs with 4b/4c.

### 4f. Guard against a 2nd engine on an active session, in `get_spark`  · PARK (agreed, do later)
`sparkutils/getspark.py` already computes `sparksession_given = SparkSession.getActiveSession()`
and prints "already provided by environment" — but then falls through to the engine dispatch
and would still `server.start()` a pysail server on `engine='sail'`. Make that detection gate
the dispatch so a pysail server is only ever started when `engine=='sail' AND no session yet`.

**Two candidate shapes — user may want the 3-branch explicit one:**

*Shape A — 2-branch, getOrCreate-first (Claude's lean, DRY):*
```python
if engine not in ('sail', 'java'):
    raise ValueError(f"engine must be 'sail' or 'java', got {engine!r}")

if engine == 'java' or sparksession_given is not None:   # De Morgan of "sail AND no session"
    if engine == 'sail':
        print('[get_spark] session already active -> ignoring engine=sail, attaching')
    spark = SparkSession.builder.appName(appname).getOrCreate()
else:                                                    # sail + nothing running -> pysail server
    ... server.start() ... .remote(sc_address).create()
```

*Shape B — 3-branch explicit (user leans here; accepts one duplicated `getOrCreate()` line):*
```python
if sparksession_given is not None:
    print('[get_spark] session already active -> attaching, ignoring engine')
    spark = SparkSession.builder.appName(appname).getOrCreate()
elif engine == 'sail':
    ... server.start() ...
elif engine == 'java':
    spark = SparkSession.builder.appName(appname).getOrCreate()   # <- dup, but each branch names its engine
else:
    raise ValueError(...)
```
Trade: B duplicates `getOrCreate()` in two mutually-exclusive branches; A collapses it behind
an `or` condition + a guard clause. **Decide A vs B when unparking.**

**PROBE DONE 2026-09-10 — CONFIRMED:** the `pipe_hdb_update` / `import_update` run log shows
`get_spark` already printing *"Spark session already provided by environment"* /
*"getOrCreate will [get], not [Create]"* on the `spark_python_task`. So
`SparkSession.getActiveSession()` **is non-None** there and the guard would fire. #19 is
unblocked. When you do it:
1. add the guard to `get_spark` (shape A or B); 2. delete both `providers/*.get_spark_engine()`
+ the `args.spark_engine = get_spark_engine(...)` line + the import in scripts 1 & 2; 3. batch
the `helper_sparkutils` push with the pending [[project_sparkutils_stop_spark_fix]]; 4. accept
the STATUS print then shows the *requested* engine (or drop it). If the probe is `None` on
Databricks → keep `get_spark_engine()` in the provider, it's the only reliable spot.
Aligns with [[feedback_engine_portable_scripts]] (engine logic in precode, not the script).

### 4e. Understand the Databricks schema mechanism  · PARK  (learning item)
The "align only goes DOWN, first write defines the schema, no schema-grow path" footgun
(`write_table` databricks, lines 79–81). Want to actually understand *why* Databricks/Delta
behaves this way, not just work around it.

---

## 5. Q — comprehension answers  · PENDING REVIEW (Claude drafted; user has not read/discussed in full — revisit each)

### 5a. "origin/dataset are parameters, not hardcoded — what does this mean?"
`landing_dir(args, origin, dataset)` receives `origin='datagov'`, `dataset='resale_flat_prices'`
from the *script*, instead of `'datagov'` being written inside the provider. Add a 2nd source
later (`origin='ura', dataset='rentals'`) and the same function builds `datalake/landing/ura/rentals`
with zero provider edits — the script just passes different values. Same reasoning as the
`tier` parameter on the write functions.

### 5b. "Local loops per era, Databricks unions — efficiency, no-choice, or random?"
**No-choice, in opposite directions:**
- Local: the pyarrow / pyiceberg writers each take *one* Arrow table, and the iceberg writer
  is *designed* to evolve its schema per append. Looping lets every era write its own column
  set. Unioning first would force one schema up front and kill the evolution demo.
- Databricks: `df.writeTo(fqn)` writes *one* DataFrame. Five eras in one `.create()` /
  `.overwrite()` means combining them first, and `unionByName(allowMissingColumns=True)` is
  Spark's way to combine mismatched schemas. Looping instead = 5 Delta commits = the
  concurrency/commit overhead we already hit.
The tool on each side pushes you to the shape it uses. Not a coin flip.

### 5c. "write_window is separate because why? Is THIS the idea?"
**Yes, that's the idea.** Bronze import = "add/replace a batch of whole *eras*, schema still
settling". Silver stage = "replace one contiguous *month window*, schema already fixed".
Different input (many DFs vs one), different overwrite bound (derived from data vs given by
caller), different schema handling (align-down vs none). Written separate because forcing
them together made both harder to read. 3b is whether that's still true.

### 5d. "Only 2 read paths — MVP first?"
**Yes, MVP.** `read_bronze` reads whichever format is canonical on each side (hive locally,
Delta on Databricks) because script 2 only needs *one* correct read of t1, and those are the
defaults. Reading the iceberg copy too only matters if you want to cross-check the two copies
agree, or make iceberg the source of truth. That's the 4-path version in 3e.

---

## 6. PLAN — #20 · helper families + symmetrical write/read  · DONE 2026-09-10 (local-verified; databricks code-review only)

### AS BUILT

**Files:**
- NEW `src/helper_catalog_io.py` — `align_down(df, target_schema)`, `create_or_overwrite(df, *, fqn, fmt, span, spark)`, `read(spark, fqn)`. The `df.writeTo(fqn)` mechanics for any JVM-Spark + catalog platform.
- `src/helper_pyarrow_io.py` — added `read(spark, root)`.
- `src/helper_iceberg_io.py` — added `read(spark, *, table_fqn, location=None)` (pyiceberg scan → arrow → `spark.createDataFrame`, Sail-safe — verified). Env var `HDB_ICEBERG_DIR` → `ICEBERG_WAREHOUSE_DIR` (was dead — nothing set it). `'hdb'` SqlCatalog label kept (internal, changing it risks `_catalog.db` compat).
- `src/providers/local.py` + `src/providers/databricks.py` — `write_table` + `write_window` **replaced by one `write_tier(data, *, tier, origin, dataset, write_format, part_col, columns_contract=None, bounds=None, spark, args)`**. `data` = a DF or a list of DFs (normalized internally). `bounds=None` → derive the span from the data (bronze); `bounds=(start_month, end_month)` → exact window (silver). Returns `(n_out, months_in)` from both. `_parse_formats()` in each provider validates `{parquet,delta,iceberg}` and rejects the format the env doesn't support with a clear message.
- `read_tier(spark, args, *, tier, origin, dataset, fmt=...)` — `fmt` defaults to the canonical copy per env (`'parquet'` local, `'delta'` dbx); `fmt='iceberg'` reads the twin. All 4 paths work (#11).
- Scripts `1_import_to_t1.py` / `2_stage_to_t2.py` — import `write_tier`; `--write_format` default `parquet,iceberg`, help text, no `choices` (validation is in the provider); IPYTHON blocks `'hive'` → `'parquet'`.
- `deploy_databricks/databricks_deploy.sh` — `import_task` + `stage_task` pass `--write_format delta,iceberg` (was `both`).

**Provider now = pure hdb orchestration:** local loops frames + pads parquet to `columns_contract` + builds `datalake/...` paths; databricks unions frames + computes span + builds FQNs. All *mechanics* (partition replace, catalog create/overwrite, align-down, schema evolution) are in the 3 helpers.

**Local verification (all green, Sail + JVM):** `make run` · `import --eras all --write_format parquet,iceberg` (5-era loop, iceberg evolution intact) · `stage` windows single-month / 3-month / 4-year · single-format `parquet` / `iceberg` · `stage --spark_engine java` · `read_tier(fmt='iceberg')` on Sail · parquet & iceberg t1 row counts match (986,355 each) · format rejections (`delta` local, `parqet` typo) raise cleanly.

**Databricks: LIVE-VERIFIED 2026-09-10.** `backfill deploy` (after fixing a stale `--write_format
both` in the job JSON → `delta,iceberg`) → 8/8 tasks green, **all 4 UC tables = 986,355 rows =
local count exactly** (rows-written metric = 3,945,420 = 986,355 × 4). D-checks: schema OK
(~12 cols, `tx_monthdate: date`), delta ≡ iceberg on every metric, both genuine managed table
types (Provider delta / iceberg), transform sane. **Probe 0c CLOSED:** `remaining_lease IS NULL`
count = pre-2015 row count = **709,050 exact** → `align_down`'s null-pad handles the narrow eras,
managed Iceberg needs NO `mergeSchema`. UC lineage shows t1→t2 (1 upstream / 4 downstream).
Only D8 (idempotency of the narrow-window `update` run) still to tick.
`create_or_overwrite` always align-downs now (old `write_window` skipped it — harmless for t2's
fixed schema, more robust). Note: a false "overwrite is a full-replace" alarm was raised +
retracted — the apparent collapse was a manual `DROP TABLE` + partial `update` re-run, not a bug;
`backfill`'s 986,355 (sum of 5 serial `.overwrite(span)` eras) proves the predicate IS selective.

### Time (engineering-time estimates per phase)

| phase | min |
|---|---|
| 0 verify + fix `get_spark_engine` blocker | 10 |
| 1 freeze helper interface | 20 |
| 2 extract `helper_catalog_io.py` | 35 |
| 3 `--write_format {parquet,delta,iceberg}` | 25 |
| 4 symmetric reads (`read` on 3 helpers, 4 paths) | 25 |
| 5 collapse `write_table`+`write_window` → `write_tier` | 35 |
| 6 cleanup (env var, worktable, plan as-built) | 20 |
| **TOTAL** | **170** (~2h50m) |

Executed order 0 → 1 → 3 → (2+4+5 as one pass over both providers) → 6 — phases 2/4/5 all
rewrite the same two functions, so one pass rather than three.

---

### original plan (design agreed 2026-09-10)

**Goal:** providers hold only hdb orchestration (era loop / union, `columns_contract`, FQN
naming, span). All write/read *mechanics* live in reusable, project-agnostic helpers, called
uniformly. **Non-goals this round:** separate git repo for the helpers; Glue/Dataproc
providers (only make the shape allow them); touching era/union domain logic.

### Format matrix — AGREED
| `--write_format` | local | databricks |
|---|---|---|
| `parquet` | ✅ `helper_pyarrow_io` | ❌ reject — "not a managed-table format on UC" |
| `delta`   | ❌ reject — "not used locally" | ✅ `helper_catalog_io` (`.using('delta')`) |
| `iceberg` | ✅ `helper_pyiceberg_io` | ✅ `helper_catalog_io` (`.using('iceberg')`) |
Each provider validates its own supported subset. The old `hive`-means-different-things map is gone.

### Helper files — 3 total, NOT per-platform
```
helper_pyarrow_io.py      parquet dir -> path               (exists, keep)
helper_pyiceberg_io.py    iceberg -> path + sqlite catalog   (exists, keep)
helper_catalog_io.py      delta|iceberg -> catalog FQN       (NEW — Databricks + future Glue/Dataproc/EMR)
```
Name: `helper_catalog_io` for now; may switch to `helper_table_io` later if the look bothers me.
It writes to a **name in a live catalog service** (Unity Catalog / Glue Data Catalog / Dataproc
Metastore), NOT to a path — input is a `DataFrame` + `catalog.schema.table`, and the catalog
wiring is `get_spark`'s job. Needs a JVM-Spark engine that ships the format catalogs — does NOT
work on Sail (no JVM, no Delta/Iceberg JARs), which is why the path-based helpers stay separate.
No `helper_delta_io.py` (local delta not wanted). No `brick*` duplicates. Glue/Dataproc reuse
`helper_catalog_io` — `df.writeTo(fqn)` is plain PySpark API, and the JVM-Spark platforms
(Databricks/Glue/Dataproc/EMR) all ship the Delta/Iceberg catalog providers, so the call is
identical across them;
what differs (catalog config, FQN convention) is `get_spark`'s job, not the writer's.

### Phases
- **0 — verify.** ~~0a delta-rs probe~~ dropped. **0b** confirm no managed parquet tables on
  UC Free serverless (5 min). **0c** Databricks managed-Iceberg `mergeSchema` behavior —
  deferred, needs a live run. **0d** RESOLVED: `--write_format` → `{parquet, delta, iceberg}`,
  comma-separated (`delta,iceberg`), `both` removed. Deploy passes `--write_format delta,iceberg`;
  local `make` passes `--write_format parquet,iceberg`. Databricks `parquet` and local `delta`
  each raise a clear error.
- **1 — freeze the helper interface** (design here first). `Target`: `path` (parquet) /
  `path + catalog` (local iceberg) / `fqn` (spark+catalog). Input: `pyarrow.Table` for local
  helpers, `DataFrame` for `helper_catalog_io`; `.toArrow()` stays in the provider. Verbs both
  sides implement: `replace_partitions(...)`, `read(...)`.
- **2 — extract `helper_catalog_io.py`.** Pull `align_down(df, schema)` +
  `create_or_overwrite(df, *, fqn, fmt, span, spark)` out of `databricks.write_table`.
  `write_table` and `write_window` both call it. **Closes #14 / #17** (Databricks side).
- **3 — local `write_format` rename.** `local.write_table` dispatches `{parquet→pyarrow_io,
  iceberg→pyiceberg_io}`, rejects `delta`. Small — helpers already exist.
- **4 — symmetric reads.** `read_tier` dispatches local → helper `read` by path;
  databricks → `helper_catalog_io.read(fqn)`. **Resolve #11 / #3e** here.
- **5 — collapse `write_table` + `write_window`** (#13). One `write(...)` through the uniform
  helper signatures, if it reads well. **Closes #13, #16.**
- **6 — cleanup.** De-hdb `helper_pyiceberg_io` (`HDB_ICEBERG_DIR`, `'hdb'` catalog name →
  params). Worktable: close #13/#14/#16/#17, reshape #11, mark #18 = fork B done.

### Ordering
Phase 2 first (safe, self-contained, the concrete win). Phase 3 additive. 4–5 are the reshape,
depend on 1–3. 0c gates only the iceberg-evolution *decision*, not the extraction.

---

## Phase 6 expanded: `helper_pyiceberg_io.py` cleanup (captured 2026-09-11)

Fleshes out the one-liner "de-hdb helper_pyiceberg_io" above. Sequenced plan, from the
hands-on partition-spec / schema-evolution study session:

1. **DECIDED — split catalog + namespace provisioning out of `get_table()`.** Currently
   `cat.create_namespace(namespace)` happens silently inline, on every call, wrapped in a
   swallow-all `try/except`. Make provisioning (catalog + namespace) its own explicit step,
   never implicit inside "get me a table."
2. **DISCUSS — redesign `get_table()` → `get_or_create_table()` / `expose_table()`.** Compare
   to pyarrow's `write_dataset`, which needs no "does it exist" ceremony at all - it just
   writes, empty dir or not. Aiming for a cleaner get/create split (rough 70/30 weighting)
   instead of the current one long try/except chain. Naming and exact shape not settled.
3. **DECIDED — the core write path (`expose_table(fqn)` -> build partition filter ->
   `tbl.overwrite(...)`) stays STRICT, no schema evolution.** Keep it simple/testable in
   isolation first.
4. **DECIDED — schema evolution becomes its own explicit function, `write_evolution()`** -
   not bolted onto every write by default (unlike the current `write_partition`, which always
   calls `_evolve_schema` + `_align_to_table`). If evolution fails, fail loud - "sound out
   first" - don't swallow it.
5. **PARK — before hand-rolling evolution guards, check what pyiceberg already enforces
   natively.** Same 4 checks `write_partition_guarded` does on the pyarrow side (partition-key
   match / missing cols / new cols / type change). Already have empirical evidence from this
   session's "naughty" tests of what pyiceberg catches on its own. Where iceberg already
   guards itself, write the finding down and make the Databricks provider rely on the same
   native guarantee instead of re-implementing pyarrow-style checks that aren't needed.
6. **Q — `write_evolution()` flagged as genuinely hard.** Once understood, may hand this one
   to Claude to implement directly rather than hand-write, unlike the rest of this session's
   hands-on style.
7. **PARK — longer-term target: local JVM Iceberg, then all the way through to Databricks.**
   Scoped to the strict/no-evolution path first; evolution testing on JVM/Databricks deferred
   until `write_evolution()` itself is understood and built.

   **When this gets picked up (2026-09-13):** point local JVM Iceberg (Java `JdbcCatalog`,
   e.g. via a local Spark session) at the *same* `_icebergcatalog.db` + warehouse dir pyiceberg
   already uses - **hard constraint: never duplicate the warehouse.** One set of parquet data
   files + manifests, shared between pyiceberg and JVM Iceberg, always.

   Reasoning this rests on (verify empirically before trusting, not yet tested with a real
   JVM): Iceberg's table format (metadata.json/manifests/data files) is language-agnostic, so
   the warehouse itself is safely shareable no matter what. The catalog is the less-certain
   part - pyiceberg's `SqlCatalog` and Java's `JdbcCatalog` are both implementations of the
   same JDBC-catalog convention and likely share the same `iceberg_tables` schema (confirmed
   columns: `catalog_name`, `table_namespace`, `table_name`, `metadata_location`,
   `previous_metadata_location`) - but pyiceberg has a "v0 vs v1" schema-version distinction
   (`_init_catalog()` in `pyiceberg/catalog/sql.py`) that Java's implementation may not match
   exactly, and the sqlite JDBC driver (`org.xerial:sqlite-jdbc`) needs to be added to the JVM
   classpath manually.

   **Also test (added 2026-09-13, from the "overwrite is 2 commits" finding above).**

   **Acceptance criterion - the one conclusion this test needs to produce:** replace an
   already-populated partition via real local JVM Iceberg (`df.writeTo(table).overwritePartitions()`),
   then run the same check used to find this in pyiceberg -
   ```python
   for entry in tbl.history():
       snap = tbl.metadata.snapshot_by_id(entry.snapshot_id)
       print(entry.snapshot_id, snap.summary.operation, snap.parent_snapshot_id)
   ```
   - **1 new snapshot** (a single commit covering both the removed and added files) -> JVM
     Iceberg is atomic here, pyiceberg's 2-commit behavior is confirmed pyiceberg-specific.
   - **2 new snapshots** (DELETE then APPEND, same shape as pyiceberg) -> the gap is an
     Iceberg-format/protocol-level reality, not a pyiceberg bug, and every writer path
     (including Databricks) needs to be assumed to have the same reader-visible gap.

   Leading hypothesis is "1" (Java's older, more optimized `OverwriteFiles` API vs pyiceberg's
   younger, independent reimplementation) - but that's an informed guess, not yet verified
   against a real JVM in this environment. This test is what actually settles it either way.

   **Test order:** read-only first (`spark.sql("SELECT * FROM jdbc_catalog.t1.resale_flat_prices")`)
   before ever writing from both sides - proves compatibility at zero risk. Any bugs that turn
   up get reconciled between the two catalog implementations (schema-version mismatch, path/URI
   format differences, etc.) rather than worked around by giving JVM Iceberg its own separate
   warehouse copy - the whole point is one shared source of truth, not two catalogs each with
   their own datasets to keep in sync.

---

## `write_partition_guarded()` check #5 (type change) gap - review (2026-09-13)

**Verified empirically, corrects an earlier wrong claim:** iceberg's native
`_check_pyarrow_schema_compatible` does NOT silently allow "safe" numeric widening
(int32 table -> int64 incoming data) - it rejects it, same mechanism and same error
shape as any other type mismatch. So there's no case where our check #5 is *stricter*
than iceberg for numeric widening - they're aligned. (Originally claimed the opposite;
disproven by a clean, isolated test - logging the corrected finding, not the wrong one.)

**What the same test actually found, real gap worth reviewing:** iceberg's native check
also rejects a REQUIRED-vs-optional mismatch on an otherwise-identical column (same
`.type`, different nullability) - confirmed via a clean test (`era: required string` vs
incoming `era: optional string`, `ValueError: Mismatch in fields`). But our own check #5
only compares `current_schema.field(n).type != incoming.field(n).type` - it never looks at
`.nullable`/required-ness at all. So a same-typed, nullability-only mismatch currently
falls into the "loud" bucket (iceberg catches it) *by accident*, not because it's one of
the five named checks - unlike every other loud case, which we deliberately chose to make
explicit anyway (see write_partition_guarded()'s docstring). Decide: add an explicit
sixth check for nullability mismatches (mirroring check #5's shape), or leave it as an
unnamed case iceberg happens to catch on its own.

---

## CRITICAL: tbl.overwrite() is NOT a single atomic commit - it's two (2026-09-13)

**Corrects a foundational claim stated as fact since early this session.** `write_partition()`'s
docstring used to say `overwrite(df, overwrite_filter=...)` does the delete + append "as ONE
atomic commit (one snapshot)... never a gap where the partition is emptied but not yet
refilled." That claim is WRONG for this pyiceberg version, verified by direct testing, not
assumed.

**What actually happens**, confirmed via `tbl.history()` + per-snapshot `.summary.operation`:
a call to `write_partition()`/`overwrite()` against a partition that already has matching
data produces **two separate snapshots**, not one:
```
snapshot N    op=DELETE   (removes the old matching file(s))
snapshot N+1  op=APPEND   (adds the new file(s))    <- current_snapshot() after the call
```
(A *first-ever* write to a partition with nothing to delete only produces one - the append -
which is why this went unnoticed for so long; every verification test this session happened
to be a fresh table or a fresh partition value.)

**The gap is real and reader-visible**, not just an implementation curiosity - confirmed
directly:
```python
mid_snapshot_id = tbl.history()[-2].snapshot_id   # the DELETE snapshot
tbl.scan(snapshot_id=mid_snapshot_id).to_arrow().to_pylist()
# -> []  (the partition is genuinely, visibly EMPTY at this point in history)
```
Any concurrent reader (a dashboard, a Databricks SQL warehouse, an analyst query, another
pipeline stage) that happens to query the table in the ~5-50ms window between these two
commits sees the partition as empty - real, if narrow, data loss from the reader's
perspective. This is exactly the failure mode the original (wrong) docstring claimed was
architecturally impossible.

**Not yet investigated - next step if this matters for real concurrent access:** whether
pyiceberg's lower-level `Transaction` API (`with tbl.transaction() as txn: txn.delete(...);
txn.append(...)`, committing once at context-exit) produces a genuinely single commit where
the convenience `tbl.overwrite()` wrapper doesn't - untested, a real hypothesis worth checking
before assuming this can't be fixed. Given this project's actual usage pattern is a batch
pipeline (writer runs, finishes, then readers query - not continuous concurrent access), this
may be low real-world risk today, but it's a correctness gap worth knowing about explicitly
rather than continuing to believe the disproven "one atomic commit" claim.

---

## `write_partition_guarded` unit tests - NON-NEGOTIABLE (2026-09-15)

**Why:** `write_partition_guarded` (both `helper_pyarrow_io.py` and `helper_pyiceberg_io.py`)
now has real branching surface - `on_newcols` (`'evolve'/'push'`, `'drop'`, `'error'`) x
`on_missingcols` (`'pad_null'`, `'error'`), plus check 0 (null partition keys, always-on),
plus two near-identical engine implementations that have to stay in sync. Already found one
real gap by hand in the REPL (the original block 5 demo never actually exercised `pad_null` -
`on_newcols='error'` on the first call prevented the table from ever getting wide enough to
have anything genuinely missing). Without tests, the next edit to either helper has no way to
prove it didn't silently break one of the other paths.

Also the difference between "documented" and "not a black box to another engineer": a
docstring describes intended behavior and has to be trusted; a passing test proves it, re-
verified on every run.

**Task: learn how to write unit tests** (pytest), using this exact surface as the vehicle -
`modules/pipe_hdb/src/unit_tests/1_import_to_t1 unit_test_for_write_partition_guarded.py`
already exists as the REPL-demo starting point, not yet real pytest.

Shape to convert into:
- `tmp_path` fixture (pytest's own disposable temp dir) instead of `datalake/hive/...` or
  `datalake/iceberg` - fresh every test, auto-cleaned, no `double_safe_purge` /
  interactive-confirmation problem, no shared state between runs or test order dependence.
- `@pytest.mark.parametrize` over the 6 `(on_newcols, on_missingcols)` combos instead of 6
  near-duplicate hand-written blocks - one test function, one source of truth for what each
  combo is expected to do.
- Fix the block-5 gap in the conversion, not just carry it forward: that block needs its own
  widen-first setup call (`evolve`), not the shared `_reset_demo_table()` (which always resets
  to the narrow `data_old` schema), before the two calls that actually assert `error`/`pad_null`.
- Cover check 0 (null partition key) and check 4 (type mismatch) too, not just the two
  behaviour toggles - both are currently only verified by one-off scratch runs earlier this
  session, not committed tests.
- Mirror the same test shape across both `helper_pyarrow_io` and `helper_pyiceberg_io` -
  the whole point of today's renames (`on_newcols`/`on_missingcols`/`'error'` identical in
  both) was to make this possible without two different test vocabularies.

---

## Separate checker-print stdout from computational noise - PARKED, reverted (2026-09-18)

**The problem, real and still open:** the new diagnostic "checker" prints added this session
(`args_added: [...]` in `add_provider_args`, `resolved args: {args}` in `1_import_to_t1.py`,
`[provision] env=... -> ...` in `0_run_pyiceberg_provision.py`) get visually lost inside the
much heavier computational stdout a real run produces (Spark schema printouts, per-era row
counts, pyiceberg/pyarrow chatter). Confirmed directly from a real IPython run's output.

**Proposed fix, worked through but not applied:**
1. Route only the checker prints to `sys.stderr` (`print(..., file=sys.stderr)`) - leaves
   every pre-existing business-logic print (row counts, era lists, `RUNTIME SUMMARY`, etc.)
   untouched on stdout, since those ARE wanted at runtime, just not what's getting lost.
2. In `run.sh`, wrap the whole `case "$CALL_TASK" in ... esac` block in a single
   `{ ... } 1>"$LOG_FILE"` group - ONE redirect point that every entry path (every `make`
   target via `$(RUN)`, and any direct `./run.sh <task>` call) automatically inherits, no
   per-target Makefile edits needed. stderr (the checker prints) stays live in the terminal;
   stdout (the noisy stream) gets captured to `run.log`. The `*)` usage-error block already
   prints via `cat >&2`, so it's unaffected by the stdout-only redirect either way.

**Status: reverted, not applied.** Step 2 was implemented in `run.sh` and immediately reverted
at the user's explicit instruction ("i dont want to do it reverse back my run.sh") - confirmed
clean revert, zero trace left (`grep LOG_FILE run.sh` empty). Step 1 (the `file=sys.stderr`
conversions) was never applied to any file. Current state: all checker prints still go to
plain stdout, `run.sh` is byte-identical to before this thread started.

**Open for next time:** the underlying problem (checker prints drowned out) is still real and
unaddressed - only the specific run.sh-redirect mechanism was rejected. Worth asking what
approach *would* be acceptable before proposing another one - e.g. plain `sys.stderr` alone
(no run.sh change at all, just visually distinguishable in a terminal that colors stderr
differently) might already be enough without touching the Makefile/run.sh layer at all.
