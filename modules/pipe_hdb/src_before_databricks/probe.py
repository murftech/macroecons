"""THROWAWAY. Answers the questions phase 2's design depends on, then gets deleted.

Run this as a one-task job with NO `dependencies` block in the environment spec, so
it reports what environment_version ships on its own. Adding `-r requirements-
databricks.txt` would make the version list describe our own pins instead of the base,
which is the opposite of what we need to know.

Everything here is a print. It writes nothing, reads no table, and touches no Volume,
so it is safe to run against any catalog/schema - it does not need them at all.
"""

import platform
import sys


def banner(title):
    """the run log is one long undifferentiated stream - these make it skimmable"""
    print()
    print('=' * 70)
    print(f'== {title}')
    print('=' * 70)


# ── python ────────────────────────────────────────────────────────────────────
# environment_version 4 and 5 are both meant to be Python 3.12.3, and pyproject
# pins requires-python to '==3.12.*'. if this disagrees, the lock was resolved
# against a different interpreter than the one that will run the code.

banner('python')
print(sys.version)
print(platform.platform())


# ── __file__ and argv ─────────────────────────────────────────────────────────
# spark_python_task runs the file through an internal exec() wrapper, so __file__
# is expected to be undefined. both pipeline scripts currently do
# `Path(__file__).resolve().parent` and walk up looking for .git - if this prints
# a NameError, that whole approach has to be replaced by --src-dir, and this is
# the evidence.

banner('__file__ and argv')
try:
    print(f'__file__ = {__file__}')
except NameError as e:
    print(f'__file__ raised NameError: {e}   <- --src-dir is mandatory, not optional')

print(f'sys.argv = {sys.argv}')


# ── spark session ─────────────────────────────────────────────────────────────
# getOrCreate() returns the session the platform already made; it does not build
# a new one the way it does on the laptop.

banner('spark session')
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print(f'spark.version = {spark.version}')

# the three settings helper_getspark.py sets locally. reading them here says
# whether they are already correct, settable, or rejected outright on serverless.
for key in (
    'spark.sql.ansi.enabled',
    'spark.sql.session.timeZone',
    'spark.sql.sources.partitionOverwriteMode',
):
    try:
        print(f'{key} = {spark.conf.get(key)}')
    except Exception as e:
        print(f'{key} -> {type(e).__name__}: {e}')


# ── sparkContext ──────────────────────────────────────────────────────────────
# THE EXPECTED FIRST FAILURE. helper_getspark.py ends with
# `spark.sparkContext.setLogLevel('ERROR')`, and serverless is Spark Connect,
# where sparkContext is not exposed. both pipeline scripts call get_spark(), so
# if this raises, nothing runs on Databricks until the engine branch goes into
# the helper.

banner('sparkContext (expected to fail on serverless)')
try:
    spark.sparkContext.setLogLevel('ERROR')
    print('sparkContext.setLogLevel OK - helper_getspark.py works unchanged')
except Exception as e:
    print(f'{type(e).__name__}: {e}')
    print('-> get_spark() needs the engine branch before either script can run')


# ── installed versions ────────────────────────────────────────────────────────
# with requirements-databricks.txt installing only polars, every other library
# here comes from the base image at whatever version Databricks chose, while the
# laptop is on pandas 3.0.5 / plotly 7.0.0. this list is how wide that gap is.
#
# polars is the exception and doubles as a test of the install mechanism itself:
# it is the ONLY thing our requirements file adds, so its absence means the -r
# never ran - which would be equally true of any real dependency added later.

banner('installed versions')
from importlib.metadata import PackageNotFoundError, version

for pkg in ('pandas', 'numpy', 'pyarrow', 'plotly', 'narwhals', 'requests', 'pyspark'):
    try:
        print(f'{pkg}=={version(pkg)}')
    except PackageNotFoundError:
        print(f'{pkg}: NOT INSTALLED')

try:
    print(f'polars=={version("polars")}   <- proves -r requirements-databricks.txt ran')
except PackageNotFoundError:
    print('polars: NOT INSTALLED   <- expected when running the probe with no '
          'dependencies block; a problem once the real job declares one')


# ── to_date under ANSI ────────────────────────────────────────────────────────
# the divergence that actually matters. 0_import_datagov.py's docstring claims a
# bad month RAISES rather than silently becoming null, and that claim rests on
# ANSI mode being on - true locally under Spark 4.0.1.
#
# asking the engine the question directly beats trusting spark.sql.ansi.enabled:
# Databricks sets defaults per runtime, and what the config reports and what the
# expression does are not guaranteed to be the same story.

banner('to_date on a bad value')
from pyspark.sql.functions import col, to_date

df = spark.createDataFrame([('2024-01',), ('not-a-month',)], ['month'])
try:
    rows = df.withColumn('tx_monthdate', to_date(col('month'), 'yyyy-MM')).collect()
    print('NO EXCEPTION - ANSI is off here')
    print(f'rows = {rows}')
    print('-> a malformed month becomes null instead of stopping the run.')
    print('   step 0 does not have the guard its docstring describes.')
except Exception as e:
    print(f'RAISED (ANSI on, matches local): {type(e).__name__}')
    print(f'{e}')
    print('-> step 0 behaves the same here as on the laptop. nothing to change.')


# ── can we read the REMOTE java version? ──────────────────────────────────────
# On Connect `spark.sparkContext._jvm` is gone, so the local trick is unavailable.
# But "the JVM is remote" only means we cannot CHANGE it - whether we can READ it is
# a separate question, and these three are the candidates. All three execute
# server-side, so none of them depend on a JVM gateway.
#
# Locally, java_method returns '17.0.20.1'. Databricks is understood to restrict
# java_method on shared/serverless compute (arbitrary Java reflection from SQL is a
# natural thing to lock down) - but that is absorbed, not verified, which is exactly
# what this section is for.

banner('remote java version - can we read it?')

for label, statement in (
    ('java_method(java.version)',
     "SELECT java_method('java.lang.System','getProperty','java.version') AS v"),
    ('java_method(java.vendor)',
     "SELECT java_method('java.lang.System','getProperty','java.vendor') AS v"),
    ('current_version()',
     'SELECT current_version() AS v'),
):
    try:
        rows = spark.sql(statement).collect()
        print(f'{label:<28} -> {rows[0][0]}')
    except Exception as e:
        # print the type only; the full message drags a JVM stacktrace into the log
        print(f'{label:<28} -> BLOCKED {type(e).__name__}')

# the conf route: works on classic clusters, expected to be refused on serverless
# the same way partitionOverwriteMode is.
for key in ('spark.databricks.clusterUsageTags.sparkVersion',
            'spark.databricks.clusterUsageTags.clusterId'):
    try:
        print(f'{key:<48} -> {spark.conf.get(key)}')
    except Exception as e:
        print(f'{key:<48} -> {type(e).__name__}')


# ── helper_getspark ──────────────────────────────────────────────────────────
# the actual candidate replacement for helper_getspark.py. everything above this
# line measures the runtime; this measures OUR code against it.
#
# get_spark() calls getOrCreate(), which returns the session already built above
# rather than a new one - so this is genuinely testing "apply our settings to a
# live serverless session", which is exactly what the pipeline scripts will do.
#
# sibling import, no --src-dir needed: sys.path[0] is the script's own directory,
# and sys.argv[0] above confirms that directory is ${SRC_REMOTE}/src.

banner('helper_getspark (the candidate)')
try:
    from helper_getspark import get_spark, stop_spark

    spark2 = get_spark('probe-getspark2')
    print()
    print('-- read back what actually stuck --')
    for key in (
        'spark.sql.session.timeZone',
        'spark.sql.sources.partitionOverwriteMode',
        'spark.sql.execution.arrow.pyspark.enabled',
    ):
        try:
            print(f'  {key} = {spark2.conf.get(key)}')
        except Exception as e:
            print(f'  {key} -> {type(e).__name__} (unreadable, as expected here)')

    # the one operation the pipeline actually needs the session for
    probe_df = spark2.createDataFrame([('2024-01',)], ['month'])
    print(f"\n  to_date works: {probe_df.withColumn('d', to_date(col('month'), 'yyyy-MM')).collect()}")

    # UNVERIFIED on serverless - this run is what verifies it
    print()
    stop_spark(spark2)

except Exception as e:
    print(f'helper_getspark FAILED: {type(e).__name__}: {e}')


banner('probe complete')
