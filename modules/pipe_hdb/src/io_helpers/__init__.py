"""Engine read/write mechanics + the shared schema guards - one write path per
format/engine combination, side by side (parquet, pyiceberg/Sail, sparkiceberg/JVM,
sparkdelta/JVM), plus shared_schema_guards_arrow[_spark] which several of them import
from. Namespace only - no re-exports, same convention as providers/__init__.py.
"""
