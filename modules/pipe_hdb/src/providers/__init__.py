"""Environment providers for the pipe_hdb scripts.

The runtime branch is in each SCRIPT (app.py style): the script detects
IS_DATABRICKS / IS_IPYTHON / IS_SH and does an explicit

    if IS_DATABRICKS:
        from providers.databricks import add_provider_args, get_landing_dir, ...
    else:
        from providers.local import add_provider_args, get_landing_dir, ...

so the branching is visible where it's read. This package is intentionally just a
namespace - no detection, no re-exports.
"""
