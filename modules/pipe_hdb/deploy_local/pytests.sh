uv run pytest deploy_local/unit_tests/                 # whole suite
uv run pytest deploy_local/unit_tests/ -k iceberg       # only files/tests matching "iceberg"
uv run pytest deploy_local/unit_tests/ -x               # stop at the first failure
uv run pytest deploy_local/unit_tests/ -v               # show every test name, not just dots
uv run pytest deploy_local/unit_tests/ --lf             # re-run only what failed last time

uv run pytest deploy_local/unit_tests/ -k error__narrow_T1_shaped_batch_genuinely_fails -v

-k "pad_null__new_col_evolves"

uv run pytest "deploy_local/unit_tests/test_write_partition_guarded_pyarrow.py::test_guard_combo[drop-error__narrow_T1_shaped_batch_genuinely_fails]" -v