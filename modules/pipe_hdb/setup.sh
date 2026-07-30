#!/bin/bash
set -e

# run this from the repo root: ./modules/pipe_hdb/setup.sh

# deliberately 3.13, not 3.14 like streamlit/setup.sh - polars only officially supports
# up to 3.13 as of writing, and 3.14 + polars segfaults on this machine
rm -rf modules/pipe_hdb/venv
python3.13 -m venv modules/pipe_hdb/venv
source modules/pipe_hdb/venv/bin/activate
pip install --upgrade pip
pip install -r modules/pipe_hdb/requirements.txt
