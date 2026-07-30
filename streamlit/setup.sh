#!/bin/bash
set -e

# run this from the repo root: ./streamlit/setup.sh
# then activate separately in your own shell: source streamlit/venv/bin/activate
# (sourcing inside this script wouldn't persist to your outer shell anyway)

cd "$(dirname "$0")/.."
rm -rf streamlit/venv
python3.13 -m venv streamlit/venv
streamlit/venv/bin/pip install --upgrade pip
streamlit/venv/bin/pip install -r streamlit/requirements.txt
