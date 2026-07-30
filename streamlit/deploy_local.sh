#!/bin/bash
set -e

# forces cwd to the repo root regardless of where this script is invoked from - docker build
cd "$(dirname "$0")/.."

source streamlit/venv/bin/activate
streamlit run streamlit/app.py
