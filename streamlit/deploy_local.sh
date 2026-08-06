#!/bin/bash
set -e

# forces cwd to the repo root regardless of where this script is invoked from - docker build
cd "$(dirname "$0")/.."

if [ -f venv/bin/activate ]; then
    source streamlit/venv/bin/activate
else
    source streamlit/venv/Scripts/activate
fi

streamlit run streamlit/app.py
