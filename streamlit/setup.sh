#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


rm -rf streamlit/venv
python3.13 -m venv streamlit/venv
source streamlit/venv/bin/activate
streamlit/venv/bin/pip install -r streamlit/requirements.txt
streamlit/venv/bin/pip install --upgrade pip
