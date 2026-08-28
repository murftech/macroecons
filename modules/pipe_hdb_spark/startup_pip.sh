#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done

echo $(pwd)
# # deliberately 3.13, not 3.14 like streamlit/setup.sh - polars only officially supports
# # up to 3.13 as of writing, and 3.14 + polars segfaults on this machine
# rm -rf modules/pipe_hdb/venv
python3.13 -m venv modules/pipe_hdb/.venv

if [ -f venv/bin/activate ]; then
    source modules/pipe_hdb/.venv/bin/activate
else
    source modules/pipe_hdb/.venv/Scripts/activate
fi


pip install -r modules/pipe_hdb/requirements.txt
# pip install --upgrade pip
