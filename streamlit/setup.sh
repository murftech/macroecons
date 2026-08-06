#!/bin/bash
set -e

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


rm -rf streamlit/venv
python3.13 -m venv streamlit/venv

if [ -f venv/bin/activate ]; then
    source streamlit/venv/bin/activate
else
    source streamlit/venv/Scripts/activate
fi


pip install -r streamlit/requirements.txt
pip install --upgrade pip
