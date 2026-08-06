#!/bin/sh
set -e

# gh auth login 
# gh repo list --limit 100
# gh repo clone https

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done




# git checkout qa
# rm -rf venv
python3.13 -m venv venv

if [ -f venv/bin/activate ]; then
    source venv/bin/activate
else
    source venv/Scripts/activate
fi

pip install -r requirements_dev_commons.txt
pip install --upgrade pip