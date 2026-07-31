#!/bin/sh
# gh auth login 
# gh repo list --limit 100
# gh repo clone murftech/my-massive-app prod-finance

# run this
# chmod +x startup.sh

cd "$(dirname "$0")"
while [ ! -d .git ] && [ "$PWD" != "/" ]; do cd ..; done


# git checkout qa
# rm -rf venv
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements_dev_commons.txt
pip install -e /Users/murftech/Root/production/packages
pip install --upgrade pip
