#!/bin/sh
# gh auth login 
# gh repo list --limit 100
# gh repo clone murftech/my-massive-app prod-finance

# run this
# chmod +x startup.sh

# git checkout qa
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e /Users/murftech/Root/production/packages
pip install --upgrade pip
