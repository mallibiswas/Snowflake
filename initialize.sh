#!/bin/bash

# If you run in to errors, please refer back to https://docs.getdbt.com/dbt-cli/installation
sudo apt-get update
sudo apt-get install build-essential libssl-dev libffi-dev python-dev
sudo apt-get install libpq-dev
sudo apt-get install python3-dev
sudo apt-get install python3-pip
sudo apt-get install python3-venv
sudo apt-get remove python-cffi
sudo pip3 install --upgrade cffi
python3 -m venv dbt-env             # create the environment
source dbt-env/bin/activate         # activate the environment

pip3 install -r requirements.txt