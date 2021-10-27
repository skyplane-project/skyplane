#!/bin/bash
set -ex
sudo apt-get update
sudo apt-get install -y curl wget git vim python3 python3-pip iperf iperf3

sudo pip3 install --upgrade pip setuptools virtualenv

if [ -d ~/venv ]; then
    rm -rf ~/venv
fi
virtualenv -p python3 ~/venv

# install requirements.txt in the virtualenv
source ~/venv/bin/activate
pip install -r requirements.txt