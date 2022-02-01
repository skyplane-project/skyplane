#!/bin/bash
# install coin-or and cylp on ubuntu
set -xe
if [ -x "$(command -v conda)" ]; then
    conda install -y coin-or-cbc numpy pkg-config -c conda-forge
    python -m pip install cylp
else
    echo "conda is not installed, installing via pip"
    sudo apt-get install -y coinor-cbc build-essential
    python -m pip install cylp
    exit 1
fi