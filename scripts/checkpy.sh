#!/bin/bash
cd $(dirname $0)/..

set -xe
pip install -e .
pip install pytype black
black --line-length 140 skylark
pytype skylark
