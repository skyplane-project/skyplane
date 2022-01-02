#!/bin/bash
cd $(dirname $0)/..

set -x
pip install -e .
pip install pytype black pylint

black --line-length 140 skylark

# run pylint and write html report data/pylint_out.txt
pylint $(git ls-files '*.py') --rcfile=.pylintrc > data/pylint_out.txt

# run pytype and write output to data/pytype_out.txt
pytype skylark &> data/pytype_out.txt
