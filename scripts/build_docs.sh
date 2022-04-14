#!/bin/bash
pip install -r docs/requirements-doc.txt
sphinx-build -b html docs docs/_build/html