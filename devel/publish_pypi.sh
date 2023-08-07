#!/bin/bash

set -ex

rm -rf dist/*

python -m build

twine upload dist/*