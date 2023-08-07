#!/bin/bash

set -ex

mkdir -p dist
rm -r dist/*

python -m build

twine upload dist/*