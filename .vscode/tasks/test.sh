#!/bin/bash
set -ex

pytest -s --cov=remfile --cov-report=xml --cov-report=term tests/