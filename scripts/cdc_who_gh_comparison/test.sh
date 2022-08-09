#!/bin/bash
set -eo pipefail

poetry run python3 setup.py
poetry run python3 -m pytest -rs -vv .