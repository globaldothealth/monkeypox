#!/bin/bash
set -eo pipefail

poetry run python3 -m pytest -rs -vv .
