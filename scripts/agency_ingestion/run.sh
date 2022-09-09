#!/bin/bash
set -eo pipefail

if [[ -v LOCALSTACK_URL ]]; then
	echo "Localstack configured, running setup script"
	poetry run python3 setup.py
fi

poetry run python3 run.py
