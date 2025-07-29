#!/bin/bash
# Load environment variables from .env and run dbt from local profile

set -a
source .env
set +a

export DBT_PROFILES_DIR="$(pwd)/profiles"

if [ -z "$1" ]; then
  echo "Usage: ./run-dbt.sh [dbt-command]"
  echo "Example: ./run-dbt.sh run"
  exit 1
fi

dbt "$@"

