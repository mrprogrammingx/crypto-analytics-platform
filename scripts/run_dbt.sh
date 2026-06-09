#!/usr/bin/env bash
set -euo pipefail

# Script to run `dbt run` while automatically sourcing the repository .env
# Usage: bash scripts/run_dbt.sh [dbt-args]
# Examples:
#   bash scripts/run_dbt.sh           # runs `dbt run`
#   bash scripts/run_dbt.sh test      # runs `dbt test`
#   bash scripts/run_dbt.sh -- --profiles-dir . run --select my_model

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  # export all variables from .env into the environment
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

# Prefer the project's venv dbt if it exists
DBT_BIN="$ROOT_DIR/crypto_analytics_dbt/.venv/bin/dbt"
if [ ! -x "$DBT_BIN" ]; then
  DBT_BIN="$(command -v dbt || true)"
  if [ -z "$DBT_BIN" ]; then
    echo "dbt not found. Install dbt or create .venv in crypto_analytics_dbt/.venv" >&2
    exit 1
  fi
fi

cd "$ROOT_DIR/crypto_analytics_dbt"
if [ "$#" -eq 0 ]; then
  # default to run
  exec "$DBT_BIN" run --profiles-dir .
else
  # pass through all args to dbt
  exec "$DBT_BIN" "$@"
fi
