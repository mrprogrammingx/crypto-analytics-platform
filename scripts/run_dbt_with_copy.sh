#!/usr/bin/env bash
set -euo pipefail
# Copy the repo DuckDB warehouse to a temp file and run dbt against the copy to avoid locking issues.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DB="${DUCKDB_DATABASE:-$ROOT/warehouse/analytics.duckdb}"

if [ ! -f "$SRC_DB" ]; then
  echo "Source DuckDB file not found: $SRC_DB"
  exit 1
fi

TMPDIR="${TMPDIR:-/tmp}"
COPY="$TMPDIR/analytics_copy_$(date +%s).duckdb"
cp "$SRC_DB" "$COPY"

cleanup() {
  rc=$?
  if [ "${KEEP_COPY:-0}" != "1" ]; then
    rm -f "$COPY" || true
  else
    echo "Keeping copy at: $COPY"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

export DUCKDB_DATABASE="$COPY"

cd "$ROOT"
# Forward all args to the existing run_dbt.sh wrapper; it will source .env and run dbt inside the project's venv
bash scripts/run_dbt.sh "$@"
