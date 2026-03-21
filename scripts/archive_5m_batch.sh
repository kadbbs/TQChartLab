#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${ROOT_DIR}/data/duckdb/ticks.duckdb"
LOG_PATH="${1:-${ROOT_DIR}/logs/archive_multi_5m.log}"
START_DATE="${START_DATE:-2024-03-21}"
END_DATE="${END_DATE:-2026-03-21}"

wait_for_db_unlock() {
  while lsof "$DB_PATH" | awk 'NR>1 {print $1}' | grep -q .; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] db busy: $(lsof "$DB_PATH" 2>/dev/null | tail -n +2 | tr '\n' ';')"
    sleep 10
  done
}

run_job() {
  local exchange_id="$1"
  local product_id="$2"
  local name="$3"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] start ${name} (${exchange_id}.${product_id})"
  "${ROOT_DIR}/myvenv/bin/python" "${ROOT_DIR}/scripts/archive_time_bars_duckdb.py" \
    --db-path "${DB_PATH}" \
    --provider tq \
    --exchange-id "${exchange_id}" \
    --product-id "${product_id}" \
    --duration-seconds 300 \
    --start-date "${START_DATE}" \
    --end-date "${END_DATE}"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] finish ${name} (${exchange_id}.${product_id})"
}

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] waiting for duckdb lock release on ${DB_PATH}"
  wait_for_db_unlock
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] db lock released, start batch archive"
  run_job "SHFE" "rb" "螺纹钢"
  run_job "CZCE" "TA" "PTA"
  run_job "DCE" "jm" "焦煤"
  run_job "CZCE" "MA" "甲醇"
  run_job "DCE" "c" "玉米"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] all batch jobs done"
} >> "${LOG_PATH}" 2>&1
