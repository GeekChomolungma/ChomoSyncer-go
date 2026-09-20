#!/usr/bin/env bash
# Shared helpers for the repair scripts in this directory. Source, do not execute.
#
# Connection (env):
#   CH_HOST      default 127.0.0.1
#   CH_PORT      default 9000  (native protocol, clickhouse-client)
#   CH_USER      default default
#   CH_PASSWORD  default empty
#   CH_DB        default market. The SQL files in ../clickhouse hard-code the
#                `market.` prefix; ch_file rewrites it on the fly, so a fix can
#                be rehearsed against a scratch database first:
#                  CH_DB=market_rehearsal ./001_fix_kline_rollup_lookback.sh ...

CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DB="${CH_DB:-market}"

FIX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CH_SQL_DIR="$(cd "$FIX_DIR/../clickhouse" && pwd)"

command -v clickhouse-client >/dev/null 2>&1 || { echo "clickhouse-client not found in PATH" >&2; exit 2; }

# ch [clickhouse-client args...]  — run a query (SQL via -q or stdin).
ch() {
  clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER" \
    ${CH_PASSWORD:+--password "$CH_PASSWORD"} "$@"
}

# ch_file FILE [clickhouse-client args...]  — run a whole SQL file, rewriting the
# `market.` schema prefix to $CH_DB. Extra args (e.g. --param_m_start=...) pass through.
ch_file() {
  local f="$1"; shift
  sed "s/market\\./${CH_DB}./g; s/DATABASE IF NOT EXISTS market/DATABASE IF NOT EXISTS ${CH_DB}/" "$f" \
    | ch --multiquery "$@"
}

log() { printf '%s %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }
die() { echo "ERROR: $*" >&2; exit 1; }
