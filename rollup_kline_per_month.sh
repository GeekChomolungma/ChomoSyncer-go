#!/usr/bin/env bash
# Fold the 1m kline history into the rollup tables (5m/15m/1h/4h/1d), one calendar month at a time,
# by running deploy/clickhouse/003_rollup_backfill.sql per month. Idempotent; run it AFTER
# 002_kline_rollups.sql. The open-interest counterpart is rollup_oi_per_month.sh.
#
#   ./rollup_kline_per_month.sh --password '<pw>'
#   ./rollup_kline_per_month.sh --password '<pw>' --from 2024-01-01 --to 2026-10-01
#   CLICKHOUSE_PASSWORD='<pw>' ./rollup_kline_per_month.sh --dry-run
set -u

HOST=127.0.0.1
PORT=9000
USER_NAME=default
PASSWORD=${CLICKHOUSE_PASSWORD:-}
FROM=2000-01-01
TO=2029-10-01
DRY_RUN=0
SQL=deploy/clickhouse/003_rollup_backfill.sql

usage() {
  cat <<USAGE
Usage: $0 [options]
  --host H         ClickHouse host (default $HOST)
  --port P         ClickHouse native port (default $PORT)
  --user U         ClickHouse user (default $USER_NAME)
  --password PW    ClickHouse password (default: \$CLICKHOUSE_PASSWORD, else empty)
  --from DATE      first month to fold, YYYY-MM-DD, a month start (default $FROM)
  --to DATE        stop before this month start (exclusive, default $TO)
  --dry-run        print what would run, execute nothing
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --host) HOST=$2; shift 2 ;;
    --port) PORT=$2; shift 2 ;;
    --user) USER_NAME=$2; shift 2 ;;
    --password) PASSWORD=$2; shift 2 ;;
    --from) FROM=$2; shift 2 ;;
    --to) TO=$2; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$FROM$TO" in *[!0-9-]*) echo "--from/--to must be YYYY-MM-DD" >&2; exit 2 ;; esac
case "$FROM" in *-01) ;; *) echo "--from must be a month start (YYYY-MM-01): a range that cuts a bucket writes a partial one" >&2; exit 2 ;; esac
case "$TO" in *-01) ;; *) echo "--to must be a month start (YYYY-MM-01)" >&2; exit 2 ;; esac

m=$FROM
while [ "$m" \< "$TO" ]; do
  nxt=$(date -u -d "$m +1 month" +%F)
  echo ">>> $m .. $nxt"
  if [ "$DRY_RUN" = 1 ]; then
    echo "    clickhouse-client --host $HOST --port $PORT --user $USER_NAME --password *** --param_m_start='$m 00:00:00' --param_m_end='$nxt 00:00:00' --queries-file $SQL"
  else
    clickhouse-client --host "$HOST" --port "$PORT" --user "$USER_NAME" --password "$PASSWORD" \
      --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00" \
      --queries-file "$SQL" || { echo "failed at month $m" >&2; exit 1; }
  fi
  m=$nxt
done
