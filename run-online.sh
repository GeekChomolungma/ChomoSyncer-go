#!/usr/bin/env bash
# Live service: WS collector + dispatcher + Redis writers + ClickHouse archive.
# Counterpart of run-backfill.sh. The ONLY difference is no -backfill-offline-only,
# so config.yaml's backfill.offline_only (false) takes effect and the full online
# pipeline runs. On SIGTERM the process drains and flushes gracefully.
#
# Run the offline gap-fill (run-backfill.sh) to completion and verify with
# cmd/test-tools/check_clickhouse_integrity.py BEFORE starting this.
#
# USAGE
#   nohup ./run-online.sh >/dev/null 2>&1 &          # background
#   tail -F logs/online.log                          # watch
#   curl -s localhost:9090/readyz                    # 200 once cold-start backfill done
#   pkill -TERM -f 'chomosyncer-go -config config.yaml$'   # graceful stop
# (systemd unit is the better long-run option — see docs/OPERATIONS.md §B.2.)

set -Eeuo pipefail
cd /home/alex/code-repo/ChomoSyncer-go
LOG_DIR=${LOG_DIR:-./logs}
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

# Log timestamps in UTC (host OS tz is Asia/Shanghai; Go's time.Now() defaults
# to it). UTC matches ClickHouse's DateTime64(3,'UTC') columns 1:1, no mental +8.
export TZ=UTC

# Process substitution (not a pipe) so this script's exit code == the daemon's.
exec ./bin/chomosyncer-go -config config.yaml \
  > >(exec rotatelogs -e -n 30 "$LOG_DIR/online.log" 100M) 2>&1
