#!/usr/bin/env bash
set -Eeuo pipefail
cd /home/alex/code-repo/ChomoSyncer-go
LOG_DIR=${LOG_DIR:-./logs}
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

# 用进程替换而不是管道:脚本退出码 = 采集器退出码(管道会丢掉它)
exec ./bin/chomosyncer-go -config config.yaml -backfill-offline-only \
  > >(exec rotatelogs -e -n 30 "$LOG_DIR/backfill.log" 100M) 2>&1

# exec ./bin/chomosyncer-go -config config.yaml -backfill-offline-only \
#   > >(exec rotatelogs -e "$LOG_DIR/backfill-%Y%m%d_%H%M%S.log" 100M) 2>&1


# USAGE:
# nohup ./run-backfill.sh >/dev/null 2>&1 &