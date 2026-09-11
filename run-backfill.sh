#!/usr/bin/env bash
set -Eeuo pipefail
cd /home/alex/code-repo/ChomoSyncer-go
LOG_DIR=${LOG_DIR:-./logs}
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi

# 日志时间戳统一用 UTC(宿主机 OS 时区是 Asia/Shanghai，Go time.Now() 默认按它打时间戳；
# 改成 UTC 跟 ClickHouse 里 DateTime64(3,'UTC') 存的时间直接对得上，省得每次心算 +8）。
export TZ=UTC

# 用进程替换而不是管道:脚本退出码 = 采集器退出码(管道会丢掉它)
exec ./bin/chomosyncer-go -config config.yaml -backfill-offline-only \
  > >(exec rotatelogs -e -n 30 "$LOG_DIR/backfill.log" 100M) 2>&1

# exec ./bin/chomosyncer-go -config config.yaml -backfill-offline-only \
#   > >(exec rotatelogs -e "$LOG_DIR/backfill-%Y%m%d_%H%M%S.log" 100M) 2>&1


# USAGE:
# nohup ./run-backfill.sh >/dev/null 2>&1 &