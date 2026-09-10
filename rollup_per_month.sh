m="2000-01-01"
while [ "$m" \< "2026-10-01" ]; do
  nxt=$(date -u -d "$m +1 month" +%F)
  echo ">>> $m .. $nxt"
  clickhouse-client --host 127.0.0.1 --port 9000 --user default --password alex \
    --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00" \
    --queries-file deploy/clickhouse/003_rollup_backfill.sql || break
  m="$nxt"
done
