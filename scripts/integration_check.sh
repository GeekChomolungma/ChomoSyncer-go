#!/usr/bin/env bash
# Forward integration check for a running ChomoSyncer-go against real Binance.
#
# Assumes the stack is already up:
#   docker compose -f deploy/docker-compose.yml --profile app up -d --build
#
# It samples /metrics twice (SETTLE apart), asserts the pipeline counters moved,
# and spot-checks Redis + ClickHouse content.
#
# Deps: curl, docker (for `docker compose exec`). Override with env vars:
#   METRICS_URL   default http://localhost:9090
#   CH_HTTP       default http://localhost:8123
#   COMPOSE       default "docker compose -f deploy/docker-compose.yml"
#   SETTLE        default 90   (seconds between samples; >=70 to catch a 1m close)
set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:9090}"
CH_HTTP="${CH_HTTP:-http://localhost:8123}"
COMPOSE="${COMPOSE:-docker compose -f deploy/docker-compose.yml}"
SETTLE="${SETTLE:-90}"

pass=0; fail=0
ok()   { echo "  PASS  $*"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $*"; fail=$((fail+1)); }

# sum of a counter family (all label sets), 0 if absent
metric_sum() {
  curl -sf "$METRICS_URL/metrics" \
    | awk -v k="$1" '$1 ~ "^"k"([{ ]|$)" {s+=$2} END {printf "%.0f", s+0}'
}
redis_cli() { $COMPOSE exec -T redis redis-cli "$@"; }
ch_q()      { curl -sf "$CH_HTTP/?database=market" --data-binary "$1"; }

echo "== 1. liveness =="
curl -sf "$METRICS_URL/healthz" >/dev/null && ok "/healthz" || { bad "/healthz unreachable"; exit 1; }
shards=$(metric_sum ws_shards_active)
[ "$shards" -gt 0 ] && ok "ws_shards_active=$shards" || bad "no active WS shards"
usz=$(metric_sum universe_size)
[ "$usz" -gt 0 ] && ok "universe_size=$usz" || bad "universe_size=0 (REST discovery failed?)"
down=$(curl -sf "$METRICS_URL/metrics" | awk '/^ws_connection_status\{/ && $2==0 {n++} END{print n+0}')
[ "$down" -eq 0 ] && ok "all shards connected" || bad "$down shard(s) show ws_connection_status=0"

echo "== 2. sampling counters over ${SETTLE}s =="
declare -A before
for m in kline_ingested_total redis_bars_pushed_total redis_livebar_updates_total \
         clickhouse_rows_flushed_total dispatcher_section_published_total; do
  before[$m]=$(metric_sum "$m")
done
sleep "$SETTLE"
for m in "${!before[@]}"; do
  now=$(metric_sum "$m"); d=$((now - before[$m]))
  [ "$d" -gt 0 ] && ok "$m +$d" || bad "$m did not advance (before=${before[$m]} now=$now)"
done

echo "== 3. drops / errors (should stay flat) =="
for m in dispatcher_events_dropped_total redis_livebar_dropped_total \
         collector_dispatch_errors_total dispatcher_section_publish_errors_total; do
  v=$(metric_sum "$m")
  [ "$v" -eq 0 ] && ok "$m=0" || bad "$m=$v (backpressure / downstream errors)"
done
buf=$(metric_sum clickhouse_buffer_size)
[ "$buf" -lt 20000 ] && ok "clickhouse_buffer_size=$buf" || bad "clickhouse_buffer_size=$buf >= 20000 (CH write can't keep up)"

echo "== 4. Redis content =="
llen=$(redis_cli --no-raw LLEN kline:BTCUSDT:1m | tr -d '"')
[ "${llen:-0}" -gt 0 ] && [ "${llen:-0}" -le 200 ] && ok "LLEN kline:BTCUSDT:1m=$llen (<=200)" || bad "kline:BTCUSDT:1m LLEN=$llen"
hx=$(redis_cli HGET livebar:BTCUSDT:1m c | tr -d '"')
[ -n "${hx:-}" ] && ok "livebar:BTCUSDT:1m close=$hx" || bad "livebar:BTCUSDT:1m missing"
xlen=$(redis_cli --no-raw XLEN stream:market:kline_ready | tr -d '"')
[ "${xlen:-0}" -gt 0 ] && ok "XLEN stream:market:kline_ready=$xlen" || bad "kline_ready stream empty"

echo "== 5. ClickHouse content =="
cnt=$(ch_q "SELECT count() FROM fapi_kline_1m" | tr -d '[:space:]')
[ "${cnt:-0}" -gt 0 ] && ok "fapi_kline_1m rows=$cnt" || bad "fapi_kline_1m empty"
lag=$(ch_q "SELECT toInt64(now() - max(start_time)) FROM fapi_kline_1m" | tr -d '[:space:]')
[ "${lag:-9999}" -lt 180 ] && ok "newest 1m bar ${lag}s old" || bad "newest 1m bar ${lag}s old (stale)"
bad_ohlc=$(ch_q "SELECT count() FROM fapi_kline_1m WHERE high < low OR high < open OR high < close OR low > open OR low > close" | tr -d '[:space:]')
[ "${bad_ohlc:-1}" -eq 0 ] && ok "OHLC invariants hold" || bad "$bad_ohlc rows violate high>=max(o,c) / low<=min(o,c)"

echo
echo "== result: $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
