#!/usr/bin/env bash
# ============================================================================
# 001 — repair kline rollups corrupted by the un-aligned lookback bound.
#
# SYMPTOM
#   fapi_kline_{5m,15m,1h,4h,1d} rows OLDER than the MV lookback window
#   (3 / 3 / 3 / 7 / 10 days) hold only a slice of their bucket — e.g. a 1h
#   volume equal to the volume of the last 1m bar of that hour.
#
# CAUSE
#   002_kline_rollups.sql used `WHERE start_time >= now() - INTERVAL 3 DAY`.
#   The bound cut buckets in half; the partial recompute carried the newest
#   rollup_version and overwrote the correct row (ReplacingMergeTree keeps the
#   highest version). See the header of ../clickhouse/002_kline_rollups.sql.
#
# WHAT THIS SCRIPT DOES  (subcommand `apply`)
#
#   FULL REBUILD  — `apply` with no --from / --to   (the simple, recommended path)
#     1. Applies the fixed 002, whose leading block DROPs all five refreshable MVs
#        and all five derived tables (5m/15m/1h/4h/1d), then recreates them empty.
#        fapi_kline_1m is never touched.
#     2. Re-runs 003 month by month over the WHOLE 1m history, refilling every
#        derived table from fapi_kline_1m FINAL.
#     3. Verifies (`check`).
#     While it runs, the 5m+ tables are empty/partial — readers of 5m+ klines see
#     incomplete data until step 2 finishes. Stopping chomosyncer-go is not
#     required for correctness (it only writes 1m), but is harmless.
#
#   PARTIAL RE-FOLD — `apply --from YYYY-MM [--to YYYY-MM]`
#     Keeps the existing tables (the DROP block of 002 is stripped): drops and
#     recreates only the MVs, then re-folds just that month range. Use it to
#     resume an interrupted full rebuild (pass the month of the last chunk logged)
#     or to re-fold a narrow range.
#
#   Either way: the MVs are dropped and recreated (CREATE ... IF NOT EXISTS would
#   NOT replace a live MV) and this always happens BEFORE the 003 fold — otherwise
#   an old MV re-corrupts buckets that its sliding lower bound passes next.
#
# USAGE
#   ./001_fix_kline_rollup_lookback.sh check [--days N]    read-only; default 10 days
#                                                          (--days 4000 = whole history, slow)
#   ./001_fix_kline_rollup_lookback.sh apply               FULL REBUILD
#   ./001_fix_kline_rollup_lookback.sh apply --from 2026-08 [--to 2026-10]   PARTIAL
#   add --yes to skip the confirmation prompt
#
#   Connection: see lib.sh (CH_HOST / CH_PORT / CH_USER / CH_PASSWORD / CH_DB).
#   Rehearse first:  CH_DB=market_rehearsal ./001_fix_kline_rollup_lookback.sh apply --yes
# ============================================================================
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# label  INTERVAL-expression
INTERVALS=("5m|5 MINUTE" "15m|15 MINUTE" "1h|1 HOUR" "4h|4 HOUR" "1d|1 DAY")

preflight_1m() {
  [ "$(ch -q "EXISTS TABLE ${CH_DB}.fapi_kline_1m")" = "1" ] || die "${CH_DB}.fapi_kline_1m not found"
}

# Derived tables must exist in the 002 layout (partial mode and check need them).
preflight() {
  preflight_1m
  for spec in "${INTERVALS[@]}"; do
    iv="${spec%%|*}"
    has=$(ch -q "SELECT count() FROM system.columns WHERE database='${CH_DB}' AND table='fapi_kline_${iv}' AND name='rollup_version'")
    [ "$has" = "1" ] || die "${CH_DB}.fapi_kline_${iv} missing or in the legacy layout (no rollup_version); this fix assumes the 002 layout"
  done
}

# Are the installed MVs already the fixed version?
mv_state() {
  echo "--- MV definitions (fixed = uses toTimeZone(now(), 'UTC'))"
  ch -q "SELECT name, if(create_table_query LIKE '%toTimeZone(now(), ''UTC'')%', 'fixed', 'OLD') AS state
         FROM system.tables WHERE database='${CH_DB}' AND name LIKE 'fapi_kline\\_%\\_rmv' ORDER BY name FORMAT PrettyCompactMonoBlock"
}

# Count rollup buckets that disagree with a fresh re-aggregation of 1m.
# Only CLOSED buckets are compared (upper bound = start of the current bucket, UTC).
check() {
  local days=10
  while [ $# -gt 0 ]; do case "$1" in --days) days="$2"; shift 2;; *) die "unknown option $1";; esac; done
  preflight
  mv_state
  echo "--- mismatched closed buckets over the last ${days} UTC days (rollup vs re-aggregated 1m)"
  local total=0
  for spec in "${INTERVALS[@]}"; do
    local iv="${spec%%|*}" unit="${spec#*|}"
    local lo="toStartOfDay(toTimeZone(now(), 'UTC') - INTERVAL ${days} DAY)"
    local hi="toStartOfInterval(toTimeZone(now(), 'UTC'), INTERVAL ${unit})"
    local n
    n=$(ch -q "
      SELECT count() FROM
      (
        SELECT symbol, toStartOfInterval(start_time, INTERVAL ${unit}) AS b,
               sum(volume) AS v, max(high) AS h, min(low) AS l, toUInt32(sum(trades_count)) AS n
        FROM ${CH_DB}.fapi_kline_1m FINAL
        WHERE start_time >= ${lo} AND start_time < ${hi}
        GROUP BY symbol, b
      ) AS m
      LEFT JOIN
      (
        SELECT symbol, start_time AS b, volume, high, low, trades_count
        FROM ${CH_DB}.fapi_kline_${iv} FINAL
        WHERE start_time >= ${lo} AND start_time < ${hi}
      ) AS r ON r.symbol = m.symbol AND r.b = m.b
      WHERE abs(r.volume - m.v) > 1e-6 * greatest(1, m.v) OR r.trades_count != m.n OR r.high != m.h OR r.low != m.l")
    printf '%-4s mismatched=%s\n' "$iv" "$n"
    total=$((total + n))
  done
  echo "TOTAL mismatched=$total"
  [ "$total" -eq 0 ]
}

apply() {
  local from="" to="" yes=0 full=1
  while [ $# -gt 0 ]; do case "$1" in
    --from) from="$2"; full=0; shift 2;; --to) to="$2"; full=0; shift 2;; --yes) yes=1; shift;;
    *) die "unknown option $1";; esac; done
  if [ "$full" -eq 1 ]; then preflight_1m; else preflight; fi

  [ -n "$from" ] || from=$(ch -q "SELECT formatDateTime(toStartOfMonth(min(start_time)), '%Y-%m') FROM ${CH_DB}.fapi_kline_1m")
  [ -n "$to" ]   || to=$(ch -q "SELECT formatDateTime(toStartOfMonth(max(start_time)) + INTERVAL 1 MONTH, '%Y-%m') FROM ${CH_DB}.fapi_kline_1m")
  local months
  months=$(python3 - "$from" "$to" <<'PY'
import sys
fy,fm=map(int,sys.argv[1].split('-')); ty,tm=map(int,sys.argv[2].split('-'))
print((ty-fy)*12+(tm-fm)+1)
PY
)
  echo "database        : ${CH_DB} @ ${CH_HOST}:${CH_PORT}"
  if [ "$full" -eq 1 ]; then
    echo "mode            : FULL REBUILD"
    echo "will DROP       : fapi_kline_{5m,15m,1h,4h,1d}_rmv and fapi_kline_{5m,15m,1h,4h,1d}  (ALL 5m+ data)"
    echo "will KEEP       : fapi_kline_1m"
  else
    echo "mode            : PARTIAL RE-FOLD (tables kept)"
    echo "will DROP       : fapi_kline_{5m,15m,1h,4h,1d}_rmv only"
  fi
  echo "will re-apply   : 002_kline_rollups.sql (fixed)"
  echo "will re-fold    : 003 for ${from} .. ${to}  (~${months} month-chunks x 5 intervals)"
  if [ "$yes" -ne 1 ]; then
    read -r -p "Type the database name (${CH_DB}) to continue: " ans
    [ "$ans" = "$CH_DB" ] || die "aborted"
  fi

  log "step 1/4: dropping refreshable MVs"
  for spec in "${INTERVALS[@]}"; do
    ch -q "DROP VIEW IF EXISTS ${CH_DB}.fapi_kline_${spec%%|*}_rmv"
  done

  if [ "$full" -eq 1 ]; then
    log "step 2/4: applying fixed 002 (drops and recreates all derived tables empty)"
    ch_file "$CH_SQL_DIR/002_kline_rollups.sql"
  else
    log "step 2/4: applying fixed 002 (DROP block stripped; tables kept, MVs recreated)"
    sed '/^-- >>> REBUILD-DROPS/,/^-- <<< REBUILD-DROPS/d' "$CH_SQL_DIR/002_kline_rollups.sql" \
      | sed "s/market\\./${CH_DB}./g; s/DATABASE IF NOT EXISTS market/DATABASE IF NOT EXISTS ${CH_DB}/" \
      | ch --multiquery
  fi

  log "step 3/4: re-folding history with 003 (from fapi_kline_1m FINAL; newest rollup_version wins)"
  local m="${from}-01" end nxt
  end="${to}-01"
  while [ "$m" \< "$end" ] || [ "$m" = "$end" ]; do
    nxt=$(date -u -d "$m +1 month" +%F)
    log "  chunk $m .. $nxt"
    ch_file "$CH_SQL_DIR/003_rollup_backfill.sql" \
      --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00"
    m="$nxt"
  done

  log "step 4/4: verifying"
  if check --days 10; then
    log "OK — rollups agree with 1m. Optional: OPTIMIZE TABLE ${CH_DB}.fapi_kline_<iv> FINAL to reclaim superseded rows."
  else
    die "verification found mismatches; inspect the output above"
  fi
}

case "${1:-}" in
  check) shift; check "$@";;
  apply) shift; apply "$@";;
  *) awk 'NR>1 && /^set -euo pipefail/{exit} NR>1{sub(/^# ?/,""); print}' "${BASH_SOURCE[0]}"; exit 2;;
esac
