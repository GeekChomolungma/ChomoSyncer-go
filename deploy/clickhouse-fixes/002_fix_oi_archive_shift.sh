#!/usr/bin/env bash
# ============================================================================
# 002 — move the OI archive rows of 2024-03-04 .. 2026-09-20 five minutes later.
#
# SYMPTOM
#   In fapi_oi_5m, the src_rank = 3 (archive) rows whose archive file is dated
#   2024-03-04 or later hold, at start_time = t, the open interest at t + 10m
#   instead of t + 5m: 5 minutes of lookahead against fapi_kline_5m. Rows from
#   older files (<= 2024-03-03) and the live/hist rows (src_rank 1/2, written by
#   the service from 2026-09-21) are correct.
#
# CAUSE
#   Binance changed the meaning of the archive's create_time on 2024-03-04: files
#   up to 2024-03-03 stamp the snapshot instant T, later files stamp the window
#   START (the snapshot is at T + 5m). The one-off OI archive import used
#   start_time = create_time - 5m for every file. Evidence:
#   new_requirements/oi.md §3.7. The stored snap_time of an affected row is its
#   create_time, so the affected rows are exactly
#       src_rank = 3 AND snap_time >= '2024-03-04 00:00:00'.
#
# WHAT `apply` DOES, in order (every step resumable; progress is recorded in
# <db>.fix_002_state, so a re-run continues where it stopped)
#   1. Backs the affected rows up into <db>.fapi_oi_5m_bak_fix002 (kept afterwards;
#      drop it yourself once satisfied).
#   2. Deletes them from fapi_oi_5m (ALTER ... DELETE, synchronous).
#   3. Re-inserts them from the backup with start_time + 5m and snap_time + 5m
#      (created_at kept). The keys move, so this cannot be an UPDATE.
#   4. Records "done", then `refold`: refreshes the four fapi_oi_*_rmv views and
#      re-runs 006 month by month from 2024-03 to the current month.
#   5. `check`.
#   Stop chomosyncer-go first: the script refuses to run while it is running.
#
# USAGE
#   ./002_fix_oi_archive_shift.sh check                 read-only; non-zero exit on failure
#   ./002_fix_oi_archive_shift.sh apply [--yes]         the repair (asks you to type the db name)
#   ./002_fix_oi_archive_shift.sh refold                re-run only step 4
#   Connection: see lib.sh (CH_HOST / CH_PORT / CH_USER / CH_PASSWORD / CH_DB).
#   Rehearse first:  CH_DB=market_rehearsal ./002_fix_oi_archive_shift.sh apply --yes
# ============================================================================
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CUTOFF="2024-03-04 00:00:00"          # first create_time of the new archive convention
FIRST_MONTH="2024-03-01"              # rollup months to re-fold: this .. current month
T="${CH_DB}.fapi_oi_5m"
BAK="${CH_DB}.fapi_oi_5m_bak_fix002"
STATE="${CH_DB}.fix_002_state"
AFFECTED="src_rank = 3 AND snap_time >= toDateTime64('${CUTOFF}', 3, 'UTC')"
INTERVALS=("15m|15 MINUTE" "1h|1 HOUR" "4h|4 HOUR" "1d|1 DAY")

q() { ch --session_timezone UTC -q "$1" </dev/null; }

# The fix leaves no archive row at start_time 2024-03-03 23:55 (the bar the switch
# skipped); before it, the first row of every 2024-03-04 file sits there. This tells
# an already-fixed table apart even after the state table has been dropped.
edge_rows() {
  q "SELECT count() FROM ${T} WHERE src_rank = 3 AND start_time = toDateTime64('2024-03-03 23:55:00', 3, 'UTC')"
}
looks_applied() { has_step done || [ "$(edge_rows)" = "0" ]; }

has_step() {
  [ "$(q "EXISTS TABLE ${STATE}")" = "1" ] || return 1
  [ "$(q "SELECT count() FROM ${STATE} WHERE step = '$1'")" != "0" ]
}
mark_step() {
  q "CREATE TABLE IF NOT EXISTS ${STATE} (step String, at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY step"
  q "INSERT INTO ${STATE} (step) VALUES ('$1')"
}

preflight() {
  [ "$(q "EXISTS TABLE ${T}")" = "1" ] || die "${T} not found"
}
# apply and refold rewrite rows: refuse while the service could be writing
require_stopped() {
  if [ "$CH_DB" = "market" ] && pgrep -x chomosyncer-go >/dev/null 2>&1; then
    die "chomosyncer-go is running; stop it first"
  fi
}

# REST comparison: does the archive row at start_time = s hold Binance's label s+5m
# (correct) or s+10m (the old shift)? Only possible while the newest affected archive
# days are inside openInterestHist's ~30-day retention.
rest_check() {
  local sym="$1" from_ms to_ms
  from_ms=$(q "SELECT toUnixTimestamp64Milli(greatest(now64(3) - INTERVAL 29 DAY, max(snap_time) - INTERVAL 1 DAY)) FROM ${T} WHERE ${AFFECTED} AND symbol = '${sym}'")
  to_ms=$(q "SELECT toUnixTimestamp64Milli(max(snap_time)) FROM ${T} WHERE ${AFFECTED} AND symbol = '${sym}'")
  if [ -z "$to_ms" ] || [ "$from_ms" -ge "$to_ms" ]; then
    echo "  ${sym}: no affected archive rows inside Binance's 30-day window; REST comparison skipped"
    return 0
  fi
  local url="https://fapi.binance.com/futures/data/openInterestHist?symbol=${sym}&period=5m&limit=500&startTime=${from_ms}&endTime=${to_ms}"
  local res
  res=$(q "
    WITH rest AS (SELECT timestamp AS ts, toFloat64(sumOpenInterest) AS v
                  FROM url('${url}', 'JSONEachRow', 'timestamp Int64, sumOpenInterest String')),
         db AS (SELECT toUnixTimestamp64Milli(start_time) AS s, sum_open_interest AS v
                FROM ${T} FINAL WHERE symbol = '${sym}' AND src_rank = 3
                  AND start_time >= fromUnixTimestamp64Milli(${from_ms}) - INTERVAL 15 MINUTE)
    SELECT (SELECT count() FROM db INNER JOIN rest ON rest.ts = db.s + 300000 AND rest.v = db.v),
           (SELECT count() FROM db INNER JOIN rest ON rest.ts = db.s + 600000 AND rest.v = db.v),
           (SELECT count() FROM rest)
    FORMAT TSV")
  local ok5 ok10 n
  read -r ok5 ok10 n <<<"$res"
  if [ -z "${n:-}" ]; then
    echo "  ${sym}: REST comparison query failed"
    return 1
  fi
  echo "  ${sym}: ${n} REST labels; archive rows matching label start+5m: ${ok5} (correct), start+10m: ${ok10} (old shift)"
  # Before the fix every row matches +10m; after it every row must match +5m.
  if looks_applied; then
    [ "$ok5" -gt 0 ] && [ "$ok10" -eq 0 ]
  else
    [ "$ok10" -gt 0 ] && [ "$ok5" -eq 0 ] && echo "  ${sym}: shows the old shift, as expected before apply"
  fi
}

cmd_check() {
  preflight
  local fail=0
  echo "== state: $(looks_applied && echo 'applied' || echo 'NOT applied')"
  local bad
  bad=$(q "SELECT count() FROM ${T} WHERE src_rank = 3 AND snap_time - start_time != 300")
  echo "== archive rows with snap_time != start_time + 5m: ${bad} (must be 0)"
  [ "$bad" = "0" ] || fail=1
  # The switch leaves one bar without data: snapshot 2024-03-04 00:00 is in neither
  # file. Fixed: no archive row at start 2024-03-03 23:55. Not fixed: the first row
  # of every 2024-03-04 file sits there.
  local edge
  edge=$(edge_rows)
  echo "== archive rows at start_time 2024-03-03 23:55 (the bar the switch skipped): ${edge} (0 when fixed)"
  if looks_applied; then [ "$edge" = "0" ] || fail=1; fi
  echo "== REST comparison"
  for s in BTCUSDT ETHUSDT DOGEUSDT; do
    if ! rest_check "$s"; then
      fail=1
    fi
  done
  if [ "$(q "EXISTS TABLE ${CH_DB}.fapi_oi_1d")" = "1" ]; then
    # every 5m row lands in exactly one bucket of each rollup
    local raw
    raw=$(q "SELECT count() FROM ${T} FINAL WHERE start_time >= toDateTime64('${FIRST_MONTH}', 3, 'UTC')")
    echo "== rollups since ${FIRST_MONTH}: 5m rows ${raw}"
    for iv in "${INTERVALS[@]}"; do
      local tb="${CH_DB}.fapi_oi_${iv%%|*}" sm
      sm=$(q "SELECT sum(samples) FROM ${tb} FINAL WHERE start_time >= toDateTime64('${FIRST_MONTH}', 3, 'UTC')")
      echo "   ${tb}: samples ${sm}"
      if looks_applied && [ "$sm" != "$raw" ]; then fail=1; fi
    done
  fi
  [ "$fail" = "0" ] && echo "CHECK OK" || { echo "CHECK FAILED"; return 1; }
}

cmd_refold() {
  preflight
  require_stopped
  if [ "$(q "EXISTS TABLE ${CH_DB}.fapi_oi_1d")" != "1" ]; then
    log "no OI rollup tables in ${CH_DB}; nothing to refold"
    return 0
  fi
  local t0
  t0=$(q "SELECT toString(now64(3))")
  for iv in "${INTERVALS[@]}"; do
    local v="${CH_DB}.fapi_oi_${iv%%|*}_rmv"
    if [ "$(q "EXISTS TABLE ${v}")" = "1" ]; then
      log "refresh ${v}"
      q "SYSTEM REFRESH VIEW ${v}"
      q "SYSTEM WAIT VIEW ${v}"
    fi
  done
  local m end
  m="$FIRST_MONTH"
  end=$(date -u -d "$(date -u +%Y-%m-01) +1 month" +%F)
  while [ "$m" \< "$end" ]; do
    local nxt
    nxt=$(date -u -d "$m +1 month" +%F)
    log "fold 006 ${m} .. ${nxt}"
    ch_file "$CH_SQL_DIR/006_oi_rollup_backfill.sql" --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00"
    m="$nxt"
  done
  # A bucket whose only rows moved into the next bucket now has no 5m rows; 006 groups
  # existing rows only, so it never rewrites such a bucket. Every bucket that still has
  # rows was just rewritten (rollup_version >= t0): what is older is an orphan.
  for iv in "${INTERVALS[@]}"; do
    local tb="${CH_DB}.fapi_oi_${iv%%|*}" n
    n=$(q "SELECT count() FROM ${tb} WHERE start_time >= toDateTime64('${FIRST_MONTH}', 3, 'UTC') AND rollup_version < toDateTime64('${t0}', 3, 'UTC')")
    log "prune ${n} orphan buckets from ${tb}"
    [ "$n" = "0" ] || q "ALTER TABLE ${tb} DELETE WHERE start_time >= toDateTime64('${FIRST_MONTH}', 3, 'UTC') AND rollup_version < toDateTime64('${t0}', 3, 'UTC') SETTINGS mutations_sync = 2"
  done
}

cmd_apply() {
  local yes=0
  [ "${1:-}" = "--yes" ] && yes=1
  preflight
  require_stopped
  if ! has_step done && ! has_step backup_ok && [ "$(edge_rows)" = "0" ]; then
    die "no archive row at 2024-03-03 23:55: the table looks already fixed (state table gone?). Refusing to shift again"
  fi
  if has_step done; then
    log "already applied (${STATE} says done); re-folding and checking only"
    cmd_refold
    cmd_check
    return
  fi
  local n
  n=$(q "SELECT count() FROM ${T} WHERE ${AFFECTED}")
  log "rows to move +5m in ${T}: ${n}"
  if [ "$yes" != "1" ]; then
    read -r -p "Type the database name (${CH_DB}) to continue: " ans
    [ "$ans" = "$CH_DB" ] || die "aborted"
  fi

  if ! has_step backup_ok; then
    log "1/3 backup -> ${BAK}"
    q "DROP TABLE IF EXISTS ${BAK}"
    q "CREATE TABLE ${BAK} AS ${T}"
    q "INSERT INTO ${BAK} SELECT * FROM ${T} WHERE ${AFFECTED}"
    local b
    b=$(q "SELECT count() FROM ${BAK}")
    [ "$b" = "$n" ] || die "backup has ${b} rows, expected ${n}"
    mark_step backup_ok
  else
    log "1/3 backup already complete (${BAK}: $(q "SELECT count() FROM ${BAK}") rows)"
  fi

  log "2/3 delete the affected rows from ${T}"
  q "ALTER TABLE ${T} DELETE WHERE ${AFFECTED} SETTINGS mutations_sync = 2"
  local left
  left=$(q "SELECT count() FROM ${T} WHERE ${AFFECTED}")
  [ "$left" = "0" ] || die "${left} affected rows still present after the delete"

  log "3/3 re-insert them 5 minutes later"
  q "INSERT INTO ${T} (symbol, start_time, sum_open_interest, snap_time, src_rank, created_at)
     SELECT symbol, start_time + INTERVAL 5 MINUTE, sum_open_interest, snap_time + INTERVAL 5 MINUTE, src_rank, created_at
     FROM ${BAK} FINAL"
  local want got
  want=$(q "SELECT count() FROM ${BAK} FINAL")
  got=$(q "SELECT count() FROM ${T} FINAL WHERE src_rank = 3 AND snap_time >= toDateTime64('${CUTOFF}', 3, 'UTC') + INTERVAL 5 MINUTE")
  [ "$got" = "$want" ] || die "moved ${got} rows, expected ${want}; re-run apply (it resumes from the backup)"
  mark_step done
  log "moved ${got} rows. Backup kept in ${BAK}; drop it once satisfied: DROP TABLE ${BAK}"

  cmd_refold
  cmd_check
}

case "${1:-}" in
  check)  cmd_check ;;
  apply)  shift; cmd_apply "$@" ;;
  refold) cmd_refold ;;
  *) sed -n '2,40p' "$0"; exit 2 ;;
esac
