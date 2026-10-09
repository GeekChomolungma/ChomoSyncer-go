#!/usr/bin/env bash
# ============================================================================
# 003 — move the off-grid OI archive rows onto the 5-minute grid (or drop them).
#
# SYMPTOM
#   ~14.6k src_rank = 3 rows of fapi_oi_5m have a start_time that is not a whole
#   5-minute boundary (e.g. 2022-05-09 00:05:07): Binance's archive stamped some
#   snapshots seconds late (99.9% are < 60 s past a boundary; a few up to 4 min).
#   They never join fapi_kline_5m on (symbol, start_time), so to a consumer the bar
#   is missing, and check_oi_consistency.py reports them as "not on the grid".
#
# RULE (per off-grid row; target = start_time rounded to the NEAREST boundary)
#   - an on-grid row already exists at (symbol, target)   -> drop the off-grid row
#   - several off-grid rows share a target                -> keep the one closest
#                                                            to it, drop the others
#   - otherwise                                           -> move it to target
#                                                            (snap_time = target + 5m)
#   Measured before writing this: 14,587 move, 36 drop (slot taken), 1 drop
#   (collision); no row had its nearest slot taken but the other one free.
#
# WHAT `apply` DOES, in order (resumable via <db>.fix_003_state)
#   1. Backs the off-grid rows up into <db>.fapi_oi_5m_bak_fix003.
#   2. Writes the decision per row into <db>.fix_003_plan (kept afterwards).
#   3. Deletes every off-grid row from fapi_oi_5m.
#   4. Inserts the "move" rows at their target.
#   5. `refold`: re-runs 006 for every month the plan touches and prunes rollup
#      buckets that no longer have 5m rows.
#   6. `check`.
#   chomosyncer-go may keep running: it never writes off-grid or rank-3 rows.
#
# USAGE
#   ./003_fix_oi_offgrid_rows.sh check | plan | apply [--yes] | refold
#   `plan` is read-only: prints what apply would do.
#   Connection: see lib.sh. Rehearse:  CH_DB=market_rehearsal ./003_fix_oi_offgrid_rows.sh apply --yes
# ============================================================================
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

T="${CH_DB}.fapi_oi_5m"
BAK="${CH_DB}.fapi_oi_5m_bak_fix003"
PLAN="${CH_DB}.fix_003_plan"
STATE="${CH_DB}.fix_003_state"
OFF="src_rank = 3 AND toUnixTimestamp64Milli(start_time) % 300000 != 0"
INTERVALS=("15m" "1h" "4h" "1d")

q() { ch --session_timezone UTC -q "$1" </dev/null; }
has_step() {
  [ "$(q "EXISTS TABLE ${STATE}")" = "1" ] || return 1
  [ "$(q "SELECT count() FROM ${STATE} WHERE step = '$1'")" != "0" ]
}
mark_step() {
  q "CREATE TABLE IF NOT EXISTS ${STATE} (step String, at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY step"
  q "INSERT INTO ${STATE} (step) VALUES ('$1')"
}

# plan_select SOURCE: the decision for every off-grid row of SOURCE (a table holding
# exactly the off-grid rows), against the on-grid rows currently in T.
plan_select() {
  cat <<SQL
WITH off AS (
       SELECT symbol, start_time, sum_open_interest, created_at,
              toDateTime64(toStartOfInterval(start_time + INTERVAL 150 SECOND, INTERVAL 5 MINUTE), 3, 'UTC') AS target,
              abs(toUnixTimestamp64Milli(start_time) - toUnixTimestamp64Milli(target)) AS dist_ms
       FROM $1 FINAL),
     ranked AS (
       SELECT *, row_number() OVER (PARTITION BY symbol, target ORDER BY dist_ms, start_time) AS rn FROM off),
     grid AS (
       SELECT DISTINCT symbol, start_time FROM ${T}
       WHERE toUnixTimestamp64Milli(start_time) % 300000 = 0
         AND (symbol, toStartOfMonth(start_time)) IN (SELECT DISTINCT symbol, toStartOfMonth(target) FROM off))
SELECT r.symbol, r.start_time AS old_start, r.target AS new_start, r.sum_open_interest, r.created_at, r.dist_ms,
       multiIf(g.symbol != '', 'drop_slot_taken', r.rn > 1, 'drop_collision', 'move') AS action
FROM ranked r LEFT JOIN grid g ON g.symbol = r.symbol AND g.start_time = r.target
SETTINGS join_use_nulls = 0
SQL
}

cmd_plan() {
  local src="$T"
  if has_step plan_ok; then
    q "SELECT action, count(), max(dist_ms) / 1000 AS max_offset_s FROM ${PLAN} GROUP BY action ORDER BY action FORMAT PrettyCompact"
    return
  fi
  local tmp="${CH_DB}.fix_003_tmp_off"
  q "DROP TABLE IF EXISTS ${tmp}"
  trap 'q "DROP TABLE IF EXISTS ${tmp}" || true' RETURN
  q "CREATE TABLE ${tmp} AS ${T}"
  q "INSERT INTO ${tmp} SELECT * FROM ${src} WHERE ${OFF}"
  q "SELECT action, count(), max(dist_ms) / 1000 AS max_offset_s FROM ($(plan_select "$tmp")) GROUP BY action ORDER BY action FORMAT PrettyCompact"
}

months_touched() {
  q "SELECT DISTINCT toString(toStartOfMonth(m)) FROM (SELECT old_start AS m FROM ${PLAN} UNION ALL SELECT new_start FROM ${PLAN}) ORDER BY 1 FORMAT TSV"
}

cmd_refold() {
  has_step plan_ok || die "no plan yet; run apply"
  if [ "$(q "EXISTS TABLE ${CH_DB}.fapi_oi_1d")" != "1" ]; then
    log "no OI rollup tables in ${CH_DB}; nothing to refold"
    return 0
  fi
  local t0 m nxt
  t0=$(q "SELECT toString(now64(3))")
  for m in $(months_touched); do
    nxt=$(date -u -d "$m +1 month" +%F)
    log "fold 006 ${m} .. ${nxt}"
    ch_file "$CH_SQL_DIR/006_oi_rollup_backfill.sql" --param_m_start="$m 00:00:00" --param_m_end="$nxt 00:00:00"
    # buckets of this month not rewritten just now have no 5m rows left (or are
    # superseded versions): prune them
    for iv in "${INTERVALS[@]}"; do
      q "ALTER TABLE ${CH_DB}.fapi_oi_${iv} DELETE
         WHERE start_time >= toDateTime64('${m}', 3, 'UTC') AND start_time < toDateTime64('${nxt}', 3, 'UTC')
           AND rollup_version < toDateTime64('${t0}', 3, 'UTC') SETTINGS mutations_sync = 2"
    done
  done
}

cmd_check() {
  local fail=0 n
  n=$(q "SELECT count() FROM ${T} WHERE toUnixTimestamp64Milli(start_time) % 300000 != 0")
  echo "== state: $(has_step done && echo 'applied' || { [ "$n" = "0" ] && echo 'clean (no off-grid rows)' || echo 'NOT applied'; })"
  echo "== off-grid rows in ${T}: ${n} (0 when fixed)"
  if has_step done; then
    [ "$n" = "0" ] || fail=1
    q "SELECT action, count() FROM ${PLAN} GROUP BY action ORDER BY action FORMAT PrettyCompact"
    local moved want
    want=$(q "SELECT count() FROM ${PLAN} WHERE action = 'move'")
    moved=$(q "SELECT count() FROM ${T} AS t FINAL INNER JOIN (SELECT symbol, new_start, sum_open_interest FROM ${PLAN} WHERE action = 'move') p
               ON p.symbol = t.symbol AND p.new_start = t.start_time WHERE t.src_rank = 3 AND t.sum_open_interest = p.sum_open_interest")
    echo "== moved rows present at their target: ${moved} of ${want}"
    [ "$moved" = "$want" ] || fail=1
    if [ "$(q "EXISTS TABLE ${CH_DB}.fapi_oi_1d")" = "1" ]; then
      local m raw months
      months=$(months_touched | sed "s/.*/'&'/" | paste -sd,)
      raw=$(q "SELECT count() FROM ${T} FINAL WHERE toString(toStartOfMonth(start_time)) IN (${months})")
      echo "== rollups of the $(months_touched | wc -l) months the plan touches: 5m rows ${raw}"
      for iv in "${INTERVALS[@]}"; do
        m=$(q "SELECT sum(samples) FROM ${CH_DB}.fapi_oi_${iv} FINAL WHERE toString(toStartOfMonth(start_time)) IN (${months})")
        echo "   fapi_oi_${iv}: samples ${m}"
        [ "$m" = "$raw" ] || fail=1
      done
    fi
  fi
  [ "$fail" = "0" ] && echo "CHECK OK" || { echo "CHECK FAILED"; return 1; }
}

cmd_apply() {
  local yes=0
  [ "${1:-}" = "--yes" ] && yes=1
  [ "$(q "EXISTS TABLE ${T}")" = "1" ] || die "${T} not found"
  if has_step done; then
    log "already applied; re-folding and checking only"
    cmd_refold; cmd_check; return
  fi
  if ! has_step backup_ok; then
    local n
    n=$(q "SELECT count() FROM ${T} WHERE ${OFF}")
    log "off-grid rows: ${n}"
    if [ "$yes" != "1" ]; then
      read -r -p "Type the database name (${CH_DB}) to continue: " ans
      [ "$ans" = "$CH_DB" ] || die "aborted"
    fi
    log "1/4 backup -> ${BAK}"
    q "DROP TABLE IF EXISTS ${BAK}"
    q "CREATE TABLE ${BAK} AS ${T}"
    q "INSERT INTO ${BAK} SELECT * FROM ${T} WHERE ${OFF}"
    [ "$(q "SELECT count() FROM ${BAK}")" = "$n" ] || die "backup row count mismatch"
    mark_step backup_ok
  fi
  if ! has_step plan_ok; then
    log "2/4 plan -> ${PLAN}"
    q "DROP TABLE IF EXISTS ${PLAN}"
    q "CREATE TABLE ${PLAN} (symbol LowCardinality(String), old_start DateTime64(3, 'UTC'), new_start DateTime64(3, 'UTC'),
         sum_open_interest Float64, created_at DateTime, dist_ms Int64, action LowCardinality(String))
       ENGINE = MergeTree ORDER BY (symbol, old_start)"
    q "INSERT INTO ${PLAN} $(plan_select "$BAK")"
    [ "$(q "SELECT count() FROM ${PLAN}")" = "$(q "SELECT count() FROM ${BAK} FINAL")" ] || die "plan row count mismatch"
    mark_step plan_ok
  fi
  cmd_plan
  log "3/4 delete the off-grid rows"
  q "ALTER TABLE ${T} DELETE WHERE ${OFF} SETTINGS mutations_sync = 2"
  log "4/4 insert the moved rows"
  q "INSERT INTO ${T} (symbol, start_time, sum_open_interest, snap_time, src_rank, created_at)
     SELECT symbol, new_start, sum_open_interest, new_start + INTERVAL 5 MINUTE, 3, created_at FROM ${PLAN} WHERE action = 'move'"
  mark_step done
  log "done. Backup: ${BAK}, plan: ${PLAN} (drop both once satisfied)"
  cmd_refold
  cmd_check
}

case "${1:-}" in
  check)  cmd_check ;;
  plan)   cmd_plan ;;
  apply)  shift; cmd_apply "$@" ;;
  refold) cmd_refold ;;
  *) sed -n '2,38p' "$0"; exit 2 ;;
esac
