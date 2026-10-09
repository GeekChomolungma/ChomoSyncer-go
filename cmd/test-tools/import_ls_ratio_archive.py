# -*- coding: utf-8 -*-
"""
Import the three long/short ratios of Binance's daily metrics archive (data.binance.vision)
into market.fapi_ls_ratio_5m (deploy/clickhouse/007_fapi_ls_ratio.sql), as src_rank = 3.

    python3 import_ls_ratio_archive.py --until 2026-10-07                  # dry run: count the work
    python3 import_ls_ratio_archive.py --until 2026-10-07 --apply          # download + insert
    python3 import_ls_ratio_archive.py --symbols BTCUSDT,ETHUSDT --from 2024-03-01 --until 2024-03-10 --apply
    python3 import_ls_ratio_archive.py --pairs-file pairs.tsv --apply      # exactly these (symbol<TAB>date) files

Which (symbol, day) files: by default every day on which market.fapi_oi_5m holds rows for the
symbol (i.e. the same files the OI archive import already read, plus the days since), within
[--from, --until]. Files that do not exist (404: not listed yet, delisted) are recorded and skipped.

Alignment (measured, see new_requirements/oi.md §3.5): the archive's create_time T changed
meaning on 2024-03-04.
    files dated <= 2024-03-03: the snapshot is at T        -> start_time = T - 5m, snap_time = T
    files dated >= 2024-03-04: the snapshot is at T + 5m   -> start_time = T,      snap_time = T + 5m
An empty ratio cell is stored as NULL.

Resumable: every (symbol, day) whose rows reached ClickHouse is appended to --state-file
(default logs/ls_ratio_archive_done.txt) and skipped by later runs. Re-importing a file is
harmless anyway (same key, same rank: ReplacingMergeTree keeps one row).

data.binance.vision is a plain file host (not the /fapi or /futures/data rate-limit pools).
The rollups are NOT refreshed by this script: run rollup_ls_ratio_per_month.sh afterwards.
"""
import argparse
import csv
import io
import os
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import Colors, get_clickhouse_client, load_config

import requests

ARCHIVE_URL = "https://data.binance.vision/data/futures/um/daily/metrics/{s}/{s}-metrics-{d}.zip"
SWITCH_DAY = date(2024, 3, 4)          # first file whose create_time is the window START (snapshot = T + 5m)
BAR = timedelta(minutes=5)
SRC_RANK_ARCHIVE = 3
RATIO_COLUMNS = ["count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio"]
INSERT_COLUMNS = ["symbol", "start_time"] + RATIO_COLUMNS + ["snap_time", "src_rank"]

_tls = threading.local()


def session() -> requests.Session:
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.mount("https://", requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1))
        _tls.s = s
    return s


def fmt(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S.000")


def parse_ratio(v: str) -> Optional[float]:
    v = (v or "").strip()
    if not v:
        return None
    try:
        f = float(v)
    except ValueError:
        return None
    if f != f or f in (float("inf"), float("-inf")) or f < 0:
        return None
    return f


def rows_of(symbol: str, day: date, content: bytes) -> Tuple[List[dict], int]:
    """Archive CSV -> table rows. Returns (rows, skipped).

    A create_time that is not a whole 5-minute boundary (Binance stamped some snapshots
    seconds late) is rounded to the NEAREST boundary, with the same rule as
    deploy/clickhouse-fixes/003_fix_oi_offgrid_rows.sh: it is dropped when the file
    already has an on-grid row there, when another off-grid row is closer to it, or
    when the boundary belongs to the neighbouring day's file.
    """
    z = zipfile.ZipFile(io.BytesIO(content))
    shift = timedelta(0) if day >= SWITCH_DAY else -BAR     # start_time = T or T - 5m
    on_grid: Dict[datetime, list] = {}
    off_grid: Dict[datetime, Tuple[float, list]] = {}       # rounded T -> (distance s, values)
    skipped = 0
    with z.open(z.namelist()[0]) as fh:
        for r in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8")):
            try:
                t = datetime.strptime(r["create_time"], "%Y-%m-%d %H:%M:%S")
            except (KeyError, ValueError):
                skipped += 1
                continue
            vals = [parse_ratio(r.get(c, "")) for c in RATIO_COLUMNS]
            if all(v is None for v in vals):
                skipped += 1
                continue
            if t.minute % 5 == 0 and t.second == 0:
                on_grid[t] = vals
                continue
            base = t.replace(minute=t.minute - t.minute % 5, second=0)
            rounded = base + BAR if (t - base) >= timedelta(seconds=150) else base
            dist = abs((t - rounded).total_seconds())
            if rounded.date() != t.date() or (rounded in off_grid and off_grid[rounded][0] <= dist):
                skipped += 1
                continue
            if rounded in off_grid:
                skipped += 1                                 # the farther one is replaced
            off_grid[rounded] = (dist, vals)
    for rounded in list(off_grid):
        if rounded in on_grid:
            skipped += 1
        else:
            on_grid[rounded] = off_grid[rounded][1]
    out = []
    for t in sorted(on_grid):
        start = t + shift
        row = {"symbol": symbol, "start_time": fmt(start), "snap_time": fmt(start + BAR), "src_rank": SRC_RANK_ARCHIVE}
        row.update(zip(RATIO_COLUMNS, on_grid[t]))
        out.append(row)
    return out, skipped


def fetch(symbol: str, day: date, retries: int = 5):
    url = ARCHIVE_URL.format(s=symbol, d=day.isoformat())
    backoff = 1.0
    for attempt in range(retries + 1):
        try:
            resp = session().get(url, timeout=30)
            if resp.status_code == 404:
                return symbol, day, None, "missing"
            if resp.status_code == 200:
                rows, skipped = rows_of(symbol, day, resp.content)
                return symbol, day, rows, skipped
            err = f"HTTP {resp.status_code}"
        except (requests.RequestException, zipfile.BadZipFile, OSError) as e:   # a bad CRC raises BadZipFile
            err = str(e)
        if attempt < retries:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    return symbol, day, None, "error: " + err


def work_list(ch, start: date, until: date, symbols: Optional[List[str]]) -> List[Tuple[str, date]]:
    where = f"start_time >= toDateTime64('{start} 00:00:00', 3, 'UTC') - INTERVAL 5 MINUTE " \
            f"AND start_time < toDateTime64('{until + timedelta(days=1)} 00:00:00', 3, 'UTC')"
    if symbols:
        where += " AND symbol IN (" + ",".join(f"'{s}'" for s in symbols) + ")"
    # A file dated D holds snapshots D 00:00..23:55 (old convention) or D 00:05..D+1 00:00 (new):
    # take both the start day and the snapshot day of every OI row, so no file is missed.
    sql = f"""
        SELECT DISTINCT symbol, d FROM (
            SELECT symbol, toDate(start_time) AS d FROM market.fapi_oi_5m WHERE {where}
            UNION ALL
            SELECT symbol, toDate(start_time + INTERVAL 5 MINUTE) AS d FROM market.fapi_oi_5m WHERE {where}
        ) WHERE d >= toDate('{start}') AND d <= toDate('{until}')
        ORDER BY d, symbol"""
    return [(r["symbol"], date.fromisoformat(str(r["d"]))) for r in ch.query(sql)]


def main() -> int:
    ap = argparse.ArgumentParser(description="Import the long/short ratios of the Binance metrics archive into fapi_ls_ratio_5m.")
    ap.add_argument("--config", default=None, help="config.yaml (for the ClickHouse connection)")
    ap.add_argument("--from", dest="start", default="2020-08-01", help="first file date, YYYY-MM-DD (default 2020-08-01)")
    ap.add_argument("--until", default="", help="last file date, YYYY-MM-DD (inclusive; required unless --pairs-file)")
    ap.add_argument("--symbols", default="", help="comma-separated symbols (default: every symbol fapi_oi_5m has on that day)")
    ap.add_argument("--table", default="market.fapi_ls_ratio_5m")
    ap.add_argument("--workers", type=int, default=24, help="concurrent downloads (default 24)")
    ap.add_argument("--batch-rows", type=int, default=300_000, help="rows per INSERT (default 300000)")
    ap.add_argument("--state-file", default="logs/ls_ratio_archive_done.txt")
    ap.add_argument("--pairs-file", default="", help="TSV of symbol<TAB>YYYY-MM-DD to (re-)import exactly; ignores --from/--until/--symbols and the state file")
    ap.add_argument("--apply", action="store_true", help="download and insert (default: dry run)")
    ap.add_argument("--ch-host"); ap.add_argument("--ch-port", type=int); ap.add_argument("--ch-user"); ap.add_argument("--ch-password")
    args = ap.parse_args()

    ch = get_clickhouse_client(load_config(args.config), args)
    if not args.until and not args.pairs_file:
        ap.error("--until is required (or give --pairs-file)")
    start = date.fromisoformat(args.start)
    until = date.fromisoformat(args.until) if args.until else date.today()
    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None

    done: Set[str] = set()
    if os.path.exists(args.state_file):
        with open(args.state_file) as fh:
            done = {l.split("\t")[0] + "\t" + l.split("\t")[1] for l in fh.read().splitlines() if l.count("\t") >= 1}
    if args.pairs_file:
        with open(args.pairs_file) as fh:
            todo = [(l.split("\t")[0], date.fromisoformat(l.split("\t")[1].strip())) for l in fh if l.strip()]
    else:
        todo = [(s, d) for s, d in work_list(ch, start, until, syms) if f"{s}\t{d}" not in done]
    print(f"{len(todo)} (symbol, day) files to import, {len(done)} already done ({start} .. {until})")
    if not args.apply or not todo:
        if not args.apply:
            print(f"{Colors.YELLOW}dry run: pass --apply to download and insert{Colors.RESET}")
        return 0

    os.makedirs(os.path.dirname(args.state_file) or ".", exist_ok=True)
    state = open(args.state_file, "a")
    buf: List[dict] = []
    pending: List[str] = []           # state lines that become durable with the next insert
    stats: Dict[str, int] = {"files": 0, "missing": 0, "errors": 0, "rows": 0, "skipped_rows": 0}
    t0 = time.time()

    def flush():
        nonlocal buf, pending
        if buf:
            for attempt in range(6):
                try:
                    ch.insert_json_rows(args.table, INSERT_COLUMNS, buf)
                    break
                except Exception as e:  # ClickHouse briefly unavailable: retry, then give up loudly
                    if attempt == 5:
                        raise
                    print(f"{Colors.YELLOW}insert failed ({e}); retrying{Colors.RESET}", flush=True)
                    time.sleep(2 ** attempt)
            stats["rows"] += len(buf)
        state.write("".join(pending)); state.flush()
        buf, pending = [], []

    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        # Submit in windows so memory stays bounded however long the work list is.
        it = iter(todo)
        inflight = set()
        def refill():
            for s, d in it:
                inflight.add(ex.submit(fetch, s, d))
                if len(inflight) >= args.workers * 8:
                    return
        refill()
        while inflight:
            fut = next(as_completed(inflight))
            inflight.discard(fut)
            sym, day, rows, info = fut.result()
            if rows is None and info == "missing":
                stats["missing"] += 1
                pending.append(f"{sym}\t{day}\tmissing\n")
            elif rows is None:
                stats["errors"] += 1
                errors.append(f"{sym} {day}: {info}")      # not recorded: the next run retries it
            else:
                stats["files"] += 1
                stats["skipped_rows"] += info
                buf.extend(rows)
                pending.append(f"{sym}\t{day}\t{len(rows)}\n")
            if len(buf) >= args.batch_rows:
                flush()
                n = stats["files"] + stats["missing"] + stats["errors"]
                el = time.time() - t0
                print(f"{n}/{len(todo)} files ({n / el:.1f}/s, eta {(len(todo) - n) / max(n / el, 1e-9) / 60:.0f} min) "
                      f"rows={stats['rows']} missing={stats['missing']} errors={stats['errors']}", flush=True)
            refill()
    flush()
    state.close()

    print(f"done in {(time.time() - t0) / 60:.1f} min: files={stats['files']} rows={stats['rows']} "
          f"missing={stats['missing']} skipped_rows={stats['skipped_rows']} errors={stats['errors']}")
    for e in errors[:20]:
        print(f"{Colors.RED}{e}{Colors.RESET}")
    if errors:
        print(f"{Colors.RED}{len(errors)} files failed; re-run the same command to retry them{Colors.RESET}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
