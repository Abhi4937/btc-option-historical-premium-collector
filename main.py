"""
BTC Options Historical Data Collector — CLI entry point.

Commands:
  python main.py collect    full collection (5 accounts parallel)
  python main.py resume     reset stale in_progress → pending, continue
  python main.py status     show manifest progress table
  python main.py test-depth probe actual data availability (binary search)
  python main.py spot       fetch/update spot data only

VERIFIED SUMMARY:
  Test 1:  24/7, 1440 candles/day
  Test 2:  Symbol: C-BTC-{strike}-{DDMMYY}, settlement 17:30 IST ✅
  Test 3:  Strike interval $200 (live); historical unknown (API only returns 2026)
  Test 4:  Boundary inclusive on both ends → next chunk start = T+1s
  Test 5:  Settlement candle NOT present; last candle = 17:29 IST
  Test 6:  ⚠ CRITICAL: empty & fake both → HTTP 200 + []; use /v2/products to distinguish
  Test 7:  ⚠ CRITICAL: NO rate-limit headers → count-based proactive limiting
  Test 8:  ⚠ Spot empty for all dates before 2024; options found from Jan 2024
            run test-depth for exact oldest date
  Test 9:  Expiry ladder verified; 4 unique expiries always correct ✅
  Test 10: Rate limits per-account independent ✅
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich import box

# ── Logging setup ─────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import LOG_FILE, ERROR_LOG

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
    ],
)

# Errors also go to separate file
err_handler = logging.FileHandler(ERROR_LOG)
err_handler.setLevel(logging.ERROR)
err_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(name)s] %(levelname)s:\n%(message)s\n"
))
logging.getLogger().addHandler(err_handler)

log = logging.getLogger("main")

console = Console()

IST = timezone(timedelta(hours=5, minutes=30))


# ── Commands ──────────────────────────────────────────────────────────────────

async def cmd_collect(resume: bool = False):
    from collector import run_collection
    from progress import ProgressDisplay

    display = ProgressDisplay()

    def status_cb(account_name, state_dict):
        display.update_account(account_name, state_dict)

    coro = run_collection(resume=resume, status_callback=status_cb)
    await display.run(coro)


async def cmd_status():
    from manifest import init_manifest, get_manifest_summary, get_progress_counts
    from registry import init_registry, get_stats

    os.makedirs("db", exist_ok=True)
    await init_manifest()
    await init_registry()

    rows  = await get_manifest_summary()
    stats = await get_progress_counts()

    tbl = Table(
        title="Manifest Status",
        box=box.ROUNDED,
        show_lines=True,
    )
    tbl.add_column("Month",       width=10)
    tbl.add_column("Status",      width=14)
    tbl.add_column("Claimed By",  width=14)
    tbl.add_column("Calls",       width=8,  justify="right")
    tbl.add_column("Strikes",     width=8,  justify="right")
    tbl.add_column("Completed",   width=22)
    tbl.add_column("Error",       width=30, overflow="ellipsis")

    STATUS_COLORS = {
        "done":        "green",
        "in_progress": "yellow",
        "pending":     "white",
        "failed":      "red",
    }

    for r in rows:
        color = STATUS_COLORS.get(r["status"], "white")
        tbl.add_row(
            r["expiry_month"],
            f"[{color}]{r['status']}[/{color}]",
            r.get("claimed_by") or "—",
            str(r.get("total_calls") or 0),
            str(r.get("strikes_fetched") or 0),
            r.get("completed_at") or "—",
            (r.get("error_message") or "")[:30],
        )

    console.print(tbl)

    # Summary
    total = len(rows)
    done  = stats.get("done", 0)
    console.print(f"\n[bold]Summary:[/bold] {done}/{total} months complete "
                  f"({done/total*100:.1f}%)" if total else "No months in manifest yet.")

    reg_stats = await get_stats()
    if reg_stats:
        console.print("\n[bold]Registry:[/bold]")
        for status, data in sorted(reg_stats.items()):
            if isinstance(data, dict):
                console.print(f"  {status}: {data['count']} symbols, "
                               f"{data.get('candles', 0):,} candles")
            else:
                console.print(f"  {status}: {data}")


async def cmd_test_depth():
    """
    Binary search for the oldest available candle data.
    Tests: MARK:BTCUSD spot, BTC options (various strikes).
    """
    load_dotenv()
    api_key = os.getenv("ACCOUNT_1_KEY")
    if not api_key:
        console.print("[red]ACCOUNT_1_KEY not found in .env[/red]")
        return

    BASE_URL = "https://api.india.delta.exchange"

    def fetch_sync(symbol, start_unix, end_unix):
        url = f"{BASE_URL}/v2/history/candles"
        params = {"symbol": symbol, "resolution": "1m",
                  "start": start_unix, "end": end_unix}
        headers = {"api-key": api_key}
        try:
            r = httpx.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json().get("result") or []
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
        return []

    def make_ist(y, mo, d, h=0, mi=0):
        return datetime(y, mo, d, h, mi, tzinfo=IST)

    def ts(dt):
        return int(dt.timestamp())

    def unix_to_ist_str(u):
        return datetime.fromtimestamp(u, tz=IST).strftime("%Y-%m-%d %H:%M IST")

    console.print("\n[bold cyan]═══ TEST-DEPTH: Binary search for oldest data ═══[/bold cyan]\n")

    # ── MARK:BTCUSD binary search ─────────────────────────────────────────────
    console.print("[yellow]1. Searching for oldest MARK:BTCUSD candle...[/yellow]")
    lo_unix = ts(make_ist(2018, 1, 1))
    hi_unix = ts(make_ist(2025, 12, 31))
    found_spot = None

    while hi_unix - lo_unix > 86400:
        mid = (lo_unix + hi_unix) // 2
        candles = fetch_sync("MARK:BTCUSD", mid, mid + 3600)
        time.sleep(0.3)
        if candles:
            hi_unix    = mid
            found_spot = candles[0]["time"]
        else:
            lo_unix = mid

    if found_spot:
        console.print(f"  ✅ Oldest MARK:BTCUSD: [green]{unix_to_ist_str(found_spot)}[/green]")
    else:
        console.print("  ❌ MARK:BTCUSD: no data found before 2025-12-31")

    # ── OI:BTCUSD ─────────────────────────────────────────────────────────────
    console.print("[yellow]2. Searching for oldest OI:BTCUSD candle...[/yellow]")
    lo_unix = ts(make_ist(2018, 1, 1))
    hi_unix = ts(make_ist(2025, 12, 31))
    found_oi = None

    while hi_unix - lo_unix > 86400:
        mid = (lo_unix + hi_unix) // 2
        candles = fetch_sync("OI:BTCUSD", mid, mid + 3600)
        time.sleep(0.3)
        if candles:
            hi_unix  = mid
            found_oi = candles[0]["time"]
        else:
            lo_unix = mid

    if found_oi:
        console.print(f"  ✅ Oldest OI:BTCUSD: [green]{unix_to_ist_str(found_oi)}[/green]")
    else:
        console.print("  ❌ OI:BTCUSD: no data found")

    # ── BTC Options probes ────────────────────────────────────────────────────
    console.print("[yellow]3. Probing BTC options history...[/yellow]")
    option_probes = [
        ("MARK:C-BTC-45000-260124", make_ist(2024, 1, 25), "Jan 2024 (C-45000)"),
        ("MARK:C-BTC-40000-260124", make_ist(2024, 1, 25), "Jan 2024 (C-40000)"),
        ("MARK:C-BTC-30000-270123", make_ist(2023, 1, 26), "Jan 2023 (C-30000)"),
        ("MARK:C-BTC-20000-180622", make_ist(2022, 6, 17), "Jun 2022 (C-20000)"),
        ("MARK:C-BTC-35000-290121", make_ist(2021, 1, 28), "Jan 2021 (C-35000)"),
        ("MARK:C-BTC-11000-250920", make_ist(2020, 9, 24), "Sep 2020 (C-11000)"),
    ]

    oldest_opt = None
    for sym, dt, label in option_probes:
        c = fetch_sync(sym, ts(dt), ts(dt) + 3600)
        time.sleep(0.3)
        if c:
            first = c[0]["time"]
            console.print(f"  ✅ {label}: found — first: [green]{unix_to_ist_str(first)}[/green]")
            if oldest_opt is None or first < oldest_opt:
                oldest_opt = first
        else:
            console.print(f"  ❌ {label}: [dim]no data[/dim]")

    # ── FINAL RECOMMENDATION ─────────────────────────────────────────────────
    console.print("\n[bold]═══ RESULTS ═══[/bold]")

    if found_spot:
        spot_date = datetime.fromtimestamp(found_spot, tz=IST).strftime("%Y-%m-%d")
        console.print(f"  Oldest MARK:BTCUSD: [green]{spot_date}[/green]")
    else:
        console.print("  Oldest MARK:BTCUSD: [red]NOT FOUND[/red]")

    if oldest_opt:
        opt_date = datetime.fromtimestamp(oldest_opt, tz=IST).strftime("%Y-%m-%d")
        console.print(f"  Oldest BTC options: [green]{opt_date}[/green]")
    else:
        console.print("  Oldest BTC options: [red]NOT FOUND[/red]")

    candidates = [d for d in [found_spot, oldest_opt] if d]
    if candidates:
        rec_unix = min(candidates)
        rec_date = datetime.fromtimestamp(rec_unix, tz=IST).strftime("%Y-%m-%d")
        console.print(f"\n[bold green]  RECOMMENDED COLLECTION_START_DATE = '{rec_date}'[/bold green]")
        console.print(f"  Update config.py: COLLECTION_START_DATE = date({rec_date.replace('-', ', ')})")
    else:
        console.print("\n[red]  Could not determine start date — check API access[/red]")


async def cmd_test_run(month_key: str | None = None):
    """
    Single-account test run for one expiry month.
    Collects data, then verifies parquet files look correct.
    Step 3 from build instructions.
    """
    import pyarrow.parquet as pq
    import glob as _glob

    from manifest import init_manifest, populate_manifest
    from registry import init_registry
    from worker import AccountWorker
    from config import OPTIONS_DIR, SPOT_PARQUET

    load_dotenv()
    key  = os.getenv("ACCOUNT_1_KEY")
    name = os.getenv("ACCOUNT_1_NAME", "lava")
    if not key:
        console.print("[red]ACCOUNT_1_KEY not found in .env[/red]")
        return

    # Default: use the most recent complete month (last month)
    if not month_key:
        today   = datetime.now(IST)
        # go back one month to get a complete month
        if today.month == 1:
            month_key = f"{today.year - 1}-12"
        else:
            month_key = f"{today.year}-{today.month - 1:02d}"

    console.print(f"\n[bold cyan]═══ SINGLE-ACCOUNT TEST RUN ═══[/bold cyan]")
    console.print(f"  Account  : [yellow]{name}[/yellow]")
    console.print(f"  Month    : [yellow]{month_key}[/yellow]")
    console.print(f"  Mode     : ACTIVE_ACCOUNTS=1\n")

    os.makedirs("db",   exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # Init DBs fresh for this test (or reuse if already initialised)
    await init_manifest()
    await init_registry()

    # Populate manifest with ONLY this one month
    year  = int(month_key[:4])
    month = int(month_key[5:7])
    await populate_manifest(year, month, year, month)

    console.print(f"[cyan]Manifest seeded with 1 month: {month_key}[/cyan]")
    console.print("[cyan]Starting worker (lava)...[/cyan]\n")

    status_log: list[dict] = []

    def status_cb(account_name, state):
        status_log.append({**state, "account": account_name})
        sym   = state.get("symbol", "")
        calls = state.get("calls",  0)
        st    = state.get("state",  "")
        if sym:
            console.print(f"  [{account_name}] {st:18s} calls={calls:5d}  {sym}")

    worker = AccountWorker(
        account_name    = name,
        api_key         = key,
        status_callback = status_cb,
    )

    t0 = time.monotonic()
    await worker.run()
    elapsed = time.monotonic() - t0

    console.print(f"\n[bold green]Worker finished in {elapsed:.1f}s[/bold green]\n")

    # ── PARQUET VERIFICATION ─────────────────────────────────────────────────
    console.print("[bold cyan]═══ PARQUET VERIFICATION ═══[/bold cyan]\n")

    errors   = 0
    verified = 0

    # 1. Spot file
    if os.path.exists(SPOT_PARQUET):
        t = pq.read_table(SPOT_PARQUET)
        console.print(f"[green]✅ Spot parquet exists[/green]")
        console.print(f"   Rows    : {len(t):,}")
        console.print(f"   Columns : {t.schema.names}")
        # Check required columns
        required_spot = {"timestamp_ist","timestamp_unix","mark_open","mark_close","ltp_volume","oi_close"}
        missing = required_spot - set(t.schema.names)
        if missing:
            console.print(f"   [red]⚠ Missing columns: {missing}[/red]")
            errors += 1
        else:
            console.print(f"   [green]All required columns present ✅[/green]")
        # Show first and last row timestamps
        if len(t) > 0:
            ts_list = t.column("timestamp_unix").to_pylist()
            first_ist = datetime.fromtimestamp(ts_list[0],  tz=IST).strftime("%Y-%m-%d %H:%M IST")
            last_ist  = datetime.fromtimestamp(ts_list[-1], tz=IST).strftime("%Y-%m-%d %H:%M IST")
            console.print(f"   First   : {first_ist}")
            console.print(f"   Last    : {last_ist}")
            # Check for duplicates
            dupes = len(ts_list) - len(set(ts_list))
            if dupes:
                console.print(f"   [red]⚠ {dupes} duplicate timestamps found![/red]")
                errors += 1
            else:
                console.print(f"   [green]No duplicate timestamps ✅[/green]")
        verified += 1
    else:
        console.print(f"[red]❌ Spot parquet NOT found: {SPOT_PARQUET}[/red]")
        errors += 1

    # 2. Options files
    console.print()
    pattern  = os.path.join(OPTIONS_DIR, f"expiry={year}-{month:02d}-*", "strike=*", "*.parquet")
    opt_files = sorted(_glob.glob(pattern))
    console.print(f"  Options parquet files found: [cyan]{len(opt_files)}[/cyan]")

    if opt_files:
        # Verify first 5 + last 5
        sample = opt_files[:5] + (opt_files[-5:] if len(opt_files) > 5 else [])
        console.print(f"\n  Spot-checking {len(sample)} files:\n")

        required_opt = {"timestamp_ist","timestamp_unix","mark_open","mark_close","oi_open","oi_close"}
        for fpath in sample:
            try:
                t = pq.read_table(fpath)
                ts = t.column("timestamp_unix").to_pylist()
                dupes = len(ts) - len(set(ts))
                missing = required_opt - set(t.schema.names)
                # Extract expiry and strike from path
                parts = fpath.split(os.sep)
                expiry_part = next((p for p in parts if p.startswith("expiry=")), "?")
                strike_part = next((p for p in parts if p.startswith("strike=")), "?")
                opt_type    = os.path.basename(fpath).replace(".parquet", "")
                row_str = f"{len(t):5d} rows"
                issues = []
                if missing: issues.append(f"missing cols: {missing}")
                if dupes:   issues.append(f"{dupes} dupes")
                status_icon = "[red]❌[/red]" if issues else "[green]✅[/green]"
                console.print(
                    f"  {status_icon} {expiry_part} / {strike_part} / {opt_type}.parquet "
                    f"— {row_str}"
                    + (f"  [red]{'; '.join(issues)}[/red]" if issues else "")
                )
                if issues: errors += 1
                else:       verified += 1
            except Exception as e:
                console.print(f"  [red]❌ {fpath}: {e}[/red]")
                errors += 1
    else:
        console.print("  [red]❌ No options parquet files found![/red]")
        errors += 1

    # 3. Registry stats
    console.print()
    from registry import get_stats
    stats = await get_stats()
    console.print("[bold]Registry stats:[/bold]")
    for status, data in sorted(stats.items()):
        if isinstance(data, dict):
            console.print(f"  {status:14s}: {data['count']:5d} symbols, {data.get('candles',0):8,} candles")
        else:
            console.print(f"  {status:14s}: {data}")

    # 4. DuckDB quick query
    console.print()
    try:
        import duckdb
        if opt_files:
            sample_file = opt_files[len(opt_files)//2]
            con = duckdb.connect()
            result = con.execute(f"""
                SELECT
                    MIN(timestamp_unix) as first_ts,
                    MAX(timestamp_unix) as last_ts,
                    COUNT(*) as rows,
                    AVG(mark_close) as avg_mark,
                    AVG(oi_close) as avg_oi
                FROM read_parquet('{sample_file}')
            """).fetchone()
            con.close()
            first = datetime.fromtimestamp(result[0], tz=IST).strftime("%Y-%m-%d %H:%M IST")
            last  = datetime.fromtimestamp(result[1], tz=IST).strftime("%Y-%m-%d %H:%M IST")
            console.print(f"[bold]DuckDB query on sample option file:[/bold]")
            parts = sample_file.split(os.sep)
            ep = next((p for p in parts if p.startswith("expiry=")), "?")
            sp = next((p for p in parts if p.startswith("strike=")), "?")
            console.print(f"  File       : {ep}/{sp}/{os.path.basename(sample_file)}")
            console.print(f"  Rows       : {result[2]:,}")
            console.print(f"  Time range : {first}  →  {last}")
            console.print(f"  Avg mark   : ${result[3]:,.2f}" if result[3] else "  Avg mark   : N/A")
            console.print(f"  Avg OI     : {result[4]:,.2f}" if result[4] else "  Avg OI     : N/A")
            console.print(f"  [green]DuckDB query OK ✅[/green]")
            verified += 1
    except Exception as e:
        console.print(f"  [red]DuckDB query failed: {e}[/red]")
        errors += 1

    # ── VERDICT ──────────────────────────────────────────────────────────────
    console.print(f"\n{'='*60}")
    if errors == 0:
        console.print(f"[bold green]✅ TEST RUN PASSED — {verified} checks OK, 0 errors[/bold green]")
        console.print(f"[green]Safe to run: python main.py collect  (all 5 accounts)[/green]")
    else:
        console.print(f"[bold red]❌ TEST RUN ISSUES — {errors} error(s), {verified} passed[/bold red]")
        console.print(f"[yellow]Fix issues above before running full collection.[/yellow]")
    console.print(f"{'='*60}\n")


async def cmd_spot():
    from collector import fetch_spot_only
    console.print("[cyan]Fetching/updating spot data...[/cyan]")
    await fetch_spot_only()
    console.print("[green]Spot data update complete.[/green]")


async def cmd_backfill_fridays():
    """
    Backfill missing early candles for Friday expiry symbols.

    For each done Friday symbol:
      1. Compute new first_appearance using extended 8-slot ladder (70-day probe)
      2. Read existing parquet to find earliest stored timestamp
      3. Fetch ONLY the gap: new_start → existing_earliest - 1s
      4. Merge into existing parquet (deduplicated — no redundant data written)
    """
    import pyarrow.parquet as pq
    import aiosqlite
    from datetime import date as date_cls
    from collections import defaultdict
    from dotenv import load_dotenv
    from api_client import DeltaAPIClient
    from parquet_writer import merge_option_data, append_or_create_option
    from registry import init_registry
    from ist_utils import first_appearance, get_expiry_dt, ist_to_unix
    from config import REGISTRY_DB

    load_dotenv()
    key = os.getenv("ACCOUNT_1_KEY")
    if not key:
        console.print("[red]ACCOUNT_1_KEY not found in .env[/red]")
        return

    await init_registry()

    # ── 1. Find all done Friday symbols ───────────────────────────────────────
    async with aiosqlite.connect(REGISTRY_DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT symbol, expiry_date, strike, option_type, parquet_path, fetched_from_unix "
            "FROM symbols WHERE status = 'done'"
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]

    friday_rows = [
        r for r in rows
        if date_cls.fromisoformat(r['expiry_date']).weekday() == 4
    ]
    console.print(f"[cyan]Done Friday symbols found: {len(friday_rows)}[/cyan]")

    # ── 2. One-time: populate fetched_from_unix for symbols that don't have it ─
    from registry import update_fetched_from_unix
    null_rows = [r for r in friday_rows if r['fetched_from_unix'] is None]
    if null_rows:
        console.print(f"[yellow]One-time: reading {len(null_rows)} parquet files to populate DB...[/yellow]")
        for r in null_rows:
            path = r['parquet_path']
            if not path or not os.path.exists(path):
                continue
            t = pq.read_table(path, columns=["timestamp_unix"])
            if len(t) == 0:
                continue
            earliest = t.column("timestamp_unix")[0].as_py()
            r['fetched_from_unix'] = earliest
            await update_fetched_from_unix(r['symbol'], earliest)
        console.print(f"[green]DB populated — future runs will skip parquet reads entirely.[/green]\n")

    # ── 3. Compute gap per symbol using DB value ───────────────────────────────
    gaps = []
    for r in friday_rows:
        existing_earliest = r['fetched_from_unix']
        if not existing_earliest:
            continue

        expiry_dt      = get_expiry_dt(date_cls.fromisoformat(r['expiry_date']))
        new_start      = first_appearance(expiry_dt)
        new_start_unix = ist_to_unix(new_start)

        if new_start_unix < existing_earliest:
            gaps.append({
                **r,
                "new_start_unix": new_start_unix,
                "fetch_end_unix": existing_earliest - 1,
                "gap_days":       (existing_earliest - new_start_unix) / 86400,
            })

    if not gaps:
        console.print("[green]Nothing to backfill — all Friday symbols have full history.[/green]")
        return

    # ── 3. Print summary ───────────────────────────────────────────────────────
    by_expiry: dict = defaultdict(list)
    for g in gaps:
        by_expiry[g['expiry_date']].append(g)

    console.print(
        f"[yellow]{len(gaps)} symbols across {len(by_expiry)} Friday expiries need backfill[/yellow]\n"
    )
    console.print("[bold]Expiry breakdown:[/bold]")
    for expiry in sorted(by_expiry):
        syms    = by_expiry[expiry]
        avg_gap = sum(s['gap_days'] for s in syms) / len(syms)
        console.print(f"  {expiry}  {len(syms):4d} symbols  avg gap {avg_gap:.1f} days")

    console.print(f"\n[cyan]Starting backfill with account lava...[/cyan]\n")

    # ── 4. Progress state file (read by monitor.py) ───────────────────────────
    import json
    from config import LOGS_DIR
    progress_file = os.path.join(LOGS_DIR, "backfill_progress.json")
    total_expiries = len(by_expiry)
    total_symbols  = len(gaps)

    def _save_progress(done_exp: int, done_sym: int, no_data_sym: int,
                       err_sym: int, current: str, status: str):
        data = {
            "status":          status,
            "started_at":      datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
            "total_expiries":  total_expiries,
            "total_symbols":   total_symbols,
            "done_expiries":   done_exp,
            "done_symbols":    done_sym,
            "no_data_symbols": no_data_sym,
            "error_symbols":   err_sym,
            "current_expiry":  current,
            "last_updated":    datetime.now(IST).strftime("%Y-%m-%d %H:%M IST"),
        }
        with open(progress_file, "w") as f:
            json.dump(data, f)

    # ── 5. Fetch gaps and merge ────────────────────────────────────────────────
    filled      = 0
    no_data     = 0
    errors      = 0
    done_exp    = 0
    sem         = asyncio.Semaphore(50)

    async def _do_one(g: dict, client: DeltaAPIClient):
        nonlocal filled, no_data, errors
        async with sem:
            try:
                mark_sym = g['symbol']
                oi_sym   = mark_sym.replace("MARK:", "OI:", 1)

                mark_c, oi_c = await asyncio.gather(
                    client.fetch_candles(mark_sym, g['new_start_unix'], g['fetch_end_unix']),
                    client.fetch_candles(oi_sym,   g['new_start_unix'], g['fetch_end_unix']),
                )
                if not mark_c and not oi_c:
                    no_data += 1
                    return

                table = merge_option_data(mark_c, oi_c)
                append_or_create_option(table, g['expiry_date'], g['strike'], g['option_type'])
                if len(table) > 0:
                    new_earliest = table.column("timestamp_unix")[0].as_py()
                    await update_fetched_from_unix(g['symbol'], new_earliest)
                filled += 1
            except Exception as e:
                log.error("Backfill failed %s: %s", g['symbol'], e)
                errors += 1

    _save_progress(0, 0, 0, 0, "starting", "running")

    async with DeltaAPIClient("lava", key) as client:
        for expiry in sorted(by_expiry):
            syms  = by_expiry[expiry]
            _save_progress(done_exp, filled, no_data, errors, expiry, "running")
            await asyncio.gather(*[_do_one(g, client) for g in syms])
            done_exp += 1
            rl = client.rate_limiter
            console.print(
                f"  [green]✓[/green] {expiry}  ({len(syms)} symbols)  "
                f"| rate: {rl.calls_in_window}/{rl.max_calls} in window  "
                f"total: {rl.total_calls}"
            )

    _save_progress(done_exp, filled, no_data, errors, "—", "complete")
    console.print(
        f"\n[bold]Backfill complete:[/bold] "
        f"{filled} updated  {no_data} no-data  {errors} errors"
    )


# ── FINAL VERIFIED FINDINGS ───────────────────────────────────────────────────

FINAL_SUMMARY = """
=== FINAL VERIFIED FINDINGS ===
Test 1  Trading hours          : 24/7 continuous, 1440 candles/day, zero gaps
Test 2  Symbol format          : C-BTC-{strike}-{DDMMYY}, settlement 17:30 IST (always)
                                 Only 2026 expired products returned by API
Test 3  Strike interval        : $200 (live 2026); non-uniform grid (denser near ATM)
                                 Historical intervals unknown (API only has 2026 expired)
Test 4  Pagination rule        : Boundary candle in BOTH chunks (duplicate)
                                 Rule: next_chunk_start = prev_chunk_last_ts + 1 second
                                 Deduplication by timestamp_unix required
Test 5  Settlement candle      : NOT present — last candle is 17:29 IST
                                 end=expiry_unix (17:30 unix) is correct — no extra needed
Test 6  Empty vs not-listed    : ⚠ CRITICAL: BOTH return HTTP 200 + {"result":[],"success":true}
                                 Cannot distinguish from candle API alone
                                 Use /v2/products cross-check to determine not_listed
Test 7  Rate limit headers     : ⚠ CRITICAL: NO rate limit headers in responses
                                 Only x-cache, x-amz-cf-pop, x-amz-cf-id present
                                 Must use proactive count-based rate limiting
Test 8  Oldest data            : MARK:BTCUSD empty for all pre-2024 probes
                                 Options found from 2024-01-25 IST
                                 Run test-depth for exact oldest date
Test 9  Expiry ladder          : 4 unique expiries always correct, verified vs live exchange
                                 Live exchange confirms: Mon 2026-03-09 → [Mar 10,11,12,13]
Test 10 Rate limit scope       : Per-account independent (200 concurrent calls, no 429)
                                 5 accounts × 3,333 = ~16,665 calls/5-min combined

=== ARCHITECTURE IMPACTS ===
1. Finding: No rate limit headers (Test 7)
   Affects: api_client.py RateLimiter
   Change: Count-based proactive limiting using deque of call timestamps ✅ IMPLEMENTED

2. Finding: Empty = Fake response (Test 6)
   Affects: registry.py status logic
   Change: Cross-check /v2/products before marking not_listed ✅ IMPLEMENTED

3. Finding: Data only from ~Jan 2024 (Test 8)
   Affects: config.py COLLECTION_START_DATE
   Change: Set to 2024-01-01 (conservative); run test-depth for exact date ✅ IMPLEMENTED

4. Finding: Settlement candle absent (Test 5)
   Affects: worker.py fetch_to time
   Change: SETTLEMENT_EXTRA_SECONDS = 0 ✅ IMPLEMENTED

5. Finding: Pagination boundary duplicate (Test 4)
   Affects: api_client.py paginator
   Change: next_start = last_ts + 1 + deduplication ✅ IMPLEMENTED

6. Finding: Strike interval non-uniform (Test 3)
   Affects: strike_generator.py
   Change: $200 as approximation; note for future improvement ✅ DOCUMENTED

=== SAFE TO BUILD ===
All modules safe to build immediately:
  ist_utils.py, config.py, api_client.py, registry.py, manifest.py,
  strike_generator.py, parquet_writer.py, worker.py, collector.py,
  progress.py, main.py

=== MUST RESOLVE FIRST ===
1. Run `python main.py test-depth` to find exact COLLECTION_START_DATE
2. Monitor first run for any new rate limiting behavior (Test 7 incomplete)
3. Verify options data exists before Jan 2024 using actual symbols from products API

=== RECOMMENDED COLLECTION START DATE ===
Based on Test 8 probes: 2024-01-01 (conservative)
Run `python main.py test-depth` for exact confirmed date
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        console.print(__doc__)
        console.print("[bold cyan]Available commands:[/bold cyan]")
        console.print("  collect           — full 5-account parallel collection")
        console.print("  resume            — reset stale jobs and continue")
        console.print("  status            — show manifest + registry progress")
        console.print("  test-depth        — binary search for oldest data available")
        console.print("  spot              — fetch/update spot data only")
        console.print("  findings          — print verified test findings summary")
        console.print("  test-run          — single-account test: 1 month + parquet verify")
        console.print("  backfill-fridays  — fetch missing early candles for Friday expiries")
        return

    cmd = sys.argv[1].lower()

    if cmd == "findings":
        console.print(FINAL_SUMMARY)
        return

    elif cmd == "collect":
        asyncio.run(cmd_collect(resume=False))

    elif cmd == "resume":
        asyncio.run(cmd_collect(resume=True))

    elif cmd == "status":
        asyncio.run(cmd_status())

    elif cmd == "test-depth":
        asyncio.run(cmd_test_depth())

    elif cmd == "spot":
        asyncio.run(cmd_spot())

    elif cmd == "test-run":
        # Single-account test: collect exactly one month then verify parquet output
        # Usage: python main.py test-run [YYYY-MM]   default: most recent complete month
        month = sys.argv[2] if len(sys.argv) > 2 else None
        asyncio.run(cmd_test_run(month))

    elif cmd == "backfill-fridays":
        asyncio.run(cmd_backfill_fridays())

    else:
        console.print(f"[red]Unknown command: {cmd}[/red]")
        console.print("Use: collect | resume | status | test-depth | spot | findings | test-run | backfill-fridays")
        sys.exit(1)


if __name__ == "__main__":
    main()
