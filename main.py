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
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/collector.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

# Errors also go to separate file
err_handler = logging.FileHandler("logs/errors.log")
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
    from registry import get_stats

    os.makedirs("db", exist_ok=True)
    await init_manifest()

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


async def cmd_spot():
    from collector import fetch_spot_only
    console.print("[cyan]Fetching/updating spot data...[/cyan]")
    await fetch_spot_only()
    console.print("[green]Spot data update complete.[/green]")


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
        console.print("  collect    — full 5-account parallel collection")
        console.print("  resume     — reset stale jobs and continue")
        console.print("  status     — show manifest + registry progress")
        console.print("  test-depth — binary search for oldest data available")
        console.print("  spot       — fetch/update spot data only")
        console.print("  findings   — print verified test findings summary")
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

    else:
        console.print(f"[red]Unknown command: {cmd}[/red]")
        console.print("Use: collect | resume | status | test-depth | spot | findings")
        sys.exit(1)


if __name__ == "__main__":
    main()
