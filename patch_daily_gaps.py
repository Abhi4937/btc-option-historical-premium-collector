#!/usr/bin/env python3
"""
Patch missing gap data for daily (non-weekly) option expiries.

Each daily expiry's contract starts trading at ~17:30 on the day before
first_appearance midnight.  The original collection (which probed the ladder
at 09:00) started at first_appearance 00:00 and missed the ~6.5-hour window
from the previous day 17:30 → 23:59.

For Saturday and Sunday expiries this gap is confirmed to contain real
candles.  For weekday expiries (Mon–Thu) the gap window is usually empty
but the script is a no-op for those (fetches empty, skips merge).

Gap window filled per expiry:
    gap_start = (old_first_appearance − 1 day) 17:00 IST
    gap_end   = (old_first_appearance − 1 day) 23:59 IST

Usage:
    python patch_daily_gaps.py --weekday 0        # Monday only
    python patch_daily_gaps.py --weekday 0 1 2 3  # Mon-Thu
    python patch_daily_gaps.py --weekday 5        # Saturday only
    python patch_daily_gaps.py --weekday 6        # Sunday only
    python patch_daily_gaps.py --weekday 5 6      # Sat + Sun
    python patch_daily_gaps.py --all              # all non-Friday daily expiries
    python patch_daily_gaps.py --weekday 0 --dry-run  # preview only

Processes in reverse chronological order (most recent first).
"""

import asyncio
import os
import sys
import argparse
import logging
import time
from datetime import date, timedelta, datetime

import requests
import pytz
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn,
)
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich import box

console = Console()

# ── Telegram ───────────────────────────────────────────────────────────────────
BOT_TOKEN = "8675658345:AAEn9yIj3uZdu3TkPnXY2P1ubHgGMQc4COk"
CHAT_ID   = "1088952172"

def tg(msg: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from api_client import DeltaAPIClient
from ist_utils import make_ist, ist_to_unix, get_expiry_ladder
from parquet_writer import merge_option_data, append_or_create_option
from config import OPTIONS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

IST      = pytz.timezone("Asia/Kolkata")
SEM      = asyncio.Semaphore(50)

DAY_NAMES = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


# ── Old first_appearance (probed at 09:00 before the fix) ─────────────────────

def _old_first_appearance(expiry_dt: datetime) -> datetime:
    """Reproduce the original probe-at-09:00 behaviour for gap computation."""
    probe_start = expiry_dt - timedelta(days=70)
    result = expiry_dt - timedelta(days=2)
    check = probe_start.replace(hour=9, minute=0, second=0, microsecond=0)
    while check.date() < expiry_dt.date():
        ladder = get_expiry_ladder(check)
        if any(e.date() == expiry_dt.date() for e in ladder):
            result = check.replace(hour=0, minute=0, second=0)
            break
        check += timedelta(days=1)
    return result


def gap_window(expiry_dt: datetime) -> tuple[int, int]:
    """
    Return (gap_start_unix, gap_end_unix) for the missing window:
        (old_first_appearance − 1 day) 17:00 IST  →  23:59 IST
    """
    old_fa   = _old_first_appearance(expiry_dt)
    gap_date = old_fa.date() - timedelta(days=1)
    start_dt = make_ist(gap_date.year, gap_date.month, gap_date.day, 17, 0)
    end_dt   = make_ist(gap_date.year, gap_date.month, gap_date.day, 23, 59)
    return ist_to_unix(start_dt), ist_to_unix(end_dt), gap_date


# ── Directory helpers ──────────────────────────────────────────────────────────

def list_expiry_dirs(weekdays: set[int]) -> list[tuple[date, str]]:
    """
    List all expiry=YYYY-MM-DD directories, filtered by weekday set,
    sorted reverse-chronologically.
    """
    if not os.path.exists(OPTIONS_DIR):
        return []
    results = []
    for name in os.listdir(OPTIONS_DIR):
        if not name.startswith("expiry="):
            continue
        date_str = name[len("expiry="):]
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            continue
        if d.weekday() in weekdays:
            results.append((d, date_str))
    results.sort(reverse=True)
    return results


def list_strikes(expiry_str: str) -> list[int]:
    expiry_dir = os.path.join(OPTIONS_DIR, f"expiry={expiry_str}")
    strikes = []
    for name in os.listdir(expiry_dir):
        if name.startswith("strike="):
            try:
                strikes.append(int(name[len("strike="):]))
            except ValueError:
                pass
    return sorted(strikes)


# ── Per-expiry patch ───────────────────────────────────────────────────────────

async def patch_one_expiry(
    client: DeltaAPIClient,
    expiry_dt: datetime,
    expiry_str: str,
    dry_run: bool,
) -> dict:
    """Fetch gap window for every strike of expiry_str and merge. Returns stats."""
    g_start, g_end, gap_date = gap_window(expiry_dt)
    strikes = list_strikes(expiry_str)

    stats = {"strikes": len(strikes), "new_candles": 0, "updated_files": 0, "skipped": 0}

    if not strikes:
        return stats

    ddmmyy = expiry_dt.strftime("%d%m%y")

    async def fetch_and_merge(strike: int, opt_type: str):
        prefix = "C" if opt_type == "CE" else "P"
        mark_sym = f"MARK:{prefix}-BTC-{strike}-{ddmmyy}"
        oi_sym   = f"OI:{prefix}-BTC-{strike}-{ddmmyy}"

        async with SEM:
            mark_c = await client.fetch_candles(mark_sym, g_start, g_end)
            oi_c   = await client.fetch_candles(oi_sym,   g_start, g_end)

        if not mark_c and not oi_c:
            stats["skipped"] += 1
            return

        if dry_run:
            log.info("  [DRY] %s %s: %d mark + %d oi candles",
                     expiry_str, f"{strike}{opt_type}", len(mark_c), len(oi_c))
            stats["new_candles"] += len(mark_c) or len(oi_c)
            stats["updated_files"] += 1
            return

        new_table = merge_option_data(mark_c, oi_c)
        append_or_create_option(new_table, expiry_str, strike, opt_type)
        stats["new_candles"]   += len(new_table)
        stats["updated_files"] += 1

    tasks = [
        fetch_and_merge(s, t)
        for s in strikes
        for t in ("CE", "PE")
    ]
    await asyncio.gather(*tasks)
    return stats


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Patch missing 17:00-23:59 gap for daily (non-weekly) expiries"
    )
    parser.add_argument(
        "--weekday", type=int, nargs="+",
        help="Day(s) of week: 0=Mon 1=Tue 2=Wed 3=Thu 5=Sat 6=Sun",
    )
    parser.add_argument("--all",     action="store_true", help="All non-Friday daily expiries")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()

    if not args.weekday and not args.all:
        parser.print_help()
        sys.exit(1)

    if args.all:
        allowed = {0, 1, 2, 3, 5, 6}
    else:
        allowed = set(args.weekday)
        if 4 in allowed:
            print("ERROR: Friday (4) expiries are weekly/monthly — not patched here.")
            sys.exit(1)

    api_key = os.getenv("ACCOUNT_1_KEY")
    if not api_key:
        print("ERROR: ACCOUNT_1_KEY not found in .env")
        sys.exit(1)

    # Silence noisy httpx logs so they don't break the live display
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    expiries = list_expiry_dirs(allowed)
    day_labels       = ", ".join(DAY_NAMES[w] for w in sorted(allowed))
    day_labels_short = "+".join(DAY_NAMES[w] for w in sorted(allowed))

    console.print(f"[bold]Gap Patch[/bold]  days={day_labels}  expiries={len(expiries)}"
                  + ("  [yellow][DRY RUN][/yellow]" if args.dry_run else ""))

    total_new   = 0
    total_files = 0
    total_skip  = 0
    t0          = time.monotonic()

    # State shared between async tasks and the display
    state = {"expiry": "", "gap_date": "", "day": "", "candles": 0, "files": 0}

    if not args.dry_run:
        tg(f"🔧 <b>Gap patch started</b>\n"
           f"Days: {day_labels_short} | Expiries: {len(expiries)}\n"
           f"Order: newest → oldest")

    done_count = [0]  # mutable so make_table can read current value

    def make_table() -> Table:
        elapsed = time.monotonic() - t0
        n       = done_count[0]
        rate    = n / elapsed if elapsed > 0 else 0
        eta_s   = (len(expiries) - n) / rate if rate > 0 else 0
        t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
        t.add_column(style="dim")
        t.add_column()
        t.add_row("Expiry",    f"[cyan]{state['expiry']}[/cyan]  ({state['day']})")
        t.add_row("Gap date",  f"{state['gap_date']} 17:00–23:59 IST")
        t.add_row("Candles",   f"[green]{state['candles']:,}[/green] new  |  {state['files']} files updated")
        t.add_row("Rate",      f"{rate*60:.1f} expiries/min  |  ETA {eta_s/60:.0f} min")
        return t

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as prog_bar:
        task = prog_bar.add_task(f"[cyan]{day_labels_short}[/cyan]", total=len(expiries))

        with Live(make_table(), console=console, refresh_per_second=4, transient=True) as live:
            async with DeltaAPIClient("lava", api_key) as client:
                for i, (d, ds) in enumerate(expiries, 1):
                    expiry_dt = make_ist(d.year, d.month, d.day, 17, 30)
                    _, _, gap_date = gap_window(expiry_dt)

                    state["expiry"]   = ds
                    state["gap_date"] = str(gap_date)
                    state["day"]      = DAY_NAMES[d.weekday()]
                    live.update(make_table())

                    stats = await patch_one_expiry(client, expiry_dt, ds, args.dry_run)

                    total_new   += stats["new_candles"]
                    total_files += stats["updated_files"]
                    total_skip  += stats["skipped"]
                    state["candles"] = total_new
                    state["files"]   = total_files

                    done_count[0] = i
                    prog_bar.advance(task)
                    live.update(make_table())

                    # Telegram update every 10 expiries
                    if not args.dry_run and i % 10 == 0:
                        elapsed  = time.monotonic() - t0
                        rate     = i / elapsed * 60
                        eta_min  = (len(expiries) - i) / rate if rate > 0 else 0
                        tg(f"📊 <b>Gap patch</b> [{day_labels_short}]\n"
                           f"Done: {i}/{len(expiries)} expiries\n"
                           f"New candles: {total_new:,}\n"
                           f"Files updated: {total_files}\n"
                           f"ETA: ~{eta_min:.0f} min")

    elapsed_min = (time.monotonic() - t0) / 60
    console.print(f"\n[bold green]Done![/bold green]  "
                  f"expiries={len(expiries)}  files={total_files}  "
                  f"candles={total_new:,}  skipped={total_skip}  "
                  f"elapsed={elapsed_min:.1f}min")

    tg_summary = (
        f"{'[DRY RUN] ' if args.dry_run else ''}✅ <b>Gap patch done</b> [{day_labels_short}]\n"
        f"Expiries : {len(expiries)}\n"
        f"Files updated : {total_files}\n"
        f"New candles : {total_new:,}\n"
        f"Elapsed : {elapsed_min:.1f} min"
    )
    if not args.dry_run:
        tg(tg_summary)


if __name__ == "__main__":
    asyncio.run(main())
