"""
Orchestrates 5 AccountWorkers in parallel.
Each worker independently claims expiry months and processes them.
Work-stealing: as soon as a worker finishes a month it immediately claims the next.
"""

import asyncio
import logging
import os
from datetime import date

from dotenv import load_dotenv

from config import (
    ACTIVE_ACCOUNTS, ACCOUNT_NAMES,
    COLLECTION_START_DATE, MANIFEST_DB, REGISTRY_DB,
    SPOT_DIR, OPTIONS_DIR, DB_DIR, LOGS_DIR,
)
from manifest import init_manifest, populate_manifest, reset_stale_in_progress, reset_future_months
from registry import init_registry, reset_stale_in_progress as reset_registry_stale
from worker import AccountWorker
from ist_utils import now_ist

log = logging.getLogger(__name__)


def _load_accounts() -> list[dict]:
    """Load account credentials from .env file."""
    load_dotenv()
    accounts = []
    for i, name in enumerate(ACCOUNT_NAMES, start=1):
        env_name   = os.getenv(f"ACCOUNT_{i}_NAME",   name)
        env_key    = os.getenv(f"ACCOUNT_{i}_KEY")
        if not env_key:
            log.warning("No API key for account %d (%s) — skipping", i, name)
            continue
        accounts.append({"name": env_name, "key": env_key})
    return accounts


def _ensure_dirs():
    for d in [SPOT_DIR, OPTIONS_DIR, DB_DIR, LOGS_DIR]:
        os.makedirs(d, exist_ok=True)


async def run_collection(
    resume: bool = False,
    status_callback=None,
    active_accounts: int = ACTIVE_ACCOUNTS,
):
    """
    Main collection coroutine.

    Args:
        resume: if True, reset stale in_progress before starting
        status_callback: called by workers for progress updates
        active_accounts: how many accounts to use (default: all 5)
    """
    _ensure_dirs()

    # Init databases
    await init_manifest()
    await init_registry()

    # Determine date range
    start = COLLECTION_START_DATE
    now   = now_ist()
    end   = now.date()

    # Extend manifest 3 months ahead to cover all weekly Friday expiries that are
    # currently live and being collected.
    from datetime import timedelta
    end_3m = (now + timedelta(days=92)).date()

    await populate_manifest(start.year, start.month, end_3m.year, end_3m.month)

    # Reset any future months (> today) that were previously marked done back to
    # pending so newly-available Fridays (whose first_appearance just passed) get
    # picked up without manual intervention.
    reset_n = await reset_future_months(end)
    if reset_n:
        log.info("Reset %d future month(s) to pending for new Friday re-check", reset_n)

    if resume:
        stale_m = await reset_stale_in_progress()
        stale_r = await reset_registry_stale()
        log.info("Resume: reset %d manifest months + %d registry symbols", stale_m, stale_r)

    accounts = _load_accounts()
    if not accounts:
        raise RuntimeError("No API accounts configured in .env")

    n = min(active_accounts, len(accounts))
    log.info("Starting collection: %d accounts, %s → %s", n, start, end)

    workers = [
        AccountWorker(
            account_name    = acc["name"],
            api_key         = acc["key"],
            status_callback = status_callback,
        )
        for acc in accounts[:n]
    ]

    await asyncio.gather(*[w.run() for w in workers])
    log.info("Collection complete.")


async def fetch_spot_only():
    """Fetch/update spot data only (MARK:BTCUSD, BTCUSD, OI:BTCUSD)."""
    from api_client import DeltaAPIClient
    from parquet_writer import merge_spot_data, append_or_create_spot
    from config import SPOT_PARQUET
    from ist_utils import ist_to_unix, make_ist

    _ensure_dirs()
    await init_registry()

    load_dotenv()
    key = os.getenv("ACCOUNT_1_KEY")
    if not key:
        raise RuntimeError("ACCOUNT_1_KEY not found in .env")

    start = COLLECTION_START_DATE
    end   = now_ist().date()

    log.info("Fetching spot data %s → %s", start, end)

    # Process month by month to avoid huge single request
    from ist_utils import month_start_ist, month_end_ist
    import calendar

    async with DeltaAPIClient("lava", key) as client:
        y, m = start.year, start.month
        while (y, m) <= (end.year, end.month):
            ms_dt = month_start_ist(y, m)
            me_dt = month_end_ist(y, m)
            s_unix = ist_to_unix(ms_dt)
            e_unix = ist_to_unix(me_dt)

            mark_c = await client.fetch_candles("MARK:BTCUSD", s_unix, e_unix)
            ltp_c  = await client.fetch_candles("BTCUSD",      s_unix, e_unix)
            oi_c   = await client.fetch_candles("OI:BTCUSD",   s_unix, e_unix)

            log.info("%04d-%02d: mark=%d ltp=%d oi=%d",
                     y, m, len(mark_c), len(ltp_c), len(oi_c))

            if mark_c or ltp_c or oi_c:
                table = merge_spot_data(mark_c, ltp_c, oi_c)
                append_or_create_spot(table, SPOT_PARQUET)

            m += 1
            if m > 12:
                m = 1
                y += 1

    log.info("Spot fetch complete. File: %s", SPOT_PARQUET)
