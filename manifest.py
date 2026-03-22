"""
SQLite manifest — work distribution across 5 accounts.
Work unit = EXPIRY MONTH (month when strikes settle).

Why expiry month partitioning:
  Calendar month would cause conflicts (Jan's last days create Feb expiries).
  Expiry month ensures each expiry date is owned by exactly one account.
"""

import aiosqlite
import asyncio
import logging
from contextlib import asynccontextmanager

from config import MANIFEST_DB, STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_DONE, STATUS_FAILED
from ist_utils import format_ist, now_ist, all_expiry_dates_in_month, expiry_month_key

log = logging.getLogger(__name__)


@asynccontextmanager
async def _db():
    async with aiosqlite.connect(MANIFEST_DB) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=30000")
        db.row_factory = aiosqlite.Row
        yield db

CREATE_MANIFEST_SQL = """
CREATE TABLE IF NOT EXISTS manifest (
    expiry_month    TEXT PRIMARY KEY,   -- 'YYYY-MM'
    status          TEXT NOT NULL DEFAULT 'pending',
    claimed_by      TEXT,
    claimed_at      TEXT,
    completed_at    TEXT,
    total_calls     INTEGER DEFAULT 0,
    strikes_fetched INTEGER DEFAULT 0,
    error_message   TEXT,
    retry_count     INTEGER DEFAULT 0
)
"""

_lock = asyncio.Lock()


async def init_manifest():
    async with _db() as db:
        await db.execute(CREATE_MANIFEST_SQL)
        await db.commit()
    log.info("Manifest initialized: %s", MANIFEST_DB)


async def populate_manifest(start_year: int, start_month: int,
                             end_year: int, end_month: int):
    """
    Insert all expiry months in [start, end] that don't already exist.
    Only adds 'pending' rows — never overwrites existing status.
    """
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    async with _db() as db:
        for key in months:
            await db.execute(
                "INSERT OR IGNORE INTO manifest (expiry_month, status) VALUES (?, 'pending')",
                (key,)
            )
        await db.commit()
    log.info("Manifest populated: %d months (%s to %s)",
             len(months), months[0], months[-1])


async def claim_next_month(account_name: str) -> str | None:
    """
    Atomically claim the next 'pending' expiry month.
    Returns the month key (e.g. '2024-03') or None if all done.
    """
    async with _lock:
        async with _db() as db:
            async with db.execute("""
                SELECT expiry_month FROM manifest
                WHERE status='pending'
                ORDER BY expiry_month ASC
                LIMIT 1
            """) as cur:
                row = await cur.fetchone()

            if row is None:
                return None

            month_key = row[0]
            await db.execute("""
                UPDATE manifest
                SET status='in_progress', claimed_by=?, claimed_at=?
                WHERE expiry_month=?
            """, (account_name, format_ist(now_ist()), month_key))
            await db.commit()
            log.info("[%s] Claimed expiry month: %s", account_name, month_key)
            return month_key


async def mark_month_done(month_key: str, account_name: str,
                           total_calls: int, strikes_fetched: int):
    async with _db() as db:
        await db.execute("""
            UPDATE manifest
            SET status='done', completed_at=?, total_calls=?, strikes_fetched=?
            WHERE expiry_month=?
        """, (format_ist(now_ist()), total_calls, strikes_fetched, month_key))
        await db.commit()
    log.info("[%s] Month %s DONE — calls=%d strikes=%d",
             account_name, month_key, total_calls, strikes_fetched)


async def mark_month_failed(month_key: str, account_name: str, error: str):
    async with _db() as db:
        async with db.execute(
            "SELECT retry_count FROM manifest WHERE expiry_month=?", (month_key,)
        ) as cur:
            row = await cur.fetchone()
        retry = (row[0] or 0) + 1 if row else 1

        new_status = STATUS_FAILED if retry >= 3 else STATUS_PENDING
        await db.execute("""
            UPDATE manifest
            SET status=?, error_message=?, retry_count=?
            WHERE expiry_month=?
        """, (new_status, error[:500], retry, month_key))
        await db.commit()
    log.error("[%s] Month %s FAILED (retry %d): %s", account_name, month_key, retry, error)


async def reset_stale_in_progress():
    """Reset in_progress months to pending — used by resume command."""
    async with _db() as db:
        cur = await db.execute(
            "UPDATE manifest SET status='pending', claimed_by=NULL WHERE status='in_progress'"
        )
        await db.commit()
        return cur.rowcount


async def reset_future_months(today) -> int:
    """
    Reset done future months (expiry_month > today's YYYY-MM) back to pending.
    Called at the start of every run so newly-available Friday expiries
    (whose first_appearance just passed) are re-checked without manual intervention.
    today: a date object.
    """
    today_key = f"{today.year:04d}-{today.month:02d}"
    async with _db() as db:
        cur = await db.execute(
            "UPDATE manifest SET status='pending', claimed_by=NULL "
            "WHERE expiry_month > ? AND status='done'",
            (today_key,)
        )
        await db.commit()
        return cur.rowcount


async def get_manifest_summary() -> list[dict]:
    async with _db() as db:
        async with db.execute(
            "SELECT * FROM manifest ORDER BY expiry_month"
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_progress_counts() -> dict:
    async with _db() as db:
        async with db.execute("""
            SELECT status, COUNT(*) as cnt
            FROM manifest GROUP BY status
        """) as cur:
            rows = await cur.fetchall()
        return {r[0]: r[1] for r in rows}
