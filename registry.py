"""
SQLite registry — per-symbol tracking and expiry_strikes table.

VERIFIED FINDING — Test 6:
  Both empty and fake symbols return HTTP 200 + {"result": [], "success": true}.
  We CANNOT distinguish them purely from the candle API.
  Strategy:
    1. Before fetching, check /v2/products → if absent → NOT_LISTED
    2. If listed but candles return [] → EMPTY
    3. Both statuses are terminal (never retried)
"""

import asyncio
import aiosqlite
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from config import REGISTRY_DB, STATUS_PENDING, STATUS_IN_PROGRESS, STATUS_DONE
from config import STATUS_EMPTY, STATUS_NOT_LISTED, STATUS_FAILED
from ist_utils import format_ist, now_ist


_write_lock = asyncio.Lock()


@asynccontextmanager
async def _db(write: bool = False):
    """
    Open a registry DB connection.
    write=True acquires a process-level asyncio lock to serialize concurrent writers,
    preventing 'database is locked' under high parallelism.
    """
    if write:
        async with _write_lock:
            async with aiosqlite.connect(REGISTRY_DB) as db:
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("PRAGMA busy_timeout=30000")
                db.row_factory = aiosqlite.Row
                yield db
    else:
        async with aiosqlite.connect(REGISTRY_DB) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA busy_timeout=30000")
            db.row_factory = aiosqlite.Row
            yield db


log = logging.getLogger(__name__)


CREATE_SYMBOLS_SQL = """
CREATE TABLE IF NOT EXISTS symbols (
    symbol          TEXT    PRIMARY KEY,
    expiry_date     TEXT    NOT NULL,
    strike          INTEGER NOT NULL,
    option_type     TEXT    NOT NULL,   -- 'CE' or 'PE'
    from_time_ist   TEXT    NOT NULL,
    to_time_ist     TEXT    NOT NULL,
    total_candles   INTEGER DEFAULT 0,
    fetched_at      TEXT,
    claimed_by      TEXT,
    parquet_path    TEXT,
    status          TEXT    NOT NULL DEFAULT 'pending'
)
"""

CREATE_EXPIRY_STRIKES_SQL = """
CREATE TABLE IF NOT EXISTS expiry_strikes (
    expiry_date     TEXT    NOT NULL,
    strike          INTEGER NOT NULL,
    first_seen_ist  TEXT    NOT NULL,
    first_seen_unix INTEGER NOT NULL,
    last_seen_ist   TEXT,
    PRIMARY KEY (expiry_date, strike)
)
"""

CREATE_SPOT_PROGRESS_SQL = """
CREATE TABLE IF NOT EXISTS spot_progress (
    date_range_key  TEXT PRIMARY KEY,   -- 'YYYY-MM' month key
    status          TEXT NOT NULL DEFAULT 'pending',
    candles_written INTEGER DEFAULT 0,
    fetched_at      TEXT
)
"""


async def init_registry():
    async with _db(write=True) as db:
        await db.execute(CREATE_SYMBOLS_SQL)
        await db.execute(CREATE_EXPIRY_STRIKES_SQL)
        await db.execute(CREATE_SPOT_PROGRESS_SQL)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sym_expiry ON symbols (expiry_date)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sym_status ON symbols (status)")
        await db.commit()
    log.info("Registry initialized: %s", REGISTRY_DB)


# ── Symbol registration ───────────────────────────────────────────────────────

async def register_symbols_batch(rows: list[tuple]):
    """
    Batch insert all symbols for a month in one transaction.
    Each row: (symbol, expiry_date, strike, option_type,
               from_time_ist, to_time_ist, claimed_by)
    Skips symbols that already exist.
    """
    async with _db(write=True) as db:
        await db.executemany("""
            INSERT OR IGNORE INTO symbols
              (symbol, expiry_date, strike, option_type,
               from_time_ist, to_time_ist, claimed_by, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        """, rows)
        await db.commit()


async def mark_symbol_done(symbol: str, total_candles: int, parquet_path: str):
    async with _db(write=True) as db:
        await db.execute("""
            UPDATE symbols
            SET status='done', total_candles=?, parquet_path=?, fetched_at=?
            WHERE symbol=?
        """, (total_candles, parquet_path, format_ist(now_ist()), symbol))
        await db.commit()


async def mark_symbol_empty(symbol: str):
    """HTTP 200 + [] — symbol listed but no candle data."""
    async with _db(write=True) as db:
        await db.execute(
            "UPDATE symbols SET status='empty', fetched_at=? WHERE symbol=?",
            (format_ist(now_ist()), symbol)
        )
        await db.commit()


async def mark_symbol_not_listed(symbol: str):
    """Symbol not found in /v2/products — never retry."""
    async with _db(write=True) as db:
        await db.execute(
            "UPDATE symbols SET status='not_listed', fetched_at=? WHERE symbol=?",
            (format_ist(now_ist()), symbol)
        )
        await db.commit()


async def mark_symbol_failed(symbol: str, error: str):
    async with _db(write=True) as db:
        await db.execute("""
            UPDATE symbols SET status='failed', fetched_at=?
            WHERE symbol=?
        """, (format_ist(now_ist()), symbol))
        await db.commit()
    log.error("Symbol FAILED: %s — %s", symbol, error)


async def get_symbol_status(symbol: str) -> str | None:
    async with _db() as db:
        async with db.execute(
            "SELECT status FROM symbols WHERE symbol=?", (symbol,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None



async def reset_stale_in_progress():
    """No-op — in_progress status removed. Kept for API compatibility."""
    return 0


# ── Expiry strikes tracking ───────────────────────────────────────────────────


# ── Spot progress ─────────────────────────────────────────────────────────────

async def mark_spot_done(month_key: str, candles_written: int):
    async with _db(write=True) as db:
        await db.execute("""
            INSERT OR REPLACE INTO spot_progress
              (date_range_key, status, candles_written, fetched_at)
            VALUES (?, 'done', ?, ?)
        """, (month_key, candles_written, format_ist(now_ist())))
        await db.commit()


async def is_spot_done(month_key: str) -> bool:
    async with _db() as db:
        async with db.execute(
            "SELECT status FROM spot_progress WHERE date_range_key=?",
            (month_key,)
        ) as cur:
            row = await cur.fetchone()
            return row is not None and row[0] == "done"


# ── Stats ─────────────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    async with _db() as db:
        async with db.execute("""
            SELECT status, COUNT(*) as cnt, SUM(total_candles) as total
            FROM symbols GROUP BY status
        """) as cur:
            rows = await cur.fetchall()
        stats = {r[0]: {"count": r[1], "candles": r[2] or 0} for r in rows}

        async with db.execute(
            "SELECT COUNT(DISTINCT expiry_date) FROM expiry_strikes"
        ) as cur:
            row = await cur.fetchone()
            stats["unique_expiries_tracked"] = row[0] if row else 0

        return stats
