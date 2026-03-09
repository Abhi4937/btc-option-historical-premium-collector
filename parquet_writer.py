"""
Atomic Parquet writes with ZSTD compression.

Design rules:
  - Always write to .tmp file first, then os.rename() for atomicity
  - append_or_create: loads existing parquet, merges, deduplicates, rewrites
  - All timestamps stored as IST naive datetimes + unix seconds
  - Compression: ZSTD level 3, row_group_size=10000, write_statistics=True

Test 5: Last option candle is 17:29 IST (settlement 17:30 not included in data).
"""

import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from ist_utils import unix_to_ist
from config import (
    PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL, PARQUET_ROW_GROUP_SIZE,
    OPTIONS_DIR,
)

log = logging.getLogger(__name__)

# ── Schemas ───────────────────────────────────────────────────────────────────

SPOT_SCHEMA = pa.schema([
    pa.field("timestamp_ist",  pa.timestamp("s"),  nullable=False),
    pa.field("timestamp_unix", pa.int64(),          nullable=False),
    pa.field("mark_open",      pa.float64()),
    pa.field("mark_high",      pa.float64()),
    pa.field("mark_low",       pa.float64()),
    pa.field("mark_close",     pa.float64()),
    pa.field("ltp_volume",     pa.float64()),
    pa.field("oi_open",        pa.float64()),
    pa.field("oi_high",        pa.float64()),
    pa.field("oi_low",         pa.float64()),
    pa.field("oi_close",       pa.float64()),
])

OPTIONS_SCHEMA = pa.schema([
    pa.field("timestamp_ist",  pa.timestamp("s"),  nullable=False),
    pa.field("timestamp_unix", pa.int64(),          nullable=False),
    pa.field("mark_open",      pa.float64()),
    pa.field("mark_high",      pa.float64()),
    pa.field("mark_low",       pa.float64()),
    pa.field("mark_close",     pa.float64()),
    pa.field("oi_open",        pa.float64()),
    pa.field("oi_high",        pa.float64()),
    pa.field("oi_low",         pa.float64()),
    pa.field("oi_close",       pa.float64()),
])

# ── Parquet write settings ────────────────────────────────────────────────────

def _write_kwargs():
    return dict(
        compression          = PARQUET_COMPRESSION,
        compression_level    = PARQUET_COMPRESSION_LEVEL,
        row_group_size       = PARQUET_ROW_GROUP_SIZE,
        write_statistics     = True,
        use_dictionary       = True,
    )


def _atomic_write(table: pa.Table, path: str):
    """Write to .tmp then rename for atomicity."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    pq.write_table(table, tmp, **_write_kwargs())
    os.replace(tmp, path)


# ── Candle → dict helpers ─────────────────────────────────────────────────────

def _ist_naive(unix_seconds: int):
    """Returns timezone-naive IST datetime (stored as such in parquet)."""
    dt = unix_to_ist(unix_seconds)
    return dt.replace(tzinfo=None)


def _candle_map(candles: list[dict]) -> dict[int, dict]:
    """Keyed by unix timestamp."""
    return {c["time"]: c for c in candles}


# ── Spot data ─────────────────────────────────────────────────────────────────

def merge_spot_data(
    mark_candles: list[dict],
    ltp_candles:  list[dict],
    oi_candles:   list[dict],
) -> pa.Table:
    """
    Join mark, ltp, oi candles on unix timestamp.
    Only volume field taken from ltp_candles (LTP OHLC unreliable for options;
    here for spot it provides real volume signal).
    """
    mark_map = _candle_map(mark_candles)
    ltp_map  = _candle_map(ltp_candles)
    oi_map   = _candle_map(oi_candles)

    # Union of all timestamps
    all_ts = sorted(set(mark_map) | set(ltp_map) | set(oi_map))

    rows = {
        "timestamp_ist":  [],
        "timestamp_unix": [],
        "mark_open":      [],
        "mark_high":      [],
        "mark_low":       [],
        "mark_close":     [],
        "ltp_volume":     [],
        "oi_open":        [],
        "oi_high":        [],
        "oi_low":         [],
        "oi_close":       [],
    }

    for ts in all_ts:
        mk = mark_map.get(ts, {})
        lt = ltp_map.get(ts, {})
        oi = oi_map.get(ts, {})

        rows["timestamp_ist"].append(_ist_naive(ts))
        rows["timestamp_unix"].append(ts)
        rows["mark_open"].append(mk.get("open"))
        rows["mark_high"].append(mk.get("high"))
        rows["mark_low"].append(mk.get("low"))
        rows["mark_close"].append(mk.get("close"))
        rows["ltp_volume"].append(lt.get("volume"))
        rows["oi_open"].append(oi.get("open"))
        rows["oi_high"].append(oi.get("high"))
        rows["oi_low"].append(oi.get("low"))
        rows["oi_close"].append(oi.get("close"))

    return pa.table(rows, schema=SPOT_SCHEMA)


def merge_option_data(
    mark_candles: list[dict],
    oi_candles:   list[dict],
) -> pa.Table:
    """Merge MARK + OI candles for a single option strike/expiry."""
    mark_map = _candle_map(mark_candles)
    oi_map   = _candle_map(oi_candles)

    all_ts = sorted(set(mark_map) | set(oi_map))

    rows = {
        "timestamp_ist":  [],
        "timestamp_unix": [],
        "mark_open":      [],
        "mark_high":      [],
        "mark_low":       [],
        "mark_close":     [],
        "oi_open":        [],
        "oi_high":        [],
        "oi_low":         [],
        "oi_close":       [],
    }

    for ts in all_ts:
        mk = mark_map.get(ts, {})
        oi = oi_map.get(ts, {})

        rows["timestamp_ist"].append(_ist_naive(ts))
        rows["timestamp_unix"].append(ts)
        rows["mark_open"].append(mk.get("open"))
        rows["mark_high"].append(mk.get("high"))
        rows["mark_low"].append(mk.get("low"))
        rows["mark_close"].append(mk.get("close"))
        rows["oi_open"].append(oi.get("open"))
        rows["oi_high"].append(oi.get("high"))
        rows["oi_low"].append(oi.get("low"))
        rows["oi_close"].append(oi.get("close"))

    return pa.table(rows, schema=OPTIONS_SCHEMA)


# ── Write functions ───────────────────────────────────────────────────────────

def write_spot(table: pa.Table, path: str):
    """Atomic write of spot parquet."""
    _atomic_write(table, path)
    log.info("Spot written: %s (%d rows)", path, len(table))


def write_option(table: pa.Table, expiry_date: str, strike: int, option_type: str) -> str:
    """
    Write option parquet to canonical path.
    Returns the path written.

    Layout: data/options/expiry=YYYY-MM-DD/strike=NNNNN/CE.parquet
    """
    path = os.path.join(
        OPTIONS_DIR,
        f"expiry={expiry_date}",
        f"strike={strike}",
        f"{option_type}.parquet",
    )
    _atomic_write(table, path)
    log.debug("Option written: %s (%d rows)", path, len(table))
    return path


def append_or_create_spot(new_table: pa.Table, path: str):
    """
    Append new_table to existing spot parquet at path.
    Deduplicates by timestamp_unix. Writes atomically.
    """
    if os.path.exists(path):
        existing = pq.read_table(path, schema=SPOT_SCHEMA)
        combined = pa.concat_tables([existing, new_table])
    else:
        combined = new_table

    # Deduplicate by timestamp_unix (keep last occurrence)
    df = combined.to_pydict()
    seen: set[int] = set()
    idx = []
    # Traverse in reverse so newer rows win
    for i in range(len(df["timestamp_unix"]) - 1, -1, -1):
        ts = df["timestamp_unix"][i]
        if ts not in seen:
            seen.add(ts)
            idx.append(i)
    idx.sort()

    deduped = combined.take(idx)
    # Sort by timestamp
    sort_idx = pc.sort_indices(deduped, sort_keys=[("timestamp_unix", "ascending")])
    deduped = deduped.take(sort_idx)

    _atomic_write(deduped, path)
    log.info("Spot appended/deduped: %s (%d → %d rows)",
             path, len(combined), len(deduped))


def append_or_create_option(
    new_table: pa.Table,
    expiry_date: str,
    strike: int,
    option_type: str,
) -> str:
    """Append new data to existing option parquet, deduplicate, write atomically."""
    path = os.path.join(
        OPTIONS_DIR,
        f"expiry={expiry_date}",
        f"strike={strike}",
        f"{option_type}.parquet",
    )

    if os.path.exists(path):
        existing = pq.read_table(path, schema=OPTIONS_SCHEMA)
        combined = pa.concat_tables([existing, new_table])
    else:
        combined = new_table

    # Deduplicate by timestamp_unix
    seen: set[int] = set()
    idx = []
    ts_list = combined.column("timestamp_unix").to_pylist()
    for i in range(len(ts_list) - 1, -1, -1):
        if ts_list[i] not in seen:
            seen.add(ts_list[i])
            idx.append(i)
    idx.sort()

    deduped = combined.take(idx)
    sort_idx = pc.sort_indices(deduped, sort_keys=[("timestamp_unix", "ascending")])
    deduped = deduped.take(sort_idx)

    _atomic_write(deduped, path)
    return path
