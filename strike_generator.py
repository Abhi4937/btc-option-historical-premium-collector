"""
Strike chain generation, expiry ladder, and BTC option symbol builder.

VERIFIED FINDINGS:
  Test 2: Symbol format C-BTC-{strike}-{DDMMYY}, MARK: and OI: prefixes work
  Test 3: Strike interval non-uniform but $100 near ATM. Using $100 grid at ±40
           strikes = 81 strikes per snapshot. More accurate ATM rounding than $200.
  Test 9: Expiry ladder verified against live exchange.
"""

import logging
from datetime import datetime

from config import DEFAULT_STRIKE_INTERVAL, CHAIN_HALF_WIDTH
from ist_utils import ddmmyy, get_expiry_ladder, first_appearance, now_ist

log = logging.getLogger(__name__)


def get_atm_strike(mark_close: float, interval: int = DEFAULT_STRIKE_INTERVAL) -> int:
    """Floor price to nearest lower multiple of interval.
    e.g. $80,050 at interval=100 → $80,000 (not $80,100).
    """
    return int(mark_close / interval) * interval


def get_strike_chain(
    atm_strike: int,
    half_width: int = CHAIN_HALF_WIDTH,
    interval: int = DEFAULT_STRIKE_INTERVAL,
) -> list[int]:
    """
    Returns ATM ± half_width strikes = 2*half_width+1 unique strikes.
    Default: ±40 × $100 = 81 strikes.
    """
    return [
        atm_strike + i * interval
        for i in range(-half_width, half_width + 1)
        if atm_strike + i * interval > 0
    ]


def build_option_symbol(
    option_type: str,     # 'CE' or 'PE'
    strike: int,
    expiry_dt: datetime,
) -> str:
    """
    Builds option symbol WITHOUT prefix.
    Test 2: format confirmed as C-BTC-{strike}-{DDMMYY}

    Examples:
      CE, 95000, 2026-02-07 → C-BTC-95000-070226
      PE, 64600, 2026-03-09 → P-BTC-64600-090326
    """
    prefix = "C" if option_type == "CE" else "P"
    return f"{prefix}-BTC-{strike}-{ddmmyy(expiry_dt)}"


def build_mark_symbol(option_type: str, strike: int, expiry_dt: datetime) -> str:
    """MARK:C-BTC-{strike}-{DDMMYY} — used for price data."""
    return f"MARK:{build_option_symbol(option_type, strike, expiry_dt)}"


def build_oi_symbol(option_type: str, strike: int, expiry_dt: datetime) -> str:
    """OI:C-BTC-{strike}-{DDMMYY} — used for open interest data."""
    return f"OI:{build_option_symbol(option_type, strike, expiry_dt)}"


def get_symbols_for_snapshot(
    atm_strike: int,
    expiry_dt: datetime,
    interval: int = DEFAULT_STRIKE_INTERVAL,
    half_width: int = CHAIN_HALF_WIDTH,
) -> list[dict]:
    """
    Returns list of symbol dicts for all strikes × option types × prefixes
    for a single expiry.

    Each dict: {raw_symbol, mark_symbol, oi_symbol, strike, option_type, expiry_date}
    """
    strikes = get_strike_chain(atm_strike, half_width, interval)
    result = []
    for strike in strikes:
        for opt_type in ("CE", "PE"):
            raw = build_option_symbol(opt_type, strike, expiry_dt)
            result.append({
                "raw_symbol":   raw,
                "mark_symbol":  f"MARK:{raw}",
                "oi_symbol":    f"OI:{raw}",
                "strike":       strike,
                "option_type":  opt_type,
                "expiry_date":  expiry_dt.date().isoformat(),
            })
    return result


def compute_first_appearance_for_expiry(expiry_dt: datetime) -> datetime:
    """
    Returns the earliest datetime (00:00 IST) when expiry_dt
    first appears in any chain slot.
    Used to determine how far back to fetch candles for a given expiry.
    """
    return first_appearance(expiry_dt)
