"""
Configuration constants for BTC options historical data collector.

VERIFIED FINDINGS incorporated:
  Test 1:  24/7 continuous, 1440 candles/day, zero gaps
  Test 2:  Symbol format C-BTC-{strike}-{DDMMYY}, settlement always 17:30 IST
  Test 3:  Strike interval $200 (live data); historical unknown (only 2026 in API)
  Test 4:  Boundary candle appears in BOTH chunks — use start=T+1s for next chunk
  Test 5:  Settlement candle (17:30) NOT included — last candle is 17:29 IST
  Test 6:  CRITICAL — both empty AND fake symbols return HTTP 200 + []
           Cannot distinguish 'empty' from 'not_listed' via API alone
  Test 7:  CRITICAL — NO rate limit headers in responses
           Must use count-based proactive rate limiting
  Test 8:  Data older than Jan 2024 returned empty — run test-depth for exact date
  Test 9:  Expiry ladder verified; 4 unique expiries always correct
  Test 10: Rate limits are PER-ACCOUNT independent
"""

from datetime import date
import os

# ── Exchange ──────────────────────────────────────────────────────────────────
BASE_URL = "https://api.india.delta.exchange"

# ── Collection date range ─────────────────────────────────────────────────────
# Test 8 + test-depth binary search results (2026-03-09):
#   MARK:BTCUSD oldest candle : 2023-12-18
#   OI:BTCUSD oldest candle   : 2024-02-01
#   BTC options oldest probe  : 2024-01-25 (C-BTC-45000-260124)
#   Pre-2024 options (2023, 2022, 2021, 2020) : all returned empty
COLLECTION_START_DATE = date(2023, 12, 18)
COLLECTION_END_DATE   = None    # None = today at runtime

# ── Strike chain ──────────────────────────────────────────────────────────────
# Test 3: live chain is non-uniform but denser near ATM at $100 intervals.
# $100 rounding is more accurate than $200 (e.g. BTC=80050 → ATM=80100 not 80000).
# ±40 strikes × $100 = same ±$4,000 range as before, but 2× density (81 strikes).
DEFAULT_STRIKE_INTERVAL = 100
CHAIN_HALF_WIDTH        = 40    # ATM ± 40 strikes = 81 unique strikes per snapshot

# ── API limits ────────────────────────────────────────────────────────────────
# Test 4: boundary is inclusive on both start and end.
# Use 2000 candles per chunk; deduplicate on merge.
MAX_CANDLES_PER_CALL = 2000

# Test 7: NO rate-limit headers present — must count-limit proactively.
# Official docs: 10,000 units per 5-min window, OHLC = 3 units per call.
# => 10,000 / 3 = 3,333 real calls per 5-min window per account.
RATE_LIMIT_CALLS_PER_WINDOW = 3_333
RATE_LIMIT_WINDOW_SECONDS   = 300       # 5-minute fixed window

# Buffer before hitting limit (stop at 90% to be safe)
RATE_LIMIT_SAFE_CALLS = int(RATE_LIMIT_CALLS_PER_WINDOW * 0.90)   # ~5400

# Sleep buffer on 429 (if we do hit it despite proactive limiting)
RATE_LIMIT_429_BUFFER_MS = 500

# HTTP timeouts
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES             = 3
RETRY_BACKOFF_SECONDS   = [2, 5, 15]

# ── Test 5: Settlement candle ─────────────────────────────────────────────────
# Last candle before expiry is 17:29 IST — NOT 17:30.
# Using end=expiry_unix captures up to 17:29 correctly.
# SETTLEMENT_EXTRA_SECONDS = 0 (no extra needed)
SETTLEMENT_EXTRA_SECONDS = 0

# ── Storage ───────────────────────────────────────────────────────────────────
PARQUET_COMPRESSION       = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
PARQUET_ROW_GROUP_SIZE    = 10_000

# ── Parallelism ───────────────────────────────────────────────────────────────
# Test 10: per-account independent rate limits confirmed.
# 5 accounts × 3,333 = ~16,665 calls/5-min window combined.
ACTIVE_ACCOUNTS = 5

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE_DIR, "data")
SPOT_DIR     = os.path.join(BASE_DIR, "data", "spot")
OPTIONS_DIR  = os.path.join(BASE_DIR, "data", "options")
DB_DIR       = os.path.join(BASE_DIR, "db")
LOGS_DIR     = os.path.join(BASE_DIR, "logs")

MANIFEST_DB  = os.path.join(DB_DIR, "manifest.db")
REGISTRY_DB  = os.path.join(DB_DIR, "registry.db")
SPOT_PARQUET = os.path.join(SPOT_DIR, "BTCUSD_1min.parquet")
LOG_FILE     = os.path.join(LOGS_DIR, "collector.log")
ERROR_LOG    = os.path.join(LOGS_DIR, "errors.log")

# ── Account identifiers (matched to .env keys) ────────────────────────────────
ACCOUNT_NAMES = ["lava", "papa", "tejas", "tuition_miss", "abhishek"]

# ── Test 6: Registry status values ───────────────────────────────────────────
# CRITICAL: API returns HTTP 200 + [] for BOTH no-trade AND non-existent symbols.
# We CANNOT distinguish them from API response alone.
# Strategy:
#   - Cross-reference /v2/products before fetching: if not in products → not_listed
#   - If in products but API returns [] → empty
#   - Both 'empty' and 'not_listed' are never retried
STATUS_PENDING     = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_DONE        = "done"
STATUS_EMPTY       = "empty"        # listed but no candles returned
STATUS_NOT_LISTED  = "not_listed"   # not found in /v2/products
STATUS_FAILED      = "failed"
