"""
Delta Exchange API client with proactive count-based rate limiting.

VERIFIED FINDINGS incorporated:
  Test 4: Boundary is inclusive on both ends → use next_start = prev_end + 1s
           Deduplicate by timestamp_unix after every paginated fetch
  Test 6: Both empty and fake symbols → HTTP 200 + {"result": [], "success": true}
           Cannot distinguish from response; use products API for not_listed check
  Test 7: CRITICAL — NO rate-limit headers in responses.
           Must use count-based proactive rate limiting per account.
  Test 10: Rate limits are per-account; 5 independent accounts safe.
"""

import asyncio
import time
import logging
from collections import deque
from datetime import timedelta

import httpx

from config import (
    BASE_URL, MAX_CANDLES_PER_CALL,
    RATE_LIMIT_CALLS_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS,
    RATE_LIMIT_SAFE_CALLS, RATE_LIMIT_429_BUFFER_MS,
    REQUEST_TIMEOUT_SECONDS, MAX_RETRIES, RETRY_BACKOFF_SECONDS,
)
from ist_utils import unix_to_ist, format_ist

log = logging.getLogger(__name__)


class RateLimiter:
    """
    Proactive count-based rate limiter (no header reliance — Test 7).

    Tracks call timestamps in a sliding window.
    Sleeps proactively before a call would exceed RATE_LIMIT_SAFE_CALLS.
    """

    def __init__(self, account_name: str,
                 max_calls: int = RATE_LIMIT_SAFE_CALLS,
                 window_seconds: int = RATE_LIMIT_WINDOW_SECONDS,
                 on_call=None):
        self.account_name    = account_name
        self.max_calls       = max_calls
        self.window_seconds  = window_seconds
        self._timestamps: deque[float] = deque()
        self._total_calls    = 0
        self._lock           = asyncio.Lock()
        self._on_call        = on_call  # callback fired on every acquired call

    async def acquire(self):
        """Wait if necessary, then record this call."""
        async with self._lock:
            now = time.monotonic()
            # Evict calls outside the window
            cutoff = now - self.window_seconds
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_calls:
                # Must wait until oldest call falls out of window
                oldest = self._timestamps[0]
                sleep_for = (oldest + self.window_seconds) - now + 0.05
                if sleep_for > 0:
                    log.debug(
                        "[%s] Rate limit: sleeping %.1fs (%d calls in window)",
                        self.account_name, sleep_for, len(self._timestamps)
                    )
                    await asyncio.sleep(sleep_for)
                # Re-evict after sleep
                now = time.monotonic()
                cutoff = now - self.window_seconds
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()

            self._timestamps.append(time.monotonic())
            self._total_calls += 1
            if self._on_call:
                self._on_call()

    @property
    def calls_in_window(self) -> int:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        return sum(1 for t in self._timestamps if t >= cutoff)

    @property
    def total_calls(self) -> int:
        return self._total_calls


class DeltaAPIClient:
    """
    Async HTTP client for Delta Exchange India.
    One instance per worker account.
    """

    def __init__(self, account_name: str, api_key: str, on_call=None):
        self.account_name = account_name
        self.api_key      = api_key
        self.rate_limiter = RateLimiter(account_name, on_call=on_call)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"api-key": self.api_key},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def _get(self, path: str, params: dict) -> dict | None:
        """Single GET with retry logic. Returns parsed JSON or None on error."""
        for attempt in range(MAX_RETRIES):
            await self.rate_limiter.acquire()
            try:
                resp = await self._client.get(path, params=params)
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                wait = RETRY_BACKOFF_SECONDS[min(attempt, len(RETRY_BACKOFF_SECONDS) - 1)]
                log.warning("[%s] Network error (attempt %d): %s — sleeping %ds",
                            self.account_name, attempt + 1, e, wait)
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                # Test 7: no rate-limit headers confirmed, but try anyway
                reset_ms = resp.headers.get("x-rate-limit-reset")
                if reset_ms:
                    sleep_ms = int(reset_ms) + RATE_LIMIT_429_BUFFER_MS
                    sleep_s  = sleep_ms / 1000.0
                else:
                    sleep_s = RATE_LIMIT_WINDOW_SECONDS + 1.0
                log.warning("[%s] 429 received — sleeping %.1fs",
                            self.account_name, sleep_s)
                await asyncio.sleep(sleep_s)
                continue

            if resp.status_code in (400, 404):
                log.debug("[%s] HTTP %d for %s %s",
                          self.account_name, resp.status_code, path, params)
                return None

            # 5xx or other — backoff and retry
            wait = RETRY_BACKOFF_SECONDS[min(attempt, len(RETRY_BACKOFF_SECONDS) - 1)]
            log.warning("[%s] HTTP %d (attempt %d) — sleeping %ds",
                        self.account_name, resp.status_code, attempt + 1, wait)
            await asyncio.sleep(wait)

        log.error("[%s] All %d attempts failed for %s %s",
                  self.account_name, MAX_RETRIES, path, params)
        return None

    async def fetch_candles(
        self,
        symbol: str,
        start_unix: int,
        end_unix: int,
        resolution: str = "1m",
    ) -> list[dict]:
        """
        Fetch candles with time-based chunked pagination.

        ROOT CAUSE discovered in test-run: for large time ranges the API returns
        only the 2000 MOST RECENT candles, not the 2000 oldest. The original
        `chunk_start = last_ts + 1` strategy advanced near the end of the
        range and terminated early, leaving most of the history unfetched.

        FIX: divide the total range into fixed 2000-minute time windows from
        the start. Each window is ≤ 2000 minutes, so the API returns ALL
        candles in that window in one call (verified by Test 4).

        Test 4 rule preserved: next window start = prev window end + 1s to
        avoid duplicate boundary candle. Deduplicate by 'time' as safety net.
        """
        # 2000-minute window = 120,000 seconds (matches API's 2000-candle limit)
        CHUNK_SECONDS = MAX_CANDLES_PER_CALL * 60  # 120,000 s

        all_candles: list[dict] = []
        chunk_start = start_unix

        while chunk_start <= end_unix:
            chunk_end = min(chunk_start + CHUNK_SECONDS, end_unix)
            params = {
                "symbol":     symbol,
                "resolution": resolution,
                "start":      chunk_start,
                "end":        chunk_end,
            }
            data = await self._get("/v2/history/candles", params)
            if data is None:
                break

            candles = data.get("result") or []
            if not candles:
                # Empty window — still advance (gap in data)
                if chunk_end >= end_unix:
                    break
                chunk_start = chunk_end + 1
                continue

            all_candles.extend(candles)

            if chunk_end >= end_unix:
                break

            # Test 4: +1s avoids duplicate of the boundary candle
            chunk_start = chunk_end + 1

        # Deduplicate by timestamp (Test 4: safety net)
        seen: set[int] = set()
        unique: list[dict] = []
        for c in all_candles:
            t = c["time"]
            if t not in seen:
                seen.add(t)
                unique.append(c)

        unique.sort(key=lambda c: c["time"])
        return unique

    async def get_products(
        self,
        contract_type: str = "call_options",
        state: str = "live",
        underlying: str = "BTC",
    ) -> list[dict]:
        """Fetch products list from /v2/products."""
        params: dict = {
            "contract_types":          contract_type,
            "states":                  state,
            "underlying_asset_symbol": underlying,
        }
        data = await self._get("/v2/products", params)
        if data is None:
            return []
        return data.get("result") or []

    async def get_mark_price_now(self) -> float | None:
        """Return latest MARK:BTCUSD close price for ATM calculation."""
        from ist_utils import ist_to_unix, now_ist
        now = ist_to_unix(now_ist())
        candles = await self.fetch_candles("MARK:BTCUSD", now - 120, now)
        if candles:
            return candles[-1].get("close")
        return None

    async def symbol_exists_in_products(self, symbol: str) -> bool:
        """
        Test 6: Cannot distinguish empty vs not_listed from candle API alone.
        This helper checks /v2/products to confirm a symbol is listed.
        symbol should be without MARK: or OI: prefix.
        """
        data = await self._get(
            "/v2/products",
            {"symbol": symbol},
        )
        if data is None:
            return False
        result = data.get("result")
        if isinstance(result, list):
            return len(result) > 0
        if isinstance(result, dict):
            return bool(result.get("symbol"))
        return False
