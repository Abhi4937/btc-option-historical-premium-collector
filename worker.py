"""
AccountWorker — one worker per API account.

Flow per expiry month:
  1. Claim next pending month from manifest (work-stealing)
  2. Fetch spot data for date range (MARK:BTCUSD, BTCUSD, OI:BTCUSD)
  3. For each expiry date in that month:
       a. Compute first_appearance date
       b. Scan spot data to build ATM union → expiry_strikes table
       c. Register all (expiry, strike, CE/PE) symbols
       d. Fetch MARK + OI candles per symbol
       e. Merge + write parquet
  4. Mark month done → immediately claim next month

VERIFIED FINDINGS:
  Test 4: Pagination uses start=T+1s rule (handled by DeltaAPIClient.fetch_candles)
  Test 5: fetch to expiry_unix (last candle is 17:29, not 17:30)
  Test 6: Cannot distinguish empty vs not_listed — cross-check /v2/products
  Test 8: Data starts ~Jan 2024; start collection from COLLECTION_START_DATE
"""

import asyncio
import logging
import traceback
from datetime import timedelta


from api_client import DeltaAPIClient
from config import (
    DEFAULT_STRIKE_INTERVAL, CHAIN_HALF_WIDTH, SETTLEMENT_EXTRA_SECONDS,
    STATUS_DONE, STATUS_EMPTY, STATUS_NOT_LISTED, STATUS_FAILED,
    SPOT_PARQUET,
)
from ist_utils import (
    now_ist, ist_to_unix, unix_to_ist, format_ist, make_ist,
    get_expiry_dt, all_expiry_dates_in_month, first_appearance,
)
from manifest import claim_next_month, mark_month_done, mark_month_failed
from parquet_writer import (
    merge_spot_data, merge_option_data,
    append_or_create_spot, append_or_create_option,
)
from registry import (
    register_symbols_batch, mark_symbol_done, mark_symbol_empty,
    mark_symbol_not_listed, mark_symbol_failed,
    get_symbol_status, mark_spot_done, is_spot_done,
)
from strike_generator import (
    get_atm_strike, get_strike_chain,
    build_mark_symbol, build_oi_symbol, build_option_symbol,
)

log = logging.getLogger(__name__)


class AccountWorker:

    def __init__(self, account_name: str, api_key: str,
                 status_callback=None):
        """
        status_callback(account_name, state_dict) — called on progress updates
        for the Rich UI.
        """
        self.account_name      = account_name
        self.api_key           = api_key
        self.status_callback   = status_callback
        self._current_month        = None
        self._current_expiry       = None
        self._month_expiries_total = 0   # total expiries in current month
        self._month_expiries_done  = 0   # expiries fully completed
        self._month_symbols_total  = 0   # total CE+PE symbols in current month
        self._month_symbols_done   = 0   # symbols completed (done/empty/skipped)
        self._calls_this_month     = 0
        self._session_symbols_done = 0   # cumulative symbols done this session
        self._session_total_symbols= 0   # cumulative total symbols across all months this session
        self._strikes_fetched      = 0
        self._sem                  = asyncio.Semaphore(50)  # max concurrent HTTP calls per account

    def _update_status(self, **kwargs):
        if self.status_callback:
            self.status_callback(self.account_name, kwargs)

    async def run(self):
        """Main loop — claims and processes months until none remain."""
        log.info("[%s] Worker started", self.account_name)
        def _on_call():
            self._calls_this_month += 1
        async with DeltaAPIClient(self.account_name, self.api_key, on_call=_on_call) as client:
            while True:
                month_key = await claim_next_month(self.account_name)
                if month_key is None:
                    log.info("[%s] No more pending months — done", self.account_name)
                    self._update_status(state="idle", month=None)
                    break

                self._current_month    = month_key
                self._calls_this_month = 0
                self._strikes_fetched  = 0
                self._update_status(state="working", month=month_key, calls=0)

                try:
                    await self._process_month(client, month_key)
                    await mark_month_done(
                        month_key, self.account_name,
                        self._calls_this_month, self._strikes_fetched,
                    )
                    self._update_status(state="done", month=month_key,
                                        calls=self._calls_this_month)
                except Exception as e:
                    err = traceback.format_exc()
                    log.error("[%s] Month %s FAILED: %s\n%s", self.account_name, month_key, e, err)
                    await mark_month_failed(month_key, self.account_name, str(e))
                    self._update_status(state="failed", month=month_key, error=str(e))

    async def _process_month(self, client: DeltaAPIClient, month_key: str):
        """
        Process one expiry month:
          1. Ensure spot data exists for the period
          2. Process each expiry date in the month
        """
        year  = int(month_key[:4])
        month = int(month_key[5:7])

        now         = now_ist()
        month_start = make_ist(year, month, 1, 0, 0)

        # For future months (April, May…) we need spot data going back up to 70 days
        # so that strike unions can be built from the date each expiry first appeared
        # in the live ladder (which may be weeks before the calendar month starts).
        if month_start > now:
            range_start = now - timedelta(days=70)
        else:
            range_start = month_start - timedelta(days=10)

        # End of month (calendar boundary)
        if month == 12:
            range_end = make_ist(year + 1, 1, 1, 0, 0) - timedelta(seconds=1)
        else:
            range_end = make_ist(year, month + 1, 1, 0, 0) - timedelta(seconds=1)

        log.info("[%s] Processing month %s: %s → %s",
                 self.account_name, month_key,
                 format_ist(range_start), format_ist(range_end))

        # Step 1: fetch spot data
        spot_candles = await self._ensure_spot_data(
            client, month_key, range_start, range_end
        )
        if not spot_candles:
            log.warning("[%s] No spot data for %s", self.account_name, month_key)

        # Build mark price lookup: unix → mark_close
        mark_by_unix: dict[int, float] = {}
        for c in spot_candles:
            if c.get("mark_close") is not None:
                mark_by_unix[c["timestamp_unix"]] = c["mark_close"]

        # Collection rules:
        #   Expired (e <= now)        → every daily expiry, from first_appearance → settlement
        #   Near-term (today + 2d)    → today, tomorrow, day after (live daily ladder)
        #   Future Fridays            → weekly + monthly expiries within 3 months where
        #                               first_appearance(e) <= now
        three_months_ahead = (now + timedelta(days=92)).date()
        near_term_cutoff   = (now + timedelta(days=2)).date()

        expiry_dts = [
            e for e in all_expiry_dates_in_month(year, month)
            if e <= now                                          # all historical dailies
            or (e.date() <= near_term_cutoff                    # today + next 2 days (live dailies)
                and first_appearance(e) <= now)
            or (e.date() > near_term_cutoff                     # future: Fridays only
                and e.weekday() == 4
                and e.date() <= three_months_ahead
                and first_appearance(e) <= now)
        ]

        # Build all strike unions first (fast, in-memory) so we know totals upfront
        expiry_strikes: dict[str, dict[int, int]] = {}
        for expiry_dt in expiry_dts:
            strikes = await self._build_strike_union(expiry_dt, first_appearance(expiry_dt), mark_by_unix)
            expiry_strikes[expiry_dt.date().isoformat()] = strikes

        # Set month-level totals for progress display
        self._month_expiries_total  = len(expiry_dts)
        self._month_expiries_done   = 0
        self._month_symbols_total   = sum(len(s) * 2 for s in expiry_strikes.values())
        self._month_symbols_done    = 0
        self._session_total_symbols += self._month_symbols_total
        self._update_status(state="working", month=month_key,
                            expiry="—",
                            expiries=f"0/{self._month_expiries_total}",
                            strikes=f"0/{self._month_symbols_total}",
                            calls=self._calls_this_month,
                            session_symbols_done=self._session_symbols_done,
                            session_total_symbols=self._session_total_symbols)

        # Process one expiry at a time — cleaner resume, full 50 slots per expiry
        for expiry_dt in expiry_dts:
            await self._process_expiry(client, expiry_dt, mark_by_unix, range_start,
                                       expiry_strikes[expiry_dt.date().isoformat()])

    async def _ensure_spot_data(
        self,
        client: DeltaAPIClient,
        month_key: str,
        start_dt,
        end_dt,
    ) -> list[dict]:
        """
        Fetch spot (MARK:BTCUSD, BTCUSD, OI:BTCUSD) if not already done.
        Returns list of merged spot dicts for downstream ATM computation.
        """
        import pyarrow.parquet as pq, os as _os

        if await is_spot_done(month_key):
            log.debug("[%s] Spot already done for %s", self.account_name, month_key)
            # Read from parquet and return as dicts
            if _os.path.exists(SPOT_PARQUET):
                t = pq.read_table(SPOT_PARQUET,
                                  filters=[
                                      ("timestamp_unix", ">=", ist_to_unix(start_dt)),
                                      ("timestamp_unix", "<=", ist_to_unix(end_dt)),
                                  ])
                return t.to_pylist()
            return []

        now_spot = now_ist()

        # For months that start entirely in the future (April, May…) the API returns
        # nothing useful — read the existing spot parquet instead so we can build
        # strike unions for live contracts like April 24 or May 29.
        # Current/past months (e.g. March 2026) go through the normal API fetch path
        # so they pick up the latest candles up to today.
        year_k, month_k = int(month_key[:4]), int(month_key[5:7])
        month_start_dt  = make_ist(year_k, month_k, 1)

        if month_start_dt > now_spot and _os.path.exists(SPOT_PARQUET):
            t = pq.read_table(SPOT_PARQUET,
                               filters=[
                                   ("timestamp_unix", ">=", ist_to_unix(start_dt)),
                                   ("timestamp_unix", "<=", ist_to_unix(now_spot)),
                               ])
            if len(t) > 0:
                log.info("[%s] Month %s: using %d spot rows from existing parquet "
                         "(month extends to future — skipping API re-fetch)",
                         self.account_name, month_key, len(t))
                await mark_spot_done(month_key, len(t))
                return t.to_pylist()

        start_unix = ist_to_unix(start_dt)
        end_unix   = ist_to_unix(min(end_dt, now_spot))  # never request future timestamps

        log.info("[%s] Fetching spot data %s → %s",
                 self.account_name, format_ist(start_dt), format_ist(end_dt))

        self._update_status(state="fetching_spot", month=self._current_month)

        mark_c = await client.fetch_candles("MARK:BTCUSD", start_unix, end_unix)
        self._calls_this_month += 1

        ltp_c  = await client.fetch_candles("BTCUSD", start_unix, end_unix)
        self._calls_this_month += 1

        oi_c   = await client.fetch_candles("OI:BTCUSD", start_unix, end_unix)
        self._calls_this_month += 1

        log.info("[%s] Spot candles: mark=%d ltp=%d oi=%d",
                 self.account_name, len(mark_c), len(ltp_c), len(oi_c))

        if not mark_c and not ltp_c and not oi_c:
            log.warning("[%s] All spot candles empty for %s",
                        self.account_name, month_key)
            return []

        table = merge_spot_data(mark_c, ltp_c, oi_c)
        append_or_create_spot(table, SPOT_PARQUET)
        await mark_spot_done(month_key, len(table))

        return table.to_pylist()

    async def _process_expiry(
        self,
        client: DeltaAPIClient,
        expiry_dt,
        mark_by_unix: dict[int, float],
        range_start,
        strikes: dict[int, int],  # strike → first_seen_unix (passed from memory)
    ):
        """
        For a single expiry date:
          1. Compute first_appearance
          2. Fetch each (strike, CE/PE) symbol using in-memory strikes
        """
        expiry_date_str      = expiry_dt.date().isoformat()
        self._current_expiry = expiry_date_str
        appear_dt            = first_appearance(expiry_dt)
        expiry_unix          = ist_to_unix(expiry_dt) + SETTLEMENT_EXTRA_SECONDS

        log.info("[%s] Expiry %s: first_appearance=%s",
                 self.account_name, expiry_date_str, format_ist(appear_dt))

        if not strikes:
            log.warning("[%s] No strikes found for expiry %s",
                        self.account_name, expiry_date_str)
            self._month_expiries_done += 1
            return

        log.info("[%s] Expiry %s: %d strikes to fetch",
                 self.account_name, expiry_date_str, len(strikes))

        fetch_start_unix = ist_to_unix(appear_dt)
        from_ist = format_ist(unix_to_ist(fetch_start_unix))
        to_ist   = format_ist(unix_to_ist(expiry_unix))

        # Batch register all symbols in one DB transaction before fetching
        batch_rows = [
            (build_mark_symbol(opt_type, strike, expiry_dt),
             expiry_date_str, strike, opt_type,
             from_ist, to_ist, self.account_name)
            for strike in strikes
            for opt_type in ("CE", "PE")
        ]
        await register_symbols_batch(batch_rows)

        async def _fetch_with_sem(opt_type, strike):
            async with self._sem:
                await self._fetch_option(
                    client, opt_type, strike, expiry_dt,
                    fetch_start_unix, expiry_unix, expiry_date_str,
                )
                self._month_symbols_done   += 1
                self._session_symbols_done += 1
                self._update_status(
                    state="fetching", month=self._current_month,
                    expiry=self._current_expiry,
                    expiries=f"{self._month_expiries_done}/{self._month_expiries_total}",
                    strikes=f"{self._month_symbols_done}/{self._month_symbols_total}",
                    calls=self._calls_this_month,
                    session_symbols_done=self._session_symbols_done,
                    session_total_symbols=self._session_total_symbols,
                )

        tasks = [
            _fetch_with_sem(opt_type, strike)
            for strike in strikes
            for opt_type in ("CE", "PE")
        ]
        await asyncio.gather(*tasks)
        self._month_expiries_done += 1

    async def _build_strike_union(
        self,
        expiry_dt,
        appear_dt,
        mark_by_unix: dict[int, float],
    ) -> dict[int, int]:
        """
        Scan mark prices from appear_dt to expiry_dt.
        For each minute, compute ATM ± chain.
        Returns dict of strike → first_seen_unix (pure in-memory, no DB writes).
        """
        appear_unix = ist_to_unix(appear_dt)
        expiry_unix = ist_to_unix(expiry_dt)

        first_seen_by_strike: dict[int, int] = {}

        for ts, mark_close in sorted(mark_by_unix.items()):
            if ts < appear_unix or ts > expiry_unix:
                continue

            atm = get_atm_strike(mark_close)
            chain = get_strike_chain(atm, CHAIN_HALF_WIDTH, DEFAULT_STRIKE_INTERVAL)
            for s in chain:
                if s not in first_seen_by_strike:
                    first_seen_by_strike[s] = ts

        log.debug("[%s] Expiry %s: %d unique strikes in union",
                  self.account_name, expiry_dt.date().isoformat(), len(first_seen_by_strike))

        return first_seen_by_strike

    async def _fetch_option(
        self,
        client: DeltaAPIClient,
        opt_type: str,
        strike: int,
        expiry_dt,
        fetch_start_unix: int,
        expiry_unix: int,
        expiry_date_str: str,
    ):
        """Fetch MARK + OI candles for one option, merge, write parquet."""
        mark_sym = build_mark_symbol(opt_type, strike, expiry_dt)
        oi_sym   = build_oi_symbol(opt_type, strike, expiry_dt)
        raw_sym  = build_option_symbol(opt_type, strike, expiry_dt)

        # Skip if already done/empty/not_listed
        status = await get_symbol_status(mark_sym)
        if status in (STATUS_DONE, STATUS_EMPTY, STATUS_NOT_LISTED):
            return

        try:
            mark_c, oi_c = await asyncio.gather(
                client.fetch_candles(mark_sym, fetch_start_unix, expiry_unix),
                client.fetch_candles(oi_sym, fetch_start_unix, expiry_unix),
            )
            if not mark_c and not oi_c:
                # All 40K+ resolved symbols show not_listed=0: Delta lists every
                # $100-interval strike we generate. Skip products check, mark empty.
                await mark_symbol_empty(mark_sym)
                return

            # Merge and write
            table            = merge_option_data(mark_c, oi_c)
            path             = append_or_create_option(table, expiry_date_str, strike, opt_type)
            earliest_unix    = table.column("timestamp_unix")[0].as_py() if len(table) > 0 else None
            await mark_symbol_done(mark_sym, len(table), path, earliest_unix)
            self._strikes_fetched += 1

        except Exception as e:
            err = traceback.format_exc()
            log.error("[%s] Failed %s: %s", self.account_name, mark_sym, e)
            await mark_symbol_failed(mark_sym, str(e))
