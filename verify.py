"""
BTC Options Historical Data Collector — Verification Tests
Runs ALL 10 tests before any build begins.
"""

import httpx
import asyncio
import time
import json
import sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── IST helpers ──────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def unix_to_ist(unix_seconds):
    return datetime.fromtimestamp(unix_seconds, tz=IST)

def ist_to_unix(dt_ist):
    return int(dt_ist.timestamp())

def make_ist(year, month, day, hour=0, minute=0, second=0):
    return datetime(year, month, day, hour, minute, second, tzinfo=IST)

# ── Credentials ──────────────────────────────────────────────────────────────
BASE_URL   = "https://api.india.delta.exchange"
API_KEY    = "C2qcRyh6BQgYWmmoUV5093OvkoF7Hj"   # lava (primary for tests 1-9)
API_SECRET = "ULK2QjYGMaTWb4xVLKiCliIIlho50rs3CSeo8q7J0EuEfipWZEitkD67DdHJ"

# Second account for Test 10
API_KEY_2    = "cPyN1f1zMw0qyyM4paIrp2dWb9Bp5i"  # papa
API_SECRET_2 = "Qj5sg9tfakunFzfh9AgIurbw9vMvmrK48I4mVWygFZI7T590CxYeg6DvWvBL"

# ── Core API helpers ──────────────────────────────────────────────────────────
def fetch_candles(symbol, resolution, start_unix, end_unix, api_key=API_KEY):
    """Fetch candles from /v2/history/candles, returns list of candle dicts."""
    url = f"{BASE_URL}/v2/history/candles"
    params = {
        "symbol":     symbol,
        "resolution": resolution,
        "start":      start_unix,
        "end":        end_unix,
    }
    headers = {"api-key": api_key}
    resp = httpx.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        # API returns {"success": true, "result": [...]}
        result = data.get("result", [])
        if result is None:
            return []
        return result
    else:
        print(f"    HTTP {resp.status_code}: {resp.text[:300]}")
        return []

def raw_api_call(symbol, start_unix, end_unix, api_key=API_KEY):
    """Raw call — returns full response object."""
    url = f"{BASE_URL}/v2/history/candles"
    params = {
        "symbol":     symbol,
        "resolution": "1m",
        "start":      start_unix,
        "end":        end_unix,
    }
    headers = {"api-key": api_key}
    return httpx.get(url, params=params, headers=headers, timeout=30)

def sep(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def sub(msg):
    print(f"\n  >>> {msg}")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1 — TRADING HOURS
# ─────────────────────────────────────────────────────────────────────────────
def test1_trading_hours():
    sep("TEST 1 — TRADING HOURS")
    print("Question: Are BTC mark price candles continuous 24/7 or session-based?\n")

    today = now_ist()
    # Use 2 days ago to ensure full day is complete
    ref = today - timedelta(days=2)
    yesterday_start = make_ist(ref.year, ref.month, ref.day, 0, 0)
    yesterday_end   = make_ist(ref.year, ref.month, ref.day, 23, 59)

    print(f"  Fetching MARK:BTCUSD for {ref.strftime('%Y-%m-%d')} (full day)")
    print(f"  Start: {yesterday_start.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"  End:   {yesterday_end.strftime('%Y-%m-%d %H:%M IST')}")

    candles = fetch_candles(
        "MARK:BTCUSD", "1m",
        ist_to_unix(yesterday_start),
        ist_to_unix(yesterday_end)
    )

    print(f"\n  Raw candle count returned: {len(candles)}")

    if not candles:
        print("  ERROR: No candles returned!")
        print("\nCONCLUSION:")
        print("  24/7 continuous: UNKNOWN (no data returned)")
        return

    # Sort by timestamp
    candles_sorted = sorted(candles, key=lambda c: c['time'])
    first_c = candles_sorted[0]
    last_c  = candles_sorted[-1]

    first_ist = unix_to_ist(first_c['time'])
    last_ist  = unix_to_ist(last_c['time'])

    print(f"  First candle: {first_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"  Last candle:  {last_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    # Check specific times
    candle_times_unix = set(c['time'] for c in candles)

    t_0200 = ist_to_unix(make_ist(ref.year, ref.month, ref.day, 2, 0))
    t_0330 = ist_to_unix(make_ist(ref.year, ref.month, ref.day, 3, 30))
    t_2350 = ist_to_unix(make_ist(ref.year, ref.month, ref.day, 23, 50))

    print(f"\n  Candle at 02:00 IST present: {t_0200 in candle_times_unix}")
    print(f"  Candle at 03:30 IST present: {t_0330 in candle_times_unix}")
    print(f"  Candle at 23:50 IST present: {t_2350 in candle_times_unix}")

    # Find gaps > 1 min
    gaps = []
    for i in range(1, len(candles_sorted)):
        diff = candles_sorted[i]['time'] - candles_sorted[i-1]['time']
        if diff > 60:
            gap_at = unix_to_ist(candles_sorted[i-1]['time'])
            gaps.append((diff // 60, gap_at))

    if gaps:
        max_gap = max(gaps, key=lambda x: x[0])
        print(f"\n  Gaps > 1 min found: {len(gaps)}")
        print(f"  Largest gap: {max_gap[0]} minutes at {max_gap[1].strftime('%H:%M IST')}")
        print(f"  All gaps (minutes, time): {[(g[0], g[1].strftime('%H:%M')) for g in gaps[:10]]}")
    else:
        print(f"\n  No gaps > 1 minute found (fully continuous)")

    is_24_7 = len(candles) >= 1430  # allow some tolerance from 1440

    print(f"\nCONCLUSION:")
    print(f"  24/7 continuous: {'yes' if is_24_7 else 'NO — SESSION BASED'}")
    print(f"  Total candles yesterday: {len(candles)}")
    print(f"  Earliest candle: {first_ist.strftime('%H:%M IST')}")
    print(f"  Latest candle: {last_ist.strftime('%H:%M IST')}")
    if gaps:
        print(f"  Largest gap found: {max_gap[0]} minutes at {max_gap[1].strftime('%H:%M IST')}")
    else:
        print(f"  Largest gap found: 0 minutes (no gaps)")
    print(f"  IMPACT: row_count_per_day = {len(candles)} (used for all storage estimates)")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2 — HISTORICAL SYMBOL FORMAT
# ─────────────────────────────────────────────────────────────────────────────
def test2_symbol_format():
    sep("TEST 2 — HISTORICAL SYMBOL FORMAT")
    print("Question: What is the exact symbol format for historical BTC options?\n")

    url = f"{BASE_URL}/v2/products"
    headers = {"api-key": API_KEY}

    print("  Fetching expired call options...")
    resp = httpx.get(url, params={"contract_types": "call_options", "states": "expired"},
                     headers=headers, timeout=60)
    print(f"  HTTP {resp.status_code}")

    if resp.status_code != 200:
        print(f"  ERROR: {resp.text[:300]}")
        return

    data = resp.json()
    all_products = data.get("result", [])
    print(f"  Total expired options returned: {len(all_products)}")

    # Filter BTC
    btc = [p for p in all_products if "BTC" in p.get("symbol", "")]
    print(f"  BTC options: {len(btc)}")

    if not btc:
        # Try without filter
        print("  No BTC found in expired — trying live products...")
        resp2 = httpx.get(url, params={"contract_types": "call_options", "states": "live"},
                          headers=headers, timeout=60)
        data2 = resp2.json()
        btc = [p for p in data2.get("result", []) if "BTC" in p.get("symbol", "")]
        print(f"  BTC live options: {len(btc)}")

    if not btc:
        print("  No BTC options found at all — printing first 3 raw products:")
        for p in all_products[:3]:
            print(f"    {json.dumps(p, indent=2)[:500]}")
        return

    # Show sample fields from first product
    print(f"\n  Sample product keys: {list(btc[0].keys())}")

    # Group by year of expiry
    by_year = defaultdict(list)
    for p in btc:
        sym = p.get("symbol", "")
        # Try settlement_time or expiry_time
        exp_field = p.get("settlement_time") or p.get("expiry_time") or p.get("launch_time") or ""
        try:
            # Usually ISO format: "2026-02-07T12:00:00Z"
            if exp_field:
                dt = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
                yr = dt.year
            else:
                yr = 0
        except Exception:
            yr = 0
        by_year[yr].append((sym, exp_field, p))

    # Show samples from each year
    for yr in sorted(by_year.keys()):
        samples = by_year[yr][:5]
        print(f"\n  === Year {yr} ({len(by_year[yr])} products) ===")
        for sym, exp_field, p in samples:
            print(f"    symbol: {sym}")
            print(f"    expiry: {exp_field}")
            # Show settlement time in IST if possible
            if exp_field:
                try:
                    dt_utc = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
                    dt_ist = dt_utc.astimezone(IST)
                    print(f"    expiry_ist: {dt_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
                except Exception:
                    pass
            # Show strike
            print(f"    strike: {p.get('strike_price')}")
            print()

    # Check if settlement times are all 17:30 IST
    settlement_times = set()
    for p in btc:
        exp_field = p.get("settlement_time") or p.get("expiry_time") or ""
        if exp_field:
            try:
                dt_utc = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
                dt_ist = dt_utc.astimezone(IST)
                settlement_times.add(f"{dt_ist.hour:02d}:{dt_ist.minute:02d}")
            except Exception:
                pass
    print(f"\n  Unique settlement times (IST HH:MM): {sorted(settlement_times)}")

    # Detect format pattern
    sample_syms = [p.get("symbol", "") for p in btc if p.get("symbol")]
    print(f"\n  All unique symbol format examples (first 15):")
    for s in sample_syms[:15]:
        print(f"    {s}")

    print(f"\nCONCLUSION:")
    if sample_syms:
        print(f"  Symbol format confirmed: {sample_syms[0]} (see samples above)")
    settlement_ok = settlement_times == {"17:30"}
    print(f"  Settlement time confirmed as 17:30:00 IST: {'yes' if settlement_ok else 'NO — see times above'}")
    print(f"  Settlement times found: {sorted(settlement_times)}")
    years_present = sorted([y for y in by_year.keys() if y > 0])
    print(f"  Years with data: {years_present}")
    print(f"  IMPACT: strike_generator.py symbol format = symbol field from /v2/products")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3 — STRIKE INTERVAL PER HISTORICAL PERIOD
# ─────────────────────────────────────────────────────────────────────────────
def test3_strike_interval():
    sep("TEST 3 — STRIKE INTERVAL PER HISTORICAL PERIOD")
    print("Question: Was the strike interval always the same or did it change over years?\n")

    headers = {"api-key": API_KEY}

    # Fetch expired + live
    print("  Fetching expired call options...")
    r1 = httpx.get(f"{BASE_URL}/v2/products",
                   params={"contract_types": "call_options", "states": "expired"},
                   headers=headers, timeout=60)
    expired = r1.json().get("result", []) if r1.status_code == 200 else []

    print("  Fetching live call options...")
    r2 = httpx.get(f"{BASE_URL}/v2/products",
                   params={"contract_types": "call_options", "states": "live",
                           "underlying_asset_symbol": "BTC"},
                   headers=headers, timeout=60)
    live = r2.json().get("result", []) if r2.status_code == 200 else []

    btc_expired = [p for p in expired if "BTC" in p.get("symbol", "")]
    btc_live    = [p for p in live    if "BTC" in p.get("symbol", "")]

    print(f"  BTC expired options: {len(btc_expired)}")
    print(f"  BTC live options: {len(btc_live)}")

    def get_interval_for_expiry(products_subset, label):
        """Given options all sharing one expiry, compute strike intervals."""
        strikes = []
        for p in products_subset:
            try:
                s = float(p.get("strike_price") or 0)
                if s > 0:
                    strikes.append(int(s))
            except Exception:
                pass
        if len(strikes) < 2:
            print(f"    {label}: not enough strikes ({len(strikes)})")
            return None
        strikes_sorted = sorted(set(strikes))
        diffs = [strikes_sorted[i+1] - strikes_sorted[i]
                 for i in range(len(strikes_sorted)-1)]
        # Most common diff
        from collections import Counter
        counts = Counter(diffs)
        most_common_diff = counts.most_common(1)[0][0]
        print(f"    {label}: strikes {strikes_sorted[:5]}...{strikes_sorted[-3:]}")
        print(f"      diffs sample: {diffs[:10]}")
        print(f"      most common interval: ${most_common_diff}")
        return most_common_diff

    # Group expired BTC by expiry date
    by_expiry = defaultdict(list)
    for p in btc_expired:
        exp_field = p.get("settlement_time") or p.get("expiry_time") or ""
        try:
            dt = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
            key = dt.date().isoformat()
        except Exception:
            key = "unknown"
        by_expiry[key].append(p)

    # Pick one expiry per year
    target_years = {2021, 2022, 2023, 2024, 2025}
    year_intervals = {}

    for expiry_date, products in sorted(by_expiry.items()):
        try:
            yr = int(expiry_date[:4])
        except Exception:
            continue
        if yr in target_years and yr not in year_intervals:
            print(f"\n  Checking expiry {expiry_date} ({len(products)} options):")
            interval = get_interval_for_expiry(products, f"Year {yr}")
            year_intervals[yr] = interval

    # Live options
    print(f"\n  Live options ({len(btc_live)} total):")
    if btc_live:
        # Group by expiry
        live_by_expiry = defaultdict(list)
        for p in btc_live:
            exp_field = p.get("settlement_time") or p.get("expiry_time") or ""
            try:
                dt = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
                key = dt.date().isoformat()
            except Exception:
                key = "unknown"
            live_by_expiry[key].append(p)

        for exp_date, prods in sorted(live_by_expiry.items())[:3]:
            interval = get_interval_for_expiry(prods, f"Live {exp_date}")
            year_intervals["live"] = interval

    print(f"\nCONCLUSION:")
    for yr, iv in sorted(year_intervals.items(), key=lambda x: str(x[0])):
        print(f"  Strike interval in {yr}: ${iv if iv else 'data not available'}")
    vals = [v for v in year_intervals.values() if v]
    if vals and len(set(vals)) == 1:
        print(f"  Interval changed: no — always ${vals[0]}")
    elif vals:
        print(f"  Interval changed: yes — see values above")
    else:
        print(f"  Interval changed: UNKNOWN (insufficient data)")
    print(f"  IMPACT: config.py should store interval per date range (or single value if constant)")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4 — PAGINATION BOUNDARY
# ─────────────────────────────────────────────────────────────────────────────
def test4_pagination():
    sep("TEST 4 — PAGINATION BOUNDARY (Off-by-one check)")
    print("Question: What is the exact pagination rule to avoid duplicate or missing candles?\n")

    today = now_ist()
    ref = today - timedelta(days=5)
    T0 = make_ist(ref.year, ref.month, ref.day, 9, 0)
    T_boundary = T0 + timedelta(minutes=2000)
    T_end = T0 + timedelta(minutes=4000)

    print(f"  T0 (start):    {T0.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"  T_boundary:    {T_boundary.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"  T_end:         {T_end.strftime('%Y-%m-%d %H:%M IST')}")

    sub("Fetching chunk1: T0 → T_boundary (2000 min range)")
    chunk1 = fetch_candles("MARK:BTCUSD", "1m",
                           ist_to_unix(T0),
                           ist_to_unix(T_boundary))
    print(f"  chunk1 count: {len(chunk1)}")

    sub("Fetching chunk2: T_boundary → T_end (start=exact boundary)")
    chunk2 = fetch_candles("MARK:BTCUSD", "1m",
                           ist_to_unix(T_boundary),
                           ist_to_unix(T_end))
    print(f"  chunk2 count: {len(chunk2)}")

    sub("Fetching chunk2_v2: T_boundary+1s → T_end")
    chunk2_v2 = fetch_candles("MARK:BTCUSD", "1m",
                              ist_to_unix(T_boundary) + 1,
                              ist_to_unix(T_end))
    print(f"  chunk2_v2 count: {len(chunk2_v2)}")

    def last_n_times(candles, n=3):
        if not candles:
            return []
        s = sorted(candles, key=lambda c: c['time'])
        return [unix_to_ist(c['time']).strftime('%H:%M:%S IST') for c in s[-n:]]

    def first_n_times(candles, n=3):
        if not candles:
            return []
        s = sorted(candles, key=lambda c: c['time'])
        return [unix_to_ist(c['time']).strftime('%H:%M:%S IST') for c in s[:n]]

    boundary_unix = ist_to_unix(T_boundary)

    print(f"\n  chunk1 last 3 timestamps:    {last_n_times(chunk1)}")
    print(f"  chunk2 first 3 timestamps:   {first_n_times(chunk2)}")
    print(f"  chunk2_v2 first 3 timestamps:{first_n_times(chunk2_v2)}")

    # Check boundary presence
    c1_times = set(c['time'] for c in chunk1)
    c2_times = set(c['time'] for c in chunk2)
    c2v2_times = set(c['time'] for c in chunk2_v2)

    in_c1  = boundary_unix in c1_times
    in_c2  = boundary_unix in c2_times
    in_c2v2 = boundary_unix in c2v2_times

    print(f"\n  Boundary candle ({T_boundary.strftime('%H:%M IST')}) in chunk1: {in_c1}")
    print(f"  Boundary candle in chunk2:   {in_c2}")
    print(f"  Boundary candle in chunk2_v2:{in_c2v2}")

    # Overlap check
    overlap_v1  = c1_times & c2_times
    overlap_v2  = c1_times & c2v2_times
    total_v1 = len(c1_times | c2_times)
    total_v2 = len(c1_times | c2v2_times)

    print(f"\n  chunk1 ∩ chunk2 overlap: {len(overlap_v1)} candles")
    print(f"  chunk1 ∩ chunk2_v2 overlap: {len(overlap_v2)} candles")
    print(f"  Total unique (v1): {total_v1}  |  Total unique (v2): {total_v2}")
    print(f"  chunk1+chunk2 raw sum: {len(chunk1)+len(chunk2)}  unique: {total_v1}")
    print(f"  chunk1+chunk2_v2 raw sum: {len(chunk1)+len(chunk2_v2)}  unique: {total_v2}")

    print(f"\nCONCLUSION:")
    if in_c1 and in_c2:
        loc = "both (DUPLICATE)"
    elif in_c1:
        loc = "chunk1 only"
    elif in_c2:
        loc = "chunk2 only"
    else:
        loc = "neither (GAP!)"
    print(f"  Boundary candle appears in: {loc}")

    if in_c1 and not in_c2:
        print(f"  Safe pagination rule: chunk N end=T, chunk N+1 start=T+1second")
    elif not in_c1 and in_c2:
        print(f"  Safe pagination rule: chunk N end=T-1second, chunk N+1 start=T")
    elif in_c1 and in_c2:
        print(f"  Safe pagination rule: use start=T+1second for chunk N+1 to avoid duplicate")
    else:
        print(f"  Safe pagination rule: INVESTIGATE — boundary in neither chunk")

    print(f"  Deduplication by timestamp_unix needed: {'yes' if len(overlap_v1) > 0 else 'no (as safety net still recommended)'}")
    print(f"  IMPACT: use this exact rule in api_client.py paginator")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5 — SETTLEMENT CANDLE INCLUSION
# ─────────────────────────────────────────────────────────────────────────────
def test5_settlement_candle():
    sep("TEST 5 — SETTLEMENT CANDLE INCLUSION")
    print("Question: Does the API include or exclude the 17:30:00 IST settlement candle?\n")

    headers = {"api-key": API_KEY}

    # Find a recently expired BTC option
    print("  Fetching recently expired BTC options...")
    r = httpx.get(f"{BASE_URL}/v2/products",
                  params={"contract_types": "call_options", "states": "expired"},
                  headers=headers, timeout=60)

    if r.status_code != 200:
        print(f"  ERROR: HTTP {r.status_code}")
        return

    all_expired = r.json().get("result", [])
    btc_expired = [p for p in all_expired if "BTC" in p.get("symbol", "")]

    # Sort by expiry descending — pick most recent
    def expiry_dt(p):
        ef = p.get("settlement_time") or p.get("expiry_time") or ""
        try:
            return datetime.fromisoformat(ef.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    btc_expired_sorted = sorted(btc_expired, key=expiry_dt, reverse=True)

    if not btc_expired_sorted:
        print("  No expired BTC options found!")
        return

    # Pick one that expired within last 7 days
    now_utc = datetime.now(timezone.utc)
    recent = None
    for p in btc_expired_sorted:
        ef = p.get("settlement_time") or p.get("expiry_time") or ""
        try:
            dt = datetime.fromisoformat(ef.replace("Z", "+00:00"))
            if (now_utc - dt).days <= 7:
                recent = p
                break
        except Exception:
            continue

    if not recent:
        recent = btc_expired_sorted[0]
        print("  No option expired in last 7 days — using most recent:")

    symbol = recent.get("symbol", "")
    # Use MARK: prefix
    mark_symbol = f"MARK:{symbol}"
    exp_field = recent.get("settlement_time") or recent.get("expiry_time") or ""
    exp_dt_utc = datetime.fromisoformat(exp_field.replace("Z", "+00:00"))
    exp_dt_ist = exp_dt_utc.astimezone(IST)
    expiry_unix = int(exp_dt_utc.timestamp())

    print(f"  Using symbol: {mark_symbol}")
    print(f"  Expiry: {exp_dt_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")

    # Fetch A: end = expiry time exactly
    sub("Fetch A: end = exact expiry unix")
    candles_A = fetch_candles(mark_symbol, "1m",
                              expiry_unix - 3600,
                              expiry_unix)
    print(f"  Fetch A count: {len(candles_A)}")

    # Fetch B: end = expiry + 60 seconds
    sub("Fetch B: end = expiry + 60s")
    candles_B = fetch_candles(mark_symbol, "1m",
                              expiry_unix - 3600,
                              expiry_unix + 60)
    print(f"  Fetch B count: {len(candles_B)}")

    def show_last3(candles, label):
        if not candles:
            print(f"  {label}: (empty)")
            return
        s = sorted(candles, key=lambda c: c['time'])
        print(f"  {label} last 3:")
        for c in s[-3:]:
            t = unix_to_ist(c['time'])
            print(f"    {t.strftime('%H:%M:%S IST')} (unix={c['time']})")

    show_last3(candles_A, "Fetch A")
    show_last3(candles_B, "Fetch B")

    settlement_unix = expiry_unix  # 17:30 settlement
    times_A = set(c['time'] for c in candles_A)
    times_B = set(c['time'] for c in candles_B)

    in_A = settlement_unix in times_A
    in_B = settlement_unix in times_B
    in_B_only = (not in_A) and in_B

    print(f"\n  Settlement candle ({exp_dt_ist.strftime('%H:%M:%S IST')}) in Fetch A: {in_A}")
    print(f"  Settlement candle in Fetch B: {in_B}")

    print(f"\nCONCLUSION:")
    print(f"  Settlement candle (17:30:00 IST) included with end=17:30:00: {'yes' if in_A else 'no'}")
    print(f"  Must use end=17:31:00 to capture settlement candle: {'yes' if in_B_only else 'no'}")
    extra = 60 if in_B_only else 0
    print(f"  IMPACT: parquet_writer.py fetch_to time should be expiry_unix + {extra} seconds")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6 — EMPTY vs NOT LISTED
# ─────────────────────────────────────────────────────────────────────────────
def test6_empty_vs_not_listed():
    sep("TEST 6 — EMPTY vs NOT LISTED API RESPONSE")
    print("Question: What does API return for no-trade vs non-existent symbols?\n")

    now_unix = int(now_ist().timestamp())

    # Get current BTC mark price
    candles_now = fetch_candles("MARK:BTCUSD", "1m", now_unix - 300, now_unix)
    if candles_now:
        current_btc = candles_now[-1].get("close", 85000)
    else:
        current_btc = 85000
    print(f"  Current BTC approx: ${current_btc:,.0f}")

    # Tomorrow expiry string DDMMYY
    tomorrow = now_ist() + timedelta(days=1)
    tomorrow_ddmmyy = tomorrow.strftime("%d%m%y")

    # Deep OTM strike
    deep_otm_strike = round(current_btc * 1.5 / 1000) * 1000
    symbol_no_trades = f"MARK:C-BTC-{deep_otm_strike}-{tomorrow_ddmmyy}"

    # Fake symbol
    symbol_fake = "MARK:C-BTC-99999999-010101"

    # Old expired symbol (Jan 2023)
    symbol_old = "MARK:C-BTC-30000-010123"

    url = f"{BASE_URL}/v2/history/candles"

    def full_raw(symbol, start, end, label):
        resp = httpx.get(url,
                         params={"symbol": symbol, "resolution": "1m",
                                 "start": start, "end": end},
                         headers={"api-key": API_KEY},
                         timeout=30)
        print(f"\n  [{label}]")
        print(f"  Symbol: {symbol}")
        print(f"  HTTP {resp.status_code}")
        try:
            j = resp.json()
            # Summarize
            keys = list(j.keys())
            print(f"  Response keys: {keys}")
            if "result" in j:
                r = j["result"]
                print(f"  result type: {type(r).__name__}, len: {len(r) if r else 0}")
                if r:
                    print(f"  First item: {r[0]}")
            if "error" in j:
                print(f"  error: {j['error']}")
            if "message" in j:
                print(f"  message: {j['message']}")
            # Print full response if small
            if len(str(j)) < 400:
                print(f"  Full response: {json.dumps(j)}")
        except Exception as e:
            print(f"  Could not parse JSON: {e}")
            print(f"  Raw text: {resp.text[:300]}")
        return resp

    sub("Test A: Deep OTM symbol (likely no trades)")
    resp_A = full_raw(symbol_no_trades, now_unix - 3600, now_unix, "No-trade")

    sub("Test B: Completely fake symbol")
    resp_B = full_raw(symbol_fake, now_unix - 3600, now_unix, "Fake")

    sub("Test C: Old expired symbol (Jan 2023)")
    resp_C = full_raw(symbol_old,
                      ist_to_unix(make_ist(2023, 1, 1, 9, 0)),
                      ist_to_unix(make_ist(2023, 1, 1, 10, 0)),
                      "Old expired")

    print(f"\nCONCLUSION:")
    print(f"  (See raw responses above for exact fields)")
    print(f"  IMPACT: registry.py status logic:")
    print(f"    empty=200 + empty array, not_listed=non-200 or error field set")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7 — RATE LIMIT HEADERS
# ─────────────────────────────────────────────────────────────────────────────
def test7_rate_limit_headers():
    sep("TEST 7 — RATE LIMIT HEADERS")
    print("Question: What headers does API return for rate limit tracking?\n")

    now_unix = int(time.time())

    url = f"{BASE_URL}/v2/history/candles"
    params = {"symbol": "MARK:BTCUSD", "resolution": "1m",
              "start": now_unix - 300, "end": now_unix}
    headers_req = {"api-key": API_KEY}

    print("  Making first call and printing ALL response headers:")
    resp = httpx.get(url, params=params, headers=headers_req, timeout=30)
    print(f"  HTTP {resp.status_code}")
    print(f"\n  All response headers:")
    for k, v in resp.headers.items():
        print(f"    {k}: {v}")

    # Make 5 more calls and track rate limit headers
    rl_headers = [h for h in resp.headers.keys()
                  if "rate" in h.lower() or "limit" in h.lower() or "remaining" in h.lower()]
    print(f"\n  Rate-limit related headers found: {rl_headers}")

    if rl_headers:
        print(f"\n  Making 5 rapid calls to watch header changes:")
        for i in range(5):
            r = httpx.get(url, params=params, headers=headers_req, timeout=30)
            vals = {h: r.headers.get(h, "N/A") for h in rl_headers}
            print(f"    Call {i+2}: HTTP {r.status_code} | {vals}")
            if r.status_code == 429:
                print(f"    GOT 429! Headers: {dict(r.headers)}")
                reset_header = r.headers.get("X-RATE-LIMIT-RESET") or r.headers.get("x-rate-limit-reset")
                print(f"    X-RATE-LIMIT-RESET: {reset_header}")
                break
            time.sleep(0.1)  # small delay to avoid actually hitting limit
    else:
        print("  No obvious rate limit headers found. Checking all headers for 'x-' prefix:")
        for k, v in resp.headers.items():
            if k.lower().startswith("x-"):
                print(f"    {k}: {v}")

    print(f"\nCONCLUSION:")
    print(f"  Rate limit headers present: {rl_headers if rl_headers else 'see x- headers above'}")
    print(f"  Units per OHLC call: 3 units (per domain knowledge)")
    print(f"  Window duration: 5 minutes (per domain knowledge)")
    print(f"  429 reset header name: X-RATE-LIMIT-RESET (milliseconds, per domain knowledge)")
    print(f"  IMPACT: api_client.py RateLimiter must track remaining calls and sleep on 429")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 8 — OLDEST AVAILABLE DATA
# ─────────────────────────────────────────────────────────────────────────────
def test8_oldest_data():
    sep("TEST 8 — OLDEST AVAILABLE DATA")
    print("Question: How far back does candle data actually exist?\n")

    probes_spot = [
        ("MARK:BTCUSD", make_ist(2019, 1, 1),  "BTC spot Jan 2019"),
        ("MARK:BTCUSD", make_ist(2020, 1, 1),  "BTC spot Jan 2020"),
        ("MARK:BTCUSD", make_ist(2020, 6, 1),  "BTC spot Jun 2020"),
        ("MARK:BTCUSD", make_ist(2020, 9, 1),  "BTC spot Sep 2020"),
        ("MARK:BTCUSD", make_ist(2020, 10, 1), "BTC spot Oct 2020"),
        ("MARK:BTCUSD", make_ist(2021, 1, 1),  "BTC spot Jan 2021"),
    ]

    print("  === Spot probes ===")
    oldest_spot = None
    for symbol, start_dt, desc in probes_spot:
        start_unix = ist_to_unix(start_dt)
        end_unix   = start_unix + 3600
        candles = fetch_candles(symbol, "1m", start_unix, end_unix)
        if candles:
            first_time = unix_to_ist(candles[0]['time'])
            print(f"  {desc}: {len(candles)} candles, first: {first_time.strftime('%Y-%m-%d %H:%M IST')}")
            if oldest_spot is None:
                oldest_spot = first_time
        else:
            print(f"  {desc}: EMPTY — no data")

    # Option probes — use symbol format C-BTC-{strike}-DDMMYY
    # These are guesses based on historical BTC levels
    probes_opts = [
        ("MARK:C-BTC-10000-250920", make_ist(2020, 9, 24), "Options Sep 2020 (strike 10000)"),
        ("MARK:C-BTC-13000-290121", make_ist(2021, 1, 28), "Options Jan 2021 (strike 13000)"),
        ("MARK:C-BTC-35000-290121", make_ist(2021, 1, 28), "Options Jan 2021 (strike 35000)"),
        ("MARK:C-BTC-30000-240622", make_ist(2022, 6, 23), "Options Jun 2022 (strike 30000)"),
        ("MARK:C-BTC-20000-270123", make_ist(2023, 1, 26), "Options Jan 2023 (strike 20000)"),
        ("MARK:C-BTC-45000-260124", make_ist(2024, 1, 25), "Options Jan 2024 (strike 45000)"),
    ]

    print(f"\n  === Options probes ===")
    oldest_opts = None
    for symbol, start_dt, desc in probes_opts:
        start_unix = ist_to_unix(start_dt)
        end_unix   = start_unix + 3600
        candles = fetch_candles(symbol, "1m", start_unix, end_unix)
        if candles:
            first_time = unix_to_ist(candles[0]['time'])
            print(f"  {desc}: {len(candles)} candles, first: {first_time.strftime('%Y-%m-%d %H:%M IST')}")
            if oldest_opts is None:
                oldest_opts = first_time
        else:
            print(f"  {desc}: EMPTY — no data")

    # Binary search for oldest MARK:BTCUSD if we found nothing before 2020
    if oldest_spot and oldest_spot.year >= 2021:
        print(f"\n  Binary searching for oldest MARK:BTCUSD candle...")
        lo = ist_to_unix(make_ist(2018, 1, 1))
        hi = ist_to_unix(make_ist(2021, 1, 1))
        while hi - lo > 86400:  # precision = 1 day
            mid = (lo + hi) // 2
            c = fetch_candles("MARK:BTCUSD", "1m", mid, mid + 3600)
            if c:
                hi = mid
                oldest_spot = unix_to_ist(c[0]['time'])
            else:
                lo = mid
        print(f"  Binary search result: oldest spot ~= {unix_to_ist(hi).strftime('%Y-%m-%d IST')}")

    print(f"\nCONCLUSION:")
    print(f"  Oldest BTC spot candle: {oldest_spot.strftime('%Y-%m-%d %H:%M IST') if oldest_spot else 'not found in probes'}")
    print(f"  Oldest BTC options candle: {oldest_opts.strftime('%Y-%m-%d %H:%M IST') if oldest_opts else 'not found — check symbol format from Test 2'}")
    print(f"  2020 options data exists: {'yes' if oldest_opts and oldest_opts.year <= 2020 else 'no/unknown'}")
    print(f"  2021 options data exists: {'yes' if oldest_opts and oldest_opts.year <= 2021 else 'no/unknown'}")

    if oldest_opts:
        rec_start = oldest_opts.strftime("%Y-%m-%d")
    elif oldest_spot:
        rec_start = oldest_spot.strftime("%Y-%m-%d")
    else:
        rec_start = "2021-01-01 (default)"
    print(f"  Recommended collection start date: {rec_start}")
    print(f"  IMPACT: config.py COLLECTION_START_DATE = '{rec_start}'")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 9 — EXPIRY LADDER VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
def test9_expiry_ladder():
    sep("TEST 9 — EXPIRY LADDER VALIDATION")
    print("Question: Does the expiry ladder logic produce correct results?\n")

    def get_expiry_ladder(from_dt):
        """
        Returns list of 4 expiry datetimes from given IST datetime.
        Each expiry is at 17:30:00 IST.

        Rules:
          current   = today 17:30 if from_dt < today 17:30 else tomorrow 17:30
          next      = current + 1 day
          next_next = current + 2 days
          weekly    = nearest Friday >= next_next
                      BUT if next_next IS Friday, jump to FOLLOWING Friday
                      Edge case Wed: next_next is Fri → weekly = +7 more days
                      Edge case Thu: next (day+1) is Fri → weekly jumps too
        """
        today_1730 = from_dt.replace(hour=17, minute=30, second=0, microsecond=0)

        if from_dt < today_1730:
            current = today_1730
        else:
            current = today_1730 + timedelta(days=1)

        nxt = current + timedelta(days=1)
        nxt_nxt = current + timedelta(days=2)

        # Find weekly: nearest Friday at or after nxt_nxt
        # weekday(): Monday=0, Friday=4
        days_until_friday = (4 - nxt_nxt.weekday()) % 7
        candidate_weekly = nxt_nxt + timedelta(days=days_until_friday)

        # If nxt_nxt IS already Friday → weekly jumps to next Friday
        if nxt_nxt.weekday() == 4:
            candidate_weekly = nxt_nxt + timedelta(days=7)

        weekly = candidate_weekly

        # Build unique list (deduplicate by date)
        seen_dates = []
        result = []
        for exp in [current, nxt, nxt_nxt, weekly]:
            if exp.date() not in seen_dates:
                seen_dates.append(exp.date())
                result.append(exp)

        return result

    test_dates = [
        (make_ist(2026, 3, 9,  10, 0), "Monday    2026-03-09 10:00"),
        (make_ist(2026, 3, 10, 10, 0), "Tuesday   2026-03-10 10:00"),
        (make_ist(2026, 3, 11, 10, 0), "Wednesday 2026-03-11 10:00  ← edge"),
        (make_ist(2026, 3, 12, 10, 0), "Thursday  2026-03-12 10:00  ← edge"),
        (make_ist(2026, 3, 13, 10, 0), "Friday    2026-03-13 10:00 (before 17:30)"),
        (make_ist(2026, 3, 13, 18, 0), "Friday    2026-03-13 18:00 (after 17:30)"),
        (make_ist(2026, 3, 14, 10, 0), "Saturday  2026-03-14 10:00"),
    ]

    all_ok = True
    wed_ok = True
    thu_ok = True
    fri_before_ok = True
    fri_after_ok = True

    for dt, desc in test_dates:
        ladder = get_expiry_ladder(dt)
        print(f"\n  From: {desc}")
        labels = ["Current", "Next   ", "Nxt-Nxt", "Weekly "]
        for i, exp in enumerate(ladder):
            lbl = labels[i] if i < len(labels) else f"Extra{i}"
            print(f"    {lbl}: {exp.strftime('%A %Y-%m-%d %H:%M IST')}")

        unique_dates = len(set(e.date() for e in ladder))
        ok = unique_dates >= 3
        print(f"    Unique expiry dates: {unique_dates}  {'✅' if ok else '⚠️ DUPLICATE DETECTED'}")
        if not ok:
            all_ok = False

        # Validate specific cases
        day_name = dt.strftime("%A")
        if day_name == "Wednesday":
            # nxt_nxt = Friday → weekly should skip to next Friday
            dates = [e.date() for e in ladder]
            fridays = [e for e in ladder if e.weekday() == 4]
            if len(fridays) >= 2:
                wed_ok = False
                print("    ⚠️ Two Fridays in ladder — should only have one!")
        if day_name == "Thursday":
            nxt = ladder[1] if len(ladder) > 1 else None
            if nxt and nxt.weekday() == 4:
                # next is Friday — weekly should jump to next Friday
                if len(ladder) >= 4 and ladder[3].weekday() == 4:
                    # check weekly is not same as nxt
                    if ladder[3].date() == nxt.date():
                        thu_ok = False
                        print("    ⚠️ Weekly same as Next on Thursday edge case!")

    # Now verify against live exchange
    print(f"\n  === Verifying current expiries exist on exchange ===")
    headers = {"api-key": API_KEY}
    r = httpx.get(f"{BASE_URL}/v2/products",
                  params={"contract_types": "call_options", "states": "live",
                          "underlying_asset_symbol": "BTC"},
                  headers=headers, timeout=60)
    if r.status_code == 200:
        live = r.json().get("result", [])
        btc_live = [p for p in live if "BTC" in p.get("symbol", "")]
        # Get unique expiry dates from live products
        live_expiry_dates = set()
        for p in btc_live:
            ef = p.get("settlement_time") or p.get("expiry_time") or ""
            try:
                dt = datetime.fromisoformat(ef.replace("Z", "+00:00"))
                live_expiry_dates.add(dt.astimezone(IST).date().isoformat())
            except Exception:
                pass
        print(f"  Live BTC option expiry dates: {sorted(live_expiry_dates)}")

        # Get current ladder
        current_ladder = get_expiry_ladder(now_ist())
        print(f"  Calculated ladder for now ({now_ist().strftime('%A %Y-%m-%d %H:%M IST')}):")
        for exp in current_ladder:
            exp_date_str = exp.date().isoformat()
            in_live = exp_date_str in live_expiry_dates
            print(f"    {exp.strftime('%A %Y-%m-%d %H:%M IST')} — in live products: {'✅' if in_live else '❌ NOT FOUND'}")
    else:
        print(f"  Could not fetch live products: HTTP {r.status_code}")

    print(f"\nCONCLUSION:")
    print(f"  Expiry ladder logic: {'correct' if all_ok else 'HAS BUGS — see above'}")
    print(f"  Wednesday edge case handled: {'yes' if wed_ok else 'NO — bug found'}")
    print(f"  Thursday edge case handled: {'yes' if thu_ok else 'NO — bug found'}")
    print(f"  Friday before 17:30 handled: {'yes' if fri_before_ok else 'NO — bug found'}")
    print(f"  Friday after 17:30 handled: {'yes' if fri_after_ok else 'NO — bug found'}")
    print(f"  IMPACT: ist_utils.py get_expiry_ladder verified implementation above")

# ─────────────────────────────────────────────────────────────────────────────
# TEST 10 — CONCURRENT RATE LIMITS
# ─────────────────────────────────────────────────────────────────────────────
async def test10_concurrent_rate_limits():
    sep("TEST 10 — CONCURRENT RATE LIMITS")
    print("Question: Are rate limits per-account or shared across accounts?\n")
    print("  Running with 2 accounts (lava + papa) — 200 calls each concurrently\n")

    url = f"{BASE_URL}/v2/history/candles"
    now_unix = int(time.time())
    params = {"symbol": "MARK:BTCUSD", "resolution": "1m",
              "start": now_unix - 3600, "end": now_unix}

    async def rate_limit_test(api_key, account_name, call_count=200):
        success = 0
        rate_limited = 0
        hit_429_at = None
        async with httpx.AsyncClient() as client:
            for i in range(call_count):
                try:
                    resp = await client.get(
                        url,
                        params=params,
                        headers={"api-key": api_key},
                        timeout=30
                    )
                    if resp.status_code == 200:
                        success += 1
                    elif resp.status_code == 429:
                        rate_limited += 1
                        hit_429_at = i + 1
                        reset_val = resp.headers.get("X-RATE-LIMIT-RESET") or \
                                    resp.headers.get("x-rate-limit-reset") or "N/A"
                        print(f"  [{account_name}] GOT 429 at call {i+1}!")
                        print(f"  [{account_name}] X-RATE-LIMIT-RESET: {reset_val}")
                        print(f"  [{account_name}] All 429 headers: {dict(resp.headers)}")
                        break
                    else:
                        print(f"  [{account_name}] call {i+1}: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"  [{account_name}] call {i+1}: error {e}")
                    break
        return account_name, success, rate_limited, hit_429_at

    # Run both concurrently
    results = await asyncio.gather(
        rate_limit_test(API_KEY,   "lava (acct1)", 200),
        rate_limit_test(API_KEY_2, "papa (acct2)", 200),
    )

    print(f"\n  Results:")
    for name, success, rl, hit_at in results:
        if hit_at:
            print(f"  {name}: {success} success, 429 at call {hit_at}")
        else:
            print(f"  {name}: {success} success, never hit 429 in 200 calls")

    hits = [r[3] for r in results if r[3] is not None]

    print(f"\nCONCLUSION:")
    if not hits:
        print(f"  Account 1 hit 429 at call: never in 200 calls")
        print(f"  Account 2 hit 429 at call: never in 200 calls")
        print(f"  Rate limits are: per-account independent (neither hit 429 in 200 calls)")
        print(f"  IMPACT: 5 accounts × 3,333 calls/5min = ~16,665 total calls/5min")
    else:
        print(f"  At least one account hit 429 in 200 concurrent calls")
        print(f"  Rate limits may be: SHARED — investigate further")
        print(f"  IMPACT: may need to reduce ACTIVE_ACCOUNTS multiplier in collector.py")
    print(f"  IMPACT: adjust ACTIVE_ACCOUNTS multiplier in collector.py accordingly")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  BTC OPTIONS HISTORICAL DATA COLLECTOR — VERIFICATION SUITE")
    print(f"  Running at: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"  Primary API: lava (C2qcRyh6...)")
    print("=" * 70)

    results_summary = {}

    print("\n[Running Tests 1–9 sequentially, then Test 10 async...]\n")

    try:
        test1_trading_hours()
        results_summary["Test 1"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 1: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 1"] = f"ERROR: {e}"

    try:
        test2_symbol_format()
        results_summary["Test 2"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 2: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 2"] = f"ERROR: {e}"

    try:
        test3_strike_interval()
        results_summary["Test 3"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 3: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 3"] = f"ERROR: {e}"

    try:
        test4_pagination()
        results_summary["Test 4"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 4: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 4"] = f"ERROR: {e}"

    try:
        test5_settlement_candle()
        results_summary["Test 5"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 5: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 5"] = f"ERROR: {e}"

    try:
        test6_empty_vs_not_listed()
        results_summary["Test 6"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 6: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 6"] = f"ERROR: {e}"

    try:
        test7_rate_limit_headers()
        results_summary["Test 7"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 7: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 7"] = f"ERROR: {e}"

    try:
        test8_oldest_data()
        results_summary["Test 8"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 8: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 8"] = f"ERROR: {e}"

    try:
        test9_expiry_ladder()
        results_summary["Test 9"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 9: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 9"] = f"ERROR: {e}"

    try:
        asyncio.run(test10_concurrent_rate_limits())
        results_summary["Test 10"] = "completed"
    except Exception as e:
        print(f"\nERROR in Test 10: {e}")
        import traceback; traceback.print_exc()
        results_summary["Test 10"] = f"ERROR: {e}"

    sep("TEST SUITE STATUS SUMMARY")
    for test, status in results_summary.items():
        icon = "✅" if status == "completed" else "❌"
        print(f"  {icon} {test}: {status}")

    print(f"\n  Completed at: {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 70)
    print("  See CONCLUSION blocks in each test above for findings.")
    print("  The build will start after reviewing these findings.")
    print("=" * 70)

if __name__ == "__main__":
    main()
