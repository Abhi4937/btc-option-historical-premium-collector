# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## MANDATORY: Pre-flight check after every code change

Run ALL of these before saying "ready to run" — no exceptions:

```bash
# 1. Syntax
python3 -c "
import ast
for f in ['worker.py','progress.py','collector.py','monitor.py','manifest.py','registry.py','main.py','backfill.py']:
    ast.parse(open(f).read()); print(f'OK: {f}')
"

# 2. Imports
python3 -c "import worker, progress, collector, monitor, manifest, registry; print('imports OK')"

# 3. Stale references — grep for any renamed/deleted symbol
grep -n "OLD_NAME" *.py

# 4. Worker→Progress key alignment
#    Keys sent in worker._update_status() must all exist in progress.py s.get()

# 5. Attribute check — every self._ read in worker.py must be in __init__

# 6. Dry render — catches AttributeError before user runs
python3 -c "
import asyncio
from progress import ProgressDisplay
d = ProgressDisplay()
d._render()
print('render OK')
"
```

**Past failures from skipping this:**
- `_session_calls` renamed but two stale references left → crash on resume
- `_estimated_total_calls` removed from `__init__` but reference left in `_render()` → crash on resume

## What this project does

Collects BTC options historical data (1-minute OHLC + OI candles) from Delta Exchange India (`api.india.delta.exchange`) and stores it as Parquet files on disk. Runs on WSL2. Up to 5 API accounts work in parallel, each independently claiming and processing months of data.

## Commands

```bash
# Start fresh collection (accounts in parallel, ACTIVE_ACCOUNTS controls count)
python main.py collect

# Resume after crash — resets stale in_progress, continues
python main.py resume

# Show manifest + registry progress tables
python main.py status

# Single-account test: collect one month and verify parquet output
python main.py test-run [YYYY-MM]   # defaults to last complete month

# Fetch/update spot data only (MARK:BTCUSD, BTCUSD, OI:BTCUSD)
# Incremental: starts from last_parquet_ts + 60s, so only the gap is fetched.
# Falls back to full Dec 2023 → today loop only if the parquet doesn't exist.
python main.py spot

# Binary-search for oldest available data on the API
python main.py test-depth

# Print verified API test findings summary
python main.py findings

# Pre-resume health check (DB health, disk space, API keys, process state)
python3 precheck.py

# Health monitor — prints to terminal (does not send Telegram)
python monitor.py --print

# Health monitor — sends to Telegram
python monitor.py

# Background watchdog (15-min interval, sends Telegram alerts)
nohup python3 watchdog.py >> ~/btc-data/logs/watchdog.log 2>&1 &

# Fill coverage gaps: re-probe empty strikes and backfill early-lifetime
# data for done strikes that were fetched under the old narrower rule.
# Add --dry-run to preview without API calls.
python main.py backfill-all [--dry-run]
```

## Catch-up workflow (after a long pause)

Standard sequence to bring everything current after the collector has been idle for days/weeks. Verified on 2026-05-02 (~11 days of gap recovered in ~36 min):

```bash
# 1. Bring spot to today (incremental — typically 30-60s)
python main.py spot

# 2. Reset the affected manifest months back to pending so the worker re-enters
#    them. Past months that are already 'done' don't auto-reset; only current
#    and future months do (via reset_current_and_future_months).
python3 -c "
import asyncio, aiosqlite
from config import MANIFEST_DB
async def reset():
    async with aiosqlite.connect(MANIFEST_DB) as db:
        cur = await db.execute(
            \"UPDATE manifest SET status='pending', claimed_by=NULL \"
            \"WHERE expiry_month IN ('YYYY-MM','YYYY-MM')\"
        )
        await db.commit()
        print(f'Reset {cur.rowcount} months')
asyncio.run(reset())
"

# 3. (Optional, fast) Pre-populate fetched_to_unix for done symbols in those months,
#    so the worker doesn't pay a small per-symbol parquet read at run time:
python3 -c "
import asyncio, aiosqlite, os, pyarrow.parquet as pq, concurrent.futures as cf
from config import REGISTRY_DB
async def go():
    async with aiosqlite.connect(REGISTRY_DB) as db:
        async with db.execute(
            \"SELECT symbol, parquet_path FROM symbols \"
            \"WHERE status='done' AND fetched_to_unix IS NULL \"
            \"AND parquet_path IS NOT NULL \"
            \"AND substr(expiry_date,1,7) IN ('YYYY-MM','YYYY-MM')\"
        ) as cur:
            rows = await cur.fetchall()
    def _r(r):
        try:
            t = pq.read_table(r[1], columns=['timestamp_unix'])
            return (t.column('timestamp_unix')[-1].as_py() if len(t) else None, r[0])
        except: return (None, r[0])
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        ups = [u for u in ex.map(_r, rows) if u[0] is not None]
    async with aiosqlite.connect(REGISTRY_DB) as db:
        await db.executemany('UPDATE symbols SET fetched_to_unix=? WHERE symbol=?', ups)
        await db.commit()
        print(f'Populated {len(ups)} rows')
asyncio.run(go())
"

# 4. Run the catch-up
python main.py resume
```

Every existing `done` symbol is checked against `min(expiry_unix, spot_last_unix)`. Only the actual tail gap is fetched — no re-downloading. `empty` rows stay skipped (use `backfill-all` separately to re-probe).

## Scoped catch-up + start-gap fill (verified 2026-05-18)

Use this when you want **strict date scope** (e.g. "from 2026-04-21 to today") and need BOTH start AND tail coverage, not just tail. The standard catch-up above only handles tail; the start-gap fill needs a separate pass because the worker writes parquets starting at per-strike `first_seen`, which is later than expiry-level `first_appearance` for strikes that entered the ATM±40 band late or that were originally collected under a narrower lookback.

### Per-expiry audit (run first to see scope)

```bash
python3 << 'PY'
import asyncio, aiosqlite, pyarrow.parquet as pq
from datetime import date
from config import REGISTRY_DB
from ist_utils import first_appearance, get_expiry_dt
spot = pq.read_table('/home/abhis/btc-data/data/spot/BTCUSD_1min.parquet', columns=['timestamp_unix'])
SPOT_LAST = spot.column('timestamp_unix')[-1].as_py()
async def main():
    async with aiosqlite.connect(REGISTRY_DB) as db:
        async with db.execute(
            "SELECT expiry_date, status, fetched_from_unix, fetched_to_unix "
            "FROM symbols WHERE expiry_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD' ORDER BY expiry_date"
        ) as cur:
            rows = await cur.fetchall()
    by_exp = {}
    for r in rows: by_exp.setdefault(r[0], []).append(r)
    print(f"{'expiry':<12} {'done':>5} {'startOK':>8} {'startGap':>9} {'tailOK':>7} {'tailGap':>8}")
    for exp_str in sorted(by_exp):
        exp_dt = get_expiry_dt(date.fromisoformat(exp_str))
        fa = int(first_appearance(exp_dt).timestamp())
        end = min(exp_dt.replace(hour=17, minute=29).timestamp(), SPOT_LAST)
        done = [r for r in by_exp[exp_str] if r[1]=='done']
        sok = sum(1 for r in done if r[2] is not None and r[2] <= fa + 60)  # 60s tolerance
        tok = sum(1 for r in done if r[3] is not None and r[3] >= end - 60)
        print(f"{exp_str:<12} {len(done):>5} {sok:>8} {len(done)-sok:>9} {tok:>7} {len(done)-tok:>8}")
asyncio.run(main())
PY
```

The **60s tolerance** matches `backfill._process_done` (l.129). Without it, every strike whose `fetched_from_unix` is even one second after `first_appearance` reads as a gap.

### Scoped resume (only one month, e.g. current month)

`run_collection()` auto-resets ALL current+future months. To restrict the scope (e.g. don't touch June/July/Aug when only May needs work), monkey-patch the reset:

```bash
python3 -c "
import asyncio, manifest, aiosqlite
from config import MANIFEST_DB
async def scoped_reset(today):
    async with aiosqlite.connect(MANIFEST_DB) as db:
        cur = await db.execute(
            \"UPDATE manifest SET status='pending', claimed_by=NULL \"
            \"WHERE status='done' AND expiry_month='YYYY-MM'\"
        )
        await db.commit()
        return cur.rowcount
manifest.reset_current_and_future_months = scoped_reset
from collector import run_collection
asyncio.run(run_collection())
"
```

To rescue *broken* `empty` rows that should have data (Friday probed before data existed, or any case where `parquets_on_disk > done_count` in the audit), reset their status before resume:

```bash
python3 -c "
import asyncio, aiosqlite
from config import REGISTRY_DB
async def go():
    async with aiosqlite.connect(REGISTRY_DB) as db:
        cur = await db.execute(
            \"UPDATE symbols SET status='pending' \"
            \"WHERE status='empty' AND expiry_date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'\"
        )
        await db.commit()
        print(f'Reset {cur.rowcount} empties')
asyncio.run(go())
"
```

### Scoped start-gap backfill (done rows only, no empty re-probe)

Monkey-patch `_load_targets` (NOT `_load_candidate_rows` — that name doesn't exist) to filter to in-scope `done` rows. This skips the empty re-probe entirely because no `empty` rows pass the filter:

```bash
python3 -c "
import asyncio, backfill
orig = backfill._load_targets
async def filtered():
    rows = await orig()
    return [r for r in rows
            if r['status'] == 'done'
            and 'YYYY-MM-DD' <= r['expiry_date'] <= 'YYYY-MM-DD']
backfill._load_targets = filtered
asyncio.run(backfill.run_backfill(dry_run=False))
"
```

**Run it twice.** First pass fetches gaps; for strikes where API returns *some* data but not all the way back to `first_appearance`, `fetched_from_unix` is set to where the data actually starts (not first_appearance). The audit still shows these as start_gap. A second pass probes the now-smaller remaining window, gets empty, sets `fetched_from_unix = first_appearance` as a "covered" marker. After two passes, audit reads `startGap=0` everywhere.

### Permission gotcha

Collector must run as `abhis` (the WSL user), NOT `root`. If a past run touched parquet files as root, future runs as `abhis` will fail with `[Errno 13] Permission denied` when trying to overwrite them. Fix once with `sudo chown -R abhis:abhis ~/btc-data`. Verify ownership before resuming with `find ~/btc-data -not -user abhis | wc -l` (should be 0).

## Architecture

### Data flow

```
main.py → collector.py → AccountWorker (worker.py)
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
          manifest.db     registry.db    DeltaAPIClient
          (month-level)   (symbol-level) (api_client.py)
                                               │
                                         Parquet files
                                         (parquet_writer.py)
```

Live terminal progress is rendered by `ProgressDisplay` (progress.py), driven by a status callback from `run_collection()`.

### Key modules

| Module | Role |
|---|---|
| `config.py` | All constants — API limits, paths, status values, verified test findings |
| `main.py` | CLI entry point; `cmd_test_run`, `cmd_test_depth` inline |
| `collector.py` | `run_collection()` orchestrates workers; `fetch_spot_only()` for spot-only |
| `worker.py` | `AccountWorker` — claims months, builds strike unions, fetches options in parallel |
| `api_client.py` | `DeltaAPIClient` (httpx async) + `RateLimiter` (count-based, no header reliance) |
| `manifest.py` | SQLite manifest — month-level work distribution (WAL mode, asyncio lock) |
| `registry.py` | SQLite registry — per-symbol status + spot_progress + expiry_strikes tables |
| `parquet_writer.py` | Atomic write-then-rename; merge/deduplicate MARK + OI candles |
| `ist_utils.py` | All IST datetime helpers; expiry ladder logic |
| `strike_generator.py` | ATM rounding, strike chain generation, symbol builders |
| `progress.py` | `ProgressDisplay` — Rich Live terminal UI; per-account state + overall progress bar |
| `monitor.py` | Read-only DB queries → Telegram/terminal health report |
| `watchdog.py` | Background loop (15 min); stale detection, disk space, 429 rate alerts |
| `precheck.py` | Pre-resume checklist — run before `python main.py resume` |
| `verify.py` | Standalone verification tests (all 10 API behaviours); not part of normal collection |
| `backfill.py` | Second-pass filler — closes gaps left by blind status skips (`done` rows with partial coverage, `empty` rows probed under an older narrower window). Runs via `main.py backfill-all`. Writes live state to `backfill_state.json`. |

### Storage layout

All data lives on WSL-native ext4 at `~/btc-data/` (not on the NTFS `/mnt/c/` mount — this is critical for performance/correctness under concurrent writes).

```
~/btc-data/
  data/
    spot/BTCUSD_1min.parquet          # merged MARK + LTP vol + OI
    options/
      expiry=YYYY-MM-DD/
        strike=NNNNN/
          CE.parquet                  # MARK + OI candles, 1-min
          PE.parquet
  db/
    manifest.db                       # month-level work queue
    registry.db                       # symbol-level status + strike union
  logs/
    collector.log
    errors.log
    watchdog_state.json
```

### Work distribution (manifest + registry)

- **manifest.db** partitions work by **expiry month** (`YYYY-MM`). Each month is atomically claimed by one worker using an asyncio lock. Status: `pending → in_progress → done/failed`.
- **registry.db** tracks every individual symbol (`MARK:C-BTC-{strike}-{DDMMYY}`). Status values: `pending`, `done`, `empty` (listed but no candles returned), `not_listed` (absent from `/v2/products`), `failed`. Each row also stores `fetched_from_unix` (earliest candle on disk) and `fetched_to_unix` (latest candle on disk) for incremental gap detection.
- Workers use work-stealing: as soon as a month finishes it immediately claims the next.
- **Session progress split** (worker.py): four counters per account — `_session_skipped` (already done/empty/not_listed in registry, no API call), `_session_fetched` (new API fetch with candles), `_session_empty` (new API fetch returned []), `_session_failed` (fetch raised). The Rich UI and Telegram message both use "real work" = fetched + empty + failed, with skipped shown separately.

### Collection strategy (historical vs. live months)

The manifest is populated **3 months ahead** of today so live Friday expiries can be collected as they become available.

**Expiry filter per month** (in `worker.py:_process_month`):
- **Expired** (`expiry_dt <= now`): every daily expiry in the calendar month is collected.
- **Future Fridays** (`expiry_dt > now`): only weekly Friday expiries whose `first_appearance(e) <= now` and whose date is within 3 months ahead. This captures all live contracts (Apr 24, May 29, etc.) without fetching non-existent dailies.

**Future calendar months** (e.g. April, May): the spot data fetch reads from the existing parquet file instead of re-fetching the API, since the API returns nothing useful for future timestamps. The API fetch end is always capped at `min(end_dt, now_ist())`.

**Incremental spot fetch (`fetch_spot_only` in `collector.py`, 2026-05-02):** When `BTCUSD_1min.parquet` exists, `python main.py spot` reads its last `timestamp_unix` and fetches only `last_ts + 60s → now` in a single async call (MARK + LTP + OI). No more month-by-month re-loop from Dec 2023. Falls back to the full Dec 2023 → today loop only if the parquet is missing. Typical incremental run ≈ 30–60 seconds vs ~5+ minutes for the old full-loop path.

**`reset_current_and_future_months(today)`** in `manifest.py`: called at the start of every `run_collection()`. Resets any current or future month (`expiry_month >= today's YYYY-MM`) that was previously marked `done` back to `pending`. Current month is included so its strike union gets rebuilt against freshly-extended spot (important when spot catches up mid-month to a price level that introduces new ATM±40 strikes). Future months pick up newly-available Friday contracts (whose `first_appearance` just passed).

**`reset_all_done_months()`** in `manifest.py`: one-shot aggressive reset used for full resweeps. Marks every `done` month back to `pending`. Registry is untouched so already-collected strikes auto-skip via `get_symbol_status()` in `worker._fetch_option` — net API cost = only strikes newly introduced by rebuilt unions.

**Spot lookback** (`worker._process_month`): the range passed to spot parquet readback is `month_start - timedelta(days=70)` (same for past and future months). 70 days covers `first_appearance()`'s probe window (monthly2 slot can be ~60 days before settlement). If this were smaller the strike union for late-month expiries would miss early-lifetime ATM ticks.

### Session state + Telegram

`run_collection()` spawns a background task that every 5s writes `~/btc-data/logs/session_state.json` with aggregated split counters across all workers. `monitor.py` reads this file and pushes a short split-bar message to Telegram (skipped / fetched / empty / failed / rate / ETA / active accounts). If no session file exists it sends an "idle" line. `watchdog.py` still runs `monitor.py` every 15 min and also sends its own ✅/⚠️ one-liner.

### Skip-logic and gap-fill (2026-05-02 update)

`worker._fetch_option` is now **end-gap aware**. Behavior per registry status:

- **`done` + parquet ends ≥ `min(expiry, spot_last) - 60s`** → instant skip (no API call, no parquet read).
- **`done` + parquet ends earlier (tail gap)** → fetch ONLY `fetched_to_unix + 60s → min(expiry, spot_last)` and merge. Zero re-downloading.
- **`done` + `fetched_to_unix IS NULL`** (legacy rows pre-2026-05-02) → reads parquet's last timestamp once on first encounter, persists to DB, then proceeds as above.
- **`empty` / `not_listed`** → skip. Use `backfill-all` to re-probe these under a wider window.
- **`pending` / `failed` / new** → full fetch from per-strike `first_seen_unix → expiry`.

The tail-fetch ceiling is `min(expiry_unix, spot_last_unix)`. `spot_last_unix` is read once from `BTCUSD_1min.parquet` at the top of `AccountWorker.run()` and reused for all symbols processed by that worker. This guarantees we never request option data past where spot data exists (the ATM/strike-union logic depends on continuous spot coverage).

**Two start-gap problems remain (handled by `backfill-all` second pass):**

1. **`done` with partial *start* coverage** — strike fetched under an older narrower first_appearance window (e.g. 10-day lookback), so its parquet starts mid-lifetime. Under the current 70-day rule, earlier candles exist on the API but aren't on disk.
2. **`empty` probed under a narrower window** — strike returned `[]` when probed over a short window, so it was marked empty. Under a wider window, some of these do have data.

`backfill.py` (run via `python main.py backfill-all`) resolves both:

- **done gap-fill**: compare `fetched_from_unix` in the registry to `first_appearance(expiry)` under the current rule. If the gap is ≥60s, fetch `first_appearance → earliest_existing − 1s`, merge into the existing parquet.
- **empty re-probe**: for any `empty` with `fetched_from_unix` NULL or wider than first_appearance, re-fetch the full window. If candles land this time, flip to `done` with a new parquet; if still `[]`, set `fetched_from_unix = first_appearance` so future backfill runs skip it.
- **pending/failed untouched** — main worker owns these; backfill never writes them.
- **not_listed untouched** — cross-checked against /v2/products; trustworthy.

Resumability: on first run, a parallel thread-pool reads parquets for rows with NULL `fetched_from_unix` and persists the earliest candle back to the registry. Subsequent backfill runs skip this step entirely. Progress written to `~/btc-data/logs/backfill_state.json`.

Safe to run concurrently with the main collector (different DB columns get written; both use the same per-IP rate limiter though, so serial is faster).

### Rate limiting

The API has **no rate-limit headers**. `RateLimiter` uses a sliding deque of call timestamps. Official limit: 10,000 units per 5-minute window; OHLC = 3 units/call → **3,333 real calls per 5-min window per IP**. Rate limits are **per-IP** (shared across all accounts on the same machine). Safe cap set to **3,100 calls/window** (2000 → 3100 on 2026-04-21 — ~7% headroom below the ceiling; maximizes throughput while absorbing sliding-window drift). The 429 penalty is harsh: a 429 triggers a full 301s sleep (`api_client.py:143`), so never raise above 3,100 without empirical 429-free testing. Drop to 3000 or 2800 if 429s reappear in logs. With `ACTIVE_ACCOUNTS = 1`, this is effectively 3,100 calls/5-min.

### Fetch-window optimizations (2026-04-21)

Two optimizations added to cut the ~50K-calls-per-quarterly pain:

**1. Per-strike `first_seen` fetch start (`worker.py:_process_expiry`).** `_build_strike_union` already computed `first_seen_by_strike[strike]` — the minute each strike first entered our ATM±40 band. Previously ignored; `_fetch_option` now receives it as `fetch_start_unix` instead of a single expiry-level `first_appearance`. For strikes that only became relevant late in the 70-day window, this narrows the fetch range dramatically (e.g. strike 76000 first seen at day 55 → 15 days fetched instead of 70). Expected ~50% call reduction on quarterly expiries.

**2. Binary-search pagination (`api_client.py:fetch_candles`).** Old logic walked all 51 chunks even when the first 40 returned `[]`. New logic:
- One-chunk window → single call.
- First chunk has data → standard forward walk (unchanged path).
- First chunk empty, last chunk has data → **binary-search** for the earliest data-bearing chunk (~log₂51 ≈ 6 calls), then walk from there.
- First and last empty → probe the middle; if also empty, return `[]` after **3 calls total** (confident-empty fast path).

Documented limitation: a mid-range data "island" bordered on both sides by empty chunks would be partially missed. Options don't behave this way in practice (once listed, strikes have continuous 1-min candles until expiry), so this is acceptable. Verified by unit tests covering full-data, late-listed, never-listed, short-window, and mid-listed scenarios.

Combined with Option 1, expected ~70–80% fewer API calls on quarterly expiries and ~50% project-wide wall-time reduction.

### Strike chain

- `DEFAULT_STRIKE_INTERVAL = 100` — $100 intervals (denser than $200, more accurate ATM rounding)
- `CHAIN_HALF_WIDTH = 40` — ATM ± 40 strikes = **81 unique strikes per expiry snapshot**
- Live exchange uses non-uniform grid denser near ATM; $100 interval captures all listed strikes

### Critical API behaviours (do not change without re-testing)

- **Empty vs not-listed**: Both return HTTP 200 + `[]`. In practice all $100-interval strikes are listed by Delta, so the products check is skipped — all empty-candle responses are marked `empty`. See `worker.py:_fetch_option`.
- **Pagination**: API returns the 2000 **most recent** candles, not oldest. Use fixed 2000-minute time windows from start, advance `chunk_start = chunk_end + 1` to avoid boundary duplicate.
- **Symbol format**: `C-BTC-{strike}-{DDMMYY}` / `P-BTC-{strike}-{DDMMYY}`. MARK prefix: `MARK:C-BTC-...`. OI prefix: `OI:C-BTC-...`.
- **Settlement**: Last candle is 17:29 IST. Settlement candle (17:30) is absent. `SETTLEMENT_EXTRA_SECONDS = 0`.
- **Data availability**: MARK:BTCUSD spot from 2023-12-18; OI:BTCUSD from 2024-02-01; options from ~2024-01-25. Run `test-depth` for exact dates.

### Environment

API credentials are in `.env` (`ACCOUNT_1_KEY` through `ACCOUNT_5_KEY`). Loaded via `python-dotenv`. `.env` is gitignored. `ACTIVE_ACCOUNTS` in `config.py` controls how many accounts are used per run (currently 1).

Account names: `lava`, `papa`, `tejas`, `tuition_miss`, `abhishek` (matched to `.env` keys in order).
