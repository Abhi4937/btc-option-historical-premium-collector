# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## MANDATORY: Pre-flight check after every code change

Run ALL of these before saying "ready to run" — no exceptions:

```bash
# 1. Syntax
python3 -c "
import ast
for f in ['worker.py','progress.py','collector.py','monitor.py','manifest.py','registry.py']:
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
```

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
- **registry.db** tracks every individual symbol (`MARK:C-BTC-{strike}-{DDMMYY}`). Status values: `pending`, `done`, `empty` (listed but no candles returned), `not_listed` (absent from `/v2/products`), `failed`.
- Workers use work-stealing: as soon as a month finishes it immediately claims the next.

### Collection strategy (historical vs. live months)

The manifest is populated **3 months ahead** of today so live Friday expiries can be collected as they become available.

**Expiry filter per month** (in `worker.py:_process_month`):
- **Expired** (`expiry_dt <= now`): every daily expiry in the calendar month is collected.
- **Future Fridays** (`expiry_dt > now`): only weekly Friday expiries whose `first_appearance(e) <= now` and whose date is within 3 months ahead. This captures all live contracts (Apr 24, May 29, etc.) without fetching non-existent dailies.

**Future calendar months** (e.g. April, May): the spot data fetch reads from the existing parquet file instead of re-fetching the API, since the API returns nothing useful for future timestamps. The API fetch end is always capped at `min(end_dt, now_ist())`.

**`reset_future_months(today)`** in `manifest.py`: called at the start of every `run_collection()`. Resets any future months (`expiry_month > today's YYYY-MM`) that were previously marked `done` back to `pending`. This ensures newly-available Friday contracts (whose `first_appearance` just passed) are re-checked automatically without manual intervention.

### Rate limiting

The API has **no rate-limit headers**. `RateLimiter` uses a sliding deque of call timestamps. Official limit: 10,000 units per 5-minute window; OHLC = 3 units/call → **3,333 real calls per 5-min window per IP**. Rate limits are **per-IP** (shared across all accounts on the same machine). Safe cap currently set to 2,000 calls/window. With `ACTIVE_ACCOUNTS = 1`, this is effectively 2,000 calls/5-min.

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
