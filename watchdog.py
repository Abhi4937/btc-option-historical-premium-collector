"""
watchdog.py — Background health watchdog for btc-collector.

Runs continuously, checks every 15 minutes for:
  - Stale in_progress months (no symbol progress since last check)
  - DB health (locked, disk I/O errors)
  - Collector process alive or dead
  - Recent log errors (429 rate limits, disk I/O, failures)
  - Disk space on WSL data partition

Prints detailed report each cycle.
Sends one-line summary to Telegram.

Usage:
  nohup python3 watchdog.py >> ~/btc-data/logs/watchdog.log 2>&1 &
"""

import sqlite3
import os
import sys
import json
import time
import shutil
import subprocess
import requests
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import MANIFEST_DB, REGISTRY_DB, LOGS_DIR

BOT_TOKEN      = "8675658345:AAEn9yIj3uZdu3TkPnXY2P1ubHgGMQc4COk"
CHAT_ID        = "1088952172"
CHECK_INTERVAL = 900          # 15 minutes
STATE_FILE     = os.path.join(LOGS_DIR, "watchdog_state.json")
COLLECTOR_LOG  = os.path.join(LOGS_DIR, "collector.log")

# Thresholds
DISK_WARN_PCT       = 85.0    # warn if disk > 85% full
STALE_THRESHOLD     = 1       # checks with no progress = stale (1 check = 15 min)
LOG_SCAN_LINES      = 2000    # how many recent log lines to scan


# ── Telegram ──────────────────────────────────────────────────────────────────

def send(msg: str):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"[warn] Telegram send failed: {e}")


# ── State persistence ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"progress": {}, "stale_counts": {}}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# ── Checks ────────────────────────────────────────────────────────────────────

def check_db_health() -> list[str]:
    issues = []
    for name, path in [("manifest", MANIFEST_DB), ("registry", REGISTRY_DB)]:
        if not os.path.exists(path):
            issues.append(f"DB {name}: FILE MISSING at {path}")
            continue
        try:
            db = sqlite3.connect(path, timeout=5)
            db.execute("SELECT 1").fetchone()
            db.close()
        except Exception as e:
            issues.append(f"DB {name}: {e}")
    return issues


def check_stale_months(prev_state: dict) -> tuple[list, dict]:
    """
    Returns (stale_list, current_progress_snapshot).
    stale_list: [(month, account, completed, stale_cycles)]
    """
    try:
        reg = sqlite3.connect(REGISTRY_DB, timeout=10)
        man = sqlite3.connect(MANIFEST_DB, timeout=10)

        in_progress = man.execute(
            "SELECT expiry_month, claimed_by FROM manifest WHERE status='in_progress'"
        ).fetchall()
        man.close()

        current = {}
        for month, _ in in_progress:
            row = reg.execute("""
                SELECT SUM(CASE WHEN status IN ('done','empty','not_listed') THEN 1 ELSE 0 END)
                FROM symbols WHERE substr(expiry_date,1,7)=?
            """, (month,)).fetchone()
            current[month] = row[0] or 0

        reg.close()
    except Exception as e:
        return [f"DB query error: {e}"], {}

    stale = []
    prev_progress   = prev_state.get("progress", {})
    prev_stale_cnt  = prev_state.get("stale_counts", {})

    for month, account in in_progress:
        prev_val = prev_progress.get(month)
        curr_val = current.get(month, 0)
        if prev_val is not None and curr_val == prev_val:
            cycles = prev_stale_cnt.get(month, 0) + 1
            stale.append((month, account or "—", curr_val, cycles))
        else:
            pass  # reset handled in new stale_counts below

    return stale, current


def build_new_stale_counts(stale_list: list, current: dict, prev_state: dict) -> dict:
    prev_stale = prev_state.get("stale_counts", {})
    new_stale  = {}
    stale_months = {s[0] for s in stale_list}
    for month in current:
        if month in stale_months:
            new_stale[month] = prev_stale.get(month, 0) + 1
        else:
            new_stale[month] = 0
    return new_stale


def check_log_errors() -> dict:
    if not os.path.exists(COLLECTOR_LOG):
        return {"missing": True}

    with open(COLLECTOR_LOG, "r", errors="replace") as f:
        lines = f.readlines()

    recent = lines[-LOG_SCAN_LINES:]
    errors_429  = 0
    disk_errors = 0
    gen_errors  = []
    failed_syms = 0

    for line in recent:
        if "429" in line:
            errors_429 += 1
        if "disk I/O" in line.lower() or "disk i/o" in line:
            disk_errors += 1
        if "FAILED" in line:
            failed_syms += 1
        if "ERROR" in line and "429" not in line and "disk I/O" not in line.lower():
            gen_errors.append(line.strip())

    return {
        "rate_429":    errors_429,
        "disk_io":     disk_errors,
        "failed_syms": failed_syms,
        "gen_errors":  gen_errors[-5:],
        "total_lines": len(lines),
    }


def check_disk_space() -> tuple[float | None, float | None]:
    try:
        usage = shutil.disk_usage(os.path.dirname(MANIFEST_DB))
        pct      = usage.used / usage.total * 100
        free_gb  = usage.free / (1024 ** 3)
        return pct, free_gb
    except Exception:
        return None, None


def check_collector_running() -> bool | None:
    try:
        result = subprocess.run(
            ["pgrep", "-f", "main.py"],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except Exception:
        return None


def get_manifest_summary() -> dict:
    try:
        db   = sqlite3.connect(MANIFEST_DB, timeout=5)
        rows = db.execute("SELECT status, COUNT(*) FROM manifest GROUP BY status").fetchall()
        db.close()
        return dict(rows)
    except Exception:
        return {}


# ── Report builder ────────────────────────────────────────────────────────────

def run_check(prev_state: dict) -> tuple[dict, str, str]:
    """
    Returns (new_state, detailed_report, telegram_oneliner).
    """
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    issues = []

    # 1. Collector process
    running = check_collector_running()
    if running is False:
        issues.append("COLLECTOR PROCESS NOT RUNNING")

    # 2. DB health
    db_issues = check_db_health()
    issues.extend(db_issues)

    # 3. Stale months
    stale_list, current_progress = check_stale_months(prev_state)
    for month, acct, completed, cycles in stale_list:
        issues.append(f"STALE: {month} ({acct}) no progress for {cycles} check(s) [{completed} done]")

    # 4. Log errors
    log_data = check_log_errors()
    if log_data.get("missing"):
        issues.append("collector.log MISSING")
    if log_data.get("disk_io", 0) > 0:
        issues.append(f"DISK I/O ERRORS in log: {log_data['disk_io']} occurrences")
    if log_data.get("rate_429", 0) > 10:
        issues.append(f"RATE LIMIT: {log_data['rate_429']} x 429 in last {LOG_SCAN_LINES} lines")

    # 5. Disk space
    disk_pct, free_gb = check_disk_space()
    if disk_pct is not None and disk_pct > DISK_WARN_PCT:
        issues.append(f"DISK FULL: {disk_pct:.1f}% used ({free_gb:.1f} GB free)")

    # ── Manifest summary ──────────────────────────────────────────────────────
    manifest = get_manifest_summary()
    done_m    = manifest.get("done", 0)
    inprog_m  = manifest.get("in_progress", 0)
    pending_m = manifest.get("pending", 0)
    failed_m  = manifest.get("failed", 0)
    total_m   = done_m + inprog_m + pending_m + failed_m

    # ── Build detailed report ─────────────────────────────────────────────────
    sep = "-" * 60
    report_lines = [
        sep,
        f"WATCHDOG REPORT — {now}",
        sep,
        f"Collector : {'RUNNING ✓' if running else 'STOPPED ✗' if running is False else 'UNKNOWN'}",
        f"Manifest  : {done_m}/{total_m} done | {inprog_m} active | {pending_m} pending | {failed_m} failed",
        f"Disk      : {f'{disk_pct:.1f}% used, {free_gb:.1f} GB free' if disk_pct else 'N/A'}",
        "",
        "LOG STATS (last 2000 lines):",
        f"  429 rate-limit hits : {log_data.get('rate_429', 0)}",
        f"  Disk I/O errors     : {log_data.get('disk_io', 0)}",
        f"  Symbol failures     : {log_data.get('failed_syms', 0)}",
        f"  Other errors        : {len(log_data.get('gen_errors', []))}",
    ]

    if log_data.get("gen_errors"):
        report_lines.append("")
        report_lines.append("RECENT ERRORS:")
        for e in log_data["gen_errors"]:
            report_lines.append(f"  {e[:120]}")

    if stale_list:
        report_lines.append("")
        report_lines.append("STALE MONTHS (no progress since last check):")
        for month, acct, completed, cycles in stale_list:
            report_lines.append(f"  {month}  account={acct}  completed={completed}  stale_cycles={cycles}")
    else:
        report_lines.append("")
        report_lines.append("Active months: all making progress ✓")
        for month, val in sorted(current_progress.items()):
            prev_val = prev_state.get("progress", {}).get(month)
            delta = f"+{val - prev_val}" if prev_val is not None else "first check"
            report_lines.append(f"  {month}  completed={val}  ({delta})")

    if db_issues:
        report_lines.append("")
        report_lines.append("DB ISSUES:")
        for i in db_issues:
            report_lines.append(f"  {i}")

    if issues:
        report_lines.append("")
        report_lines.append(f"⚠  {len(issues)} ISSUE(S) FOUND")
    else:
        report_lines.append("")
        report_lines.append("✓  All checks passed")

    report_lines.append(sep)
    detailed = "\n".join(report_lines)

    # ── Telegram one-liner ────────────────────────────────────────────────────
    if issues:
        short_issues = "; ".join(issues[:3])
        if len(issues) > 3:
            short_issues += f" (+{len(issues)-3} more)"
        telegram_line = f"⚠️ <b>Watchdog {now}</b> — {len(issues)} issue(s): {short_issues}"
    else:
        telegram_line = (
            f"✅ <b>Watchdog {now}</b> — All OK | "
            f"{done_m}/{total_m} months done | "
            f"{inprog_m} active | "
            f"disk {f'{disk_pct:.1f}%' if disk_pct else 'N/A'}"
        )

    # ── New state ─────────────────────────────────────────────────────────────
    new_state = {
        "progress":     current_progress,
        "stale_counts": build_new_stale_counts(stale_list, current_progress, prev_state),
        "last_check":   now,
    }

    return new_state, detailed, telegram_line


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    print(f"[watchdog] Started at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"[watchdog] Check interval: {CHECK_INTERVAL // 60} minutes")
    print(f"[watchdog] State file: {STATE_FILE}")
    sys.stdout.flush()

    while True:
        state = load_state()

        try:
            new_state, report, tg_line = run_check(state)
        except Exception as e:
            report   = f"[watchdog] INTERNAL ERROR: {e}"
            tg_line  = f"⚠️ <b>Watchdog ERROR</b> — internal exception: {e}"
            new_state = state

        print(report)
        sys.stdout.flush()

        save_state(new_state)
        send(tg_line)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
