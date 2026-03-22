"""
Monitor script — checks collector health and sends Telegram update.
Run every 15 minutes via cron or manually.
"""

import sqlite3
import os
import sys
import json
import requests
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

# Add project dir to path so config is importable from anywhere
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import MANIFEST_DB, REGISTRY_DB, SPOT_PARQUET, OPTIONS_DIR, LOGS_DIR

BOT_TOKEN = "8675658345:AAEn9yIj3uZdu3TkPnXY2P1ubHgGMQc4COk"
CHAT_ID   = "1088952172"


def connect_readonly(path: str) -> sqlite3.Connection:
    # Monitor queries only; immutable avoids journal/lock writes on shared DB files.
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def send(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})


def get_manifest():
    db = connect_readonly(MANIFEST_DB)
    rows = db.execute("SELECT status, COUNT(*) FROM manifest GROUP BY status").fetchall()
    in_prog = db.execute(
        "SELECT expiry_month, claimed_by FROM manifest WHERE status='in_progress' ORDER BY expiry_month"
    ).fetchall()
    failed = db.execute(
        "SELECT expiry_month, error_message FROM manifest WHERE status='failed'"
    ).fetchall()
    db.close()
    return dict(rows), in_prog, failed


def get_registry():
    db = connect_readonly(REGISTRY_DB)
    rows = db.execute("SELECT status, COUNT(*), SUM(total_candles) FROM symbols GROUP BY status").fetchall()
    spot = db.execute("SELECT COUNT(*) FROM spot_progress WHERE status='done'").fetchone()[0]
    db.close()
    return {r[0]: {"count": r[1], "candles": r[2] or 0} for r in rows}, spot


def get_month_breakdown():
    import calendar
    reg = connect_readonly(REGISTRY_DB)
    man = connect_readonly(MANIFEST_DB)

    months = man.execute(
        "SELECT expiry_month, status, claimed_by FROM manifest ORDER BY expiry_month"
    ).fetchall()

    result = []
    for month, mstatus, claimed_by in months:
        year, mo = int(month[:4]), int(month[5:7])
        expected = calendar.monthrange(year, mo)[1]
        rows = reg.execute('''
            SELECT
                COUNT(DISTINCT expiry_date),
                SUM(CASE WHEN status="done"    THEN 1 ELSE 0 END),
                SUM(CASE WHEN status="empty"   THEN 1 ELSE 0 END),
                SUM(CASE WHEN status="pending" THEN 1 ELSE 0 END)
            FROM symbols WHERE expiry_date LIKE ?
        ''', (month + '%',)).fetchone()
        expiries, done, empty, pending = rows[0] or 0, rows[1] or 0, rows[2] or 0, rows[3] or 0
        total = done + empty + pending
        pct = (done + empty) / total * 100 if total else 0
        result.append((month, mstatus, claimed_by, expiries, expected, done, empty, pending, pct))

    reg.close()
    man.close()
    return result


def get_active_worker_stats():
    """
    For each in-progress month: symbol counts + parquet expiry coverage.
    Returns list of dicts.
    """
    options_dir = OPTIONS_DIR
    reg = connect_readonly(REGISTRY_DB)
    man = connect_readonly(MANIFEST_DB)

    months = man.execute(
        "SELECT expiry_month, claimed_by FROM manifest WHERE status='in_progress' ORDER BY expiry_month"
    ).fetchall()
    man.close()

    result = []
    for expiry_month, claimed_by in months:
        # Symbol counts
        rows = reg.execute("""
            SELECT status, COUNT(*) FROM symbols
            WHERE substr(expiry_date,1,7)=?
            GROUP BY status
        """, (expiry_month,)).fetchall()
        counts  = {s: c for s, c in rows}
        done    = counts.get("done", 0)
        empty   = counts.get("empty", 0)
        pending = counts.get("pending", 0)
        failed  = counts.get("failed", 0)
        total   = done + empty + pending + failed
        pct     = (done + empty) / total * 100 if total else 0.0

        # Parquet expiry coverage
        expected_dates = reg.execute("""
            SELECT DISTINCT expiry_date FROM symbols
            WHERE substr(expiry_date,1,7)=?
            ORDER BY expiry_date
        """, (expiry_month,)).fetchall()
        expected_dates = [r[0] for r in expected_dates]

        saved = []
        for expiry_date in expected_dates:
            folder = os.path.join(options_dir, f"expiry={expiry_date}")
            if os.path.isdir(folder):
                saved.append(expiry_date)

        last_saved = saved[-1] if saved else None

        result.append({
            "month":      expiry_month,
            "account":    claimed_by or "—",
            "done":       done,
            "empty":      empty,
            "pending":    pending,
            "failed":     failed,
            "total":      total,
            "pct":        pct,
            "saved_exp":  len(saved),
            "total_exp":  len(expected_dates),
            "last_saved": last_saved,
        })

    reg.close()
    return result


def get_spot_size():
    if not os.path.exists(SPOT_PARQUET):
        return None
    return os.path.getsize(SPOT_PARQUET) / (1024 * 1024)


def get_recent_errors():
    log_path = os.path.join(LOGS_DIR, "collector.log")
    if not os.path.exists(log_path):
        return []
    errors = []
    with open(log_path, "r") as f:
        lines = f.readlines()
    for line in lines[-500:]:
        if "[httpx]" in line:          # skip httpx request/response noise
            continue
        if "ERROR" in line or "FAILED" in line or "429" in line:
            errors.append(line.strip())
    return errors[-5:]  # last 5 errors


def get_backfill_progress():
    """Read backfill progress state file if it exists."""
    path = os.path.join(LOGS_DIR, "backfill_progress.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def main():
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")

    manifest, in_prog, failed = get_manifest()
    registry, spot_done = get_registry()
    spot_mb = get_spot_size()
    errors = get_recent_errors()
    month_rows = get_month_breakdown()
    active_stats = get_active_worker_stats()
    backfill = get_backfill_progress()

    done_m    = manifest.get("done", 0)
    inprog_m  = manifest.get("in_progress", 0)
    pending_m = manifest.get("pending", 0)
    failed_m  = manifest.get("failed", 0)
    total_m   = done_m + inprog_m + pending_m + failed_m

    sym_done    = registry.get("done", {}).get("count", 0)
    sym_empty   = registry.get("empty", {}).get("count", 0)
    sym_nl      = registry.get("not_listed", {}).get("count", 0)
    sym_pending = registry.get("pending", {}).get("count", 0)
    sym_fail    = registry.get("failed", {}).get("count", 0)
    total_candles = sum(v["candles"] for v in registry.values())

    lines = [
        f"<b>BTC Collector Health — {now}</b>",
        "",
        "<b>Overall: {done_m}/{total_m} months done</b>".format(done_m=done_m, total_m=total_m),
        f"  In Progress: {inprog_m}  |  Pending: {pending_m}  |  Failed: {failed_m}",
        "",
    ]

    if backfill:
        status    = backfill.get("status", "unknown")
        done_exp  = backfill.get("done_expiries", 0)
        total_exp = backfill.get("total_expiries", 0)
        done_sym  = backfill.get("done_symbols", 0)
        total_sym = backfill.get("total_symbols", 0)
        no_data   = backfill.get("no_data_symbols", 0)
        err_sym   = backfill.get("error_symbols", 0)
        current   = backfill.get("current_expiry", "—")
        updated   = backfill.get("last_updated", "—")
        pct       = done_exp / total_exp * 100 if total_exp else 0
        icon      = "✅" if status == "complete" else "⏳"
        lines += [
            f"<b>{icon} Backfill ({status})</b>",
            f"  Expiries : {done_exp}/{total_exp}  ({pct:.1f}%)",
            f"  Symbols  : {done_sym:,} updated  |  {no_data} no-data  |  {err_sym} errors",
        ]
        if status == "running":
            lines.append(f"  Current  : {current}")
        lines.append(f"  Updated  : {updated}")
        lines.append("")

    # Show symbols/spot/month breakdown only when collection is active
    active_months = [r for r in month_rows if r[1] != "done" and r[1] != "pending"]
    if inprog_m > 0 or pending_m > 0:
        lines += [
            "",
            "<b>Symbols</b>",
            f"  Done: {sym_done:,}  |  Empty: {sym_empty+sym_nl:,}  |  Pending: {sym_pending:,}  |  Failed: {sym_fail}",
            f"  Total candles: {total_candles:,}",
            "",
            "<b>Spot</b>",
            f"  Months: {spot_done}  |  Parquet: {f'{spot_mb:.1f} MB' if spot_mb else 'missing'}",
            "",
            "<b>Month Breakdown</b>",
            "<code>Month    Exp    Done   Empty  Pend    Pct</code>",
        ]
        for month, mstatus, claimed_by, expiries, expected, done, empty, pending, pct in month_rows:
            if mstatus == "pending":
                continue
            if mstatus == "done":
                check = "✓" if pending == 0 and expiries == expected else f"⚠{expiries}/{expected}"
                label = f"done {check}"
            else:
                owner = f"({claimed_by})" if claimed_by else ""
                label = f"active {owner}"
            lines.append(
                f"<code>{month}  {expiries:2}/{expected:<2}  {done:5}  {empty:5}  {pending:5}  {pct:5.1f}%  {label}</code>"
            )
    else:
        lines += [
            "",
            f"  Symbols: {sym_done:,} done  |  {sym_empty+sym_nl:,} empty  |  {total_candles:,} candles",
            f"  Spot: {spot_done} months  |  {f'{spot_mb:.1f} MB' if spot_mb else 'missing'}",
        ]

    if active_stats:
        lines.append("")
        lines.append("<b>In-Progress Months</b>")
        lines.append("<code>Account       Month    Syms%   Exp saved  Last expiry</code>")
        for w in active_stats:
            lines.append(
                f"<code>{w['account']:<14}{w['month']}  {w['pct']:5.1f}%  "
                f"{w['saved_exp']:2}/{w['total_exp']:<2}  "
                f"{w['last_saved'] or '—'}</code>"
            )
        lines.append("")
        lines.append("<code>Account       Month     Done   Empty  Pend   Failed</code>")
        for w in active_stats:
            lines.append(
                f"<code>{w['account']:<14}{w['month']}  {w['done']:5}  {w['empty']:5}  "
                f"{w['pending']:5}  {w['failed']:5}</code>"
            )

    if failed:
        lines.append("")
        lines.append("<b>Failed months:</b>")
        for month, err in failed:
            lines.append(f"  {month}: {(err or '')[:80]}")

    if errors:
        lines.append("")
        lines.append("<b>Recent errors:</b>")
        for e in errors:
            lines.append(f"  {e[:100]}")

    msg = "\n".join(lines)
    # Strip HTML tags for terminal display
    import re
    plain = re.sub(r"<[^>]+>", "", msg)

    if "--print" in sys.argv:
        print(plain)
    else:
        send(msg)
        print("Sent to Telegram.")
        print()
        print(plain)


if __name__ == "__main__":
    main()
