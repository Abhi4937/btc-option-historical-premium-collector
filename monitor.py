"""
Monitor script — checks collector health and sends Telegram update.
Run every 15 minutes via cron or manually.
"""

import sqlite3
import os
import requests
from datetime import datetime, timezone

BOT_TOKEN = "8675658345:AAEn9yIj3uZdu3TkPnXY2P1ubHgGMQc4COk"
CHAT_ID   = "1088952172"
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))


def send(msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})


def get_manifest():
    db = sqlite3.connect(os.path.join(BASE_DIR, "db", "manifest.db"))
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
    db = sqlite3.connect(os.path.join(BASE_DIR, "db", "registry.db"))
    rows = db.execute("SELECT status, COUNT(*), SUM(total_candles) FROM symbols GROUP BY status").fetchall()
    spot = db.execute("SELECT COUNT(*) FROM spot_progress WHERE status='done'").fetchone()[0]
    db.close()
    return {r[0]: {"count": r[1], "candles": r[2] or 0} for r in rows}, spot


def get_spot_size():
    path = os.path.join(BASE_DIR, "data", "spot", "BTCUSD_1min.parquet")
    if not os.path.exists(path):
        return None
    size_mb = os.path.getsize(path) / (1024 * 1024)
    return size_mb


def get_recent_errors():
    log_path = os.path.join(BASE_DIR, "logs", "collector.log")
    if not os.path.exists(log_path):
        return []
    errors = []
    with open(log_path, "r") as f:
        lines = f.readlines()
    for line in lines[-500:]:
        if "ERROR" in line or "FAILED" in line or "429" in line:
            errors.append(line.strip())
    return errors[-5:]  # last 5 errors


def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    manifest, in_prog, failed = get_manifest()
    registry, spot_done = get_registry()
    spot_mb = get_spot_size()
    errors = get_recent_errors()

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
    sym_inprog  = registry.get("in_progress", {}).get("count", 0)
    total_candles = sum(v["candles"] for v in registry.values())

    lines = [
        f"<b>BTC Collector Health — {now}</b>",
        "",
        "<b>Months</b>",
        f"  Done: {done_m}/{total_m}  |  In Progress: {inprog_m}  |  Pending: {pending_m}  |  Failed: {failed_m}",
    ]

    if in_prog:
        lines.append("<b>Active months:</b>")
        for month, acct in in_prog:
            lines.append(f"  {month} → {acct}")

    lines += [
        "",
        "<b>Symbols</b>",
        f"  Done: {sym_done}  |  Empty/NL: {sym_empty+sym_nl}  |  Pending: {sym_pending}  |  Failed: {sym_fail}  |  InProg: {sym_inprog}",
        f"  Total candles: {total_candles:,}",
        "",
        "<b>Spot</b>",
        f"  Months with spot: {spot_done}  |  Parquet: {f'{spot_mb:.1f} MB' if spot_mb else 'missing'}",
    ]

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

    send("\n".join(lines))
    print("Sent to Telegram.")


if __name__ == "__main__":
    main()
