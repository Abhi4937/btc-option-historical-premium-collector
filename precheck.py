"""Pre-resume health check — run before starting collector."""
import sqlite3, os, sys, shutil, subprocess
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import MANIFEST_DB, REGISTRY_DB, LOGS_DIR, OPTIONS_DIR, DB_DIR, DATA_DIR, LOG_FILE, SPOT_PARQUET
from datetime import datetime, timezone, timedelta
from dotenv import dotenv_values

IST = timezone(timedelta(hours=5, minutes=30))
ok = True

def check(label, passed, detail=""):
    global ok
    status = "OK  " if passed else "FAIL"
    if not passed:
        ok = False
    print(f"  [{status}] {label}{': ' + detail if detail else ''}")

print("=" * 55)
print("PRE-RESUME CHECK")
print("=" * 55)

# 1. Paths
print("\n1. PATHS (should all be WSL native /home/abhis/...)")
for name, path in [("DB_DIR", DB_DIR), ("DATA_DIR", DATA_DIR),
                   ("OPTIONS_DIR", OPTIONS_DIR), ("LOGS_DIR", LOGS_DIR),
                   ("MANIFEST_DB", MANIFEST_DB), ("REGISTRY_DB", REGISTRY_DB),
                   ("LOG_FILE", LOG_FILE)]:
    exists = os.path.exists(path)
    on_wsl = path.startswith("/home/")
    check(name, exists and on_wsl, path if not (exists and on_wsl) else "")

# 2. Disk space
print("\n2. DISK SPACE")
usage = shutil.disk_usage(DB_DIR)
pct = usage.used / usage.total * 100
free_gb = usage.free / (1024**3)
check("Disk space", pct < 85, f"{pct:.1f}% used, {free_gb:.1f} GB free")

# 3. DB health
print("\n3. DB HEALTH")
for name, path in [("manifest.db", MANIFEST_DB), ("registry.db", REGISTRY_DB)]:
    try:
        db = sqlite3.connect(path, timeout=5)
        db.execute("SELECT 1").fetchone()
        db.close()
        check(name, True)
    except Exception as e:
        check(name, False, str(e))

# 4. Manifest state
print("\n4. MANIFEST STATE")
man = sqlite3.connect(MANIFEST_DB)
counts = dict(man.execute("SELECT status, COUNT(*) FROM manifest GROUP BY status").fetchall())
ip = man.execute("SELECT expiry_month, claimed_by FROM manifest WHERE status='in_progress' ORDER BY expiry_month").fetchall()
failed_m = counts.get("failed", 0)
man.close()
d, i, p, f = counts.get("done",0), counts.get("in_progress",0), counts.get("pending",0), failed_m
print(f"    done={d}  in_progress={i}  pending={p}  failed={f}")
check("No failed months", failed_m == 0, f"{failed_m} failed months" if failed_m else "")
check("Stale in_progress (resume will reset)", True,
      f"{i} months: {[r[0] for r in ip]}" if i else "none")

# 5. Registry state
print("\n5. REGISTRY STATE")
reg = sqlite3.connect(REGISTRY_DB)
rc = dict(reg.execute("SELECT status, COUNT(*) FROM symbols GROUP BY status").fetchall())
failed_s = rc.get("failed", 0)
reg.close()
d2, e2, p2 = rc.get("done",0), rc.get("empty",0), rc.get("pending",0)
print(f"    done={d2:,}  empty={e2:,}  pending={p2:,}  failed={failed_s}")
check("No failed symbols", failed_s == 0, f"{failed_s} failed symbols" if failed_s else "")

# 6. API keys
print("\n6. API KEYS")
env = dotenv_values(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
for i in range(1, 6):
    name  = env.get(f"ACCOUNT_{i}_NAME", f"account_{i}")
    key   = env.get(f"ACCOUNT_{i}_KEY", "")
    secret = env.get(f"ACCOUNT_{i}_SECRET", "")
    check(name, bool(key and secret), "KEY+SECRET found" if (key and secret) else "MISSING KEY or SECRET")

# 7. Parquet coverage
print("\n7. DATA FILES")
expiry_dirs = [d for d in os.listdir(OPTIONS_DIR) if d.startswith("expiry=")]
check("Options expiry folders", len(expiry_dirs) > 0, f"{len(expiry_dirs)} folders")
sp_exists = os.path.exists(SPOT_PARQUET)
sp_size = os.path.getsize(SPOT_PARQUET) / (1024*1024) if sp_exists else 0
check("Spot parquet", sp_exists, f"{sp_size:.1f} MB" if sp_exists else "MISSING")

# 8. Process check
print("\n8. PROCESS CHECK")
r1 = subprocess.run(["pgrep", "-fa", "main.py"], capture_output=True, text=True)
r2 = subprocess.run(["pgrep", "-fa", "watchdog.py"], capture_output=True, text=True)
running = r1.returncode == 0
check("main.py NOT running", not running, "ALREADY RUNNING - stop first!" if running else "safe to resume")
check("watchdog.py running", r2.returncode == 0, "not running - start with: nohup python3 watchdog.py >> ~/btc-data/logs/watchdog.log 2>&1 &" if r2.returncode != 0 else "")

# Result
print()
print("=" * 55)
now = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
if ok:
    print(f"RESULT [{now}]: ALL CHECKS PASSED")
    print("  Run: python3 main.py resume")
else:
    print(f"RESULT [{now}]: ISSUES FOUND - fix above before resuming")
print("=" * 55)
