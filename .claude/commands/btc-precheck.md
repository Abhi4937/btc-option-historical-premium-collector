---
description: Run btc-collector pre-resume health check
---

Run `python3 precheck.py` from the btc-collector project root.

If everything passes, print a short ✅ confirmation and stop.

If anything fails (DB health, disk space, missing API keys, stuck process), print the failures and STOP. Do not propose fixes unless asked — the user will decide whether to act.
