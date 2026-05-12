---
description: Run the documented catch-up workflow after a long pause
---

Execute the "Catch-up workflow (after a long pause)" from `btc-collector/CLAUDE.md`. Steps:

1. Ask the user which month(s) need to be reset (`YYYY-MM` format, comma-separated). Do NOT guess.
2. Run `python main.py spot` (incremental — should take 30–60s).
3. Run the manifest reset Python one-liner from CLAUDE.md, with the user-provided months substituted in.
4. Run the optional fetched_to_unix pre-population one-liner (also using user-provided months).
5. Run `python main.py resume` and stream output.

If any step errors, stop and surface the error. Do not auto-recover.
