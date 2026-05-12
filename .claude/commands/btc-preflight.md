---
description: Run mandatory pre-flight checks from btc-collector/CLAUDE.md
---

Run the full pre-flight check sequence from `btc-collector/CLAUDE.md` "MANDATORY: Pre-flight check after every code change". Run as parallel sub-agents where the checks are independent:

1. **Syntax** — `python3 -c "import ast; ..."` for all 8 modules.
2. **Imports** — `python3 -c "import worker, progress, collector, monitor, manifest, registry"`.
3. **Stale references** — grep for any symbol the user names (or, if none named, recent renames inferred from `git diff HEAD~1`).
4. **Worker→Progress key alignment** — verify every key passed in `worker._update_status()` exists in `progress.py s.get(...)` calls.
5. **Attribute check** — every `self._<x>` read in `worker.py` must be assigned in `__init__`.
6. **Dry render** — `python3 -c "from progress import ProgressDisplay; ProgressDisplay()._render()"`.

Report PASS/FAIL per step. If any FAIL, do NOT proceed; report the error verbatim.

If the user asked for this AS PART OF declaring code "ready to run", treat all 6 as a hard gate.
