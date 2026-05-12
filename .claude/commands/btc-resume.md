---
description: Guarded resume — run precheck first, then resume only if it passes
---

1. Run `python3 precheck.py` from btc-collector root.
2. If precheck FAILS, stop. Print failures. Do not run `resume`.
3. If precheck PASSES, ask the user once to confirm before running `python main.py resume`.
4. On confirm, run `python main.py resume`. Stream output. Do not auto-restart on errors — surface them.

This is a guarded workflow because `resume` mutates the manifest (resets stale `in_progress`). Never run it without precheck and explicit user confirmation.
