"""
Rich live terminal progress display.
Refreshes every 2 seconds.
Shows per-account status + overall manifest progress.
"""

import asyncio
import time
from datetime import datetime

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    BarColumn, TextColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
)
from rich import box

from config import ACCOUNT_NAMES, ACTIVE_ACCOUNTS


def _fmt_dur(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


class ProgressDisplay:

    def __init__(self):
        self.console               = Console()
        self._account_states: dict[str, dict] = {}
        self._manifest_counts: dict[str, int] = {}
        self._start_time           = time.monotonic()
        self._total_months         = 0
        self._lock                 = asyncio.Lock()

    def update_account(self, account_name: str, state_dict: dict):
        existing = self._account_states.get(account_name, {})
        self._account_states[account_name] = {
            **existing,
            **state_dict,
            "_updated": time.monotonic(),
        }

    def update_manifest(self, counts: dict, total: int):
        self._manifest_counts = counts
        self._total_months    = total

    def _render(self) -> Panel:
        elapsed = time.monotonic() - self._start_time

        # ── Session symbol progress ───────────────────────────────────────────
        done    = self._manifest_counts.get("done",        0)
        pending = self._manifest_counts.get("pending",     0)
        running = self._manifest_counts.get("in_progress", 0)
        failed  = self._manifest_counts.get("failed",      0)

        # Both come from worker counters — always in sync, no DB lag
        session_syms_done = sum(
            s.get("session_symbols_done", 0) for s in self._account_states.values()
        )
        total_syms = max(sum(
            s.get("session_total_symbols", 0) for s in self._account_states.values()
        ), 1)
        pct = min(session_syms_done / total_syms, 1.0)

        bar_w = 30
        filled = int(bar_w * pct)
        bar = "█" * filled + "░" * (bar_w - filled)

        if session_syms_done > 0 and pct < 1.0:
            eta_s = elapsed / pct * (1 - pct)
            eta   = _fmt_dur(eta_s)
        else:
            eta = "--:--:--"

        total_str = f"{total_syms:,}" if total_syms > 1 else "?"
        header = (
            f"Symbols: [bold green]{session_syms_done:,}[/bold green]/{total_str} "
            f"[{bar}] [cyan]{pct*100:.1f}%[/cyan] | "
            f"Elapsed: [yellow]{_fmt_dur(elapsed)}[/yellow] | "
            f"ETA: [magenta]{eta}[/magenta]"
        )
        if failed:
            header += f"  [red]⚠ {failed} failed[/red]"

        # ── Per-account table ─────────────────────────────────────────────────
        tbl = Table(box=box.SIMPLE_HEAD, show_header=True, expand=True)
        tbl.add_column("Account",   style="bold cyan", width=14)
        tbl.add_column("Month",     width=10)
        tbl.add_column("Expiry",    width=12)
        tbl.add_column("Exp#",      width=6,  justify="right")
        tbl.add_column("State",     width=18)
        tbl.add_column("Calls",     width=8,  justify="right")
        tbl.add_column("Symbols",   width=12, justify="right")

        for name in ACCOUNT_NAMES[:ACTIVE_ACCOUNTS]:
            s = self._account_states.get(name, {})
            state    = s.get("state",    "waiting")
            month    = s.get("month",    "—")
            expiry   = s.get("expiry",   "")
            expiries = s.get("expiries", "")
            calls    = s.get("calls",    0)
            strikes  = s.get("strikes",  "")
            error    = s.get("error",    "")

            if state == "done":
                state_str = "[green]done ✅[/green]"
            elif state == "failed":
                state_str = f"[red]failed ❌[/red]"
            elif state in ("fetching", "working"):
                state_str = "[yellow]fetching...[/yellow]"
            elif state == "fetching_spot":
                state_str = "[blue]spot data...[/blue]"
            elif state == "idle":
                state_str = "[dim]idle[/dim]"
            else:
                state_str = f"[dim]{state}[/dim]"

            tbl.add_row(
                name,
                str(month) if month else "—",
                expiry or "—",
                expiries or "—",
                state_str,
                str(calls),
                strikes if strikes else (f"[red]{error[:12]}[/red]" if error else ""),
            )

        from rich.columns import Columns
        from rich.text import Text
        content = f"{header}\n"
        return Panel(
            tbl,
            title=content,
            subtitle=f"[dim]pending={pending}  running={running}  done={done}  failed={failed}[/dim]",
            border_style="cyan",
        )

    async def run(self, collection_coroutine, refresh_interval: float = 2.0):
        """
        Run the collection coroutine while displaying live progress.
        Periodically polls manifest for overall counts.
        """
        from manifest import get_progress_counts


        async def poll_manifest():
            while True:
                await asyncio.sleep(refresh_interval)
                try:
                    counts = await get_progress_counts()
                    total  = sum(counts.values())
                    self.update_manifest(counts, total)
                except Exception:
                    pass

        with Live(self._render(), refresh_per_second=0.5, console=self.console) as live:
            async def update_loop():
                while True:
                    await asyncio.sleep(refresh_interval)
                    live.update(self._render())

            await asyncio.gather(
                collection_coroutine,
                poll_manifest(),
                update_loop(),
            )
