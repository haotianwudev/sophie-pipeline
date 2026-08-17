"""DataFrameStore — shared session state, and the spine of every bulk-producing tool.

An SPX chain is ~18,000 contracts; a SQL result can be thousands of rows. Tools must never dump
bulk data into the model's context. Every bulk-producing tool registers its full result here under
a handle and returns only a compact summary (handle, shape, dtypes, head markdown).

One instance lives per AgentRuntime and is shared across every spawned agent, so a chain pulled by
one specialist is readable by a sibling or the supervisor. Thread-safe because delegate_parallel
writes concurrently.
"""

from __future__ import annotations

import re
import threading

import pandas as pd

_SLUG_RE = re.compile(r"[^a-zA-Z0-9_]+")


class DataFrameStore:
    def __init__(self) -> None:
        self._frames: dict[str, pd.DataFrame] = {}
        self._lock = threading.Lock()
        self._counter = 0

    def put(self, name: str, df: pd.DataFrame) -> str:
        """Registers df under a handle derived from `name`, deduping collisions with a suffix."""
        base = _SLUG_RE.sub("_", name).strip("_") or "df"
        with self._lock:
            handle = base
            if handle in self._frames:
                self._counter += 1
                handle = f"{base}_{self._counter}"
            self._frames[handle] = df
            return handle

    def get(self, name: str) -> pd.DataFrame:
        with self._lock:
            if name not in self._frames:
                raise KeyError(f"No stored DataFrame named '{name}'. Known handles: {list(self._frames)}")
            return self._frames[name]

    def list(self) -> list[tuple[str, tuple[int, int]]]:
        with self._lock:
            return [(name, df.shape) for name, df in self._frames.items()]

    def preview(self, name: str, n: int = 10) -> str:
        df = self.get(name)
        dtypes = ", ".join(f"{c}: {t}" for c, t in df.dtypes.astype(str).items())
        head = df.head(n).to_markdown(index=False)
        return f"handle={name} shape={df.shape}\ndtypes: {dtypes}\n\n{head}"

    def context_listing(self) -> str:
        """One line per stored frame, for injection into the system prompt."""
        entries = self.list()
        if not entries:
            return "(no DataFrames stored yet)"
        return "\n".join(f"- {name}: shape={shape}" for name, shape in entries)
