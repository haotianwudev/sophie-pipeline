"""A minimal SQLite-backed LLM cache.

langchain_community.cache.SQLiteCache is the obvious choice but langchain-community is not
installed in this repo (see docs/SOPHIE_AGENT.md's verified-constraints table) — this is the
~40-line stand-in against stdlib sqlite3, registered with langchain_core.globals.set_llm_cache().

Local-only cache file (see AgentConfig.cache_db_path); pickle is fine here since nothing untrusted
is ever written to or read from it.
"""

from __future__ import annotations

import hashlib
import pickle
import sqlite3
import threading
from pathlib import Path

from langchain_core.caches import RETURN_VAL_TYPE, BaseCache


def _key(prompt: str, llm_string: str) -> str:
    return hashlib.sha256(f"{prompt}\x1f{llm_string}".encode("utf-8")).hexdigest()


class SqliteCache(BaseCache):
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS llm_cache (key TEXT PRIMARY KEY, value BLOB NOT NULL)"
        )
        self._conn.commit()

    def lookup(self, prompt: str, llm_string: str) -> RETURN_VAL_TYPE | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM llm_cache WHERE key = ?", (_key(prompt, llm_string),)
            ).fetchone()
        return pickle.loads(row[0]) if row else None

    def update(self, prompt: str, llm_string: str, return_val: RETURN_VAL_TYPE) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO llm_cache (key, value) VALUES (?, ?)",
                (_key(prompt, llm_string), pickle.dumps(return_val)),
            )
            self._conn.commit()

    def clear(self, **kwargs) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM llm_cache")
            self._conn.commit()
