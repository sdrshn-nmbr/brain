from __future__ import annotations

import json
import math
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ATTRIBUTION_WINDOW = timedelta(minutes=15)


def utc_now() -> datetime:
    return datetime.now(UTC)


class Usage:
    """Searches and the sessions read after them.

    A read credits the most recent search, within fifteen minutes, that returned the session. Crediting every recent
    search would let one read of a popular session label a whole batch of searches as useful.
    """

    def __init__(self, data_dir: Path) -> None:
        self.db = sqlite3.connect(data_dir / "usage.sqlite", check_same_thread=False)
        self._lock = threading.Lock()
        self.db.executescript(
            """
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY,
                at TEXT NOT NULL,
                query TEXT NOT NULL,
                filters TEXT NOT NULL,
                session_ids TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS useful (
                search_id INTEGER NOT NULL REFERENCES searches(id),
                session_id INTEGER NOT NULL,
                at TEXT NOT NULL,
                PRIMARY KEY (search_id, session_id)
            );
            """
        )
        self._priors: dict[int, float] | None = None

    def record_search(self, query: str, filters: dict[str, Any], session_ids: list[int]) -> None:
        with self._lock:
            self.db.execute(
                "INSERT INTO searches(at, query, filters, session_ids) VALUES (?, ?, ?, ?)",
                (utc_now().isoformat(), query, json.dumps(filters, sort_keys=True), json.dumps(session_ids)),
            )
            self.db.commit()

    def _credited_search(self, session_id: int, at: datetime) -> int | None:
        rows = self.db.execute(
            "SELECT id, session_ids FROM searches WHERE at >= ? AND at <= ? ORDER BY at DESC, id DESC",
            ((at - ATTRIBUTION_WINDOW).isoformat(), at.isoformat()),
        ).fetchall()
        return next((search_id for search_id, returned in rows if session_id in json.loads(returned)), None)

    def record_read(self, session_id: int) -> None:
        now = utc_now()
        with self._lock:
            search_id = self._credited_search(session_id, now)
            if search_id is None:
                return
            self.db.execute(
                "INSERT OR IGNORE INTO useful(search_id, session_id, at) VALUES (?, ?, ?)",
                (search_id, session_id, now.isoformat()),
            )
            self.db.commit()
            self._priors = None

    def relabel(self, reads: Iterable[tuple[datetime, int]]) -> dict[str, int]:
        """Rebuild useful results from logged reads with the current attribution rule."""
        with self._lock:
            previous = self.db.execute("SELECT count(*) FROM useful").fetchone()[0]
            credited = 0
            with self.db:
                self.db.execute("DELETE FROM useful")
                for at, session_id in reads:
                    search_id = self._credited_search(session_id, at)
                    if search_id is not None:
                        credited += self.db.execute(
                            "INSERT OR IGNORE INTO useful(search_id, session_id, at) VALUES (?, ?, ?)",
                            (search_id, session_id, at.isoformat()),
                        ).rowcount
            self._priors = None
            return {"previousUseful": previous, "useful": credited}

    def priors(self, before: str | None = None, excluding: frozenset[int] = frozenset()) -> dict[int, float]:
        """Log-scaled counts of how often each session proved useful, optionally only from earlier searches."""
        if before is None and not excluding:
            with self._lock:
                if self._priors is None:
                    rows = self.db.execute("SELECT session_id, count(*) FROM useful GROUP BY session_id").fetchall()
                    self._priors = {session_id: math.log1p(count) for session_id, count in rows}
                return self._priors
        with self._lock:
            rows = self.db.execute(
                "SELECT search_id, session_id FROM useful WHERE at < ?", (before or "9999",)
            ).fetchall()
        counts: dict[int, int] = defaultdict(int)
        for search_id, session_id in rows:
            if search_id not in excluding:
                counts[session_id] += 1
        return {session_id: math.log1p(count) for session_id, count in counts.items()}

    def logged_searches(self) -> list[dict[str, Any]]:
        """Every distinct query and filter combination, with the sessions reads later credited to it."""
        with self._lock:
            searches = self.db.execute("SELECT id, at, query, filters FROM searches ORDER BY at").fetchall()
            useful = self.db.execute("SELECT search_id, session_id FROM useful").fetchall()
        credited: dict[int, set[int]] = defaultdict(set)
        for search_id, session_id in useful:
            credited[search_id].add(session_id)
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for search_id, at, query, filters in searches:
            options = {key: value for key, value in json.loads(filters).items() if value not in (None, False)}
            key = (query, json.dumps(options, sort_keys=True))
            entry = merged.setdefault(
                key, {"query": query, "filters": options, "at": at, "searchIds": set(), "targets": set()}
            )
            entry["searchIds"].add(search_id)
            entry["targets"] |= credited.get(search_id, set())
        return list(merged.values())

    def close(self) -> None:
        self.db.close()
