from __future__ import annotations

import json
import math
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

FOLLOW_UP_WINDOW = timedelta(minutes=30)


def utc_now() -> datetime:
    return datetime.now(UTC)


class Usage:
    """Searches and the sessions read after them. A read of a returned session marks that result useful."""

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

    def record_read(self, session_id: int) -> None:
        since = (utc_now() - FOLLOW_UP_WINDOW).isoformat()
        with self._lock:
            searches = self.db.execute("SELECT id, session_ids FROM searches WHERE at >= ?", (since,)).fetchall()
            hits = [search_id for search_id, returned in searches if session_id in json.loads(returned)]
            self.db.executemany(
                "INSERT OR IGNORE INTO useful(search_id, session_id, at) VALUES (?, ?, ?)",
                ((search_id, session_id, utc_now().isoformat()) for search_id in hits),
            )
            self.db.commit()
            if hits:
                self._priors = None

    def priors(self) -> dict[int, float]:
        with self._lock:
            if self._priors is None:
                rows = self.db.execute("SELECT session_id, count(*) FROM useful GROUP BY session_id").fetchall()
                self._priors = {session_id: math.log1p(count) for session_id, count in rows}
            return self._priors

    def evaluate(self, search: Callable[[str, dict[str, Any]], list[int]], k: int = 10) -> dict[str, Any]:
        """Replay searches that led to a read and measure where the useful sessions now rank."""
        with self._lock:
            rows = self.db.execute(
                """SELECT s.id, s.query, s.filters, group_concat(u.session_id) FROM searches s
                   JOIN useful u ON u.search_id = s.id GROUP BY s.id"""
            ).fetchall()
        recall, reciprocal = [], []
        for _search_id, query, filters, useful in rows:
            wanted = {int(value) for value in useful.split(",")}
            ranked = search(query, json.loads(filters))[:k]
            recall.append(len(wanted & set(ranked)) / len(wanted))
            reciprocal.append(next((1 / (rank + 1) for rank, value in enumerate(ranked) if value in wanted), 0.0))
        return {
            "queries": len(rows),
            f"recallAt{k}": round(sum(recall) / len(recall), 4) if recall else None,
            "mrr": round(sum(reciprocal) / len(reciprocal), 4) if reciprocal else None,
        }

    def close(self) -> None:
        self.db.close()
