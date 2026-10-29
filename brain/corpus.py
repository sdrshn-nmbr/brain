from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import zstandard as zstd

from brain.learning import Usage
from brain.provenance import Provenance
from brain.semantic import Semantic

STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "for",
    "to",
    "of",
    "in",
    "on",
    "at",
    "by",
    "with",
    "from",
    "as",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "this",
    "that",
    "it",
    "its",
    "into",
    "about",
    "whether",
    "how",
    "what",
    "when",
    "where",
    "who",
    "why",
}

BodyLoader = Callable[[str], str]
Nearest = Callable[[str, int], list[int]]
Priors = Callable[[], dict[int, float]]
SQLITE_PROGRESS_STEPS = 1_000
RRF_K = 60
SEMANTIC_POOL = 2_000
SEMANTIC_CANDIDATES = 200
SEMANTIC_WEIGHT = 0.8
INJECTED_WEIGHT = 0.3
HIT_WEIGHTS = (1.0, 0.7, 0.5)
PRIOR_WEIGHT = 0.01
ENTRY_COLUMNS = "s.*, s.id AS session_id, e.seq, e.role, e.ts, e.header_line, e.blob_id, b.hash AS blob_hash"
SEARCH_SQL = f"""
    WITH matched_blobs(blob_id, score) AS MATERIALIZED (
        SELECT rowid, bm25(blobs_fts) FROM blobs_fts WHERE blobs_fts MATCH ?
    )
    SELECT {ENTRY_COLUMNS}, m.score
    FROM matched_blobs m
    CROSS JOIN entries AS e INDEXED BY idx_entries_blob
    CROSS JOIN sessions s ON s.id = e.session_id
    CROSS JOIN blobs b ON b.id = m.blob_id
    WHERE e.blob_id = m.blob_id
"""
VECTOR_SQL = f"""
    SELECT {ENTRY_COLUMNS}
    FROM entries AS e INDEXED BY idx_entries_blob
    CROSS JOIN sessions s ON s.id = e.session_id
    CROSS JOIN blobs b ON b.id = e.blob_id
    WHERE e.blob_id IN ({{placeholders}})
"""
SESSIONS_SQL = "SELECT s.*, s.id AS session_id FROM sessions s WHERE s.id IN ({placeholders})"

logger = logging.getLogger("brain.corpus")


class SearchDeadlineExceeded(RuntimeError):
    pass


class SearchCancelled(RuntimeError):
    pass


def ensure_wal(path: Path) -> None:
    if not path.exists():
        return
    with sqlite3.connect(path, timeout=30) as connection:
        connection.execute("PRAGMA busy_timeout = 30000")
        mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if mode.lower() != "wal":
            raise RuntimeError(f"Could not migrate {path} to WAL mode")


def bounded(value: int | None, fallback: int, maximum: int) -> int:
    return max(1, min(fallback if value is None else value, maximum))


def parse_query(query: str) -> tuple[list[str], bool]:
    trimmed = query.strip()
    exact = len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {'"', "'"}
    if exact:
        return [trimmed[1:-1]], True
    return [term for term in trimmed.split() if len(term) > 1 and term.lower() not in STOPWORDS], False


def fts_expression(terms: list[str], exact: bool) -> str:
    if exact:
        return f'"{terms[0].replace(chr(34), chr(34) * 2)}"'
    return " AND ".join(
        f'"{term.replace(chr(34), chr(34) * 2)}"' if re.search(r"[^\w]", term) else term for term in terms
    )


def truncate(value: str, limit: int) -> str:
    return value if len(value) <= limit else f"{value[:limit]}\n…[truncated]"


def row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(zip(row.keys(), row, strict=True))


class Corpus:
    def __init__(
        self,
        data_dir: Path,
        load_body: BodyLoader | None = None,
        nearest: Nearest | None = None,
        priors: Priors | None = None,
        provenance: Provenance | None = None,
    ) -> None:
        self.data_dir = data_dir
        self.index_path = data_dir / "index.sqlite"
        self.objects_path = data_dir / "objects.sqlite"
        self._custom_body_loader = load_body
        self.nearest = nearest
        self.priors = priors
        self.provenance = provenance
        with self._connection(self.index_path) as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(sessions)")}
        if "repository" not in columns:
            raise RuntimeError("Corpus schema is missing sessions.repository; run the scoped ingest migration first")

    def close(self) -> None:
        return None

    @contextmanager
    def _connection(self, path: Path):
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        try:
            yield connection
        finally:
            connection.close()

    def _load_body(self, digest: str, objects_db: sqlite3.Connection | None) -> str:
        if self._custom_body_loader:
            return self._custom_body_loader(digest)
        compressed: bytes | None = None
        if objects_db:
            row = objects_db.execute("SELECT body FROM objects WHERE hash = ?", (digest,)).fetchone()
            compressed = row[0] if row else None
        if compressed is None:
            path = self.data_dir / "objects" / digest[:2] / digest[2:4] / f"{digest}.zst"
            compressed = path.read_bytes()
        return zstd.ZstdDecompressor().decompress(compressed).decode()

    @contextmanager
    def _objects_connection(self):
        if self._custom_body_loader or not self.objects_path.exists():
            yield None
            return
        with self._connection(self.objects_path) as connection:
            yield connection

    def load_bodies(self, digests: list[str]) -> list[str]:
        with self._objects_connection() as objects_db:
            return [self._load_body(digest, objects_db) for digest in digests]

    @staticmethod
    def _install_progress_handler(
        connection: sqlite3.Connection,
        cancel_event: threading.Event | None,
        deadline: float,
    ) -> None:
        connection.set_progress_handler(
            lambda: int((cancel_event is not None and cancel_event.is_set()) or time.perf_counter() >= deadline),
            SQLITE_PROGRESS_STEPS,
        )

    @staticmethod
    def _raise_interruption(
        error: sqlite3.OperationalError,
        cancel_event: threading.Event | None,
        deadline: float,
    ) -> None:
        if "interrupted" not in str(error).lower():
            raise error
        if cancel_event is not None and cancel_event.is_set():
            raise SearchCancelled("Search was cancelled") from error
        if time.perf_counter() >= deadline:
            raise SearchDeadlineExceeded("Search exceeded its SQLite execution deadline") from error
        raise error

    def search(
        self,
        query: str,
        person: str | None = None,
        repository: str | None = None,
        source: str | None = None,
        since: str | None = None,
        roles: list[str] | None = None,
        include_subagents: bool = False,
        limit: int = 10,
        max_hits_per_session: int = 5,
        deadline_seconds: float = 5,
        cancel_event: threading.Event | None = None,
    ) -> list[dict[str, Any]]:
        started = time.perf_counter()
        terms, exact = parse_query(query)
        semantic = None if exact else self.nearest
        if not terms and semantic is None:
            raise ValueError("query has no searchable terms")
        session_filters = ""
        session_params: list[Any] = []
        for column, value in (
            ("s.person", person),
            ("s.repository", repository.lower() if repository else None),
            ("s.source", source),
        ):
            if value:
                session_filters += f" AND {column} = ?"
                session_params.append(value)
        if since:
            session_filters += " AND s.started_at >= ?"
            session_params.append(since)
        if not include_subagents:
            session_filters += " AND s.is_subagent = 0"
        filters, filter_params = session_filters, list(session_params)
        selected_roles = ["user", "assistant"] if roles is None else roles
        if selected_roles:
            filters += f" AND e.role IN ({','.join('?' for _ in selected_roles)})"
            filter_params.extend(selected_roles)
        if cancel_event is not None and cancel_event.is_set():
            raise SearchCancelled("Search was cancelled")
        deadline = started + deadline_seconds
        snapshot = self.provenance.snapshot if self.provenance else None
        injected = snapshot.injected if snapshot else frozenset()
        canonical = self.provenance.canonical if self.provenance else (lambda session_id: session_id)
        vector_started = time.perf_counter()
        nearest = [blob_id for blob_id in semantic(query, SEMANTIC_POOL) if blob_id not in injected] if semantic else []
        vector_ms = (time.perf_counter() - vector_started) * 1_000
        sql_started = time.perf_counter()
        sessions: dict[int, sqlite3.Row] = {}
        try:
            with self._connection(self.index_path) as db:
                self._install_progress_handler(db, cancel_event, deadline)
                try:
                    keyword_rows = (
                        db.execute(SEARCH_SQL + filters, [fts_expression(terms, exact), *filter_params]).fetchall()
                        if terms
                        else []
                    )
                    vector_rows = (
                        db.execute(
                            VECTOR_SQL.format(placeholders=",".join("?" for _ in nearest)) + filters,
                            [*nearest, *filter_params],
                        ).fetchall()
                        if nearest
                        else []
                    )
                    in_scope = {row["blob_id"] for row in vector_rows}
                    semantic_order = [blob_id for blob_id in nearest if blob_id in in_scope][:SEMANTIC_CANDIDATES]
                    chosen = set(semantic_order)
                    located: list[tuple[int, sqlite3.Row]] = []
                    for row in [*keyword_rows, *(row for row in vector_rows if row["blob_id"] in chosen)]:
                        target = canonical(row["session_id"])
                        if target is None:
                            continue
                        if target == row["session_id"]:
                            sessions.setdefault(target, row)
                        located.append((target, row))
                    borrowed = sorted({target for target, _ in located} - sessions.keys())
                    if borrowed:
                        for row in db.execute(
                            SESSIONS_SQL.format(placeholders=",".join("?" for _ in borrowed)) + session_filters,
                            [*borrowed, *session_params],
                        ):
                            sessions[row["session_id"]] = row
                finally:
                    db.set_progress_handler(None, 0)
        except sqlite3.OperationalError as error:
            try:
                self._raise_interruption(error, cancel_event, deadline)
            except (SearchCancelled, SearchDeadlineExceeded) as interruption:
                logger.warning(
                    json.dumps(
                        {
                            "event": "search_interrupted",
                            "reason": type(interruption).__name__,
                            "elapsedMs": round((time.perf_counter() - started) * 1_000, 3),
                        }
                    )
                )
                raise
        sql_ms = (time.perf_counter() - sql_started) * 1_000

        grouping_started = time.perf_counter()
        blob_scores: dict[int, float] = defaultdict(float)
        keyword_order = sorted({row["blob_id"]: row["score"] for row in keyword_rows}.items(), key=lambda item: item[1])
        for rank, (blob_id, _score) in enumerate(keyword_order):
            blob_scores[blob_id] += (INJECTED_WEIGHT if blob_id in injected else 1.0) / (RRF_K + rank + 1)
        for rank, blob_id in enumerate(semantic_order):
            blob_scores[blob_id] += SEMANTIC_WEIGHT / (RRF_K + rank + 1)
        grouped: dict[int, dict[tuple[int, int], sqlite3.Row]] = {}
        for target, row in located:
            if target in sessions:
                grouped.setdefault(target, {}).setdefault((row["session_id"], row["seq"]), row)
        priors = self.priors() if self.priors else {}
        scored = []
        for target, entries in grouped.items():
            hits = sorted(entries.values(), key=lambda hit: blob_scores[hit["blob_id"]], reverse=True)
            score = sum(weight * blob_scores[hit["blob_id"]] for weight, hit in zip(HIT_WEIGHTS, hits, strict=False))
            score += PRIOR_WEIGHT * priors.get(target, 0.0)
            scored.append((score, sessions[target], hits))
        ordered = sorted(
            scored,
            key=lambda item: (item[0], item[1]["started_at"] or ""),
            reverse=True,
        )[: bounded(limit, 10, 50)]
        grouping_ms = (time.perf_counter() - grouping_started) * 1_000

        cas_started = time.perf_counter()
        with self._objects_connection() as objects_db:
            results = [
                {
                    "sessionId": session["session_id"],
                    "person": session["person"],
                    "source": session["source"],
                    "uuid": session["uuid"],
                    "repository": session["repository"],
                    "gitBranch": session["git_branch"],
                    "startedAt": session["started_at"],
                    "cwd": session["cwd"],
                    "entryCount": session["entry_count"],
                    "hitCount": len(hits),
                    "score": round(score, 5),
                    "hits": [
                        {
                            "seq": hit["seq"],
                            "role": hit["role"],
                            "timestamp": hit["ts"],
                            "text": truncate(self._load_body(hit["blob_hash"], objects_db), 4_000),
                            **(
                                {"fromSessionId": hit["session_id"]}
                                if hit["session_id"] != session["session_id"]
                                else {}
                            ),
                        }
                        for hit in hits[: bounded(max_hits_per_session, 5, 20)]
                    ],
                }
                for score, session, hits in ordered
            ]
        cas_ms = (time.perf_counter() - cas_started) * 1_000
        serialization_started = time.perf_counter()
        json.dumps(results, ensure_ascii=False, separators=(",", ":"))
        serialization_ms = (time.perf_counter() - serialization_started) * 1_000
        logger.info(
            json.dumps(
                {
                    "event": "search_profile",
                    "queryTerms": len(terms),
                    "exact": exact,
                    "keywordEntries": len(keyword_rows),
                    "semanticPool": len(nearest),
                    "semanticEntries": sum(1 for row in vector_rows if row["blob_id"] in chosen),
                    "creditedToOriginal": sum(1 for target, row in located if target != row["session_id"]),
                    "matchedSessions": len(grouped),
                    "returnedSessions": len(results),
                    "vectorMs": round(vector_ms, 3),
                    "sqlMs": round(sql_ms, 3),
                    "groupingMs": round(grouping_ms, 3),
                    "casReadMs": round(cas_ms, 3),
                    "serializationMs": round(serialization_ms, 3),
                    "totalMs": round((time.perf_counter() - started) * 1_000, 3),
                }
            )
        )
        return results

    def browse(
        self,
        person: str | None = None,
        repository: str | None = None,
        source: str | None = None,
        since: str | None = None,
        include_subagents: bool = False,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        for column, value in (
            ("person", person),
            ("repository", repository.lower() if repository else None),
            ("source", source),
        ):
            if value:
                clauses.append(f"{column} = ?")
                params.append(value)
        if since:
            clauses.append("started_at >= ?")
            params.append(since)
        if not include_subagents:
            clauses.append("is_subagent = 0")
        params.append(bounded(limit, 20, 100))
        with self._connection(self.index_path) as db:
            rows = db.execute(
                f"""SELECT id, person, source, uuid, repository, git_branch, model, started_at, ended_at,
                           entry_count, char_count, cwd, is_subagent
                    FROM sessions WHERE {" AND ".join(clauses)} ORDER BY started_at DESC LIMIT ?""",
                params,
            ).fetchall()
            return [row_dict(row) for row in rows]

    def read_session(
        self,
        session_id: int | None = None,
        uuid: str | None = None,
        person: str | None = None,
        offset: int = 0,
        limit: int = 50,
        max_chars: int = 50_000,
    ) -> dict[str, Any]:
        if session_id is None and not uuid:
            raise ValueError("sessionId or uuid is required")
        clauses = ["uuid LIKE ?" if session_id is None else "id = ?"]
        params: list[Any] = [f"{uuid}%" if session_id is None else session_id]
        if person:
            clauses.append("person = ?")
            params.append(person)
        with self._connection(self.index_path) as db:
            matches = db.execute(
                f"""SELECT * FROM sessions WHERE {" AND ".join(clauses)}
                    ORDER BY is_subagent, length(uuid), started_at DESC""",
                params,
            ).fetchall()
            if not matches:
                raise ValueError(f"No session matches {session_id or uuid}")
            exact = [row for row in matches if row["uuid"] == uuid]
            primaries = [row for row in matches if row["is_subagent"] == 0 and "/" not in row["uuid"]]
            ranked = exact or primaries or matches
            if len(ranked) > 1 and not person:
                return {
                    "ambiguous": True,
                    "matches": [
                        {
                            "person": row["person"],
                            "source": row["source"],
                            "uuid": row["uuid"],
                            "startedAt": row["started_at"],
                        }
                        for row in ranked[:20]
                    ],
                }
            session = ranked[0]
            rows = db.execute(
                """SELECT e.seq, e.role, e.ts, e.header_line, b.hash
                   FROM entries e JOIN blobs b ON b.id = e.blob_id
                   WHERE e.session_id = ? ORDER BY e.seq LIMIT ? OFFSET ?""",
                (session["id"], bounded(limit, 50, 200), max(0, offset)),
            ).fetchall()
        rendered: list[dict[str, Any]] = []
        used = 0
        with self._objects_connection() as objects_db:
            for entry in rows:
                body = self._load_body(entry["hash"], objects_db)
                if used + len(body) > bounded(max_chars, 50_000, 200_000):
                    break
                rendered.append(
                    {
                        "seq": entry["seq"],
                        "role": entry["role"],
                        "timestamp": entry["ts"],
                        "header": entry["header_line"],
                        "text": body,
                    }
                )
                used += len(body)
        return {
            "ambiguous": False,
            "session": {
                "sessionId": session["id"],
                "person": session["person"],
                "source": session["source"],
                "uuid": session["uuid"],
                "repository": session["repository"],
                "gitBranch": session["git_branch"],
                "model": session["model"],
                "startedAt": session["started_at"],
                "endedAt": session["ended_at"],
                "cwd": session["cwd"],
                "entryCount": session["entry_count"],
            },
            "offset": max(0, offset),
            "returnedEntries": len(rendered),
            "entries": rendered,
        }

    def stats(self) -> dict[str, Any]:
        with self._connection(self.index_path) as db:
            sessions = db.execute(
                """SELECT person, repository, count(*) AS sessions, sum(is_subagent) AS subagents,
                          min(started_at) AS first_ts, max(started_at) AS last_ts
                   FROM sessions GROUP BY person, repository ORDER BY person, repository"""
            ).fetchall()
            blobs = db.execute(
                """SELECT count(*) AS blobs, coalesce(sum(nbytes), 0) AS unique_bytes,
                          coalesce(sum(nbytes * nrefs), 0) AS logical_bytes FROM blobs"""
            ).fetchone()
            return {"sessions": [row_dict(row) for row in sessions], "blobs": row_dict(blobs)}

    def missing_session_fingerprints(self, person: str, fingerprints: list[str]) -> list[str]:
        existing: set[str] = set()
        with self._connection(self.index_path) as db:
            batch_size = min(50_000, db.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 1)
            for offset in range(0, len(fingerprints), batch_size):
                batch = fingerprints[offset : offset + batch_size]
                placeholders = ",".join("?" for _ in batch)
                existing.update(
                    row[0]
                    for row in db.execute(
                        "SELECT session_fingerprint FROM sessions "
                        f"WHERE person = ? AND session_fingerprint IN ({placeholders})",
                        [person, *batch],
                    )
                )
        return [fingerprint for fingerprint in fingerprints if fingerprint not in existing]

    def missing_blob_hashes(self, hashes: list[str]) -> list[str]:
        existing: set[str] = set()
        with self._objects_connection() as db:
            if db is None:
                return hashes
            batch_size = min(50_000, db.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
            for offset in range(0, len(hashes), batch_size):
                batch = hashes[offset : offset + batch_size]
                placeholders = ",".join("?" for _ in batch)
                existing.update(
                    row[0] for row in db.execute(f"SELECT hash FROM objects WHERE hash IN ({placeholders})", batch)
                )
        return [digest for digest in hashes if digest not in existing]


class CorpusStore:
    def __init__(self, data_dir: Path, semantic: Semantic | None = None, usage: Usage | None = None) -> None:
        self.data_dir = data_dir
        ensure_wal(data_dir / "index.sqlite")
        ensure_wal(data_dir / "objects.sqlite")
        self.semantic = semantic
        self.usage = usage
        self.provenance = Provenance(data_dir)
        self._corpus = Corpus(
            data_dir,
            nearest=semantic.nearest if semantic else None,
            priors=usage.priors if usage else None,
            provenance=self.provenance,
        )
        self._update_lock = threading.Lock()

    def read(self) -> Corpus:
        return self._corpus

    def refresh_indexes(self) -> int:
        """Classify new user messages, then embed new bodies. Returns the number of vectors added."""
        self.provenance.refresh(self._corpus.load_bodies)
        return self.semantic.index_missing(self._corpus.load_bodies) if self.semantic else 0

    def begin_update(self) -> None:
        if not self._update_lock.acquire(blocking=False):
            raise RuntimeError("Corpus ingestion is already in progress")

    def end_update(self) -> None:
        self._update_lock.release()

    def close(self) -> None:
        self._corpus.close()
        if self.usage:
            self.usage.close()
