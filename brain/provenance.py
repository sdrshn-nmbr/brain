from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("brain.provenance")

BodyLoader = Callable[[list[str]], list[str]]
PREFIX_CHARS = 400
BATCH_SIZE = 2_000
INJECTED = re.compile(
    r"\s*(?:<recommended_plugins>|# AGENTS\.md instructions|<INSTRUCTIONS>|<environment_context>|<user_instructions>"
    r"|<skill>|<skills_instructions>|<permissions instructions>|<collaboration_mode>|<app-context>"
    r"|<local-command-caveat>|<command-name>|<command-message>|<user_info>|<external_codex_apps|<codex_apps_"
    r"|<turn_aborted>|<system-reminder>|<subagent_notification>|<model_switch>|<personality_spec>"
    r"|\[Request interrupted by user|Warmup\s*$)"
)
REVIEW = re.compile(r"\s*The following is the Codex agent history")
REVIEWED = re.compile(r"Reviewed Codex session id: ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
GENERATED = re.compile(r"\s*This is an app-generated request")
REVIEWED_SESSION_SQL = "SELECT id FROM sessions WHERE uuid = ? AND is_subagent = 0 ORDER BY started_at DESC LIMIT 1"
SESSIONS_CHUNK = 900


def sessions_of(index: sqlite3.Connection, blob_ids: list[int]) -> list[tuple[int, int]]:
    """(session_id, blob_id) for every user entry that holds one of these messages."""
    pairs: list[tuple[int, int]] = []
    for offset in range(0, len(blob_ids), SESSIONS_CHUNK):
        chunk = blob_ids[offset : offset + SESSIONS_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        pairs += index.execute(
            f"SELECT session_id, blob_id FROM entries WHERE role = 'user' AND blob_id IN ({placeholders})", chunk
        ).fetchall()
    return pairs


@dataclass(frozen=True)
class Snapshot:
    injected: frozenset[int] = frozenset()
    generated: frozenset[int] = frozenset()
    reviews: dict[int, int | None] = field(default_factory=dict)


class Provenance:
    """Recognizes harness text stored under the user role.

    Codex and Claude record injected instructions, auto-review transcripts that copy another session, and app-generated
    suggestion prompts as user messages. Search down-weights injected instructions, credits a review copy to the session
    it reviewed, and leaves out generated suggestions. Classification lives in memory and is rebuilt from the corpus.
    """

    def __init__(self, data_dir: Path) -> None:
        self.index_path = data_dir / "index.sqlite"
        self._snapshot = Snapshot()
        self._injected: set[int] = set()
        self._review_blobs: dict[int, str] = {}
        self._generated_blobs: set[int] = set()
        self._last_blob = 0
        self._lock = threading.Lock()

    @property
    def snapshot(self) -> Snapshot:
        return self._snapshot

    def canonical(self, session_id: int) -> int | None:
        """The session a hit belongs to: itself, the session a review copied, or None for generated sessions and
        reviews whose original is not indexed."""
        snapshot = self._snapshot
        if session_id in snapshot.generated:
            return None
        if session_id in snapshot.reviews:
            return snapshot.reviews[session_id]
        return session_id

    def is_derived(self, session_id: int) -> bool:
        snapshot = self._snapshot
        return session_id in snapshot.generated or session_id in snapshot.reviews

    def refresh(self, load_bodies: BodyLoader) -> dict[str, int]:
        """Classify user messages added since the last refresh, then map every classified message to its current
        sessions. Re-uploading a session gives it a new ID while its messages keep theirs, so the mapping is rebuilt
        each time. Safe after each ingest."""
        if not self._lock.acquire(blocking=False):
            return {}
        try:
            started = time.perf_counter()
            with sqlite3.connect(f"file:{self.index_path}?mode=ro", uri=True) as index:
                rows = index.execute(
                    """SELECT DISTINCT b.id, b.hash FROM entries e JOIN blobs b ON b.id = e.blob_id
                       WHERE e.role = 'user' AND b.id > ? ORDER BY b.id""",
                    (self._last_blob,),
                ).fetchall()
                for offset in range(0, len(rows), BATCH_SIZE):
                    batch = rows[offset : offset + BATCH_SIZE]
                    for (blob_id, _digest), body in zip(
                        batch, load_bodies([digest for _, digest in batch]), strict=True
                    ):
                        if INJECTED.match(body, 0, PREFIX_CHARS):
                            self._injected.add(blob_id)
                        elif REVIEW.match(body, 0, PREFIX_CHARS):
                            self._injected.add(blob_id)
                            match = REVIEWED.search(body)
                            self._review_blobs[blob_id] = match.group(1) if match else ""
                        elif GENERATED.match(body, 0, PREFIX_CHARS):
                            self._generated_blobs.add(blob_id)
                review_uuids: dict[int, str] = {}
                for session_id, blob_id in sessions_of(index, list(self._review_blobs)):
                    uuid = self._review_blobs[blob_id]
                    if uuid or session_id not in review_uuids:
                        review_uuids[session_id] = uuid
                generated = {session_id for session_id, _ in sessions_of(index, list(self._generated_blobs))}
                reviews = {}
                for session_id, uuid in review_uuids.items():
                    found = index.execute(REVIEWED_SESSION_SQL, (uuid,)).fetchone() if uuid else None
                    reviews[session_id] = found[0] if found and found[0] != session_id else None
            if rows:
                self._last_blob = rows[-1][0]
            self._snapshot = Snapshot(frozenset(self._injected), frozenset(generated), reviews)
            report = {
                "classified": len(rows),
                "injected": len(self._injected),
                "generatedSessions": len(generated),
                "reviewSessions": len(reviews),
                "unmatchedReviews": sum(1 for value in reviews.values() if value is None),
            }
            logger.info(
                json.dumps({"event": "provenance_index", **report, "seconds": round(time.perf_counter() - started, 1)})
            )
            return report
        finally:
            self._lock.release()
