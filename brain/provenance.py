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
        self._review_uuids: dict[int, str] = {}
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
        """Classify user messages added since the last refresh and resolve review copies. Safe after each ingest."""
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
                injected, generated, reviewing = set(self._snapshot.injected), set(self._snapshot.generated), {}
                review_blobs, generated_blobs = [], []
                for offset in range(0, len(rows), BATCH_SIZE):
                    batch = rows[offset : offset + BATCH_SIZE]
                    for (blob_id, _digest), body in zip(
                        batch, load_bodies([digest for _, digest in batch]), strict=True
                    ):
                        if INJECTED.match(body, 0, PREFIX_CHARS):
                            injected.add(blob_id)
                        elif REVIEW.match(body, 0, PREFIX_CHARS):
                            injected.add(blob_id)
                            review_blobs.append(blob_id)
                            match = REVIEWED.search(body)
                            if match:
                                reviewing[blob_id] = match.group(1)
                        elif GENERATED.match(body, 0, PREFIX_CHARS):
                            generated_blobs.append(blob_id)
                for blob_ids, target in ((review_blobs, None), (generated_blobs, generated)):
                    for offset in range(0, len(blob_ids), 900):
                        chunk = blob_ids[offset : offset + 900]
                        for session_id, blob_id in index.execute(
                            f"""SELECT session_id, blob_id FROM entries
                                WHERE role = 'user' AND blob_id IN ({",".join("?" for _ in chunk)})""",
                            chunk,
                        ):
                            if target is None:
                                uuid = reviewing.get(blob_id)
                                if uuid or session_id not in self._review_uuids:
                                    self._review_uuids[session_id] = uuid or ""
                            else:
                                target.add(session_id)
                reviews = {}
                for session_id, uuid in self._review_uuids.items():
                    found = index.execute(REVIEWED_SESSION_SQL, (uuid,)).fetchone() if uuid else None
                    reviews[session_id] = found[0] if found and found[0] != session_id else None
            if rows:
                self._last_blob = rows[-1][0]
            self._snapshot = Snapshot(frozenset(injected), frozenset(generated), reviews)
            report = {
                "classified": len(rows),
                "injected": len(injected),
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
