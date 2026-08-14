from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import uuid
import zipfile
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from starlette.requests import Request

from brain.config import Config
from brain.corpus import CorpusStore
from collector.archive import stable_session_key

logger = logging.getLogger("brain")

ALLOWED_SOURCES = {"claude", "codex", "cursor"}
IngestArchive = Callable[[str, Path, str], Awaitable[dict[str, int]]]


def person_from_principal(principal: str) -> str:
    preferred = re.sub(r"-+", "-", re.sub(r"[^a-z0-9_-]", "-", principal.split("@", 1)[0].lower()))
    if not preferred:
        raise ValueError(f"Cannot derive a corpus owner label from principal {principal}")
    return preferred


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def validate_archive_scope(path: Path, expected_scope: dict[str, Any]) -> None:
    with zipfile.ZipFile(path) as archive:
        manifests = [name for name in archive.namelist() if name.endswith("_manifest.json")]
        if len(manifests) != 1:
            raise ValueError("archive must contain exactly one manifest")
        manifest_info = archive.getinfo(manifests[0])
        if manifest_info.file_size > 10 * 1024 * 1024:
            raise ValueError("archive manifest exceeds 10 MiB")
        manifest = json.loads(archive.read(manifest_info))
        if manifest.get("format_version") != 2:
            raise ValueError("archive format_version must be 2")
        sessions = manifest.get("sessions")
        if not isinstance(sessions, list) or len(sessions) != expected_scope["sessionCount"]:
            raise ValueError("archive session count does not match the confirmed scope")
        repositories = {session.get("repository") for session in sessions}
        sources = {session.get("source") for session in sessions}
        if repositories != set(expected_scope["repositories"]):
            raise ValueError("archive repositories do not match the confirmed scope")
        if sources != set(expected_scope["sources"]):
            raise ValueError("archive sources do not match the confirmed scope")
        since = parse_timestamp(expected_scope["since"]) if expected_scope.get("since") else None
        until = parse_timestamp(expected_scope["until"]) if expected_scope.get("until") else None
        for session in sessions:
            required = ("source", "uuid", "started_at", "session_fallback", "session_key")
            if any(not isinstance(session.get(field), str) or not session[field] for field in required):
                raise ValueError("archive sessions require canonical identity metadata")
            expected_key = stable_session_key(
                session["source"], session["uuid"], session["started_at"], session["session_fallback"]
            )
            if session["session_key"] != expected_key:
                raise ValueError("archive session_key does not match canonical identity metadata")
            started_at = session.get("started_at")
            ended_at = session.get("ended_at") or started_at
            if (since or until) and (not started_at or not ended_at):
                raise ValueError("archive session timestamps are required by the confirmed time bounds")
            if since and parse_timestamp(ended_at) < since:
                raise ValueError("archive contains a session before the confirmed since bound")
            if until and parse_timestamp(started_at) > until:
                raise ValueError("archive contains a session after the confirmed until bound")
        fingerprints = [session.get("session_fingerprint") for session in sessions]
        if len(set(fingerprints)) != len(fingerprints) or any(
            not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value) for value in fingerprints
        ):
            raise ValueError("archive session fingerprints must be unique lowercase SHA-256 digests")


class UploadManager:
    def __init__(
        self,
        data_dir: Path,
        max_upload_bytes: int,
        max_pending_bytes_per_owner: int,
        upload_ttl_seconds: int,
        upload_receive_timeout_seconds: int,
        allowed_repositories: frozenset[str],
        visibility: str,
        ingest_archive: IngestArchive,
    ) -> None:
        self.max_upload_bytes = max_upload_bytes
        self.max_pending_bytes_per_owner = max_pending_bytes_per_owner
        self.upload_ttl_seconds = upload_ttl_seconds
        self.upload_receive_timeout_seconds = upload_receive_timeout_seconds
        self.allowed_repositories = allowed_repositories
        self.visibility = visibility
        self.ingest_archive = ingest_archive
        self.uploads_dir = data_dir / "uploads"
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(data_dir / "uploads.sqlite", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._db_lock = threading.RLock()
        self._active_receives: set[str] = set()
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS uploads (
                id TEXT PRIMARY KEY,
                owner_principal TEXT NOT NULL,
                person TEXT NOT NULL,
                archive_sha256 TEXT NOT NULL,
                archive_bytes INTEGER NOT NULL,
                archive_path TEXT NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL DEFAULT 'prepared',
                phase_detail TEXT,
                scope_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT,
                result_json TEXT,
                UNIQUE(owner_principal, archive_sha256)
            );
            CREATE INDEX IF NOT EXISTS idx_uploads_owner_created ON uploads(owner_principal, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status);
            CREATE TABLE IF NOT EXISTS identities (
                owner_principal TEXT PRIMARY KEY COLLATE NOCASE,
                person TEXT NOT NULL UNIQUE
            );
            """
        )
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(uploads)")}
        if "phase" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN phase TEXT NOT NULL DEFAULT 'prepared'")
        if "phase_detail" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN phase_detail TEXT")
        self.db.execute(
            "INSERT OR IGNORE INTO identities(owner_principal, person) SELECT owner_principal, person FROM uploads"
        )
        self.db.execute(
            "UPDATE uploads SET status = 'queued', updated_at = ? WHERE status = 'processing'", (utc_now(),)
        )
        self.db.execute(
            """UPDATE uploads SET status = 'prepared', phase = 'prepared',
               phase_detail = 'recovered interrupted receive' WHERE status = 'receiving'"""
        )
        self.db.commit()
        self.garbage_collect()
        self._drain_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()

    def person_for(self, owner_principal: str) -> str:
        canonical_principal = owner_principal.strip().lower()
        with self._db_lock:
            existing = self.db.execute(
                "SELECT person FROM identities WHERE owner_principal = ?", (canonical_principal,)
            ).fetchone()
            if existing:
                return str(existing["person"])
            preferred = person_from_principal(canonical_principal)
            occupied = self.db.execute("SELECT 1 FROM identities WHERE person = ?", (preferred,)).fetchone()
            suffix = hashlib.sha256(canonical_principal.encode()).hexdigest()[:8]
            person = preferred if not occupied else f"{preferred}-{suffix}"
            self.db.execute(
                "INSERT INTO identities(owner_principal, person) VALUES (?, ?)", (canonical_principal, person)
            )
            self.db.commit()
            return person

    def start(self) -> None:
        self._schedule_drain()

    def prepare(
        self,
        owner_principal: str,
        archive_sha256: str,
        archive_bytes: int,
        scope: dict[str, Any],
        confirmed_shared: bool,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[a-f0-9]{64}", archive_sha256):
            raise ValueError("archiveSha256 must be a lowercase SHA-256 digest")
        if not 1 <= archive_bytes <= self.max_upload_bytes:
            raise ValueError(f"archiveBytes must be between 1 and {self.max_upload_bytes}")
        if not confirmed_shared or scope.get("visibility") != self.visibility:
            raise ValueError(f"The user must confirm publication with visibility {self.visibility}")
        session_count = scope.get("sessionCount")
        if not isinstance(session_count, int) or isinstance(session_count, bool) or session_count < 1:
            raise ValueError("scope.sessionCount must be a positive integer")
        repositories = scope.get("repositories") or []
        sources = scope.get("sources") or []
        if not repositories or any(repository not in self.allowed_repositories for repository in repositories):
            raise ValueError("scope.repositories contains a repository outside the Brain allowlist")
        if not sources or any(source not in ALLOWED_SOURCES for source in sources):
            raise ValueError("scope.sources contains an unsupported agent source")

        self.garbage_collect()
        with self._db_lock:
            existing = self.db.execute(
                "SELECT * FROM uploads WHERE owner_principal = ? AND archive_sha256 = ?",
                (owner_principal, archive_sha256),
            ).fetchone()
            if not existing or existing["status"] in {"complete", "failed", "cancelled", "expired"}:
                existing_id = existing["id"] if existing else ""
                pending_bytes = self.db.execute(
                    """SELECT COALESCE(SUM(archive_bytes), 0) FROM uploads
                       WHERE owner_principal = ? AND id != ?
                       AND status IN ('prepared', 'receiving', 'uploaded', 'queued', 'processing', 'failed')""",
                    (owner_principal, existing_id),
                ).fetchone()[0]
                if pending_bytes + archive_bytes > self.max_pending_bytes_per_owner:
                    raise ValueError(
                        f"Pending uploads would exceed the per-owner limit {self.max_pending_bytes_per_owner}"
                    )
            if existing:
                if existing["status"] in {"complete", "failed", "cancelled", "expired"}:
                    self.db.execute(
                        """UPDATE uploads SET archive_bytes = ?, status = 'prepared', phase = 'prepared',
                           phase_detail = NULL, scope_json = ?, updated_at = ?, error = NULL,
                           result_json = NULL WHERE id = ?""",
                        (archive_bytes, json.dumps(scope), utc_now(), existing["id"]),
                    )
                    self.db.commit()
                    return {
                        **self.status(owner_principal, existing["id"]),
                        "uploadPath": f"/uploads/{existing['id']}/archive",
                    }
                return {**self._record(existing), "uploadPath": f"/uploads/{existing['id']}/archive"}

            upload_id = str(uuid.uuid4())
            now = utc_now()
            archive_path = self.uploads_dir / f"{upload_id}.zip"
            self.db.execute(
                """INSERT INTO uploads(
                       id, owner_principal, person, archive_sha256, archive_bytes, archive_path,
                       status, scope_json, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, 'prepared', ?, ?, ?)""",
                (
                    upload_id,
                    owner_principal,
                    self.person_for(owner_principal),
                    archive_sha256,
                    archive_bytes,
                    str(archive_path),
                    json.dumps(scope),
                    now,
                    now,
                ),
            )
            self.db.commit()
        return {**self.status(owner_principal, upload_id), "uploadPath": f"/uploads/{upload_id}/archive"}

    def garbage_collect(self) -> dict[str, int]:
        cutoff = (datetime.now(UTC) - timedelta(seconds=self.upload_ttl_seconds)).isoformat()
        with self._db_lock:
            candidates = self.db.execute(
                """SELECT id, archive_path, archive_bytes FROM uploads
                   WHERE status IN ('prepared', 'receiving', 'uploaded', 'failed') AND updated_at < ?""",
                (cutoff,),
            ).fetchall()
            rows = [row for row in candidates if row["id"] not in self._active_receives]
            if rows:
                for row in rows:
                    archive_path = Path(row["archive_path"])
                    archive_path.unlink(missing_ok=True)
                    archive_path.with_suffix(".zip.partial").unlink(missing_ok=True)
                ids = [row["id"] for row in rows]
                placeholders = ",".join("?" for _ in ids)
                self.db.execute(
                    f"""UPDATE uploads SET status = 'expired', phase = 'expired',
                        phase_detail = 'abandoned archive expired', updated_at = ?
                        WHERE id IN ({placeholders})""",
                    (utc_now(), *ids),
                )
                self.db.commit()
        removed = {"uploads": len(rows), "bytes": sum(row["archive_bytes"] for row in rows)}
        if rows:
            logger.info(json.dumps({"event": "upload_garbage_collected", **removed}))
        return removed

    async def receive(self, owner_principal: str, upload_id: str, request: Request) -> dict[str, Any]:
        with self._db_lock:
            claimed = self.db.execute(
                """UPDATE uploads SET status = 'receiving', phase = 'receiving',
                   phase_detail = 'streaming archive', updated_at = ?
                   WHERE id = ? AND owner_principal = ? AND status = 'prepared'""",
                (utc_now(), upload_id, owner_principal),
            )
            self.db.commit()
            if claimed.rowcount != 1:
                current = self._owned_row(owner_principal, upload_id)
                raise ValueError(f"Upload {upload_id} cannot receive bytes while status is {current['status']}")
            row = self._owned_row(owner_principal, upload_id)
            self._active_receives.add(upload_id)
        archive_path = Path(row["archive_path"])
        partial_path = archive_path.with_suffix(".zip.partial")
        digest = hashlib.sha256()
        received = 0
        try:
            content_length = request.headers.get("content-length")
            if content_length is not None and int(content_length) != row["archive_bytes"]:
                raise ValueError(f"Content-Length {content_length} does not match prepared size {row['archive_bytes']}")
            partial_path.unlink(missing_ok=True)
            descriptor = os.open(partial_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            async with asyncio.timeout(self.upload_receive_timeout_seconds):
                with os.fdopen(descriptor, "wb") as output:
                    async for chunk in request.stream():
                        received += len(chunk)
                        if received > row["archive_bytes"]:
                            raise ValueError(f"Upload exceeded prepared size {row['archive_bytes']}")
                        digest.update(chunk)
                        output.write(chunk)
            if received != row["archive_bytes"]:
                raise ValueError(f"Received {received} bytes; expected {row['archive_bytes']}")
            actual_digest = digest.hexdigest()
            if actual_digest != row["archive_sha256"]:
                raise ValueError(f"Archive SHA-256 mismatch: received {actual_digest}")
            validate_archive_scope(partial_path, json.loads(row["scope_json"]))
            partial_path.replace(archive_path)
            self._update_state(upload_id, "uploaded", phase="uploaded", phase_detail="checksum verified")
            return self.status(owner_principal, upload_id)
        except (Exception, asyncio.CancelledError) as error:
            partial_path.unlink(missing_ok=True)
            with self._db_lock:
                self.db.execute(
                    """UPDATE uploads SET status = 'prepared', phase = 'prepared', phase_detail = NULL,
                       error = ?, updated_at = ? WHERE id = ? AND status = 'receiving'""",
                    (str(error), utc_now(), upload_id),
                )
                self.db.commit()
            raise
        finally:
            with self._db_lock:
                self._active_receives.discard(upload_id)

    def commit(self, owner_principal: str, upload_id: str) -> dict[str, Any]:
        with self._db_lock:
            claimed = self.db.execute(
                """UPDATE uploads SET status = 'queued', phase = 'queued',
                   phase_detail = 'waiting for ingestion', updated_at = ?
                   WHERE id = ? AND owner_principal = ? AND status = 'uploaded'""",
                (utc_now(), upload_id, owner_principal),
            )
            self.db.commit()
            if claimed.rowcount != 1:
                row = self._owned_row(owner_principal, upload_id)
                if row["status"] in {"complete", "queued", "processing"}:
                    return self._record(row)
                raise ValueError(f"Upload {upload_id} cannot be committed while status is {row['status']}")
        self._schedule_drain()
        return self.status(owner_principal, upload_id)

    def cancel(self, owner_principal: str, upload_id: str) -> dict[str, Any]:
        with self._db_lock:
            claimed = self.db.execute(
                """UPDATE uploads SET status = 'cancelled', phase = 'cancelled', phase_detail = NULL,
                   updated_at = ? WHERE id = ? AND owner_principal = ?
                   AND status IN ('prepared', 'uploaded', 'queued', 'failed')""",
                (utc_now(), upload_id, owner_principal),
            )
            self.db.commit()
            if claimed.rowcount != 1:
                row = self._owned_row(owner_principal, upload_id)
                raise ValueError(f"Upload {upload_id} cannot be cancelled while status is {row['status']}")
            row = self._owned_row(owner_principal, upload_id)
        archive_path = Path(row["archive_path"])
        archive_path.unlink(missing_ok=True)
        archive_path.with_suffix(".zip.partial").unlink(missing_ok=True)
        return self.status(owner_principal, upload_id)

    def status(self, owner_principal: str, upload_id: str) -> dict[str, Any]:
        return self._record(self._owned_row(owner_principal, upload_id))

    def list_uploads(self, owner_principal: str, limit: int = 20) -> list[dict[str, Any]]:
        with self._db_lock:
            rows = self.db.execute(
                "SELECT * FROM uploads WHERE owner_principal = ? ORDER BY created_at DESC LIMIT ?",
                (owner_principal, max(1, min(limit, 100))),
            ).fetchall()
            return [self._record(row) for row in rows]

    async def wait(self) -> None:
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks))

    def close(self) -> None:
        with self._db_lock:
            self.db.close()

    def _schedule_drain(self) -> None:
        task = asyncio.create_task(self._drain())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _drain(self) -> None:
        async with self._drain_lock:
            while True:
                with self._db_lock:
                    row = self.db.execute(
                        "SELECT * FROM uploads WHERE status = 'queued' ORDER BY created_at LIMIT 1"
                    ).fetchone()
                    if not row:
                        return
                    claimed = self.db.execute(
                        """UPDATE uploads SET status = 'processing', phase = 'indexing',
                           phase_detail = 'validating CAS and indexing', updated_at = ?
                           WHERE id = ? AND status = 'queued'""",
                        (utc_now(), row["id"]),
                    )
                    self.db.commit()
                    if claimed.rowcount != 1:
                        continue
                try:
                    result = await self.ingest_archive(row["person"], Path(row["archive_path"]), row["archive_sha256"])
                    self._update_state(
                        row["id"], "complete", phase="complete", phase_detail="index commit visible", result=result
                    )
                    Path(row["archive_path"]).unlink(missing_ok=True)
                except Exception as error:
                    logger.exception("upload ingestion failed", extra={"upload_id": row["id"]})
                    self._update_state(row["id"], "failed", phase="failed", error=str(error))

    def _owned_row(self, owner_principal: str, upload_id: str) -> sqlite3.Row:
        with self._db_lock:
            row = self.db.execute(
                "SELECT * FROM uploads WHERE id = ? AND owner_principal = ?",
                (upload_id, owner_principal),
            ).fetchone()
        if not row:
            raise ValueError(f"No upload {upload_id} exists for the authenticated user")
        return row

    def _update_state(
        self,
        upload_id: str,
        status: str,
        error: str | None = None,
        result: dict[str, int] | None = None,
        phase: str | None = None,
        phase_detail: str | None = None,
    ) -> None:
        with self._db_lock:
            self.db.execute(
                """UPDATE uploads SET status = ?, phase = COALESCE(?, phase), phase_detail = ?,
                   error = ?, result_json = ?, updated_at = ? WHERE id = ?""",
                (status, phase, phase_detail, error, json.dumps(result) if result else None, utc_now(), upload_id),
            )
            self.db.commit()

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "ownerPrincipal": row["owner_principal"],
            "person": row["person"],
            "archiveSha256": row["archive_sha256"],
            "archiveBytes": row["archive_bytes"],
            "status": row["status"],
            "phase": row["phase"],
            "phaseDetail": row["phase_detail"],
            "scope": json.loads(row["scope_json"]),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "error": row["error"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
        }


def create_archive_ingester(config: Config, corpus: CorpusStore) -> IngestArchive:
    async def ingest(person: str, archive_path: Path, archive_sha256: str) -> dict[str, int]:
        corpus.begin_update()
        try:
            ingest_command = (
                [config.python_executable, str(config.ingest_script)]
                if config.ingest_script is not None
                else [config.python_executable, "-m", "collector.ingest.ingest"]
            )
            process = await asyncio.create_subprocess_exec(
                *ingest_command,
                "--data-dir",
                str(config.data_dir),
                "--person",
                person,
                str(archive_path),
                *(
                    argument
                    for repository in sorted(config.allowed_repositories)
                    for argument in ("--allowed-repository", repository)
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode:
                raise RuntimeError(stderr.decode(errors="replace")[-8_000:])
            logger.info(
                json.dumps(
                    {
                        "event": "upload_ingest_output",
                        "person": person,
                        "output": stdout.decode(errors="replace")[-8_000:],
                    }
                )
            )

            with sqlite3.connect(config.data_dir / "index.sqlite") as db:
                row = db.execute(
                    """SELECT id, sessions_kept, sessions_skipped FROM imports
                       WHERE archive_sha256 = ? AND person = ?""",
                    (archive_sha256, person),
                ).fetchone()
            if not row:
                raise RuntimeError(f"Ingest completed without an import record for archive {archive_sha256}")
            result = {"importId": row[0], "sessionsKept": row[1], "sessionsSkipped": row[2]}

            return result
        finally:
            corpus.end_update()

    return ingest
