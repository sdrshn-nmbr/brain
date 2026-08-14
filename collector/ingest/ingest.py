#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["zstandard>=0.22"]
# ///
"""Ingest Brain transcript archives into a content-addressed search index.

Streams zip members → splits on entry headers → SHA-256 CAS (zstd objects) →
SQLite session/entry maps + contentless FTS5 over unique bodies.

Preserves per-entry role/ts/seq and exact header lines so sessions can be
reconstructed in original order through the Brain MCP tools.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile

import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from collector.archive import (
    FORMAT_VERSION,
    ArchiveEntry,
    object_member,
    read_entries,
    session_fingerprint,
    stable_session_key,
)

DEFAULT_DATA_DIR = Path("data")
MAX_BODY_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_OBJECT_BYTES = 64 * 1024 * 1024


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def generation_is_newer(existing: sqlite3.Row, incoming: dict, incoming_entry_count: int) -> bool:
    existing_ended = existing["ended_at"]
    incoming_ended = incoming.get("ended_at")
    if existing_ended and incoming_ended and existing_ended != incoming_ended:
        existing_time = datetime.fromisoformat(existing_ended.replace("Z", "+00:00"))
        incoming_time = datetime.fromisoformat(str(incoming_ended).replace("Z", "+00:00"))
        return incoming_time > existing_time
    if incoming_ended and not existing_ended:
        return True
    if existing_ended and not incoming_ended:
        return False
    return incoming_entry_count > int(existing["entry_count"] or 0)


def legacy_session_fallback(export_path: str, uuid: str) -> str:
    name = Path(export_path).name
    for suffix in (".entries.ndjson.zst", ".entries.json", ".md"):
        name = name.removesuffix(suffix)
    match = re.match(r"^(.*)__(?:\d{8}-\d{4}|unknown-time)__", name)
    return match.group(1) if match else uuid.replace("/", "__")


def init_objects_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS objects (
            hash TEXT PRIMARY KEY,
            body BLOB NOT NULL
        ) WITHOUT ROWID;
        """
    )
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        PRAGMA temp_store=MEMORY;

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imports (
            id INTEGER PRIMARY KEY,
            person TEXT NOT NULL,
            archive_path TEXT NOT NULL,
            archive_sha256 TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            export_timestamp TEXT,
            repository_root TEXT,
            sessions_kept INTEGER NOT NULL DEFAULT 0,
            sessions_skipped INTEGER NOT NULL DEFAULT 0,
            UNIQUE(person, archive_sha256)
        );

        CREATE TABLE IF NOT EXISTS skipped (
            import_id INTEGER NOT NULL,
            export_path TEXT NOT NULL,
            reason TEXT NOT NULL,
            cwd TEXT,
            PRIMARY KEY (import_id, export_path)
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY,
            import_id INTEGER NOT NULL,
            person TEXT NOT NULL,
            source TEXT NOT NULL,
            uuid TEXT NOT NULL,
            backend TEXT,
            cwd TEXT,
            repository TEXT,
            git_branch TEXT,
            model TEXT,
            started_at TEXT,
            ended_at TEXT,
            entry_count INTEGER,
            char_count INTEGER,
            export_path TEXT NOT NULL,
            preamble TEXT,
            is_subagent INTEGER NOT NULL DEFAULT 0,
            session_fingerprint TEXT,
            session_key TEXT,
            UNIQUE(person, source, export_path)
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_person_started
            ON sessions(person, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_sessions_uuid ON sessions(uuid);
        CREATE INDEX IF NOT EXISTS idx_sessions_branch ON sessions(git_branch);

        CREATE TABLE IF NOT EXISTS blobs (
            id INTEGER PRIMARY KEY,
            hash TEXT NOT NULL UNIQUE,
            nbytes INTEGER NOT NULL,
            nrefs INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS entries (
            session_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            role TEXT NOT NULL,
            ts TEXT,
            header_line TEXT NOT NULL,
            blob_id INTEGER NOT NULL,
            PRIMARY KEY (session_id, seq)
        );
        CREATE INDEX IF NOT EXISTS idx_entries_blob ON entries(blob_id);
        CREATE INDEX IF NOT EXISTS idx_entries_role ON entries(role);

        CREATE VIRTUAL TABLE IF NOT EXISTS blobs_fts USING fts5(
            body,
            content='',
            tokenize='porter unicode61 remove_diacritics 2'
        );
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
    if "repository" not in columns:
        conn.execute("ALTER TABLE sessions ADD COLUMN repository TEXT")
    if "session_fingerprint" not in columns:
        conn.execute("ALTER TABLE sessions ADD COLUMN session_fingerprint TEXT")
    if "session_key" not in columns:
        conn.execute("ALTER TABLE sessions ADD COLUMN session_key TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_repository ON sessions(repository)")
    schema_version = (
        int(conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()[0])
        if conn.execute("SELECT 1 FROM meta WHERE key = 'schema_version'").fetchone()
        else 1
    )
    if schema_version < 3:
        conn.executescript(
            """
            CREATE TABLE imports_v3 (
                id INTEGER PRIMARY KEY,
                person TEXT NOT NULL,
                archive_path TEXT NOT NULL,
                archive_sha256 TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                export_timestamp TEXT,
                repository_root TEXT,
                sessions_kept INTEGER NOT NULL DEFAULT 0,
                sessions_skipped INTEGER NOT NULL DEFAULT 0,
                UNIQUE(person, archive_sha256)
            );
            INSERT INTO imports_v3 SELECT * FROM imports;
            DROP TABLE imports;
            ALTER TABLE imports_v3 RENAME TO imports;
            """
        )
    if schema_version < 4:
        for session in conn.execute("SELECT id, person, source, uuid FROM sessions ORDER BY id").fetchall():
            entries = [
                {
                    "seq": row["seq"],
                    "role": row["role"],
                    "ts": row["ts"],
                    "header_line": row["header_line"],
                    "body_sha256": row["hash"],
                }
                for row in conn.execute(
                    """
                    SELECT e.seq, e.role, e.ts, e.header_line, b.hash
                    FROM entries e
                    JOIN blobs b ON b.id = e.blob_id
                    WHERE e.session_id = ?
                    ORDER BY e.seq
                    """,
                    (session["id"],),
                )
            ]
            fingerprint = session_fingerprint(
                session["source"],
                session["uuid"],
                [
                    ArchiveEntry(
                        seq=entry["seq"],
                        role=entry["role"],
                        timestamp=entry["ts"],
                        tool_name=None,
                        header_line=entry["header_line"],
                        body_sha256=entry["body_sha256"],
                    )
                    for entry in entries
                ],
            )
            duplicate = conn.execute(
                """
                SELECT id FROM sessions
                WHERE person = ? AND session_fingerprint = ? AND id < ?
                ORDER BY id LIMIT 1
                """,
                (session["person"], fingerprint, session["id"]),
            ).fetchone()
            if duplicate:
                conn.execute("DELETE FROM entries WHERE session_id = ?", (session["id"],))
                conn.execute("DELETE FROM sessions WHERE id = ?", (session["id"],))
            else:
                conn.execute(
                    "UPDATE sessions SET session_fingerprint = ? WHERE id = ?",
                    (fingerprint, session["id"]),
                )
        conn.execute(
            """
            UPDATE blobs
            SET nrefs = (SELECT COUNT(*) FROM entries WHERE entries.blob_id = blobs.id)
            """
        )
        conn.execute(
            """
            UPDATE imports
            SET sessions_kept = (SELECT COUNT(*) FROM sessions WHERE sessions.import_id = imports.id)
            """
        )
    if schema_version < 5:
        generations: dict[tuple[str, str, str], list[sqlite3.Row]] = defaultdict(list)
        for session in conn.execute(
            "SELECT id, person, source, uuid, started_at, ended_at, entry_count, export_path FROM sessions"
        ):
            key = stable_session_key(
                session["source"],
                session["uuid"],
                session["started_at"],
                legacy_session_fallback(session["export_path"], session["uuid"]),
            )
            generations[(session["person"], session["source"], key)].append(session)
        for (_person, _source, key), sessions in generations.items():
            winner = max(
                sessions,
                key=lambda session: (session["ended_at"] or "", session["entry_count"] or 0, session["id"]),
            )
            for session in sessions:
                if session["id"] == winner["id"]:
                    continue
                conn.execute("DELETE FROM entries WHERE session_id = ?", (session["id"],))
                conn.execute("DELETE FROM sessions WHERE id = ?", (session["id"],))
            conn.execute("UPDATE sessions SET session_key = ? WHERE id = ?", (key, winner["id"]))
        conn.execute("UPDATE blobs SET nrefs = (SELECT COUNT(*) FROM entries WHERE entries.blob_id = blobs.id)")
        conn.execute(
            """
            UPDATE imports
            SET sessions_kept = (SELECT COUNT(*) FROM sessions WHERE sessions.import_id = imports.id)
            """
        )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_person_fingerprint
        ON sessions(person, session_fingerprint)
        WHERE session_fingerprint IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_person_key
        ON sessions(person, source, session_key)
        WHERE session_key IS NOT NULL
        """
    )
    conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '5')")
    conn.commit()


class Ingestor:
    def __init__(
        self,
        data_dir: Path,
        reindex_fts: bool = True,
        allowed_repositories: frozenset[str] | None = None,
    ) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / "index.sqlite"
        self.objects_path = data_dir / "objects.sqlite"
        self.reindex_fts = reindex_fts
        self.allowed_repositories = allowed_repositories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)
        self.objects_conn = sqlite3.connect(self.objects_path)
        init_objects_db(self.objects_conn)
        self.dctx = zstd.ZstdDecompressor()
        self._blob_cache: dict[str, int] = {}
        self._load_blob_cache()
        # ingest-pass stats
        self.total_body_bytes = 0
        self.unique_body_bytes_added = 0
        self.entries_seen = 0
        self.blobs_reused = 0
        self.blobs_created = 0
        self.role_counts: Counter[str] = Counter()
        self.dup_bytes_saved = 0

    def _load_blob_cache(self) -> None:
        for row in self.conn.execute("SELECT id, hash FROM blobs"):
            self._blob_cache[row["hash"]] = row["id"]

    def close(self) -> None:
        if self.conn.in_transaction:
            self.conn.rollback()
        if self.objects_conn.in_transaction:
            self.objects_conn.rollback()
        self.objects_conn.close()
        self.conn.close()

    def store_blob(self, zipped: ZipFile, wrapper: str, digest: str) -> tuple[int, bool]:
        existing = self._blob_cache.get(digest)
        if existing is not None:
            nbytes = int(self.conn.execute("SELECT nbytes FROM blobs WHERE id = ?", (existing,)).fetchone()[0])
            self.total_body_bytes += nbytes
            self.dup_bytes_saved += nbytes
            self.blobs_reused += 1
            self.conn.execute("UPDATE blobs SET nrefs = nrefs + 1 WHERE id = ?", (existing,))
            return existing, False

        object_row = self.objects_conn.execute("SELECT body FROM objects WHERE hash = ?", (digest,)).fetchone()
        if object_row:
            compressed = object_row[0]
        else:
            member = object_member(wrapper, digest)
            if member not in zipped.NameToInfo:
                raise ValueError(f"archive omits unknown CAS object {digest}")
            if zipped.getinfo(member).file_size > MAX_COMPRESSED_OBJECT_BYTES:
                raise ValueError(f"compressed CAS object exceeds 64 MiB for {digest}")
            compressed = zipped.read(member)
            self.objects_conn.execute(
                "INSERT OR IGNORE INTO objects(hash, body) VALUES (?, ?)",
                (digest, compressed),
            )
        content_size = zstd.frame_content_size(compressed)
        if content_size in {zstd.CONTENTSIZE_UNKNOWN, zstd.CONTENTSIZE_ERROR}:
            raise ValueError(f"CAS object has no trusted decompressed size for {digest}")
        if content_size > MAX_BODY_BYTES:
            raise ValueError(f"CAS object exceeds 64 MiB decompressed for {digest}")
        raw = self.dctx.decompress(compressed, max_output_size=MAX_BODY_BYTES)
        if sha256_bytes(raw) != digest:
            raise ValueError(f"CAS object SHA-256 mismatch for {digest}")
        body = raw.decode("utf-8")
        self.total_body_bytes += len(raw)
        cur = self.conn.execute(
            "INSERT INTO blobs(hash, nbytes, nrefs) VALUES (?, ?, 1)",
            (digest, len(raw)),
        )
        blob_id = int(cur.lastrowid)
        self._blob_cache[digest] = blob_id
        self.blobs_created += 1
        self.unique_body_bytes_added += len(raw)
        if self.reindex_fts:
            self.conn.execute(
                "INSERT INTO blobs_fts(rowid, body) VALUES (?, ?)",
                (blob_id, body),
            )
        return blob_id, True

    def ingest_zip(self, person: str, zip_path: Path) -> int:
        t0 = time.time()
        print(f"\n=== ingest {person}: {zip_path.name} ===")
        archive_sha = sha256_file(zip_path)
        archive_ref = f"sha256:{archive_sha}"

        with ZipFile(zip_path) as archive:
            manifest_names = [name for name in archive.namelist() if name.endswith("_manifest.json")]
            if len(manifest_names) != 1:
                raise ValueError("archive must contain exactly one manifest")
            replay_manifest = json.loads(archive.read(manifest_names[0]))
            replay_sessions = replay_manifest.get("sessions") or []

        existing = self.conn.execute(
            "SELECT id, sessions_kept FROM imports WHERE person = ? AND archive_sha256 = ?",
            (person, archive_sha),
        ).fetchone()
        if existing:
            fingerprints = [session.get("session_fingerprint") for session in replay_sessions]
            present = 0
            for offset in range(0, len(fingerprints), 500):
                batch = fingerprints[offset : offset + 500]
                placeholders = ",".join("?" for _ in batch)
                present += int(
                    self.conn.execute(
                        f"""SELECT COUNT(*) FROM sessions WHERE person = ?
                            AND session_fingerprint IN ({placeholders})""",
                        [person, *batch],
                    ).fetchone()[0]
                )
            superseded = len(fingerprints) - present
            self.conn.execute(
                "UPDATE imports SET sessions_kept = ?, sessions_skipped = ? WHERE id = ?",
                (present, superseded, existing["id"]),
            )
            self.conn.commit()
            print(
                f"  already imported (import_id={existing['id']} present={present} superseded={superseded}); skipping"
            )
            return int(existing["id"])

        kept = 0
        skipped = 0
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            with ZipFile(zip_path) as zf:
                manifest_name = next(n for n in zf.namelist() if n.endswith("_manifest.json"))
                man = json.loads(zf.read(manifest_name))
                if man.get("format_version") != FORMAT_VERSION:
                    raise ValueError(f"Brain archive format_version must be {FORMAT_VERSION}")
                wrapper = manifest_name.rsplit("/", 1)[0]
                cur = self.conn.execute(
                    """
                INSERT INTO imports(
                    person, archive_path, archive_sha256, imported_at,
                    export_timestamp, repository_root
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        person,
                        archive_ref,
                        archive_sha,
                        datetime.now(UTC).isoformat(),
                        man.get("export_timestamp"),
                        man.get("repository_root"),
                    ),
                )
                import_id = int(cur.lastrowid)
                sessions = man.get("sessions") or []
                print(f"  manifest sessions: {len(sessions)}")

                for i, s in enumerate(sessions):
                    export_path = s.get("export_path") or ""
                    entries_path = s.get("entries_path") or ""
                    cwd = s.get("cwd")
                    repository = str(s.get("repository") or "").lower()
                    if self.allowed_repositories is not None and repository not in self.allowed_repositories:
                        self.conn.execute(
                            "INSERT INTO skipped(import_id, export_path, reason, cwd) VALUES (?,?,?,?)",
                            (import_id, export_path, "repository_not_allowed", cwd),
                        )
                        skipped += 1
                        continue
                    if not entries_path or entries_path not in zf.NameToInfo:
                        self.conn.execute(
                            "INSERT INTO skipped(import_id, export_path, reason, cwd) VALUES (?,?,?,?)",
                            (import_id, export_path, "missing_entries_member", cwd),
                        )
                        skipped += 1
                        continue
                    entries = read_entries(zf, entries_path)
                    source = s.get("source") or "unknown"
                    uuid = s.get("uuid") or Path(export_path).name.split("__", 1)[0]
                    expected_session_key = stable_session_key(
                        source,
                        uuid,
                        s.get("started_at"),
                        str(s.get("session_fallback") or ""),
                    )
                    if not s.get("session_fallback") or s.get("session_key") != expected_session_key:
                        raise ValueError(f"invalid canonical session key for {uuid}")
                    fingerprint = session_fingerprint(source, uuid, entries)
                    if s.get("session_fingerprint") != fingerprint:
                        raise ValueError(f"session fingerprint mismatch for {uuid}")
                    if self.conn.execute(
                        "SELECT id FROM sessions WHERE person=? AND session_fingerprint=?",
                        (person, fingerprint),
                    ).fetchone():
                        self.conn.execute(
                            """
                            INSERT OR IGNORE INTO skipped(import_id, export_path, reason, cwd)
                            VALUES (?,?,?,?)
                            """,
                            (import_id, export_path, "duplicate_session_content", cwd),
                        )
                        skipped += 1
                        continue
                    session_key = s.get("session_key")
                    if session_key:
                        replaced = self.conn.execute(
                            """SELECT id, ended_at, entry_count FROM sessions
                               WHERE person=? AND source=? AND session_key=?""",
                            (person, source, session_key),
                        ).fetchone()
                        if replaced:
                            if not generation_is_newer(replaced, s, len(entries)):
                                self.conn.execute(
                                    """INSERT OR IGNORE INTO skipped(import_id, export_path, reason, cwd)
                                       VALUES (?,?,?,?)""",
                                    (import_id, export_path, "stale_session_generation", cwd),
                                )
                                skipped += 1
                                continue
                            self.conn.execute("DELETE FROM entries WHERE session_id=?", (replaced["id"],))
                            self.conn.execute("DELETE FROM sessions WHERE id=?", (replaced["id"],))
                    is_sub = 1 if "subagent" in export_path else 0
                    sc = self.conn.execute(
                        """
                    INSERT INTO sessions(
                        import_id, person, source, uuid, backend, cwd, repository, git_branch,
                        model, started_at, ended_at, entry_count, char_count,
                        export_path, preamble, is_subagent, session_fingerprint, session_key
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                        (
                            import_id,
                            person,
                            source,
                            uuid,
                            s.get("backend"),
                            cwd,
                            repository,
                            s.get("git_branch"),
                            s.get("model"),
                            s.get("started_at"),
                            s.get("ended_at"),
                            s.get("entry_count") or len(entries),
                            s.get("char_count") or 0,
                            export_path,
                            "",
                            is_sub,
                            fingerprint,
                            session_key,
                        ),
                    )
                    session_id = int(sc.lastrowid)

                    entry_rows: list[tuple] = []
                    for e in entries:
                        self.entries_seen += 1
                        self.role_counts[e.role] += 1
                        blob_id, _ = self.store_blob(zf, wrapper, e.body_sha256)
                        entry_rows.append((session_id, e.seq, e.role, e.timestamp, e.header_line, blob_id))
                        if len(entry_rows) == 1_000:
                            self.conn.executemany(
                                """INSERT INTO entries(session_id, seq, role, ts, header_line, blob_id)
                                   VALUES (?,?,?,?,?,?)""",
                                entry_rows,
                            )
                            entry_rows.clear()
                    if entry_rows:
                        self.conn.executemany(
                            """INSERT INTO entries(session_id, seq, role, ts, header_line, blob_id)
                               VALUES (?,?,?,?,?,?)""",
                            entry_rows,
                        )
                    kept += 1
                    if (i + 1) % 50 == 0 or (i + 1) == len(sessions):
                        elapsed = time.time() - t0
                        print(
                            f"  … {i + 1}/{len(sessions)} "
                            f"kept={kept} skipped={skipped} "
                            f"blobs+={self.blobs_created} reuse={self.blobs_reused} "
                            f"({elapsed:.0f}s)",
                            flush=True,
                        )

            self.conn.execute(
                "UPDATE imports SET sessions_kept=?, sessions_skipped=? WHERE id=?",
                (kept, skipped, import_id),
            )
            self.conn.execute(
                "UPDATE blobs SET nrefs = (SELECT COUNT(*) FROM entries WHERE entries.blob_id = blobs.id)"
            )
            self.objects_conn.commit()
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            self.objects_conn.rollback()
            self._blob_cache.clear()
            self._load_blob_cache()
            raise
        print(f"  done import_id={import_id} kept={kept} skipped={skipped} in {time.time() - t0:.1f}s")
        return import_id

    def print_dedup_report(self) -> None:
        row = self.conn.execute(
            """
            SELECT
              COUNT(*) AS n_blobs,
              COALESCE(SUM(nbytes), 0) AS unique_bytes,
              COALESCE(SUM(nrefs), 0) AS total_refs,
              COALESCE(SUM(nbytes * nrefs), 0) AS logical_bytes
            FROM blobs
            """
        ).fetchone()
        n_sess = self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        n_ent = self.conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        n_skip = self.conn.execute("SELECT COUNT(*) FROM skipped").fetchone()[0]
        obj_bytes = int(self.objects_conn.execute("SELECT COALESCE(SUM(length(body)), 0) FROM objects").fetchone()[0])
        db_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0

        unique = int(row["unique_bytes"])
        logical = int(row["logical_bytes"])
        saved = logical - unique
        ratio = (saved / logical) if logical else 0.0

        print("\n========== DEDUP VERIFICATION ==========")
        print(f"sessions indexed:     {n_sess}")
        print(f"entries indexed:      {n_ent}")
        print(f"sessions skipped:     {n_skip}")
        print(f"unique blobs:         {row['n_blobs']}")
        print(f"logical body bytes:   {logical / 1e9:.3f} GB  (sum of all entry bodies)")
        print(f"unique body bytes:    {unique / 1e9:.3f} GB")
        print(f"bytes saved by CAS:   {saved / 1e9:.3f} GB  ({ratio:.1%} duplicate)")
        print(f"zstd objects on disk: {obj_bytes / 1e9:.3f} GB")
        print(f"index.sqlite size:    {db_bytes / 1e6:.1f} MB")
        print(f"roles: {dict(self.role_counts.most_common())}")

        print("\nTop duplicated blobs (refcount ≥ 2, by saved bytes):")
        top = self.conn.execute(
            """
            SELECT hash, nbytes, nrefs, (nbytes * (nrefs - 1)) AS saved
            FROM blobs
            WHERE nrefs >= 2
            ORDER BY saved DESC
            LIMIT 15
            """
        ).fetchall()
        if not top:
            print("  (none — no shared entry bodies found)")
        for r in top:
            print(
                f"  refs={r['nrefs']:4d}  body={r['nbytes'] / 1e6:7.2f}MB  "
                f"saved={r['saved'] / 1e6:7.2f}MB  {r['hash'][:16]}…"
            )

        print("\nFork-family signal (sessions sharing identical first 5 entry hashes):")
        # Build prefix signatures for primary sessions only
        rows = self.conn.execute(
            """
            SELECT s.id, s.person, s.source, s.uuid, s.git_branch, s.started_at,
                   GROUP_CONCAT(b.hash, '|') AS sig
            FROM sessions s
            JOIN entries e ON e.session_id = s.id
            JOIN blobs b ON b.id = e.blob_id
            WHERE s.is_subagent = 0 AND e.seq < 5
            GROUP BY s.id
            HAVING COUNT(*) >= 5
            """
        ).fetchall()
        families: dict[str, list] = defaultdict(list)
        for r in rows:
            families[r["sig"]].append(r)
        multi = sorted(
            (v for v in families.values() if len(v) >= 2),
            key=len,
            reverse=True,
        )
        print(f"  primary sessions with ≥5 entries: {len(rows)}")
        print(f"  fork families (shared first-5 body hashes): {len(multi)}")
        for fam in multi[:10]:
            people = Counter(x["person"] for x in fam)
            print(
                f"  family size={len(fam)} people={dict(people)} "
                f"eg uuid={fam[0]['uuid'][:8]} branch={fam[0]['git_branch']}"
            )

        # Per-person breakdown
        print("\nPer person:")
        for r in self.conn.execute(
            """
            SELECT person,
                   COUNT(*) AS sessions,
                   SUM(CASE WHEN is_subagent=1 THEN 1 ELSE 0 END) AS subagents,
                   SUM(char_count) AS chars
            FROM sessions
            GROUP BY person
            """
        ):
            print(
                f"  [{r['person']}] sessions={r['sessions']} "
                f"subagents={r['subagents']} char_count={(r['chars'] or 0) / 1e9:.2f}GB"
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Brain transcript archives into the local CAS index")
    ap.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"default: {DEFAULT_DATA_DIR}",
    )
    ap.add_argument(
        "--person",
        action="append",
        nargs=2,
        metavar=("NAME", "ZIP"),
        help="Corpus owner label and path to a Brain archive (repeatable)",
    )
    ap.add_argument(
        "--allowed-repository",
        action="append",
        default=None,
        help="Canonical host/owner/repository accepted during ingestion (repeatable)",
    )
    ap.add_argument(
        "--report-only",
        action="store_true",
        help="Only print dedup report for existing index",
    )
    ap.add_argument(
        "--report",
        action="store_true",
        help="Print the full dedup and fork-family report after ingestion",
    )
    ap.add_argument(
        "--no-fts",
        action="store_true",
        help="Skip FTS indexing during ingest (faster; rebuild later)",
    )
    args = ap.parse_args()

    args.data_dir.mkdir(parents=True, exist_ok=True)
    allowed_repositories = (
        frozenset(repository.lower() for repository in args.allowed_repository) if args.allowed_repository else None
    )
    ing = Ingestor(args.data_dir, reindex_fts=not args.no_fts, allowed_repositories=allowed_repositories)
    try:
        if args.report_only:
            ing.print_dedup_report()
            return
        if not args.person:
            print("At least one --person NAME ZIP pair is required", file=sys.stderr)
            sys.exit(1)

        for name, zpath in args.person:
            p = Path(zpath).expanduser().resolve()
            if not p.exists():
                print(f"missing zip: {p}", file=sys.stderr)
                sys.exit(1)
            ing.ingest_zip(name, p)

        if args.report:
            ing.print_dedup_report()
    finally:
        ing.close()


if __name__ == "__main__":
    main()
