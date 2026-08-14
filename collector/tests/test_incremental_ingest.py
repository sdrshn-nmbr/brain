from __future__ import annotations

import json
import sqlite3
import threading
import zipfile
from pathlib import Path
from typing import cast

import pytest
import zstandard as zstd

from brain.corpus import Corpus
from collector.archive import (
    FORMAT_VERSION,
    ArchiveEntry,
    object_member,
    session_fingerprint,
    sha256_bytes,
    stable_session_key,
    write_entries,
)
from collector.ingest.ingest import Ingestor


def write_archive(
    path: Path,
    uuid: str,
    bodies: list[str],
    *,
    session_key: str | None = None,
    ended_at: str = "2026-08-10T00:01:00Z",
) -> None:
    write_multi_archive(path, [(uuid, bodies, session_key, ended_at)])


def write_multi_archive(
    path: Path,
    specs: list[tuple[str, list[str], str | None, str]],
) -> None:
    wrapper = path.stem
    manifest_sessions = []
    archive_sessions = []
    for uuid, bodies, session_key, ended_at in specs:
        session_fallback = session_key or uuid
        entries_path = f"{wrapper}/codex/{uuid}.entries.ndjson.zst"
        entries = [
            ArchiveEntry(
                seq=index,
                role="assistant",
                timestamp=f"2026-08-10T00:00:0{index}Z",
                tool_name=None,
                header_line=f"### [{index:04d}] role=assistant",
                body_sha256=sha256_bytes(body.encode()),
            )
            for index, body in enumerate(bodies)
        ]
        manifest_sessions.append(
            {
                "source": "codex",
                "uuid": uuid,
                "session_fallback": session_fallback,
                "session_key": stable_session_key("codex", uuid, "2026-08-10T00:00:00Z", session_fallback),
                "session_fingerprint": session_fingerprint("codex", uuid, entries),
                "backend": "jsonl",
                "cwd": "/workspace/widget",
                "repository": "github.com/acme/widget",
                "started_at": "2026-08-10T00:00:00Z",
                "ended_at": ended_at,
                "entry_count": len(entries),
                "char_count": sum(map(len, bodies)),
                "export_path": entries_path,
                "entries_path": entries_path,
            }
        )
        archive_sessions.append((entries_path, entries, bodies))
    manifest = {
        "format_version": FORMAT_VERSION,
        "export_timestamp": "2026-08-10T00:00:00Z",
        "sessions": manifest_sessions,
    }
    compressor = zstd.ZstdCompressor(level=3)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr(f"{wrapper}/_manifest.json", json.dumps(manifest))
        written = set()
        for entries_path, entries, bodies in archive_sessions:
            write_entries(archive, entries_path, entries)
            for entry, body in zip(entries, bodies, strict=True):
                if entry.body_sha256 in written:
                    continue
                archive.writestr(object_member(wrapper, entry.body_sha256), compressor.compress(body.encode()))
                written.add(entry.body_sha256)


def rewrite_manifest(path: Path, mutate) -> None:
    replacement = path.with_suffix(".replacement.zip")
    with zipfile.ZipFile(path) as source, zipfile.ZipFile(replacement, "w", zipfile.ZIP_STORED) as output:
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename.endswith("_manifest.json"):
                manifest = json.loads(payload)
                mutate(manifest)
                payload = json.dumps(manifest).encode()
            output.writestr(info, payload)
    replacement.replace(path)


def test_incremental_wal_preserves_existing_cas_objects(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    write_archive(first, "11111111-1111-1111-1111-111111111111", ["shared body"])
    write_archive(second, "22222222-2222-2222-2222-222222222222", ["shared body", "new body"])

    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", first)
        ingestor.ingest_zip("alice", second)
    finally:
        ingestor.close()

    index = sqlite3.connect(data_dir / "index.sqlite")
    objects = sqlite3.connect(data_dir / "objects.sqlite")
    try:
        assert index.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert index.execute("PRAGMA synchronous").fetchone() == (2,)
        assert objects.execute("PRAGMA synchronous").fetchone() == (2,)
        assert objects.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 2
        assert index.execute("SELECT count(*) FROM blobs").fetchone()[0] == 2
        assert index.execute("SELECT max(nrefs) FROM blobs").fetchone()[0] == 2
        bodies = {
            zstd.ZstdDecompressor().decompress(row[0]).decode() for row in objects.execute("SELECT body FROM objects")
        }
        assert bodies == {"shared body", "new body"}
    finally:
        index.close()
        objects.close()


def test_archive_idempotency_is_scoped_to_person(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    archive = tmp_path / "shared.zip"
    write_archive(archive, "33333333-3333-3333-3333-333333333333", ["same archive body"])
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", archive)
        ingestor.ingest_zip("bob", archive)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM imports").fetchone()[0] == 2
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 2
        assert index.execute("SELECT count(*) FROM blobs").fetchone()[0] == 1
        assert index.execute("SELECT nrefs FROM blobs").fetchone()[0] == 2


def test_reexported_session_is_deduplicated_by_content(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "first-export.zip"
    second = tmp_path / "later-export.zip"
    session_uuid = "44444444-4444-4444-4444-444444444444"
    write_archive(first, session_uuid, ["same session body"])
    write_archive(second, session_uuid, ["same session body"])
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", first)
        second_import = ingestor.ingest_zip("alice", second)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1
        assert index.execute("SELECT reason FROM skipped WHERE import_id=?", (second_import,)).fetchone() == (
            "duplicate_session_content",
        )


def test_changed_session_replaces_previous_generation(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    uuid = "55555555-5555-5555-5555-555555555555"
    write_archive(first, uuid, ["old body"], session_key="stable-key")
    write_archive(second, uuid, ["new body"], session_key="stable-key", ended_at="2026-08-10T00:02:00Z")
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", first)
        ingestor.ingest_zip("alice", second)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1
        assert index.execute("SELECT nrefs FROM blobs ORDER BY hash").fetchall() in [
            [(0,), (1,)],
            [(1,), (0,)],
        ]


def test_readers_stay_online_until_atomic_index_commit(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    uuid = "66666666-6666-6666-6666-666666666666"
    write_archive(first, uuid, ["old generation"], session_key="stable-key")
    write_archive(second, uuid, ["new generation"], session_key="stable-key", ended_at="2026-08-10T00:02:00Z")
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", first)
    finally:
        ingestor.close()

    body_staged = threading.Event()
    allow_commit = threading.Event()
    original_store_blob = Ingestor.store_blob

    def paused_store_blob(self, zipped, wrapper, digest):
        result = original_store_blob(self, zipped, wrapper, digest)
        body_staged.set()
        assert allow_commit.wait(2)
        return result

    monkeypatch.setattr(Ingestor, "store_blob", paused_store_blob)
    failure: list[Exception] = []

    def ingest_changed_session() -> None:
        writer = Ingestor(data_dir)
        try:
            writer.ingest_zip("alice", second)
        except Exception as error:  # pragma: no cover - asserted through failure
            failure.append(error)
        finally:
            writer.close()

    thread = threading.Thread(target=ingest_changed_session)
    thread.start()
    assert body_staged.wait(2)
    corpus = Corpus(data_dir)
    assert corpus.search('"old generation"', person="alice")
    assert corpus.search('"new generation"', person="alice") == []
    allow_commit.set()
    thread.join(2)
    assert not thread.is_alive()
    assert failure == []
    assert corpus.search('"old generation"', person="alice") == []
    assert corpus.search('"new generation"', person="alice")


def test_failed_archive_does_not_expose_a_partial_import(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    archive_path = tmp_path / "invalid.zip"
    wrapper = "invalid"
    body = "body"
    digest = sha256_bytes(body.encode())
    valid = ArchiveEntry(0, "assistant", None, None, "### [0000] role=assistant", digest)
    invalid_entries = [valid, valid]
    entries_path = f"{wrapper}/codex/session.entries.ndjson.zst"
    manifest = {
        "format_version": FORMAT_VERSION,
        "sessions": [
            {
                "source": "codex",
                "uuid": "session",
                "session_fallback": "session",
                "session_key": stable_session_key("codex", "session", "2026-08-10T00:00:00Z", "session"),
                "session_fingerprint": session_fingerprint("codex", "session", invalid_entries),
                "cwd": "/workspace/widget",
                "repository": "github.com/acme/widget",
                "started_at": "2026-08-10T00:00:00Z",
                "ended_at": "2026-08-10T00:01:00Z",
                "export_path": entries_path,
                "entries_path": entries_path,
            }
        ],
    }
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_STORED) as archive:
        write_entries(archive, entries_path, invalid_entries)
        archive.writestr(object_member(wrapper, digest), zstd.ZstdCompressor().compress(body.encode()))
        archive.writestr(f"{wrapper}/_manifest.json", json.dumps(manifest))
    ingestor = Ingestor(data_dir)
    try:
        with pytest.raises(ValueError, match="duplicate or unordered"):
            ingestor.ingest_zip("alice", archive_path)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM imports").fetchone()[0] == 0
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0
        assert index.execute("SELECT count(*) FROM blobs").fetchone()[0] == 0


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest["sessions"][0].pop("session_fallback"),
        lambda manifest: manifest["sessions"][0].update(session_key="f" * 64),
    ],
)
def test_ingest_rejects_missing_or_forged_canonical_session_identity(tmp_path: Path, mutate) -> None:
    data_dir = tmp_path / "data"
    archive = tmp_path / "malformed.zip"
    write_archive(archive, "66666666-6666-6666-6666-666666666666", ["body"])
    rewrite_manifest(archive, mutate)

    ingestor = Ingestor(data_dir)
    try:
        with pytest.raises(ValueError, match="invalid canonical session key"):
            ingestor.ingest_zip("alice", archive)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0


def test_index_commit_failure_leaves_only_reusable_orphan_object(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    archive = tmp_path / "crash.zip"
    write_archive(archive, "77777777-7777-7777-7777-777777777777", ["survives crash"])
    ingestor = Ingestor(data_dir)
    real_connection = ingestor.conn

    class FailingCommit:
        def __getattr__(self, name):
            return getattr(real_connection, name)

        def commit(self):
            raise RuntimeError("injected index commit failure")

    ingestor.conn = cast(sqlite3.Connection, FailingCommit())
    try:
        with pytest.raises(RuntimeError, match="injected index commit failure"):
            ingestor.ingest_zip("alice", archive)
    finally:
        ingestor.close()

    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT count(*) FROM imports").fetchone()[0] == 0
        assert index.execute("SELECT count(*) FROM sessions").fetchone()[0] == 0
        assert index.execute("SELECT count(*) FROM blobs").fetchone()[0] == 0
    with sqlite3.connect(data_dir / "objects.sqlite") as objects:
        assert objects.execute("SELECT count(*) FROM objects").fetchone()[0] == 1

    retry = Ingestor(data_dir)
    try:
        retry.ingest_zip("alice", archive)
    finally:
        retry.close()
    assert Corpus(data_dir).search('"survives crash"', person="alice")


def test_stale_generation_cannot_replace_newer_session(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    old = tmp_path / "old.zip"
    new = tmp_path / "new.zip"
    stale = tmp_path / "stale.zip"
    uuid = "88888888-8888-8888-8888-888888888888"
    write_archive(old, uuid, ["old generation"], session_key="stable-key", ended_at="2026-08-10T00:01:00Z")
    write_archive(new, uuid, ["new generation"], session_key="stable-key", ended_at="2026-08-10T00:03:00Z")
    write_archive(stale, uuid, ["stale generation"], session_key="stable-key", ended_at="2026-08-10T00:02:00Z")
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", old)
        ingestor.ingest_zip("alice", new)
        stale_import = ingestor.ingest_zip("alice", stale)
    finally:
        ingestor.close()
    corpus = Corpus(data_dir)
    assert corpus.search('"new generation"', person="alice")
    assert corpus.search('"stale generation"', person="alice") == []
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute("SELECT reason FROM skipped WHERE import_id=?", (stale_import,)).fetchone() == (
            "stale_session_generation",
        )


def test_rejects_decompression_bomb_body(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    archive = tmp_path / "bomb.zip"
    write_archive(archive, "99999999-9999-9999-9999-999999999999", ["x" * (64 * 1024 * 1024 + 1)])
    ingestor = Ingestor(data_dir)
    try:
        with pytest.raises(ValueError, match="exceeds 64 MiB decompressed"):
            ingestor.ingest_zip("alice", archive)
    finally:
        ingestor.close()


def test_schema_v4_session_key_migration_allows_first_v2_export_to_replace(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    old = tmp_path / "old.zip"
    new = tmp_path / "new.zip"
    uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    write_archive(old, uuid, ["old migration generation"], ended_at="2026-08-10T00:01:00Z")
    write_archive(new, uuid, ["new migration generation"], ended_at="2026-08-10T00:02:00Z")
    ingestor = Ingestor(data_dir)
    try:
        ingestor.ingest_zip("alice", old)
    finally:
        ingestor.close()
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        index.execute("DROP INDEX idx_sessions_person_key")
        index.execute("UPDATE sessions SET session_key = NULL")
        index.execute("UPDATE meta SET value = '4' WHERE key = 'schema_version'")
        index.commit()

    migrated = Ingestor(data_dir)
    try:
        assert migrated.conn.execute("SELECT session_key FROM sessions").fetchone()[0] == stable_session_key(
            "codex", uuid, "2026-08-10T00:00:00Z", uuid
        )
        migrated.ingest_zip("alice", new)
    finally:
        migrated.close()
    corpus = Corpus(data_dir)
    assert corpus.search('"new migration generation"', person="alice")
    assert corpus.search('"old migration generation"', person="alice") == []


def test_replaying_superseded_multi_session_archive_never_deletes_live_sessions(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    original = tmp_path / "original.zip"
    replacement = tmp_path / "replacement.zip"
    first_uuid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    second_uuid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    write_multi_archive(
        original,
        [
            (first_uuid, ["original first"], "first-key", "2026-08-10T00:01:00Z"),
            (second_uuid, ["unrelated second"], "second-key", "2026-08-10T00:01:00Z"),
        ],
    )
    write_archive(
        replacement,
        first_uuid,
        ["replacement first"],
        session_key="first-key",
        ended_at="2026-08-10T00:02:00Z",
    )
    ingestor = Ingestor(data_dir)
    try:
        original_import = ingestor.ingest_zip("alice", original)
        ingestor.ingest_zip("alice", replacement)
        assert ingestor.ingest_zip("alice", original) == original_import
    finally:
        ingestor.close()
    corpus = Corpus(data_dir)
    assert corpus.search('"replacement first"', person="alice")
    assert corpus.search('"unrelated second"', person="alice")
    assert corpus.search('"original first"', person="alice") == []
    with sqlite3.connect(data_dir / "index.sqlite") as index:
        assert index.execute(
            "SELECT sessions_kept, sessions_skipped FROM imports WHERE id=?", (original_import,)
        ).fetchone() == (1, 1)
