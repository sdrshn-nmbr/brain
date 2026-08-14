from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import zstandard as zstd


@pytest.fixture
def corpus_dir(tmp_path: Path) -> Path:
    db = sqlite3.connect(tmp_path / "index.sqlite")
    db.executescript(
        """
        CREATE TABLE sessions (
            id INTEGER PRIMARY KEY,
            person TEXT NOT NULL,
            source TEXT NOT NULL,
            uuid TEXT NOT NULL,
            repository TEXT,
            git_branch TEXT,
            model TEXT,
            started_at TEXT,
            ended_at TEXT,
            entry_count INTEGER,
            char_count INTEGER,
            cwd TEXT,
            is_subagent INTEGER NOT NULL DEFAULT 0,
            preamble TEXT,
            session_fingerprint TEXT
        );
        CREATE TABLE blobs (
            id INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            nbytes INTEGER NOT NULL,
            nrefs INTEGER NOT NULL
        );
        CREATE TABLE entries (
            session_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            role TEXT NOT NULL,
            ts TEXT,
            header_line TEXT NOT NULL,
            blob_id INTEGER NOT NULL
        );
        CREATE INDEX idx_entries_blob ON entries(blob_id);
        CREATE VIRTUAL TABLE blobs_fts USING fts5(body, content='');
        """
    )
    sessions = [
        (
            1,
            "alice",
            "codex",
            "11111111-1111-1111-1111-111111111111",
            "github.com/acme/widget",
            "alice/branch",
            "gpt-test",
            "2026-08-10T10:00:00Z",
            "2026-08-10T11:00:00Z",
            1,
            28,
            "/work/widget",
            0,
            None,
            None,
        ),
        (
            2,
            "alice",
            "codex",
            "22222222-2222-2222-2222-222222222222",
            "github.com/acme/widget",
            "alice/branch",
            "gpt-test",
            "2026-08-10T10:00:00Z",
            "2026-08-10T11:00:00Z",
            1,
            28,
            "/work/widget",
            0,
            None,
            None,
        ),
        (
            3,
            "alice",
            "codex",
            "33333333-3333-3333-3333-333333333333",
            "github.com/acme/widget",
            "alice/branch",
            "gpt-test",
            "2026-08-10T12:00:00Z",
            "2026-08-10T13:00:00Z",
            1,
            100,
            "/work/widget",
            0,
            None,
            None,
        ),
        (
            4,
            "alice",
            "codex",
            "44444444-4444-4444-4444-444444444444",
            "github.com/acme/widget",
            "alice/branch",
            "gpt-test",
            "2026-08-10T14:00:00Z",
            "2026-08-10T15:00:00Z",
            1,
            100,
            "/work/widget",
            0,
            None,
            None,
        ),
    ]
    db.executemany("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", sessions)
    bodies = [
        "warehouse detection boundary",
        "warehouse detection boundary",
        "Vector index compaction directly demonstrates the relevant storage tradeoff between Vector and DCompaction",
        "deployment makes this broad search representative",
    ]
    roles = ["assistant", "tool_result", "assistant", "assistant"]
    for index, (body, role) in enumerate(zip(bodies, roles, strict=True), start=1):
        digest = chr(96 + index) * 64
        db.execute("INSERT INTO blobs VALUES (?, ?, ?, ?)", (index, digest, len(body), 1))
        db.execute(
            "INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?)",
            (index, 0, role, "2026-08-10T10:30:00Z", f"### [0000] role={role}", index),
        )
        db.execute("INSERT INTO blobs_fts(rowid, body) VALUES (?, ?)", (index, body))
    db.commit()
    db.close()

    objects = sqlite3.connect(tmp_path / "objects.sqlite")
    objects.execute("CREATE TABLE objects (hash TEXT PRIMARY KEY, body BLOB NOT NULL) WITHOUT ROWID")
    compressor = zstd.ZstdCompressor()
    for index, body in enumerate(bodies, start=1):
        objects.execute("INSERT INTO objects VALUES (?, ?)", (chr(96 + index) * 64, compressor.compress(body.encode())))
    objects.commit()
    objects.close()
    return tmp_path
