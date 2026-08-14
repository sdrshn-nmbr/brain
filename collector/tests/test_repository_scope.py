from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
from rich.console import Console

from collector.archive import ArchiveEntry, read_entries, write_entries
from collector.export import export_history
from collector.export.export_history import (
    Job,
    archive_id_slug,
    discover_jobs,
    parse_job,
    target_identifier,
    target_mtime,
)
from collector.export.repository_scope import normalize_repository_slug, resolve_repository
from collector.ingest.ingest import Ingestor


def test_exporter_is_self_contained_without_personal_past_skill(tmp_path: Path) -> None:
    exporter = Path(export_history.__file__)
    environment = {**os.environ, "HOME": str(tmp_path)}

    result = subprocess.run(
        [sys.executable, str(exporter), "--help"],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not (exporter.parents[1] / "sources" / "attachment_source.py").exists()


@pytest.mark.parametrize(
    ("remote", "expected"),
    [
        ("https://github.com/acme/widget", "github.com/acme/widget"),
        ("https://gitlab.com/acme/model.git", "gitlab.com/acme/model"),
        ("git@github.com:acme/widget.git", "github.com/acme/widget"),
        ("ssh://git@gitlab.com/acme/model.git", "gitlab.com/acme/model"),
        ("https://gitlab.example.com/acme/platform/model", "gitlab.example.com/acme/platform/model"),
        ("file:///tmp/widget", None),
    ],
)
def test_normalize_repository_slug(remote: str, expected: str | None) -> None:
    assert normalize_repository_slug(remote) == expected


def test_resolve_repository_uses_git_metadata(tmp_path: Path) -> None:
    repo = tmp_path / "renamed-local-folder"
    nested = repo / "src" / "deep"
    nested.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "remote",
            "add",
            "origin",
            "git@gitlab.com:acme/model.git",
        ],
        check=True,
    )

    identity = resolve_repository(str(nested))

    assert identity is not None
    assert identity.slug == "gitlab.com/acme/model"
    assert identity.root == str(repo.resolve())


def test_resolve_repository_rejects_missing_historical_path(tmp_path: Path) -> None:
    assert resolve_repository(str(tmp_path / "deleted-worktree")) is None


def test_archive_id_slug_distinguishes_claude_forks(tmp_path: Path) -> None:
    source = tmp_path / "source-record-id.jsonl"
    job = Job(source="claude", target=source, backend="jsonl")

    assert archive_id_slug(job, "parent-session-id") == "parent-session-id__source-source-record-id"


def test_codex_desktop_side_chats_are_export_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = {
        "session_id": "side-chat-id",
        "cwd": "/workspace/widget",
        "first_timestamp": 100.0,
        "last_timestamp": 200.0,
        "messages": [{"role": "user", "timestamp": 100.0, "text": "side-chat prompt"}],
    }
    monkeypatch.setattr(export_history.codex_source, "store_exists", lambda: True)
    monkeypatch.setattr(
        export_history.codex_source,
        "discover_export_targets",
        lambda include_archived: [],
    )
    monkeypatch.setattr(
        export_history.codex_source,
        "discover_side_chat_export_targets",
        lambda: [record],
    )

    jobs = discover_jobs({"codex"}, include_subagents=True, include_archived=True)["codex"]

    assert jobs == [Job(source="codex", target=record, backend="desktop-side-chat")]
    assert target_identifier(jobs[0]) == "sidechat:side-chat-id"
    assert target_mtime(jobs[0]) == 200.0


def test_codex_desktop_side_chat_uses_past_full_session_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = {"session_id": "side-chat-id", "cwd": "/workspace/widget"}
    parsed = SimpleNamespace(cwd="/workspace/widget")
    monkeypatch.setattr(
        export_history.codex_source,
        "parse_side_chat_export_target",
        lambda target: parsed,
    )
    monkeypatch.setattr(export_history, "resolve_repository", lambda cwd: None)

    outcome = parse_job(Job(source="codex", target=record, backend="desktop-side-chat"))

    assert outcome.session is parsed


def test_codex_desktop_side_chat_supports_original_past_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = {
        "session_id": "side-chat-id",
        "cwd": "/workspace/widget",
        "last_timestamp": 200.0,
    }
    parsed = SimpleNamespace(cwd="/workspace/widget")
    monkeypatch.setattr(export_history.codex_source, "store_exists", lambda: True)
    monkeypatch.setattr(
        export_history.codex_source,
        "discover_export_targets",
        lambda include_archived: [],
    )
    monkeypatch.setattr(export_history.codex_source, "discover_side_chat_export_targets", None)
    monkeypatch.setattr(export_history.codex_source, "parse_side_chat_export_target", None)
    monkeypatch.setattr(export_history.codex_source, "_desktop_side_chat_records", lambda: [record])
    monkeypatch.setattr(
        export_history.codex_source,
        "_side_chat_full_session",
        lambda target: parsed,
    )
    monkeypatch.setattr(export_history, "resolve_repository", lambda cwd: None)

    jobs = discover_jobs({"codex"}, include_subagents=True, include_archived=True)["codex"]
    outcome = parse_job(jobs[0])

    assert outcome.session is parsed


def test_export_writes_zstd_ndjson_and_unique_cas_objects(tmp_path: Path, monkeypatch) -> None:
    session = export_history.common.FullSession(
        source="codex",
        session_id="session-id",
        cwd=str(tmp_path),
        started_at="2026-08-10T00:00:00Z",
        entries=[
            export_history.common.FullEntry(0, "user", None, "same body"),
            export_history.common.FullEntry(1, "assistant", None, "same body"),
        ],
    )
    job = Job("codex", tmp_path / "session.jsonl", "jsonl")
    repository = export_history.RepositoryIdentity(
        "github.com/acme/widget", str(tmp_path), "git@github.com:acme/widget.git"
    )
    monkeypatch.setattr(
        export_history,
        "discover_jobs",
        lambda *_args, **_kwargs: {"codex": [job]},
    )
    monkeypatch.setattr(
        export_history,
        "parse_job",
        lambda _job: export_history.Outcome(job, session, repository),
    )
    output = tmp_path / "export.zip"
    args = SimpleNamespace(
        source="codex",
        exclude_subagents=False,
        exclude_archived=False,
        since=None,
        until=None,
        repository=["github.com/acme/widget"],
        project=None,
        output=str(output),
        workers=1,
    )
    export_history.run_export(args, Console(file=io.StringIO(), force_terminal=False))
    with ZipFile(output) as archive:
        names = archive.namelist()
        assert not any(name.endswith(".md") or name.endswith(".entries.json") for name in names)
        assert len([name for name in names if "/objects/" in name]) == 1
        manifest_name = next(name for name in names if name.endswith("_manifest.json"))
        manifest = json.loads(archive.read(manifest_name))
        assert manifest["format_version"] == 2
        assert manifest["object_count"] == 1
        assert manifest["sessions"][0]["entries_path"].endswith(".entries.ndjson.zst")


def test_structured_entries_preserve_sequence_gaps(tmp_path: Path) -> None:
    archive = tmp_path / "entries.zip"
    records = [
        ArchiveEntry(0, "user", None, None, "### [0000] role=user", "a" * 64),
        ArchiveEntry(2, "assistant", None, None, "### [0002] role=assistant", "b" * 64),
    ]
    with ZipFile(archive, "w") as zf:
        write_entries(zf, "entries.ndjson.zst", records)
    with ZipFile(archive) as zf:
        entries = read_entries(zf, "entries.ndjson.zst")

    assert [entry.seq for entry in entries] == [0, 2]


def test_ingestor_keeps_wal_artifacts_online(tmp_path: Path) -> None:
    ingestor = Ingestor(tmp_path)
    ingestor.close()

    connection = sqlite3.connect(f"file:{tmp_path / 'index.sqlite'}?mode=ro", uri=True)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute("PRAGMA table_info(sessions)").fetchall()
    finally:
        connection.close()
