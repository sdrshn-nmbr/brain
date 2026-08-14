from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import zstandard as zstd

from collector import sync
from collector.archive import ArchiveEntry, object_member, write_entries


def test_lightweight_mcp_call_accepts_json_and_sse(monkeypatch) -> None:
    captured = {}

    def urlopen(request, timeout):
        captured["accept"] = request.get_header("Accept")
        captured["timeout"] = timeout
        return io.BytesIO(json.dumps({"result": {"structuredContent": {"result": {"status": "ok"}}}}).encode())

    monkeypatch.setattr(sync.urllib.request, "urlopen", urlopen)
    assert sync.mcp_call("https://brain.example/mcp", "stats", {}) == {"status": "ok"}
    assert captured == {"accept": "application/json, text/event-stream", "timeout": 300}


def test_upload_scope_excludes_preview_only_metadata() -> None:
    scope = {
        "repositories": ["github.com/acme/widget"],
        "sources": ["codex"],
        "since": None,
        "until": None,
        "sessionCount": 1,
        "visibility": "team",
        "desktopSideChatCount": 3,
        "incompleteDesktopSideChatCount": 2,
    }

    assert sync.upload_scope(scope) == {
        "repositories": ["github.com/acme/widget"],
        "sources": ["codex"],
        "since": None,
        "until": None,
        "sessionCount": 1,
        "visibility": "team",
    }


def test_manifest_plan_builds_only_missing_sessions_and_objects(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "full.zip"
    wrapper = "export"
    first_hash = "a" * 64
    second_hash = "b" * 64
    first_fingerprint = "1" * 64
    second_fingerprint = "2" * 64
    sessions = []
    compressor = zstd.ZstdCompressor()
    with zipfile.ZipFile(source, "w", zipfile.ZIP_STORED) as archive:
        for index, (fingerprint, digest) in enumerate(
            [(first_fingerprint, first_hash), (second_fingerprint, second_hash)]
        ):
            entries_path = f"{wrapper}/codex/session-{index}.entries.ndjson.zst"
            write_entries(
                archive,
                entries_path,
                [ArchiveEntry(0, "assistant", None, None, "### [0000] role=assistant", digest)],
            )
            archive.writestr(object_member(wrapper, digest), compressor.compress(f"body-{index}".encode()))
            sessions.append(
                {
                    "source": "codex",
                    "uuid": f"session-{index}",
                    "repository": "github.com/acme/widget",
                    "session_fingerprint": fingerprint,
                    "export_path": entries_path,
                    "entries_path": entries_path,
                }
            )
        manifest = {"format_version": 2, "sessions": sessions, "object_count": 2}
        archive.writestr(f"{wrapper}/_manifest.json", json.dumps(manifest))

    def fake_mcp_call(_endpoint: str, name: str, _arguments: dict):
        if name == "plan_upload":
            return {"missingSessionFingerprints": [second_fingerprint], "presentSessionCount": 1}
        assert name == "missing_blobs"
        return {"missingBlobHashes": [second_hash], "presentBlobCount": 0}

    monkeypatch.setattr(sync, "mcp_call", fake_mcp_call)
    delta, profile = sync.plan_delta("https://brain.example/mcp", source, manifest)
    assert delta is not None
    try:
        delta_manifest = sync.read_manifest(delta)
        assert [session["uuid"] for session in delta_manifest["sessions"]] == ["session-1"]
        with zipfile.ZipFile(delta) as archive:
            assert object_member(wrapper, first_hash) not in archive.namelist()
            assert object_member(wrapper, second_hash) in archive.namelist()
        assert profile["sessionsSkipped"] == 1
        assert profile["objectsSkipped"] == 0
        assert profile["deltaBytes"] < profile["originalBytes"]
    finally:
        delta.unlink()


def test_adaptive_plan_reuses_and_never_deletes_original_archive(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "full.zip"
    wrapper = "export"
    digest = "a" * 64
    fingerprint = "1" * 64
    entries_path = f"{wrapper}/codex/session.entries.ndjson.zst"
    manifest = {
        "format_version": 2,
        "sessions": [
            {
                "source": "codex",
                "uuid": "session",
                "session_fingerprint": fingerprint,
                "entries_path": entries_path,
            }
        ],
        "object_count": 1,
    }
    with zipfile.ZipFile(source, "w", zipfile.ZIP_STORED) as archive:
        write_entries(
            archive,
            entries_path,
            [ArchiveEntry(0, "assistant", None, None, "### [0000] role=assistant", digest)],
        )
        archive.writestr(object_member(wrapper, digest), zstd.ZstdCompressor().compress(b"body"))
        archive.writestr(f"{wrapper}/_manifest.json", json.dumps(manifest))

    def fake_mcp_call(_endpoint: str, name: str, _arguments: dict):
        if name == "plan_upload":
            return {"missingSessionFingerprints": [fingerprint], "presentSessionCount": 0}
        assert name == "missing_blobs"
        return {"missingBlobHashes": [digest], "presentBlobCount": 0}

    monkeypatch.setattr(sync, "mcp_call", fake_mcp_call)
    selected, profile = sync.plan_delta("https://brain.example/mcp", source, manifest)

    assert selected == source
    assert profile["usedOriginalArchive"] is True
    sync.cleanup_delta(selected, profile["usedOriginalArchive"])
    assert source.exists()


def test_session_negotiation_batches_at_server_limit(tmp_path: Path) -> None:
    source = tmp_path / "full.zip"
    source.write_bytes(b"archive")
    manifest = {
        "format_version": 2,
        "sessions": [{"session_fingerprint": f"{index:064x}"} for index in range(10_001)],
        "object_count": 0,
    }
    batches: list[int] = []

    def fake_call(name: str, arguments: dict) -> dict:
        assert name == "plan_upload"
        batches.append(len(arguments["sessionFingerprints"]))
        return {"missingSessionFingerprints": [], "presentSessionCount": len(arguments["sessionFingerprints"])}

    selected, profile = sync.plan_delta("https://brain.example/mcp", source, manifest, call=fake_call)

    assert selected is None
    assert batches == [10_000, 1]
    assert profile["sessionsSkipped"] == 10_001
