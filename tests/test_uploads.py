from __future__ import annotations

import asyncio
import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from starlette.requests import Request

from brain.uploads import UploadManager, person_from_principal
from collector.archive import stable_session_key


def archive_scope() -> dict:
    return {
        "repositories": ["github.com/acme/widget"],
        "sources": ["codex"],
        "since": "2026-08-01",
        "until": None,
        "sessionCount": 3,
        "visibility": "team",
    }


def valid_archive(session_overrides: dict | None = None) -> bytes:
    manifest = {
        "format_version": 2,
        "sessions": [
            {
                "source": "codex",
                "uuid": f"session-{index}",
                "session_fallback": f"session-{index}",
                "session_key": stable_session_key(
                    "codex", f"session-{index}", "2026-08-02T00:00:00Z", f"session-{index}"
                ),
                "repository": "github.com/acme/widget",
                "session_fingerprint": f"{index:064x}",
                "started_at": "2026-08-02T00:00:00Z",
                "ended_at": "2026-08-02T01:00:00Z",
            }
            | (session_overrides or {})
            for index in range(1, 4)
        ],
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("export/_manifest.json", json.dumps(manifest))
    return output.getvalue()


def request_with_body(body: bytes) -> Request:
    sent = False

    async def receive() -> dict:
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "PUT",
            "path": "/upload",
            "headers": [(b"content-length", str(len(body)).encode())],
        },
        receive,
    )


def paused_request(body: bytes, entered: asyncio.Event, release: asyncio.Event) -> Request:
    sent = False

    async def receive() -> dict:
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        entered.set()
        await release.wait()
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "PUT",
            "path": "/upload",
            "headers": [(b"content-length", str(len(body)).encode())],
        },
        receive,
    )


@pytest.fixture
def manager(tmp_path: Path):
    calls: list[tuple[str, Path, str]] = []

    async def ingest(person: str, path: Path, digest: str) -> dict[str, int]:
        calls.append((person, path, digest))
        return {"importId": 7, "sessionsKept": 3, "sessionsSkipped": 0}

    upload_manager = UploadManager(
        tmp_path,
        1024 * 1024,
        2 * 1024 * 1024,
        7 * 24 * 60 * 60,
        60,
        frozenset({"github.com/acme/widget"}),
        "team",
        ingest,
    )
    yield upload_manager, calls
    upload_manager.close()


def test_derives_person_from_principal() -> None:
    assert person_from_principal("Alice@Example.com") == "alice"


def test_enforces_pending_archive_bytes_per_owner(tmp_path: Path) -> None:
    async def ingest(_person: str, _path: Path, _digest: str) -> dict[str, int]:
        return {"importId": 1, "sessionsKept": 0, "sessionsSkipped": 0}

    upload_manager = UploadManager(tmp_path, 1024, 150, 3600, 60, frozenset({"github.com/acme/widget"}), "team", ingest)
    try:
        upload_manager.prepare("alice@example.com", "a" * 64, 100, archive_scope(), True)
        with pytest.raises(ValueError, match="per-owner limit"):
            upload_manager.prepare("alice@example.com", "b" * 64, 51, archive_scope(), True)
        upload_manager.prepare("bob@example.com", "b" * 64, 100, archive_scope(), True)
    finally:
        upload_manager.close()


def test_expires_abandoned_archives_and_releases_quota(tmp_path: Path) -> None:
    async def ingest(_person: str, _path: Path, _digest: str) -> dict[str, int]:
        return {"importId": 1, "sessionsKept": 0, "sessionsSkipped": 0}

    upload_manager = UploadManager(tmp_path, 1024, 100, 60, 10, frozenset({"github.com/acme/widget"}), "team", ingest)
    try:
        prepared = upload_manager.prepare("alice@example.com", "a" * 64, 100, archive_scope(), True)
        archive_path = upload_manager.uploads_dir / f"{prepared['id']}.zip"
        archive_path.write_bytes(b"abandoned")
        stale = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
        upload_manager.db.execute(
            "UPDATE uploads SET status = 'receiving', updated_at = ? WHERE id = ?", (stale, prepared["id"])
        )
        upload_manager.db.commit()

        removed = upload_manager.garbage_collect()
        assert removed == {"uploads": 1, "bytes": 100}
        assert not archive_path.exists()
        assert upload_manager.status("alice@example.com", prepared["id"])["status"] == "expired"
        upload_manager.prepare("alice@example.com", "b" * 64, 100, archive_scope(), True)
    finally:
        upload_manager.close()


def test_persists_distinct_people_for_colliding_login_labels(manager) -> None:
    upload_manager, _calls = manager
    first = upload_manager.person_for("a.b@example.com")
    second = upload_manager.person_for("a+b@example.com")
    assert first == "a-b"
    assert second.startswith("a-b-")
    assert second != first
    assert upload_manager.person_for("A+B@example.com") == second


async def test_streams_verifies_and_ingests_archive(manager) -> None:
    upload_manager, calls = manager
    body = valid_archive()
    digest = hashlib.sha256(body).hexdigest()
    prepared = upload_manager.prepare("alice@example.com", digest, len(body), archive_scope(), True)
    uploaded = await upload_manager.receive(
        "alice@example.com",
        prepared["id"],
        request_with_body(body),
    )
    assert uploaded["status"] == "uploaded"
    assert upload_manager.commit("alice@example.com", prepared["id"])["status"] == "queued"
    await upload_manager.wait()
    completed = upload_manager.status("alice@example.com", prepared["id"])
    assert completed["status"] == "complete"
    assert completed["phase"] == "complete"
    assert completed["phaseDetail"] == "index commit visible"
    assert completed["result"] == {"importId": 7, "sessionsKept": 3, "sessionsSkipped": 0}
    assert len(calls) == 1


async def test_rejects_wrong_owner_and_checksum(manager) -> None:
    upload_manager, _calls = manager
    body = b"expected"
    digest = hashlib.sha256(body).hexdigest()
    prepared = upload_manager.prepare("alice@example.com", digest, len(body), archive_scope(), True)
    with pytest.raises(ValueError, match="authenticated user"):
        upload_manager.status("bob@example.com", prepared["id"])
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        await upload_manager.receive(
            "alice@example.com",
            prepared["id"],
            request_with_body(b"tampered"),
        )


async def test_wrong_content_length_releases_receive_claim(manager) -> None:
    upload_manager, _calls = manager
    body = valid_archive()
    prepared = upload_manager.prepare(
        "alice@example.com", hashlib.sha256(body).hexdigest(), len(body), archive_scope(), True
    )
    with pytest.raises(ValueError, match="Content-Length"):
        await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body + b"x"))
    assert upload_manager.status("alice@example.com", prepared["id"])["status"] == "prepared"


async def test_rejects_archive_that_does_not_match_confirmed_scope(manager) -> None:
    upload_manager, _calls = manager
    body = valid_archive()
    scope = {**archive_scope(), "sources": ["claude"]}
    prepared = upload_manager.prepare("alice@example.com", hashlib.sha256(body).hexdigest(), len(body), scope, True)
    with pytest.raises(ValueError, match="sources do not match"):
        await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body))


async def test_rejects_archive_outside_confirmed_time_bounds(manager) -> None:
    upload_manager, _calls = manager
    body = valid_archive()
    scope = {**archive_scope(), "since": "2026-08-03"}
    prepared = upload_manager.prepare("alice@example.com", hashlib.sha256(body).hexdigest(), len(body), scope, True)
    with pytest.raises(ValueError, match="before the confirmed since"):
        await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body))


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"session_fallback": None}, "identity metadata"),
        ({"session_key": "f" * 64}, "canonical identity"),
    ],
)
async def test_rejects_missing_or_forged_canonical_session_identity(manager, overrides, error) -> None:
    upload_manager, _calls = manager
    body = valid_archive(overrides)
    prepared = upload_manager.prepare(
        "alice@example.com", hashlib.sha256(body).hexdigest(), len(body), archive_scope(), True
    )
    with pytest.raises(ValueError, match=error):
        await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body))


async def test_only_one_concurrent_put_can_claim_an_upload(manager) -> None:
    upload_manager, _calls = manager
    body = valid_archive()
    prepared = upload_manager.prepare(
        "alice@example.com", hashlib.sha256(body).hexdigest(), len(body), archive_scope(), True
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    first = asyncio.create_task(
        upload_manager.receive("alice@example.com", prepared["id"], paused_request(body, entered, release))
    )
    await entered.wait()
    assert upload_manager.status("alice@example.com", prepared["id"])["status"] == "receiving"
    with pytest.raises(ValueError, match="status is receiving"):
        await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body))
    release.set()
    assert (await first)["status"] == "uploaded"


async def test_cancelled_receive_releases_claim_and_quota(manager) -> None:
    upload_manager, _calls = manager
    body = valid_archive()
    prepared = upload_manager.prepare(
        "alice@example.com", hashlib.sha256(body).hexdigest(), len(body), archive_scope(), True
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    receive = asyncio.create_task(
        upload_manager.receive("alice@example.com", prepared["id"], paused_request(body, entered, release))
    )
    await entered.wait()
    receive.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receive
    assert upload_manager.status("alice@example.com", prepared["id"])["status"] == "prepared"
    assert prepared["id"] not in upload_manager._active_receives


async def test_cancel_wins_before_drain_claim_without_resurrection(manager) -> None:
    upload_manager, calls = manager
    body = valid_archive()
    prepared = upload_manager.prepare(
        "alice@example.com", hashlib.sha256(body).hexdigest(), len(body), archive_scope(), True
    )
    await upload_manager.receive("alice@example.com", prepared["id"], request_with_body(body))
    assert upload_manager.commit("alice@example.com", prepared["id"])["status"] == "queued"
    assert upload_manager.cancel("alice@example.com", prepared["id"])["status"] == "cancelled"
    await upload_manager.wait()
    assert calls == []
    assert upload_manager.status("alice@example.com", prepared["id"])["status"] == "cancelled"


def test_prepare_is_idempotent_and_requires_confirmation(manager) -> None:
    upload_manager, _calls = manager
    body = b"same archive"
    digest = hashlib.sha256(body).hexdigest()
    first = upload_manager.prepare("alice@example.com", digest, len(body), archive_scope(), True)
    second = upload_manager.prepare("alice@example.com", digest, len(body), archive_scope(), True)
    assert second["id"] == first["id"]
    upload_manager.cancel("alice@example.com", first["id"])
    assert upload_manager.prepare("alice@example.com", digest, len(body), archive_scope(), True)["status"] == "prepared"
    with pytest.raises(ValueError, match="confirm publication"):
        upload_manager.prepare("bob@example.com", digest, len(body), archive_scope(), False)
