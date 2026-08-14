from __future__ import annotations

import asyncio
import hashlib
import io
import json
import socket
import threading
import zipfile
from pathlib import Path

import httpx
import httpx2
import pytest
import uvicorn
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver import Context

from brain.auth import AccessLevel, TokenCredential
from brain.config import Config
from brain.observability import RequestLog
from brain.server import create_app, create_server
from collector.archive import stable_session_key


def server_config(data_dir: Path, port: int) -> Config:
    credentials = tuple(
        TokenCredential(hashlib.sha256(token.encode()).hexdigest(), principal, access)
        for token, principal, access in (
            ("append-token", "alice@example.com", AccessLevel.APPEND),
            ("read-token", "workload:cloud-agent", AccessLevel.READ),
            ("admin-token", "admin@example.com", AccessLevel.ADMIN),
        )
    )
    return Config(
        data_dir=data_dir,
        host="127.0.0.1",
        port=port,
        allowed_hosts=["localhost", "127.0.0.1", "[::1]"],
        allowed_repositories=frozenset({"github.com/acme/widget"}),
        visibility="team",
        auth_mode="token",
        token_credentials=credentials,
        trusted_principal_header="X-Brain-Principal",
        trusted_access_header="X-Brain-Access",
        trusted_name_header="X-Brain-Name",
        tailscale_allowed_users=None,
        tailscale_admin_users=frozenset(),
        tailscale_app_capability="brain.example/cap/read",
        max_upload_bytes=1024 * 1024,
        max_pending_bytes_per_owner=1024 * 1024,
        upload_ttl_seconds=7 * 24 * 60 * 60,
        upload_receive_timeout_seconds=60,
        request_log_retention=1000,
        max_mcp_request_bytes=1024 * 1024,
        search_timeout_seconds=5,
        python_executable="python3",
        ingest_script=data_dir / "unused-ingest.py",
    )


def upload_archive_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr(
            "export/_manifest.json",
            json.dumps(
                {
                    "format_version": 2,
                    "sessions": [
                        {
                            "source": "codex",
                            "uuid": "server-smoke",
                            "session_fallback": "server-smoke",
                            "session_key": stable_session_key(
                                "codex", "server-smoke", "2026-08-10T00:00:00Z", "server-smoke"
                            ),
                            "repository": "github.com/acme/widget",
                            "session_fingerprint": "f" * 64,
                            "started_at": "2026-08-10T00:00:00Z",
                            "ended_at": "2026-08-10T01:00:00Z",
                        }
                    ],
                }
            ),
        )
    return output.getvalue()


async def test_remote_mcp_client_auth_search_and_read(corpus_dir: Path) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    app = create_app(server_config(corpus_dir, port))
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started

        async with httpx.AsyncClient() as unauthenticated:
            response = await unauthenticated.post(f"http://127.0.0.1:{port}/mcp", json={})
            assert response.status_code == 401

        append_headers = {"Authorization": "Bearer append-token"}
        async with httpx2.AsyncClient(headers=append_headers) as http_client:
            transport = streamable_http_client(f"http://127.0.0.1:{port}/mcp", http_client=http_client)
            async with Client(transport) as client:
                tools = await client.list_tools()
                assert sorted(tool.name for tool in tools.tools) == [
                    "access",
                    "admin_request_stats",
                    "admin_requests",
                    "browse",
                    "cancel_upload",
                    "commit_upload",
                    "list_my_uploads",
                    "missing_blobs",
                    "plan_upload",
                    "prepare_upload",
                    "read_session",
                    "search",
                    "stats",
                    "upload_status",
                ]
                fingerprint = "f" * 64
                access = await client.call_tool("access", {})
                assert access.structured_content["result"]["accessLevel"] == "append"
                assert access.structured_content["result"]["allowedRepositories"] == ["github.com/acme/widget"]
                assert access.structured_content["result"]["visibility"] == "team"
                planned = await client.call_tool("plan_upload", {"sessionFingerprints": [fingerprint]})
                assert planned.structured_content["result"]["missingSessionFingerprints"] == [fingerprint]
                blobs = await client.call_tool("missing_blobs", {"blobHashes": ["a" * 64, "f" * 64]})
                assert blobs.structured_content["result"]["missingBlobHashes"] == ["f" * 64]
                search = await client.call_tool(
                    "search",
                    {
                        "query": "detection boundary",
                        "person": "alice",
                        "repository": "github.com/acme/widget",
                    },
                )
                results = search.structured_content["result"]
                assert results[0]["sessionId"] == 1
                session = await client.call_tool("read_session", {"sessionId": 1, "limit": 2})
                assert session.structured_content["result"]["returnedEntries"] == 1

                archive = upload_archive_bytes()
                prepared = await client.call_tool(
                    "prepare_upload",
                    {
                        "archiveSha256": hashlib.sha256(archive).hexdigest(),
                        "archiveBytes": len(archive),
                        "scope": {
                            "repositories": ["github.com/acme/widget"],
                            "sources": ["codex"],
                            "since": "2026-08-10",
                            "until": None,
                            "sessionCount": 1,
                            "visibility": "team",
                        },
                        "confirmedShared": True,
                    },
                )
                upload = prepared.structured_content["result"]
                async with httpx.AsyncClient(headers=append_headers) as uploader:
                    response = await uploader.put(
                        f"http://127.0.0.1:{port}{upload['uploadPath']}",
                        content=archive,
                    )
                    assert response.status_code == 200
                    assert response.json()["status"] == "uploaded"
                committed = await client.call_tool("commit_upload", {"uploadId": upload["id"]})
                assert committed.is_error is False
                assert committed.structured_content["result"]["status"] in {"queued", "processing", "failed"}

        async with httpx.AsyncClient(headers=append_headers) as oversized:
            response = await oversized.post(
                f"http://127.0.0.1:{port}/mcp",
                json={"method": "tools/call", "params": {"name": "search", "arguments": {"query": "x" * 1_100_000}}},
            )
            assert response.status_code == 413

            async def chunked_body():
                for _ in range(5):
                    yield b"x" * 300_000

            response = await oversized.post(
                f"http://127.0.0.1:{port}/mcp",
                content=chunked_body(),
                headers={"Content-Type": "application/json"},
            )
            assert response.status_code == 413

        async with httpx2.AsyncClient(headers={"Authorization": "Bearer read-token"}) as http_client:
            transport = streamable_http_client(f"http://127.0.0.1:{port}/mcp", http_client=http_client)
            async with Client(transport) as client:
                access = await client.call_tool("access", {})
                result = access.structured_content["result"]
                assert result["actor"] == "workload:cloud-agent"
                assert result["accessLevel"] == "read"
                assert result["tools"] == ["access", "search", "browse", "read_session", "stats"]
                stats = await client.call_tool("stats", {})
                assert stats.is_error is False
                rejected = await client.call_tool("missing_blobs", {"blobHashes": ["a" * 64]})
                assert rejected.is_error is True
                assert "Append access" in rejected.content[0].text

        async with httpx2.AsyncClient(headers={"Authorization": "Bearer admin-token"}) as http_client:
            transport = streamable_http_client(f"http://127.0.0.1:{port}/mcp", http_client=http_client)
            async with Client(transport) as client:
                records = await client.call_tool("admin_requests", {"actor": "workload:cloud-agent"})
                assert records.is_error is False
                workload_records = records.structured_content["result"]
                assert {record["mcpName"] for record in workload_records} >= {"stats", "missing_blobs"}
                missing_blobs_record = next(
                    record for record in workload_records if record["mcpName"] == "missing_blobs"
                )
                assert missing_blobs_record["errorCode"] == "mcp_tool_error"
                search_records = await client.call_tool("admin_requests", {"name": "search"})
                search_arguments = search_records.structured_content["result"][0]["arguments"]
                assert search_arguments["queryChars"] == len("detection boundary")
                assert "detection boundary" not in str(search_arguments)
                summary = await client.call_tool("admin_request_stats", {})
                assert summary.structured_content["result"]["requestCount"] >= 4

        async with httpx2.AsyncClient(headers=append_headers) as http_client:
            transport = streamable_http_client(f"http://127.0.0.1:{port}/mcp", http_client=http_client)
            async with Client(transport) as client:
                rejected = await client.call_tool("admin_requests", {})
                assert rejected.is_error is True
                assert "administrator" in rejected.content[0].text
    finally:
        server.should_exit = True
        await task


async def test_cancelled_mcp_search_interrupts_worker(
    corpus_dir: Path,
    monkeypatch,
) -> None:
    started = threading.Event()
    interrupted = threading.Event()

    class BlockingCorpus:
        def search(self, *, cancel_event: threading.Event, **_kwargs):
            started.set()
            cancel_event.wait(timeout=2)
            if cancel_event.is_set():
                interrupted.set()
            return []

    class BlockingStore:
        def read(self):
            return BlockingCorpus()

    monkeypatch.setattr("brain.server.identity_from_context", lambda _ctx, _config: "alice@example.com")
    request_log = RequestLog(corpus_dir, 100)
    mcp = create_server(server_config(corpus_dir, 1), BlockingStore(), object(), request_log)
    invocation = asyncio.create_task(mcp.call_tool("search", {"query": "deployment"}, Context(mcp_server=mcp)))
    assert await asyncio.to_thread(started.wait, 1)
    invocation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await invocation
    assert await asyncio.to_thread(interrupted.wait, 1)
    request_log.close()
