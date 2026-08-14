from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import uuid
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx2
import zstandard as zstd
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from collector.archive import (
    ArchiveEntry,
    object_member,
    session_fingerprint,
    sha256_bytes,
    stable_session_key,
    write_entries,
)
from collector.sync import sha256_file, upload_archive

EXPECTED_TOOLS = [
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


def auth_headers() -> dict[str, str]:
    token = os.environ.get("BRAIN_TOKEN", "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def build_smoke_archive(path: Path, repository: str, visibility: str) -> tuple[str, str, dict]:
    wrapper = "brain-smoke"
    session_uuid = str(uuid.uuid4())
    unique_term = f"brainsmoke{uuid.uuid4().hex}"
    body = f"Brain end-to-end verification {unique_term}"
    digest = sha256_bytes(body.encode())
    entry = ArchiveEntry(0, "user", None, None, "### [0000] role=user", digest)
    entries_path = f"{wrapper}/codex/{session_uuid}.entries.ndjson.zst"
    fingerprint = session_fingerprint("codex", session_uuid, [entry])
    started_at = datetime.now(UTC).isoformat()
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as zipped:
        write_entries(zipped, entries_path, [entry])
        zipped.writestr(object_member(wrapper, digest), zstd.ZstdCompressor().compress(body.encode()))
        zipped.writestr(
            f"{wrapper}/_manifest.json",
            json.dumps(
                {
                    "format_version": 2,
                    "export_timestamp": started_at,
                    "sessions": [
                        {
                            "source": "codex",
                            "uuid": session_uuid,
                            "session_fallback": session_uuid,
                            "session_key": stable_session_key("codex", session_uuid, started_at, session_uuid),
                            "session_fingerprint": fingerprint,
                            "repository": repository,
                            "started_at": started_at,
                            "ended_at": started_at,
                            "entry_count": 1,
                            "char_count": len(body),
                            "export_path": entries_path,
                            "entries_path": entries_path,
                        }
                    ],
                    "object_count": 1,
                },
                separators=(",", ":"),
            ),
        )
    scope = {
        "repositories": [repository],
        "sources": ["codex"],
        "since": None,
        "until": None,
        "sessionCount": 1,
        "visibility": visibility,
    }
    return session_uuid, unique_term, scope


async def run() -> None:
    url = os.environ.get("BRAIN_MCP_URL")
    if not url:
        raise RuntimeError("BRAIN_MCP_URL is required")
    commit_upload = os.environ.get("BRAIN_SMOKE_COMMIT") == "1"
    report: dict[str, object] = {}

    async with httpx2.AsyncClient(headers=auth_headers(), timeout=60.0) as http_client:
        transport = streamable_http_client(url, http_client=http_client)
        async with Client(transport) as client:
            tools = await client.list_tools()
            tool_names = sorted(tool.name for tool in tools.tools)
            if tool_names != EXPECTED_TOOLS:
                raise RuntimeError(f"Unexpected tools: {', '.join(tool_names)}")
            access_response = await client.call_tool("access", {})
            access = access_response.structured_content["result"]
            repositories = access.get("allowedRepositories") or []
            if not repositories:
                raise RuntimeError("Brain reports no allowed repositories")
            stats = await client.call_tool("stats", {})
            report.update(
                {
                    "protocolVersion": str(client.protocol_version),
                    "tools": tool_names,
                    "access": access,
                    "stats": stats.structured_content["result"],
                }
            )

            if access["accessLevel"] == "admin":
                request_stats = await client.call_tool("admin_request_stats", {})
                report["observedRequests"] = request_stats.structured_content["result"]["requestCount"]

            if access["accessLevel"] not in {"append", "admin"}:
                report["upload"] = "skipped: read-only identity"
                print(json.dumps(report, indent=2))
                return

            with tempfile.TemporaryDirectory(prefix="brain-upload-smoke-") as temporary:
                archive = Path(temporary) / "smoke.zip"
                session_uuid, unique_term, scope = build_smoke_archive(archive, repositories[0], access["visibility"])
                prepared_response = await client.call_tool(
                    "prepare_upload",
                    {
                        "archiveSha256": sha256_file(archive),
                        "archiveBytes": archive.stat().st_size,
                        "scope": scope,
                        "confirmedShared": True,
                    },
                )
                prepared = prepared_response.structured_content["result"]
                upload_archive(url, prepared["uploadPath"], archive)
                if not commit_upload:
                    cancelled = await client.call_tool("cancel_upload", {"uploadId": prepared["id"]})
                    if cancelled.structured_content["result"]["status"] != "cancelled":
                        raise RuntimeError("Transport smoke upload was not cancelled")
                    report["upload"] = "prepare-put-cancel"
                    print(json.dumps(report, indent=2))
                    return

                committed = await client.call_tool("commit_upload", {"uploadId": prepared["id"]})
                record = committed.structured_content["result"]
                deadline = time.monotonic() + 60
                while record["status"] in {"queued", "processing"} and time.monotonic() < deadline:
                    await asyncio.sleep(0.1)
                    status = await client.call_tool("upload_status", {"uploadId": prepared["id"]})
                    record = status.structured_content["result"]
                if record["status"] != "complete":
                    raise RuntimeError(f"Smoke ingestion ended in {record['status']}: {record.get('error')}")

                search = await client.call_tool(
                    "search",
                    {"query": unique_term, "repository": repositories[0], "limit": 5},
                )
                result = next(
                    (item for item in search.structured_content["result"] if item["uuid"] == session_uuid),
                    None,
                )
                if result is None:
                    raise RuntimeError("Committed smoke session was not searchable")
                session = await client.call_tool("read_session", {"sessionId": result["sessionId"]})
                if unique_term not in "".join(
                    entry["text"] for entry in session.structured_content["result"]["entries"]
                ):
                    raise RuntimeError("Committed smoke session could not be read back")
                report["upload"] = "prepare-put-commit-search-read"
                report["smokeSession"] = session_uuid
                print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(run())
