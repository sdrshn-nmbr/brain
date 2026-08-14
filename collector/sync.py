#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from collector.archive import iter_session_hashes, object_member

DEFAULT_ENDPOINT = os.environ.get("BRAIN_MCP_URL", "http://127.0.0.1:8788/mcp")
SOURCES = ("claude", "codex", "cursor")


def request_headers() -> dict[str, str]:
    token = os.environ.get("BRAIN_TOKEN", "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


def endpoint_connection(endpoint: str) -> tuple[http.client.HTTPConnection, str, str]:
    origin = urlsplit(endpoint)
    if not origin.hostname or origin.scheme not in {"http", "https"}:
        raise ValueError("Brain endpoint must be an HTTP or HTTPS URL")
    if origin.scheme == "http" and origin.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Remote Brain endpoints must use HTTPS")
    connection_type = http.client.HTTPSConnection if origin.scheme == "https" else http.client.HTTPConnection
    port = origin.port or (443 if origin.scheme == "https" else 80)
    return connection_type(origin.hostname, port, timeout=300), origin.netloc, origin.path or "/mcp"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(archive: Path) -> dict:
    with zipfile.ZipFile(archive) as zipped:
        manifest_name = next(
            (name for name in zipped.namelist() if name.endswith("_manifest.json")),
            None,
        )
        if not manifest_name:
            raise ValueError("export archive has no _manifest.json")
        manifest = json.loads(zipped.read(manifest_name))
    if not isinstance(manifest, dict):
        raise ValueError("export manifest must be an object")
    return manifest


def mcp_call(endpoint: str, name: str, arguments: dict) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {
                    "name": "brain-sync",
                    "version": "1.1.0",
                },
                "io.modelcontextprotocol/clientCapabilities": {},
            },
        },
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": name,
            **request_headers(),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            message = json.load(response)
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        raise RuntimeError(f"MCP {name} failed with HTTP {error.code}: {body}") from error
    return decode_mcp_response(name, message)


def decode_mcp_response(name: str, message: dict) -> dict:
    if "error" in message:
        raise RuntimeError(f"MCP {name} failed: {json.dumps(message['error'])}")
    tool_result = message.get("result") or {}
    if tool_result.get("isError"):
        raise RuntimeError(f"MCP {name} returned an error: {json.dumps(tool_result)}")
    result = (tool_result.get("structuredContent") or {}).get("result")
    if not isinstance(result, dict) and not isinstance(result, list):
        raise RuntimeError(f"MCP {name} returned no structured result")
    return result


class MCPClient:
    def __init__(self, endpoint: str) -> None:
        self.connection, self.host, self.path = endpoint_connection(endpoint)

    def __enter__(self) -> MCPClient:
        return self

    def __exit__(self, *_args) -> None:
        self.connection.close()

    def call(self, name: str, arguments: dict) -> dict:
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": arguments,
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientInfo": {"name": "brain-sync", "version": "1.1.0"},
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                },
            },
            separators=(",", ":"),
        ).encode()
        self.connection.request(
            "POST",
            self.path,
            body=payload,
            headers={
                "Host": self.host,
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2026-07-28",
                "Mcp-Method": "tools/call",
                "Mcp-Name": name,
                **request_headers(),
            },
        )
        response = self.connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"MCP {name} failed with HTTP {response.status}: {body.decode(errors='replace')}")
        return decode_mcp_response(name, json.loads(body))


def upload_archive(endpoint: str, upload_path: str, archive: Path) -> None:
    connection, host, _path = endpoint_connection(endpoint)
    size = archive.stat().st_size
    connection.putrequest("PUT", upload_path, skip_host=True)
    connection.putheader("Host", host)
    connection.putheader("Content-Type", "application/zip")
    connection.putheader("Content-Length", str(size))
    for name, value in request_headers().items():
        connection.putheader(name, value)
    connection.endheaders()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            connection.send(chunk)
    response = connection.getresponse()
    body = response.read().decode(errors="replace")
    connection.close()
    if response.status != 200:
        raise RuntimeError(f"archive upload failed with HTTP {response.status}: {body}")


def run_export(args: argparse.Namespace, output: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "collector.export.export_history",
        "--output",
        str(output),
        "--source",
        args.source,
    ]
    for repository in args.repository or []:
        command.extend(["--repository", repository])
    for option in ("project", "since", "until"):
        value = getattr(args, option)
        if value:
            command.extend([f"--{option}", value])
    if args.exclude_subagents:
        command.append("--exclude-subagents")
    if args.exclude_archived:
        command.append("--exclude-archived")
    command.extend(["--workers", str(args.workers)])
    subprocess.run(command, check=True)


def scope_from_manifest(manifest: dict, visibility: str = "organization") -> dict:
    sessions = manifest.get("sessions") or []
    filters = manifest.get("filters") or {}
    repositories = sorted({str(session.get("repository")) for session in sessions if session.get("repository")})
    sources = sorted({str(session.get("source")) for session in sessions if session.get("source")})
    if any(source not in SOURCES for source in sources):
        raise ValueError("manifest contains an unsupported source")
    desktop_side_chats = [session for session in sessions if session.get("backend") == "desktop-side-chat"]
    incomplete_side_chats = [session for session in desktop_side_chats if not session.get("assistant_text_available")]
    return {
        "repositories": repositories,
        "sources": sources,
        "since": filters.get("since"),
        "until": filters.get("until"),
        "sessionCount": len(sessions),
        "desktopSideChatCount": len(desktop_side_chats),
        "incompleteDesktopSideChatCount": len(incomplete_side_chats),
        "visibility": visibility,
    }


def upload_scope(scope: dict) -> dict:
    fields = ("repositories", "sources", "since", "until", "sessionCount", "visibility")
    return {field: scope[field] for field in fields}


def print_preview(scope: dict, manifest: dict, archive: Path) -> None:
    totals = manifest.get("totals") or {}
    print("\nBrain publication preview")
    print(f"  repositories: {', '.join(scope['repositories'])}")
    print(f"  sources: {', '.join(scope['sources'])}")
    print(f"  since: {scope['since'] or 'all history'}")
    print(f"  until: {scope['until'] or 'now'}")
    print(f"  sessions: {scope['sessionCount']}")
    if scope["desktopSideChatCount"]:
        print(f"  Codex Desktop side chats: {scope['desktopSideChatCount']}")
    if scope["incompleteDesktopSideChatCount"]:
        print(
            "  WARNING: Codex Desktop does not persist assistant text for "
            f"{scope['incompleteDesktopSideChatCount']} side chat(s); only persisted "
            "prompts and tool calls are included"
        )
    print(f"  archive: {archive} ({archive.stat().st_size / 1e6:.1f} MB)")
    for source in scope["sources"]:
        source_totals = totals.get(source) or {}
        print(
            f"  {source}: exported={source_totals.get('exported', 0)} "
            f"failed={source_totals.get('failed', 0)} "
            f"repository-skipped={source_totals.get('skipped_by_repository', 0)} "
            f"filter-skipped={source_totals.get('skipped_by_filter', 0)}"
        )
    print(f"  visibility: {scope['visibility']}")


def build_delta_archive(
    source_path: Path,
    destination: Path,
    manifest: dict,
    missing_session_fingerprints: set[str],
    missing_blob_hashes: set[str],
) -> tuple[dict, int]:
    selected_sessions = [
        session
        for session in manifest.get("sessions") or []
        if session.get("session_fingerprint") in missing_session_fingerprints
    ]
    delta_manifest = {
        **manifest,
        "sessions": selected_sessions,
        "object_count": len(missing_blob_hashes),
    }
    with zipfile.ZipFile(source_path) as source:
        manifest_name = next(name for name in source.namelist() if name.endswith("_manifest.json"))
        wrapper = manifest_name.rsplit("/", 1)[0]
        with zipfile.ZipFile(destination, "w", zipfile.ZIP_STORED) as output:
            for session in selected_sessions:
                entries_path = str(session["entries_path"])
                output.writestr(entries_path, source.read(entries_path))
            for digest in missing_blob_hashes:
                member = object_member(wrapper, digest)
                output.writestr(member, source.read(member))
            output.writestr(manifest_name, json.dumps(delta_manifest, ensure_ascii=False, separators=(",", ":")))
    return delta_manifest, len(selected_sessions)


def plan_delta(
    endpoint: str,
    archive: Path,
    manifest: dict,
    call: Callable[[str, dict], dict] | None = None,
) -> tuple[Path | None, dict]:
    invoke = call or (lambda name, arguments: mcp_call(endpoint, name, arguments))
    sessions = manifest.get("sessions") or []
    fingerprints = [str(session.get("session_fingerprint") or "") for session in sessions]
    if any(len(value) != 64 for value in fingerprints):
        raise ValueError("archive does not contain v2 session fingerprints")
    started = time.perf_counter()
    missing_sessions: set[str] = set()
    for offset in range(0, len(fingerprints), 10_000):
        plan = invoke("plan_upload", {"sessionFingerprints": fingerprints[offset : offset + 10_000]})
        missing_sessions.update(plan["missingSessionFingerprints"])
    plan_ms = (time.perf_counter() - started) * 1_000
    if not missing_sessions:
        return None, {
            "planMs": plan_ms,
            "sessionsSkipped": len(sessions),
            "objectsSkipped": manifest.get("object_count", 0),
            "originalBytes": archive.stat().st_size,
            "deltaBytes": 0,
            "usedOriginalArchive": False,
        }

    with zipfile.ZipFile(archive) as zipped:
        selected = [session for session in sessions if session["session_fingerprint"] in missing_sessions]
        hashes = sorted(set(iter_session_hashes(zipped, selected)))
    missing_blobs: set[str] = set()
    blob_plan_started = time.perf_counter()
    for offset in range(0, len(hashes), 50_000):
        result = invoke("missing_blobs", {"blobHashes": hashes[offset : offset + 50_000]})
        missing_blobs.update(result["missingBlobHashes"])
    blob_plan_ms = (time.perf_counter() - blob_plan_started) * 1_000
    with zipfile.ZipFile(archive) as zipped:
        manifest_name = next(name for name in zipped.namelist() if name.endswith("_manifest.json"))
        wrapper = manifest_name.rsplit("/", 1)[0]
        estimated_delta_bytes = sum(zipped.getinfo(str(session["entries_path"])).file_size for session in selected)
        estimated_delta_bytes += sum(
            zipped.getinfo(object_member(wrapper, digest)).file_size for digest in missing_blobs
        )
        estimated_delta_bytes += zipped.getinfo(manifest_name).file_size
    everything_missing = len(missing_sessions) == len(sessions) and len(missing_blobs) == len(hashes)
    if everything_missing or estimated_delta_bytes >= archive.stat().st_size * 0.8:
        return archive, {
            "planMs": plan_ms,
            "blobPlanMs": blob_plan_ms,
            "sessionsSkipped": len(sessions) - len(missing_sessions),
            "objectsSkipped": len(hashes) - len(missing_blobs),
            "originalBytes": archive.stat().st_size,
            "deltaBytes": archive.stat().st_size,
            "usedOriginalArchive": True,
        }
    descriptor, path = tempfile.mkstemp(prefix="brain-delta-", suffix=".zip", dir=archive.parent)
    os.close(descriptor)
    delta_path = Path(path)
    build_delta_archive(archive, delta_path, manifest, missing_sessions, missing_blobs)
    return delta_path, {
        "planMs": plan_ms,
        "blobPlanMs": blob_plan_ms,
        "sessionsSkipped": len(sessions) - len(missing_sessions),
        "objectsSkipped": len(hashes) - len(missing_blobs),
        "originalBytes": archive.stat().st_size,
        "deltaBytes": delta_path.stat().st_size,
        "usedOriginalArchive": False,
    }


def cleanup_delta(path: Path | None, used_original_archive: bool) -> None:
    if path is not None and not used_original_archive:
        path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview, confirm, and publish local agent transcripts to Brain")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--source", choices=("all", *SOURCES), default="all")
    parser.add_argument(
        "--repository",
        action="append",
        help="Repository to publish; accepts host/owner/repository, a Git remote URL, or GitHub owner/repository",
    )
    parser.add_argument("--visibility", default=os.environ.get("BRAIN_VISIBILITY", "organization"))
    parser.add_argument("--project")
    parser.add_argument("--since")
    parser.add_argument("--until")
    parser.add_argument("--exclude-subagents", action="store_true")
    parser.add_argument("--exclude-archived", action="store_true")
    parser.add_argument("--workers", type=int, default=8)
    archive_group = parser.add_mutually_exclusive_group()
    archive_group.add_argument("--output", type=Path)
    archive_group.add_argument(
        "--archive",
        type=Path,
        help="Preview or publish an existing Brain export without rebuilding it",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and preview the archive without publishing it",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Publish without the interactive confirmation prompt",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Queue ingestion and return without polling for completion",
    )
    args = parser.parse_args()

    if not args.archive and not args.repository:
        parser.error("at least one --repository is required when building an archive")

    if args.archive:
        output = args.archive.expanduser().resolve()
        if not output.is_file():
            raise FileNotFoundError(output)
    else:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        output = (
            (args.output or Path.home() / "Downloads" / f"agent-chats-export-{timestamp}.zip").expanduser().resolve()
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        run_export(args, output)
    manifest = read_manifest(output)
    scope = scope_from_manifest(manifest, args.visibility)
    if scope["sessionCount"] == 0:
        raise RuntimeError("the selected scope contains no sessions")
    print_preview(scope, manifest, output)
    if args.dry_run:
        print("\nDry run complete; no transcript data was sent.")
        return
    if not args.yes:
        answer = input("\nPublish this exact scope to Brain? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Upload cancelled; the local archive was preserved.")
            return

    with MCPClient(args.endpoint) as client:
        access = client.call("access", {})
        configured_repositories = set(access.get("allowedRepositories") or [])
        if not set(scope["repositories"]).issubset(configured_repositories):
            raise RuntimeError("The archive contains a repository outside the server allowlist")
        if scope["visibility"] != access.get("visibility"):
            raise RuntimeError(
                f"The archive visibility {scope['visibility']} does not match "
                f"server visibility {access.get('visibility')}"
            )
        delta_path, profile = plan_delta(args.endpoint, output, manifest, call=client.call)
        print(
            "Incremental plan: "
            f"sessions-skipped={profile['sessionsSkipped']} "
            f"objects-skipped={profile['objectsSkipped']} "
            f"plan={profile['planMs']:.1f}ms "
            f"blob-plan={profile.get('blobPlanMs', 0):.1f}ms "
            f"bytes={profile['originalBytes'] / 1e6:.1f}MB→{profile['deltaBytes'] / 1e6:.1f}MB "
            f"original-reused={profile['usedOriginalArchive']}"
        )
        if delta_path is None:
            print("Brain is already current; no archive bytes were uploaded.")
            return

        try:
            publication_archive = delta_path
            publication_manifest = read_manifest(publication_archive)
            publication_scope = upload_scope(scope_from_manifest(publication_manifest, args.visibility))
            archive_sha256 = sha256_file(publication_archive)
            prepared = client.call(
                "prepare_upload",
                {
                    "archiveSha256": archive_sha256,
                    "archiveBytes": publication_archive.stat().st_size,
                    "scope": publication_scope,
                    "confirmedShared": True,
                },
            )
            upload_id = str(prepared["id"])
            status = str(prepared["status"])
            record = prepared
            if status == "prepared":
                print(f"Uploading {publication_archive.stat().st_size / 1e6:.1f} MB as {upload_id}...")
                upload_archive(args.endpoint, str(prepared["uploadPath"]), publication_archive)
                status = "uploaded"
            if status == "uploaded":
                record = client.call("commit_upload", {"uploadId": upload_id})
                status = str(record["status"])
            if args.no_wait:
                print(f"Upload {upload_id} is {status}.")
                return

            previous_phase = None
            while status in {"queued", "processing"}:
                time.sleep(2)
                record = client.call("upload_status", {"uploadId": upload_id})
                status = str(record["status"])
                phase = record.get("phase")
                if phase != previous_phase:
                    print(f"  {status}: {phase} ({record.get('phaseDetail') or ''})")
                    previous_phase = phase
            if status != "complete":
                raise RuntimeError(f"upload {upload_id} ended in {status}: {record.get('error')}")
            print(f"Brain upload complete: {json.dumps(record['result'], sort_keys=True)}")
        finally:
            cleanup_delta(delta_path, profile["usedOriginalArchive"])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted; any local archive was preserved.", file=sys.stderr)
        raise SystemExit(130) from None
