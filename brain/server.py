from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, Literal
from urllib.parse import urlsplit

import uvicorn
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from brain.auth import (
    AuthenticationError,
    Identity,
    authenticate,
    require_admin,
    require_append,
)
from brain.config import Config, load_config
from brain.corpus import CorpusStore
from brain.observability import RequestLog, observable_arguments, observable_client, utc_now
from brain.uploads import UploadManager, create_archive_ingester
from collector.ingest.ingest import init_db, init_objects_db

logger = logging.getLogger("brain")

Source = Literal["claude", "codex", "cursor"]

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)
WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=False)
DELETE = ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=True, open_world_hint=False)


class UploadScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    repositories: list[str] = Field(min_length=1)
    sources: list[Source] = Field(min_length=1)
    since: str | None
    until: str | None
    sessionCount: int = Field(gt=0)
    visibility: str = Field(min_length=1, max_length=100)


def authenticate_headers(headers: Mapping[str, str] | None, config: Config) -> Identity:
    return authenticate(
        headers,
        mode=config.auth_mode,
        token_credentials=config.token_credentials,
        trusted_principal_header=config.trusted_principal_header,
        trusted_access_header=config.trusted_access_header,
        trusted_name_header=config.trusted_name_header,
        tailscale_allowed_users=config.tailscale_allowed_users,
        tailscale_admin_users=config.tailscale_admin_users,
        tailscale_app_capability=config.tailscale_app_capability,
        tailscale_require_capability=config.tailscale_require_capability,
    )


def identity_from_context(ctx: Context, config: Config) -> Identity:
    return authenticate_headers(ctx.headers, config)


def append_login_from_context(ctx: Context, config: Config) -> str:
    return require_append(identity_from_context(ctx, config))


def host_allowed(value: str, allowed_hosts: list[str]) -> bool:
    host = value.strip().lower()
    if host in allowed_hosts:
        return True
    if host.startswith("[") and "]:" in host:
        host = host[: host.index("]") + 1]
    elif host.count(":") == 1 and host.rsplit(":", 1)[1].isdigit():
        host = host.rsplit(":", 1)[0]
    return host in allowed_hosts


class AuthorizationMiddleware:
    def __init__(self, app: ASGIApp, config: Config, request_log: RequestLog) -> None:
        self.app = app
        self.config = config
        self.request_log = request_log

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") == "/healthz":
            await self.app(scope, receive, send)
            return
        request_id = str(uuid.uuid4())
        started_at = utc_now()
        started = time.perf_counter()
        headers = {key.decode().lower(): value.decode() for key, value in scope.get("headers", [])}
        if not host_allowed(headers.get("host", ""), self.config.allowed_hosts):
            await JSONResponse({"error": "invalid_host"}, status_code=421)(scope, receive, send)
            await asyncio.to_thread(
                self._record_request,
                request_id,
                started_at,
                None,
                scope,
                headers,
                bytearray(),
                bytearray(),
                421,
                started,
                0,
                0,
                "invalid_host",
            )
            return
        origin = headers.get("origin")
        if origin and not host_allowed(urlsplit(origin).netloc, self.config.allowed_hosts):
            await JSONResponse({"error": "invalid_origin"}, status_code=403)(scope, receive, send)
            await asyncio.to_thread(
                self._record_request,
                request_id,
                started_at,
                None,
                scope,
                headers,
                bytearray(),
                bytearray(),
                403,
                started,
                0,
                0,
                "invalid_origin",
            )
            return
        identity: Identity | None = None
        auth_error: AuthenticationError | None = None
        try:
            identity = authenticate_headers(headers, self.config)
        except AuthenticationError as error:
            auth_error = error
        logger.info(
            json.dumps(
                {
                    "event": "mcp_request" if scope.get("path") == "/mcp" else "upload_request",
                    "requestId": request_id,
                    "actor": identity.principal if identity else None,
                    "method": headers.get("mcp-method"),
                    "name": headers.get("mcp-name"),
                }
            )
        )
        capture_mcp_body = scope.get("path") == "/mcp"
        request_body = bytearray()
        response_body = bytearray()
        request_bytes = 0
        response_bytes = 0
        max_mcp_body = self.config.max_mcp_request_bytes

        content_length = headers.get("content-length")
        if capture_mcp_body and content_length:
            try:
                declared_bytes = int(content_length)
            except ValueError:
                declared_bytes = max_mcp_body + 1
            if declared_bytes > max_mcp_body:
                await JSONResponse({"error": "request_too_large"}, status_code=413)(scope, receive, send)
                await asyncio.to_thread(
                    self._record_request,
                    request_id,
                    started_at,
                    identity,
                    scope,
                    headers,
                    request_body,
                    bytearray(),
                    413,
                    started,
                    declared_bytes,
                    0,
                    "request_too_large",
                )
                return

        async def receive_with_capture():
            nonlocal request_bytes
            message = await receive()
            if message["type"] == "http.request":
                body = message.get("body", b"")
                request_bytes += len(body)
                if capture_mcp_body and request_bytes <= max_mcp_body:
                    request_body.extend(body)
                if capture_mcp_body and request_bytes > max_mcp_body:
                    raise ValueError("MCP request body exceeds configured limit")
            return message

        status_code: int | None = None

        async def send_with_status(message) -> None:
            nonlocal response_bytes, status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                response_bytes += len(body)
                if len(response_body) + len(body) <= 1024 * 1024:
                    response_body.extend(body)
            await send(message)

        if auth_error:
            logger.warning(
                json.dumps(
                    {
                        "event": "mcp_auth_rejected",
                        "requestId": request_id,
                        "status": auth_error.status_code,
                        "reason": str(auth_error),
                    }
                )
            )
            await JSONResponse({"error": "authentication_required"}, status_code=auth_error.status_code)(
                scope, receive_with_capture, send_with_status
            )
            await asyncio.to_thread(
                self._record_request,
                request_id,
                started_at,
                identity,
                scope,
                headers,
                request_body,
                response_body,
                status_code or auth_error.status_code,
                started,
                request_bytes,
                response_bytes,
                "authentication_rejected",
            )
            return
        try:
            await self.app(scope, receive_with_capture, send_with_status)
        except ValueError as error:
            if str(error) != "MCP request body exceeds configured limit" or status_code is not None:
                raise
            await JSONResponse({"error": "request_too_large"}, status_code=413)(scope, receive, send_with_status)
        finally:
            await asyncio.to_thread(
                self._record_request,
                request_id,
                started_at,
                identity,
                scope,
                headers,
                request_body,
                response_body,
                status_code or 500,
                started,
                request_bytes,
                response_bytes,
                None if status_code and status_code < 400 else "request_failed",
            )
            logger.info(
                json.dumps(
                    {
                        "event": "transport_profile",
                        "requestId": request_id,
                        "path": scope.get("path"),
                        "method": headers.get("mcp-method"),
                        "name": headers.get("mcp-name"),
                        "status": status_code,
                        "totalMs": round((time.perf_counter() - started) * 1_000, 3),
                    }
                )
            )

    def _record_request(
        self,
        request_id: str,
        started_at: str,
        identity: Identity | None,
        scope: Scope,
        headers: dict[str, str],
        request_body: bytearray,
        response_body: bytearray,
        status_code: int,
        started: float,
        request_bytes: int,
        response_bytes: int,
        error_code: str | None,
    ) -> None:
        arguments = None
        client = None
        mcp_method = headers.get("mcp-method")
        mcp_name = headers.get("mcp-name")
        if scope.get("path") == "/mcp" and request_body:
            try:
                payload = json.loads(request_body)
                if isinstance(payload, dict):
                    mcp_method = mcp_method or payload.get("method")
                params = payload.get("params", {}) if isinstance(payload, dict) else {}
                if isinstance(params, dict):
                    arguments = params.get("arguments")
                    mcp_name = mcp_name or params.get("name")
                metadata = params.get("_meta") if isinstance(params, dict) else None
                client = observable_client(metadata)
            except (UnicodeDecodeError, json.JSONDecodeError):
                error_code = error_code or "invalid_json"
        arguments = observable_arguments(mcp_name, arguments)
        if scope.get("path") == "/mcp" and response_body:
            try:
                response_payload = json.loads(response_body)
                if isinstance(response_payload, dict):
                    result = response_payload.get("result")
                    if response_payload.get("error") is not None:
                        error_code = "mcp_protocol_error"
                    elif isinstance(result, dict) and result.get("isError") is True:
                        error_code = "mcp_tool_error"
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
        try:
            self.request_log.record(
                {
                    "requestId": request_id,
                    "startedAt": started_at,
                    "completedAt": utc_now(),
                    "actor": identity.principal if identity else None,
                    "identityKind": identity.kind if identity else None,
                    "accessLevel": identity.access if identity else None,
                    "path": scope.get("path", ""),
                    "httpMethod": scope.get("method", ""),
                    "mcpMethod": mcp_method,
                    "mcpName": mcp_name,
                    "arguments": arguments,
                    "client": client,
                    "statusCode": status_code,
                    "durationMs": round((time.perf_counter() - started) * 1_000, 3),
                    "requestBytes": request_bytes,
                    "responseBytes": response_bytes,
                    "userAgent": headers.get("user-agent"),
                    "errorCode": error_code,
                }
            )
        except Exception:
            logger.exception("request observability write failed", extra={"request_id": request_id})


def create_server(config: Config, corpus: CorpusStore, uploads: UploadManager, request_log: RequestLog) -> MCPServer:
    mcp = MCPServer(
        "brain",
        version="0.1.0",
        title="Brain",
        description="Search and selectively share repository-scoped agent session history.",
        instructions=(
            "Search with search, browse, and read_session. This is a stateless remote MCP; reading needs no local "
            "Brain package. Publishing needs the local one-shot brain-sync command because the server cannot read "
            "transcripts from the client filesystem. Always preview and confirm an archive before publishing it."
        ),
        website_url="https://github.com/sdrshn-nmbr/brain",
    )

    @mcp.tool(
        title="Inspect my Brain access",
        description="Return the authenticated principal, access level, publication scope, and available tools.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def access(ctx: Context) -> dict[str, Any]:
        identity = identity_from_context(ctx, config)
        tools = ["access", "search", "browse", "read_session", "stats"]
        if identity.can_append:
            tools.extend(
                [
                    "plan_upload",
                    "missing_blobs",
                    "prepare_upload",
                    "commit_upload",
                    "upload_status",
                    "list_my_uploads",
                    "cancel_upload",
                ]
            )
        if identity.is_admin:
            tools.extend(["admin_requests", "admin_request_stats"])
        return {
            "result": {
                "actor": identity.principal,
                "identityKind": identity.kind,
                "accessLevel": identity.access,
                "tools": tools,
                "allowedRepositories": sorted(config.allowed_repositories),
                "visibility": config.visibility,
            }
        }

    @mcp.tool(
        title="Search agent history",
        description="Full-text search over agent sessions in the configured repository scope.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    async def search(
        query: str,
        ctx: Context,
        person: str | None = None,
        repository: str | None = None,
        source: Source | None = None,
        since: str | None = None,
        roles: list[str] | None = None,
        includeSubagents: bool = False,
        limit: int = 10,
        maxHitsPerSession: int = 5,
    ) -> dict[str, Any]:
        identity_from_context(ctx, config)
        cancel_event = threading.Event()
        try:
            result = await asyncio.to_thread(
                corpus.read().search,
                query=query,
                person=person,
                repository=repository,
                source=source,
                since=since,
                roles=roles,
                include_subagents=includeSubagents,
                limit=limit,
                max_hits_per_session=maxHitsPerSession,
                deadline_seconds=config.search_timeout_seconds,
                cancel_event=cancel_event,
            )
        except asyncio.CancelledError:
            cancel_event.set()
            logger.warning(json.dumps({"event": "search_cancelled"}))
            raise
        return {"result": result}

    @mcp.tool(
        title="Browse recent agent sessions",
        description="List recent sessions with optional person, repository, source, and date filters.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def browse(
        ctx: Context,
        person: str | None = None,
        repository: str | None = None,
        source: Source | None = None,
        since: str | None = None,
        includeSubagents: bool = False,
        limit: int = 20,
    ) -> dict[str, Any]:
        identity_from_context(ctx, config)
        return {
            "result": corpus.read().browse(
                person=person,
                repository=repository,
                source=source,
                since=since,
                include_subagents=includeSubagents,
                limit=limit,
            )
        }

    @mcp.tool(
        title="Read an agent session",
        description="Read a full-fidelity session by result sessionId or by UUID prefix with explicit pagination.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def read_session(
        ctx: Context,
        sessionId: int | None = None,
        uuid: str | None = None,
        person: str | None = None,
        offset: int = 0,
        limit: int = 50,
        maxChars: int = 50_000,
    ) -> dict[str, Any]:
        identity_from_context(ctx, config)
        return {
            "result": corpus.read().read_session(
                session_id=sessionId,
                uuid=uuid,
                person=person,
                offset=offset,
                limit=limit,
                max_chars=maxChars,
            )
        }

    @mcp.tool(
        title="Inspect Brain coverage",
        description="Return corpus coverage by person and repository plus content-addressed storage totals.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def stats(ctx: Context) -> dict[str, Any]:
        identity_from_context(ctx, config)
        return {"result": corpus.read().stats()}

    @mcp.tool(
        title="Plan an incremental Brain upload",
        description="Return only session fingerprints not already present for the authenticated user.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def plan_upload(sessionFingerprints: list[str], ctx: Context) -> dict[str, Any]:
        if len(sessionFingerprints) > 10_000 or any(
            not re.fullmatch(r"[a-f0-9]{64}", value) for value in sessionFingerprints
        ):
            raise ValueError("sessionFingerprints must contain at most 10000 lowercase SHA-256 digests")
        owner = append_login_from_context(ctx, config)
        missing = corpus.read().missing_session_fingerprints(uploads.person_for(owner), sessionFingerprints)
        return {
            "result": {
                "missingSessionFingerprints": missing,
                "presentSessionCount": len(sessionFingerprints) - len(missing),
            }
        }

    @mcp.tool(
        title="Find missing Brain CAS objects",
        description="Return CAS body hashes not already stored; call in bounded batches after planning sessions.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def missing_blobs(blobHashes: list[str], ctx: Context) -> dict[str, Any]:
        append_login_from_context(ctx, config)
        if len(blobHashes) > 50_000 or any(not re.fullmatch(r"[a-f0-9]{64}", value) for value in blobHashes):
            raise ValueError("blobHashes must contain at most 50000 lowercase SHA-256 digests")
        missing = corpus.read().missing_blob_hashes(blobHashes)
        return {"result": {"missingBlobHashes": missing, "presentBlobCount": len(blobHashes) - len(missing)}}

    @mcp.tool(
        title="Prepare a personal Brain transcript upload",
        description="Create an upload slot after the user confirms the exact shared publication scope.",
        annotations=WRITE,
        structured_output=True,
    )
    def prepare_upload(
        archiveSha256: str,
        archiveBytes: int,
        scope: UploadScope,
        confirmedShared: Literal[True],
        ctx: Context,
    ) -> dict[str, Any]:
        owner = append_login_from_context(ctx, config)
        return {
            "result": uploads.prepare(
                owner,
                archiveSha256,
                archiveBytes,
                scope.model_dump(),
                confirmedShared,
            )
        }

    @mcp.tool(
        title="Commit an uploaded transcript archive",
        description="Queue a checksum-verified archive for repository validation and content-addressed ingestion.",
        annotations=WRITE,
        structured_output=True,
    )
    async def commit_upload(uploadId: str, ctx: Context) -> dict[str, Any]:
        return {"result": uploads.commit(append_login_from_context(ctx, config), uploadId)}

    @mcp.tool(
        title="Check a Brain transcript upload",
        description="Return the authenticated user's upload state and final CAS ingestion counts.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def upload_status(uploadId: str, ctx: Context) -> dict[str, Any]:
        return {"result": uploads.status(append_login_from_context(ctx, config), uploadId)}

    @mcp.tool(
        title="List my Brain transcript uploads",
        description="List uploads owned by the authenticated principal.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def list_my_uploads(ctx: Context, limit: int = 20) -> dict[str, Any]:
        return {"result": uploads.list_uploads(append_login_from_context(ctx, config), limit)}

    @mcp.tool(
        title="Cancel my pending Brain transcript upload",
        description="Cancel and remove an owned archive before ingestion completes.",
        annotations=DELETE,
        structured_output=True,
    )
    def cancel_upload(uploadId: str, ctx: Context) -> dict[str, Any]:
        return {"result": uploads.cancel(append_login_from_context(ctx, config), uploadId)}

    @mcp.tool(
        title="Inspect Brain request activity",
        description="Admin-only structured request records with cursor pagination and optional filters.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def admin_requests(
        ctx: Context,
        limit: int = 100,
        beforeId: int | None = None,
        since: str | None = None,
        actor: str | None = None,
        name: str | None = None,
        statusCode: int | None = None,
    ) -> dict[str, Any]:
        require_admin(identity_from_context(ctx, config))
        return {
            "result": request_log.list_records(
                limit=limit,
                before_id=beforeId,
                since=since,
                actor=actor,
                name=name,
                status_code=statusCode,
            )
        }

    @mcp.tool(
        title="Summarize Brain request activity",
        description="Admin-only request counts and latency by actor, operation, and HTTP status.",
        annotations=READ_ONLY,
        structured_output=True,
    )
    def admin_request_stats(ctx: Context, since: str | None = None) -> dict[str, Any]:
        require_admin(identity_from_context(ctx, config))
        return {"result": request_log.stats(since=since)}

    @mcp.custom_route("/healthz", methods=["GET"], include_in_schema=False)
    async def healthz(_request: Request) -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @mcp.custom_route("/uploads/{upload_id}/archive", methods=["PUT"], include_in_schema=False)
    async def receive_upload(request: Request) -> JSONResponse:
        try:
            owner = require_append(authenticate_headers(dict(request.headers), config))
            upload = await uploads.receive(owner, request.path_params["upload_id"], request)
            logger.info(json.dumps({"event": "upload_received", "user": owner, "uploadId": upload["id"]}))
            return JSONResponse(upload)
        except Exception as error:
            logger.exception("upload rejected", extra={"upload_id": request.path_params.get("upload_id")})
            return JSONResponse({"error": "upload_rejected", "detail": str(error)}, status_code=400)

    return mcp


def create_app(config: Config | None = None):
    config = config or load_config()
    config.data_dir.mkdir(parents=True, exist_ok=True)
    index_path = config.data_dir / "index.sqlite"
    objects_path = config.data_dir / "objects.sqlite"
    if not index_path.exists():
        with sqlite3.connect(index_path) as index:
            index.row_factory = sqlite3.Row
            init_db(index)
    if not objects_path.exists():
        with sqlite3.connect(objects_path) as objects:
            init_objects_db(objects)
    corpus = CorpusStore(config.data_dir)
    uploads = UploadManager(
        config.data_dir,
        config.max_upload_bytes,
        config.max_pending_bytes_per_owner,
        config.upload_ttl_seconds,
        config.upload_receive_timeout_seconds,
        config.allowed_repositories,
        config.visibility,
        create_archive_ingester(config, corpus),
    )
    request_log = RequestLog(config.data_dir, config.request_log_retention)
    mcp = create_server(config, corpus, uploads, request_log)
    transport_hosts = [*config.allowed_hosts, *(f"{host}:*" for host in config.allowed_hosts)]
    transport_origins = [
        *(f"https://{host}" for host in config.allowed_hosts),
        *(f"http://{host}" for host in config.allowed_hosts),
        *(f"https://{host}:*" for host in config.allowed_hosts),
        *(f"http://{host}:*" for host in config.allowed_hosts),
    ]
    app = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=transport_hosts,
            allowed_origins=transport_origins,
        ),
        host=config.host,
    )
    app.add_middleware(AuthorizationMiddleware, config=config, request_log=request_log)
    sdk_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        uploads.start()
        try:
            async with sdk_lifespan(application):
                yield
        finally:
            await uploads.wait()
            uploads.close()
            request_log.close()
            corpus.close()

    app.router.lifespan_context = lifespan
    app.state.brain_config = config
    app.state.corpus = corpus
    app.state.uploads = uploads
    app.state.mcp = mcp
    app.state.request_log = request_log
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    config = load_config()
    logger.info(
        json.dumps(
            {
                "event": "server_started",
                "address": f"http://{config.host}:{config.port}/mcp",
                "dataDir": str(config.data_dir),
                "allowedHosts": config.allowed_hosts,
            }
        )
    )
    uvicorn.run(create_app(config), host=config.host, port=config.port, log_level="info")


if __name__ == "__main__":
    main()
