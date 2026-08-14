from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


MAX_OBSERVABLE_TEXT = 200
MAX_OBSERVABLE_JSON_BYTES = 8 * 1024


def _bounded_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value[:MAX_OBSERVABLE_TEXT]


def _query_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    return {
        "queryChars": len(value),
        "querySha256": hashlib.sha256(value.encode()).hexdigest(),
    }


def observable_arguments(name: str | None, arguments: Any) -> dict[str, Any] | None:
    if not isinstance(arguments, dict):
        return None
    if name == "plan_upload" and isinstance(arguments.get("sessionFingerprints"), list):
        return {"sessionFingerprintCount": len(arguments["sessionFingerprints"])}
    if name == "missing_blobs" and isinstance(arguments.get("blobHashes"), list):
        return {"blobHashCount": len(arguments["blobHashes"])}
    if name == "search":
        result = _query_summary(arguments.get("query")) or {}
        for key in ("person", "repository", "source", "since"):
            value = _bounded_text(arguments.get(key))
            if value is not None:
                result[key] = value
        for key in ("includeSubagents", "limit", "maxHitsPerSession"):
            if isinstance(arguments.get(key), (bool, int)):
                result[key] = arguments[key]
        roles = arguments.get("roles")
        if isinstance(roles, list):
            result["roleCount"] = len(roles)
        return result
    if name == "browse":
        result = {}
        for key in ("person", "repository", "source", "since"):
            value = _bounded_text(arguments.get(key))
            if value is not None:
                result[key] = value
        for key in ("includeSubagents", "limit"):
            if isinstance(arguments.get(key), (bool, int)):
                result[key] = arguments[key]
        return result
    if name == "read_session":
        result = {}
        for key in ("uuid", "person"):
            value = _bounded_text(arguments.get(key))
            if value is not None:
                result[key] = value
        for key in ("sessionId", "offset", "limit", "maxChars"):
            if isinstance(arguments.get(key), int):
                result[key] = arguments[key]
        return result
    if name == "prepare_upload":
        scope = arguments.get("scope")
        scope_summary = None
        if isinstance(scope, dict):
            scope_summary = {
                "repositories": scope.get("repositories"),
                "sources": scope.get("sources"),
                "since": _bounded_text(scope.get("since")),
                "until": _bounded_text(scope.get("until")),
                "sessionCount": scope.get("sessionCount"),
                "visibility": scope.get("visibility"),
            }
        return {
            "archiveSha256": _bounded_text(arguments.get("archiveSha256")),
            "archiveBytes": arguments.get("archiveBytes"),
            "scope": scope_summary,
        }
    if name in {"commit_upload", "upload_status", "cancel_upload"}:
        return {"uploadId": _bounded_text(arguments.get("uploadId"))}
    if name == "list_my_uploads":
        return {"limit": arguments.get("limit")}
    if name == "admin_requests":
        return {
            key: arguments.get(key)
            for key in ("limit", "beforeId", "since", "actor", "name", "statusCode")
            if key in arguments
        }
    if name in {"access", "stats", "admin_request_stats"}:
        return {"since": _bounded_text(arguments.get("since"))} if "since" in arguments else {}
    return {"argumentNames": sorted(str(key)[:MAX_OBSERVABLE_TEXT] for key in arguments)[:50]}


def observable_client(metadata: Any) -> dict[str, Any] | None:
    if not isinstance(metadata, dict):
        return None
    info = metadata.get("io.modelcontextprotocol/clientInfo")
    result: dict[str, Any] = {}
    if isinstance(info, dict):
        result["info"] = {
            key: _bounded_text(info.get(key))
            for key in ("name", "title", "version")
            if _bounded_text(info.get(key)) is not None
        }
    protocol = _bounded_text(metadata.get("io.modelcontextprotocol/protocolVersion"))
    if protocol is not None:
        result["protocolVersion"] = protocol
    return result or None


def bounded_json(value: Any) -> str | None:
    if value is None:
        return None
    serialized = json.dumps(value, separators=(",", ":"))
    encoded = serialized.encode()
    if len(encoded) <= MAX_OBSERVABLE_JSON_BYTES:
        return serialized
    return json.dumps(
        {
            "omitted": True,
            "jsonBytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        },
        separators=(",", ":"),
    )


class RequestLog:
    def __init__(self, data_dir: Path, retention: int) -> None:
        self.retention = retention
        self._lock = threading.RLock()
        self.db = sqlite3.connect(data_dir / "requests.sqlite", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.execute("PRAGMA synchronous = NORMAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                actor TEXT,
                identity_kind TEXT,
                access_level TEXT,
                path TEXT NOT NULL,
                http_method TEXT NOT NULL,
                mcp_method TEXT,
                mcp_name TEXT,
                arguments_json TEXT,
                client_json TEXT,
                status_code INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                request_bytes INTEGER NOT NULL,
                response_bytes INTEGER NOT NULL,
                user_agent TEXT,
                error_code TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_requests_started ON requests(started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_requests_actor_started ON requests(actor, started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_requests_name_started ON requests(mcp_name, started_at DESC);
            """
        )
        self.db.commit()
        self._writes = 0

    def record(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.db.execute(
                """INSERT INTO requests(
                       request_id, started_at, completed_at, actor, identity_kind, access_level,
                       path, http_method, mcp_method, mcp_name, arguments_json, client_json,
                       status_code, duration_ms, request_bytes, response_bytes, user_agent, error_code
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event["requestId"],
                    event["startedAt"],
                    event.get("completedAt", utc_now()),
                    event.get("actor"),
                    event.get("identityKind"),
                    event.get("accessLevel"),
                    event["path"],
                    event["httpMethod"],
                    event.get("mcpMethod"),
                    event.get("mcpName"),
                    bounded_json(event.get("arguments")),
                    bounded_json(event.get("client")),
                    event["statusCode"],
                    event["durationMs"],
                    event.get("requestBytes", 0),
                    event.get("responseBytes", 0),
                    event.get("userAgent"),
                    event.get("errorCode"),
                ),
            )
            self._writes += 1
            if self._writes % 100 == 0:
                self.db.execute(
                    """DELETE FROM requests WHERE id <= COALESCE(
                           (SELECT id FROM requests ORDER BY id DESC LIMIT 1 OFFSET ?), 0
                       )""",
                    (self.retention,),
                )
            self.db.commit()

    def list_records(
        self,
        *,
        limit: int,
        before_id: int | None = None,
        since: str | None = None,
        actor: str | None = None,
        name: str | None = None,
        status_code: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if before_id is not None:
            clauses.append("id < ?")
            values.append(before_id)
        if since:
            clauses.append("started_at >= ?")
            values.append(since)
        if actor:
            clauses.append("actor = ?")
            values.append(actor)
        if name:
            clauses.append("mcp_name = ?")
            values.append(name)
        if status_code is not None:
            clauses.append("status_code = ?")
            values.append(status_code)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(max(1, min(limit, 500)))
        with self._lock:
            rows = self.db.execute(
                f"SELECT * FROM requests {where} ORDER BY id DESC LIMIT ?",
                values,
            ).fetchall()
        return [self._record(row) for row in rows]

    def stats(self, *, since: str | None = None) -> dict[str, Any]:
        where = "WHERE started_at >= ?" if since else ""
        values = (since,) if since else ()
        with self._lock:
            rows = self.db.execute(
                f"SELECT actor, identity_kind, mcp_name, status_code, error_code, duration_ms FROM requests {where}",
                values,
            ).fetchall()
        durations = sorted(float(row["duration_ms"]) for row in rows)

        def percentile(fraction: float) -> float | None:
            if not durations:
                return None
            return round(durations[min(len(durations) - 1, int((len(durations) - 1) * fraction))], 3)

        def counts(field: str) -> dict[str, int]:
            result: dict[str, int] = {}
            for row in rows:
                value = str(row[field] if row[field] is not None else "unknown")
                result[value] = result.get(value, 0) + 1
            return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))

        return {
            "since": since,
            "requestCount": len(rows),
            "byActor": counts("actor"),
            "byIdentityKind": counts("identity_kind"),
            "byOperation": counts("mcp_name"),
            "byStatus": counts("status_code"),
            "byError": counts("error_code"),
            "latencyMs": {
                "p50": percentile(0.50),
                "p95": percentile(0.95),
                "max": durations[-1] if durations else None,
            },
        }

    def close(self) -> None:
        with self._lock:
            self.db.close()

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "requestId": row["request_id"],
            "startedAt": row["started_at"],
            "completedAt": row["completed_at"],
            "actor": row["actor"],
            "identityKind": row["identity_kind"],
            "accessLevel": row["access_level"],
            "path": row["path"],
            "httpMethod": row["http_method"],
            "mcpMethod": row["mcp_method"],
            "mcpName": row["mcp_name"],
            "arguments": json.loads(row["arguments_json"]) if row["arguments_json"] else None,
            "client": json.loads(row["client_json"]) if row["client_json"] else None,
            "statusCode": row["status_code"],
            "durationMs": row["duration_ms"],
            "requestBytes": row["request_bytes"],
            "responseBytes": row["response_bytes"],
            "userAgent": row["user_agent"],
            "errorCode": row["error_code"],
        }
