from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from brain.auth import AccessLevel, TokenCredential

AUTH_MODES = {"token", "trusted-header", "tailscale", "none"}
REPOSITORY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*){2,}$")


def comma_separated(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def normalized_repositories(value: str | None) -> frozenset[str]:
    repositories = frozenset(item.lower().removesuffix(".git") for item in comma_separated(value))
    invalid = sorted(repository for repository in repositories if not REPOSITORY_PATTERN.fullmatch(repository))
    if invalid:
        raise ValueError(
            "BRAIN_ALLOWED_REPOSITORIES entries must use host/owner/repository form; invalid: " + ", ".join(invalid)
        )
    return repositories


def _load_token_credentials(values: Mapping[str, str]) -> tuple[TokenCredential, ...]:
    inline = values.get("BRAIN_TOKENS_JSON")
    path = values.get("BRAIN_TOKENS_FILE")
    if inline and path:
        raise ValueError("Set only one of BRAIN_TOKENS_JSON or BRAIN_TOKENS_FILE")
    if path:
        token_path = Path(path).expanduser()
        try:
            raw = token_path.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"Cannot read BRAIN_TOKENS_FILE: {token_path}") from error
    else:
        raw = inline or "[]"
    try:
        records = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("Brain token configuration must be valid JSON") from error
    if not isinstance(records, list):
        raise ValueError("Brain token configuration must be a JSON array")
    credentials: list[TokenCredential] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Each Brain token credential must be a JSON object")
        digest = str(record.get("tokenSha256", "")).lower()
        principal = str(record.get("principal", "")).strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("Each Brain token credential requires a lowercase tokenSha256")
        if not principal or len(principal) > 200 or any(character.isspace() for character in principal):
            raise ValueError("Each Brain token credential requires a valid principal")
        try:
            access = AccessLevel(str(record.get("access", "read")))
        except ValueError as error:
            raise ValueError("Brain token access must be read, append, or admin") from error
        name = str(record["name"]).strip() if record.get("name") else None
        credentials.append(TokenCredential(digest, principal, access, name))
    digests = [credential.token_sha256 for credential in credentials]
    if len(digests) != len(set(digests)):
        raise ValueError("Brain token digests must be unique")
    return tuple(credentials)


def _positive_int(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _boolean(values: Mapping[str, str], name: str, default: bool) -> bool:
    raw = values.get(name, str(default)).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True)
class Config:
    data_dir: Path
    host: str
    port: int
    allowed_hosts: list[str]
    allowed_repositories: frozenset[str]
    visibility: str
    auth_mode: str
    token_credentials: tuple[TokenCredential, ...]
    trusted_principal_header: str
    trusted_access_header: str
    trusted_name_header: str
    tailscale_allowed_users: frozenset[str] | None
    tailscale_admin_users: frozenset[str]
    tailscale_app_capability: str
    tailscale_require_capability: bool
    max_upload_bytes: int
    max_pending_bytes_per_owner: int
    upload_ttl_seconds: int
    upload_receive_timeout_seconds: int
    request_log_retention: int
    max_mcp_request_bytes: int
    search_timeout_seconds: float
    python_executable: str
    ingest_script: Path | None


def load_config(env: Mapping[str, str] | None = None) -> Config:
    values = os.environ if env is None else env
    port = _positive_int(values, "BRAIN_PORT", 8788)
    if port > 65_535:
        raise ValueError(f"BRAIN_PORT must be an integer from 1 to 65535; received {port}")
    max_upload_bytes = _positive_int(values, "BRAIN_MAX_UPLOAD_BYTES", 10 * 1024 * 1024 * 1024)
    max_pending_bytes_per_owner = _positive_int(values, "BRAIN_MAX_PENDING_BYTES_PER_OWNER", max_upload_bytes)
    upload_ttl_seconds = _positive_int(values, "BRAIN_UPLOAD_TTL_SECONDS", 7 * 24 * 60 * 60)
    upload_receive_timeout_seconds = _positive_int(values, "BRAIN_UPLOAD_RECEIVE_TIMEOUT_SECONDS", 600)
    request_log_retention = _positive_int(values, "BRAIN_REQUEST_LOG_RETENTION", 100_000)
    max_mcp_request_bytes = _positive_int(values, "BRAIN_MAX_MCP_REQUEST_BYTES", 1024 * 1024)
    try:
        search_timeout_seconds = float(values.get("BRAIN_SEARCH_TIMEOUT_SECONDS", "5"))
    except ValueError as error:
        raise ValueError("BRAIN_SEARCH_TIMEOUT_SECONDS must be a positive number") from error
    if search_timeout_seconds <= 0:
        raise ValueError("BRAIN_SEARCH_TIMEOUT_SECONDS must be a positive number")

    auth_mode = values.get("BRAIN_AUTH_MODE", "tailscale").strip().lower()
    if auth_mode not in AUTH_MODES:
        raise ValueError("BRAIN_AUTH_MODE must be token, trusted-header, tailscale, or none")
    token_credentials = _load_token_credentials(values)
    if auth_mode == "token" and not token_credentials:
        raise ValueError("BRAIN_AUTH_MODE=token requires BRAIN_TOKENS_FILE or BRAIN_TOKENS_JSON")
    public_hosts = comma_separated(values.get("BRAIN_PUBLIC_HOSTS"))
    bind_host = values.get("BRAIN_HOST", "127.0.0.1").strip()
    if auth_mode == "none" and bind_host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError("BRAIN_AUTH_MODE=none may only bind to loopback")
    repositories = normalized_repositories(values.get("BRAIN_ALLOWED_REPOSITORIES"))
    if not repositories:
        raise ValueError("BRAIN_ALLOWED_REPOSITORIES must list at least one host/owner/repository")

    data_dir = Path(values.get("BRAIN_DATA_DIR", "./data")).expanduser().resolve()
    visibility = values.get("BRAIN_VISIBILITY", "organization").strip()
    if not visibility or len(visibility) > 100:
        raise ValueError("BRAIN_VISIBILITY must be a non-empty label of at most 100 characters")
    tailscale_allowed = frozenset(
        value.lower() for value in comma_separated(values.get("BRAIN_TAILSCALE_ALLOWED_USERS"))
    )
    tailscale_admin = frozenset(value.lower() for value in comma_separated(values.get("BRAIN_TAILSCALE_ADMIN_USERS")))
    return Config(
        data_dir=data_dir,
        host=bind_host,
        port=port,
        allowed_hosts=["localhost", "127.0.0.1", "[::1]", *public_hosts],
        allowed_repositories=repositories,
        visibility=visibility,
        auth_mode=auth_mode,
        token_credentials=token_credentials,
        trusted_principal_header=values.get("BRAIN_TRUSTED_PRINCIPAL_HEADER", "X-Brain-Principal"),
        trusted_access_header=values.get("BRAIN_TRUSTED_ACCESS_HEADER", "X-Brain-Access"),
        trusted_name_header=values.get("BRAIN_TRUSTED_NAME_HEADER", "X-Brain-Name"),
        tailscale_allowed_users=tailscale_allowed or None,
        tailscale_admin_users=tailscale_admin,
        tailscale_app_capability=values.get("BRAIN_TAILSCALE_APP_CAPABILITY", "example.com/cap/brain"),
        tailscale_require_capability=_boolean(values, "BRAIN_TAILSCALE_REQUIRE_CAPABILITY", False),
        max_upload_bytes=max_upload_bytes,
        max_pending_bytes_per_owner=max_pending_bytes_per_owner,
        upload_ttl_seconds=upload_ttl_seconds,
        upload_receive_timeout_seconds=upload_receive_timeout_seconds,
        request_log_retention=request_log_retention,
        max_mcp_request_bytes=max_mcp_request_bytes,
        search_timeout_seconds=search_timeout_seconds,
        python_executable=values.get("BRAIN_PYTHON", "python3"),
        ingest_script=(
            Path(values["BRAIN_INGEST_SCRIPT"]).expanduser().resolve() if values.get("BRAIN_INGEST_SCRIPT") else None
        ),
    )
