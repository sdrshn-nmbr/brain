from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class AccessLevel(StrEnum):
    READ = "read"
    APPEND = "append"
    ADMIN = "admin"


ACCESS_RANK = {AccessLevel.READ: 0, AccessLevel.APPEND: 1, AccessLevel.ADMIN: 2}


@dataclass(frozen=True)
class TokenCredential:
    token_sha256: str
    principal: str
    access: AccessLevel
    name: str | None = None


@dataclass(frozen=True)
class Identity:
    principal: str
    kind: str
    access: AccessLevel
    name: str | None = None

    @property
    def can_append(self) -> bool:
        return self.access in {AccessLevel.APPEND, AccessLevel.ADMIN}

    @property
    def is_admin(self) -> bool:
        return self.access == AccessLevel.ADMIN


class AuthenticationError(Exception):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


def _normalized_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    return {key.lower(): value for key, value in (headers or {}).items()}


def _clean_principal(value: str) -> str:
    principal = value.strip().lower()
    if not principal or len(principal) > 200 or any(character.isspace() for character in principal):
        raise AuthenticationError("Identity principal is invalid", 401)
    return principal


def _bearer_token(headers: Mapping[str, str]) -> str:
    scheme, separator, token = headers.get("authorization", "").partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not token.strip():
        raise AuthenticationError("A bearer token is required", 401)
    return token.strip()


def authenticate_token(headers: Mapping[str, str] | None, credentials: tuple[TokenCredential, ...]) -> Identity:
    normalized = _normalized_headers(headers)
    digest = hashlib.sha256(_bearer_token(normalized).encode()).hexdigest()
    matched: TokenCredential | None = None
    for credential in credentials:
        if hmac.compare_digest(digest, credential.token_sha256):
            matched = credential
    if matched is None:
        raise AuthenticationError("Bearer token is invalid", 401)
    return Identity(matched.principal, "token", matched.access, matched.name)


def authenticate_trusted_headers(
    headers: Mapping[str, str] | None,
    principal_header: str,
    access_header: str,
    name_header: str,
) -> Identity:
    normalized = _normalized_headers(headers)
    principal = _clean_principal(normalized.get(principal_header.lower(), ""))
    raw_access = normalized.get(access_header.lower(), AccessLevel.READ).strip().lower()
    try:
        access = AccessLevel(raw_access)
    except ValueError as error:
        raise AuthenticationError("Trusted access header is invalid", 401) from error
    name = normalized.get(name_header.lower(), "").strip() or None
    return Identity(principal, "trusted-header", access, name)


def authenticate_tailscale_headers(
    headers: Mapping[str, str] | None,
    allowed_users: frozenset[str] | None,
    admin_users: frozenset[str],
    app_capability: str,
    require_capability: bool,
) -> Identity:
    normalized = _normalized_headers(headers)
    capability_header = normalized.get("tailscale-app-capabilities", "")
    try:
        capabilities = json.loads(capability_header) if capability_header else {}
    except json.JSONDecodeError as error:
        raise AuthenticationError("Tailscale app capability header is invalid", 401) from error
    rules = capabilities.get(app_capability, []) if isinstance(capabilities, dict) else []
    capability_access: AccessLevel | None = None
    capability_actor = "tailscale-workload"
    for rule in rules if isinstance(rules, list) else []:
        if not isinstance(rule, dict):
            continue
        values = rule.get("access", [])
        access_values = (
            {values}
            if isinstance(values, str)
            else {value for value in values if isinstance(value, str)}
            if isinstance(values, list)
            else set()
        )
        rule_access: AccessLevel | None = None
        for level in (AccessLevel.ADMIN, AccessLevel.APPEND, AccessLevel.READ):
            if level in access_values:
                rule_access = level
                break
        if rule_access is None:
            continue
        actor = rule.get("actor", capability_actor)
        if not isinstance(actor, str):
            raise AuthenticationError("Tailscale app capability actor is invalid", 401)
        if capability_access is None or ACCESS_RANK[rule_access] > ACCESS_RANK[capability_access]:
            capability_access = rule_access
            capability_actor = _clean_principal(actor)

    login = normalized.get("tailscale-user-login", "").strip().lower()
    if login:
        principal = _clean_principal(login)
        if allowed_users and principal not in allowed_users:
            raise AuthenticationError(f"Tailscale user {principal} is not authorized", 403)
        if require_capability and capability_access is None:
            raise AuthenticationError("A configured Tailscale app capability is required", 403)
        default_access = AccessLevel.ADMIN if principal in admin_users else AccessLevel.APPEND
        if require_capability:
            assert capability_access is not None
            access = capability_access
        else:
            access = max((default_access, capability_access or AccessLevel.READ), key=ACCESS_RANK.__getitem__)
        return Identity(principal, "tailscale-user", access, normalized.get("tailscale-user-name", "").strip() or None)

    if capability_access is not None:
        return Identity(f"workload:{capability_actor}", "tailscale-workload", AccessLevel.READ, capability_actor)
    raise AuthenticationError("Tailscale user identity or configured app capability is required", 401)


def authenticate(
    headers: Mapping[str, str] | None,
    *,
    mode: str,
    token_credentials: tuple[TokenCredential, ...],
    trusted_principal_header: str,
    trusted_access_header: str,
    trusted_name_header: str,
    tailscale_allowed_users: frozenset[str] | None,
    tailscale_admin_users: frozenset[str],
    tailscale_app_capability: str,
    tailscale_require_capability: bool,
) -> Identity:
    if mode == "token":
        return authenticate_token(headers, token_credentials)
    if mode == "trusted-header":
        return authenticate_trusted_headers(
            headers,
            trusted_principal_header,
            trusted_access_header,
            trusted_name_header,
        )
    if mode == "tailscale":
        return authenticate_tailscale_headers(
            headers,
            tailscale_allowed_users,
            tailscale_admin_users,
            tailscale_app_capability,
            tailscale_require_capability,
        )
    if mode == "none":
        return Identity("local", "development", AccessLevel.ADMIN, "Local developer")
    raise AuthenticationError("Authentication mode is invalid", 500)


def require_append(identity: Identity) -> str:
    if not identity.can_append:
        raise AuthenticationError("Append access is required", 403)
    return identity.principal


def require_admin(identity: Identity) -> None:
    if not identity.is_admin:
        raise AuthenticationError("Brain administrator access is required", 403)
