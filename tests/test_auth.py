import hashlib

import pytest

from brain.auth import (
    AccessLevel,
    AuthenticationError,
    TokenCredential,
    authenticate,
    authenticate_tailscale_headers,
    authenticate_token,
    authenticate_trusted_headers,
    require_admin,
    require_append,
)


def credential(token: str, principal: str, access: AccessLevel) -> TokenCredential:
    return TokenCredential(hashlib.sha256(token.encode()).hexdigest(), principal, access)


def test_bearer_tokens_map_to_stable_principals_and_access() -> None:
    identity = authenticate_token(
        {"Authorization": "Bearer append-secret"},
        (credential("append-secret", "alice@example.com", AccessLevel.APPEND),),
    )
    assert identity.principal == "alice@example.com"
    assert identity.kind == "token"
    assert require_append(identity) == "alice@example.com"


def test_invalid_bearer_token_is_rejected() -> None:
    with pytest.raises(AuthenticationError, match="invalid"):
        authenticate_token(
            {"Authorization": "Bearer wrong"},
            (credential("right", "alice@example.com", AccessLevel.APPEND),),
        )


def test_trusted_headers_accept_explicit_access_from_a_proxy() -> None:
    identity = authenticate_trusted_headers(
        {"X-Identity": "Agent-17", "X-Access": "read", "X-Name": "Build agent"},
        "X-Identity",
        "X-Access",
        "X-Name",
    )
    assert identity.principal == "agent-17"
    assert identity.access == AccessLevel.READ
    assert identity.name == "Build agent"
    with pytest.raises(AuthenticationError, match="Append access"):
        require_append(identity)


def test_tailscale_adapter_normalizes_users_and_keeps_workloads_read_only() -> None:
    user = authenticate_tailscale_headers(
        {"Tailscale-User-Login": "Alice@Example.com"},
        None,
        frozenset({"alice@example.com"}),
        "brain.example/cap/read",
        False,
    )
    assert user.access == AccessLevel.ADMIN
    require_admin(user)

    workload = authenticate_tailscale_headers(
        {"Tailscale-App-Capabilities": ('{"brain.example/cap/read":[{"access":["read"],"actor":"cloud-agent"}]}')},
        None,
        frozenset(),
        "brain.example/cap/read",
        False,
    )
    assert workload.principal == "workload:cloud-agent"
    assert workload.access == AccessLevel.READ
    with pytest.raises(AuthenticationError, match="Append access"):
        require_append(workload)


def test_none_mode_is_an_explicit_local_admin_identity() -> None:
    identity = authenticate(
        {},
        mode="none",
        token_credentials=(),
        trusted_principal_header="X-Brain-Principal",
        trusted_access_header="X-Brain-Access",
        trusted_name_header="X-Brain-Name",
        tailscale_allowed_users=None,
        tailscale_admin_users=frozenset(),
        tailscale_app_capability="brain.example/cap/read",
        tailscale_require_capability=False,
    )
    assert identity.principal == "local"
    assert identity.is_admin


@pytest.mark.parametrize(
    ("access", "expected"),
    [("read", AccessLevel.READ), ("append", AccessLevel.APPEND), ("admin", AccessLevel.ADMIN)],
)
def test_tailscale_capabilities_can_authorize_humans(access: str, expected: AccessLevel) -> None:
    identity = authenticate_tailscale_headers(
        {
            "Tailscale-User-Login": "alice@example.com",
            "Tailscale-App-Capabilities": f'{{"brain.example/cap/access":[{{"access":["{access}"]}}]}}',
        },
        None,
        frozenset(),
        "brain.example/cap/access",
        True,
    )
    assert identity.access == expected


def test_tailscale_required_capability_rejects_a_human_without_one() -> None:
    with pytest.raises(AuthenticationError, match="capability is required"):
        authenticate_tailscale_headers(
            {"Tailscale-User-Login": "alice@example.com"},
            None,
            frozenset(),
            "brain.example/cap/access",
            True,
        )


def test_tailscale_tagged_workloads_remain_read_only_with_admin_capability() -> None:
    identity = authenticate_tailscale_headers(
        {"Tailscale-App-Capabilities": '{"brain.example/cap/access":[{"access":["admin"]}]}'},
        None,
        frozenset(),
        "brain.example/cap/access",
        True,
    )
    assert identity.access == AccessLevel.READ


def test_tailscale_uses_the_strongest_matching_capability_rule() -> None:
    identity = authenticate_tailscale_headers(
        {
            "Tailscale-User-Login": "alice@example.com",
            "Tailscale-App-Capabilities": (
                '{"brain.example/cap/access":[{"access":["read","append"]},{"access":["read","append","admin"]}]}'
            ),
        },
        None,
        frozenset(),
        "brain.example/cap/access",
        True,
    )
    assert identity.access == AccessLevel.ADMIN
