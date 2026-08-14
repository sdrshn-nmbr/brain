import hashlib
import json

import pytest

from brain.auth import AccessLevel
from brain.config import load_config


def token_config() -> str:
    return json.dumps(
        [
            {
                "tokenSha256": hashlib.sha256(b"secret").hexdigest(),
                "principal": "admin@example.com",
                "access": "admin",
            }
        ]
    )


def base_env() -> dict[str, str]:
    return {
        "BRAIN_ALLOWED_REPOSITORIES": "github.com/acme/widget,gitlab.com/acme/platform/model",
        "BRAIN_TOKENS_JSON": token_config(),
    }


def test_token_auth_is_the_secure_default() -> None:
    config = load_config(base_env())
    assert config.host == "127.0.0.1"
    assert config.auth_mode == "token"
    assert config.token_credentials[0].access == AccessLevel.ADMIN
    assert config.allowed_repositories == frozenset({"github.com/acme/widget", "gitlab.com/acme/platform/model"})


def test_none_mode_cannot_bind_publicly() -> None:
    with pytest.raises(ValueError, match="loopback"):
        load_config(
            {
                "BRAIN_AUTH_MODE": "none",
                "BRAIN_HOST": "0.0.0.0",
                "BRAIN_ALLOWED_REPOSITORIES": "github.com/acme/widget",
            }
        )


def test_token_mode_requires_credentials() -> None:
    with pytest.raises(ValueError, match="requires"):
        load_config({"BRAIN_ALLOWED_REPOSITORIES": "github.com/acme/widget"})


def test_repository_allowlist_is_required_and_validated() -> None:
    with pytest.raises(ValueError, match="at least one"):
        load_config({"BRAIN_AUTH_MODE": "none"})
    with pytest.raises(ValueError, match="host/owner/repository"):
        load_config({"BRAIN_AUTH_MODE": "none", "BRAIN_ALLOWED_REPOSITORIES": "acme/widget"})


def test_rejects_invalid_search_timeout() -> None:
    with pytest.raises(ValueError, match="positive number"):
        load_config({**base_env(), "BRAIN_SEARCH_TIMEOUT_SECONDS": "0"})


@pytest.mark.parametrize(
    "name",
    [
        "BRAIN_MAX_UPLOAD_BYTES",
        "BRAIN_MAX_PENDING_BYTES_PER_OWNER",
        "BRAIN_UPLOAD_TTL_SECONDS",
        "BRAIN_UPLOAD_RECEIVE_TIMEOUT_SECONDS",
        "BRAIN_REQUEST_LOG_RETENTION",
        "BRAIN_MAX_MCP_REQUEST_BYTES",
    ],
)
def test_rejects_invalid_positive_integer_settings(name: str) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        load_config({**base_env(), name: "0"})
