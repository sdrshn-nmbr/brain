import json
import stat
from pathlib import Path

from brain.auth import AccessLevel, authenticate_token
from brain.config import load_config
from brain.token import add_credential


def test_generated_token_round_trips_without_storing_plaintext(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    token = add_credential(path, "Alice@Example.com", AccessLevel.ADMIN, "Alice")
    assert token not in path.read_text()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    config = load_config(
        {
            "BRAIN_ALLOWED_REPOSITORIES": "github.com/acme/widget",
            "BRAIN_TOKENS_FILE": str(path),
        }
    )
    identity = authenticate_token({"Authorization": f"Bearer {token}"}, config.token_credentials)
    assert identity.principal == "alice@example.com"
    assert identity.access == AccessLevel.ADMIN


def test_adds_credentials_without_replacing_existing_ones(tmp_path: Path) -> None:
    path = tmp_path / "tokens.json"
    add_credential(path, "alice", AccessLevel.ADMIN)
    add_credential(path, "build-agent", AccessLevel.READ)
    records = json.loads(path.read_text())
    assert [(record["principal"], record["access"]) for record in records] == [
        ("alice", "admin"),
        ("build-agent", "read"),
    ]
