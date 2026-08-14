from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import tempfile
from pathlib import Path

from brain.auth import AccessLevel


def add_credential(path: Path, principal: str, access: AccessLevel, name: str | None = None) -> str:
    token = secrets.token_urlsafe(32)
    if path.exists():
        records = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError(f"Token file must contain a JSON array: {path}")
    else:
        records = []
    record = {
        "tokenSha256": hashlib.sha256(token.encode()).hexdigest(),
        "principal": principal.strip().lower(),
        "access": access.value,
    }
    if name:
        record["name"] = name.strip()
    records.append(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(records, output, indent=2)
            output.write("\n")
        Path(temporary).replace(path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return token


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a Brain bearer token and store only its SHA-256 digest")
    parser.add_argument("--principal", required=True, help="Stable user or workload identity")
    parser.add_argument("--access", choices=tuple(AccessLevel), default=AccessLevel.APPEND)
    parser.add_argument("--name", help="Optional display name")
    parser.add_argument("--output", type=Path, default=Path("tokens.json"))
    args = parser.parse_args()
    token = add_credential(args.output.expanduser().resolve(), args.principal, AccessLevel(args.access), args.name)
    print(f"Token file: {args.output.expanduser().resolve()}")
    print("Token (shown once):")
    print(token)


if __name__ == "__main__":
    main()
