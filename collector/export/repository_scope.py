from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class RepositoryIdentity:
    slug: str
    root: str
    remote_url: str


def normalize_repository_slug(remote_url: str) -> str | None:
    value = remote_url.strip()
    if not value:
        return None

    if "://" in value:
        parsed = urlparse(value)
        if not parsed.hostname:
            return None
        host = parsed.hostname
        path = parsed.path
    else:
        scp_match = re.fullmatch(r"(?:[^@/:]+@)?([^/:]+):(.+)", value)
        if not scp_match:
            return None
        host, path = scp_match.groups()

    path_parts = [part for part in path.removesuffix(".git").split("/") if part]
    if len(path_parts) < 2 or not host:
        return None
    return "/".join((host.lower(), *(part.lower() for part in path_parts)))


def normalize_repository_selector(value: str) -> str | None:
    candidate = value.strip()
    if re.fullmatch(r"[^/]+/[^/]+", candidate):
        candidate = f"https://github.com/{candidate}"
    elif re.fullmatch(r"[^/]+(?:/[^/]+){2,}", candidate) and "://" not in candidate:
        candidate = "https://" + candidate
    return normalize_repository_slug(candidate)


def _git(cwd: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


@lru_cache(maxsize=4096)
def resolve_repository(cwd: str | None) -> RepositoryIdentity | None:
    if not cwd:
        return None
    path = Path(cwd).expanduser()
    if not path.exists():
        return None
    if path.is_file():
        path = path.parent

    root = _git(path, "rev-parse", "--show-toplevel")
    if not root:
        return None
    remote_url = _git(Path(root), "remote", "get-url", "origin")
    if not remote_url:
        return None
    slug = normalize_repository_slug(remote_url)
    if not slug:
        return None
    return RepositoryIdentity(slug=slug, root=str(Path(root).resolve()), remote_url=remote_url)
