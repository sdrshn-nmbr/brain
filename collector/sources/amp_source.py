"""Amp thread history adapter.

Amp keeps threads on ampcode.com rather than on disk, including threads that ran in orbs. The signed-in `amp` CLI
lists and exports them, so one machine can publish every thread the account owns. Orb working directories do not exist
on the publishing machine; each export carries the thread's Git remote instead.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import common

NAME = "amp"
LIST_PAGE_SIZE = 500
CLI_TIMEOUT_SECONDS = 120


class AmpCLIError(RuntimeError):
    pass


def store_exists() -> bool:
    signed_in = os.environ.get("AMP_API_KEY") or (Path.home() / ".local" / "share" / "amp" / "accounts.json").exists()
    return bool(shutil.which("amp") and signed_in)


def run_amp(*arguments: str) -> Any:
    command = ["amp", *arguments]
    result = subprocess.run(command, capture_output=True, text=True, timeout=CLI_TIMEOUT_SECONDS, check=False)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        detail = (result.stderr or result.stdout).strip()[:500]
        raise AmpCLIError(f"{' '.join(command)} exited {result.returncode} without JSON: {detail}") from None


def discover_export_targets() -> list[dict]:
    threads: list[dict] = []
    while True:
        page = run_amp(
            "threads",
            "list",
            "--json",
            "--include-archived",
            "--limit",
            str(LIST_PAGE_SIZE),
            "--offset",
            str(len(threads)),
        )
        if not isinstance(page, list):
            raise AmpCLIError(f"amp threads list returned {type(page).__name__}, expected a list")
        threads.extend(page)
        if len(page) < LIST_PAGE_SIZE:
            return threads


def file_uri_path(uri: str | None) -> str | None:
    return uri.removeprefix("file://") if uri else None


def render_tool_run(run: dict) -> str:
    if run.get("status") == "error":
        return "error: " + json.dumps(run.get("error"), ensure_ascii=False)
    result = run.get("result")
    if isinstance(result, str):
        return result
    return json.dumps(result, ensure_ascii=False, indent=2)


def thread_entries(messages: list[dict]) -> list[common.FullEntry]:
    tool_names = {
        block["id"]: block.get("name")
        for message in messages
        for block in message.get("content") or []
        if block.get("type") == "tool_use"
    }
    entries: list[common.FullEntry] = []

    def add(role: str, text: str | None, timestamp: str | None, tool_name: str | None = None) -> None:
        if text and text.strip():
            entries.append(common.FullEntry(len(entries), role, timestamp, text, tool_name))

    for message in messages:
        timestamp = message.get("createdAt")
        for block in message.get("content") or []:
            kind = block.get("type")
            if kind == "text":
                add("assistant" if message.get("role") == "assistant" else "user", block.get("text"), timestamp)
            elif kind == "thinking":
                add("thinking", block.get("thinking"), timestamp)
            elif kind == "tool_use":
                add("tool_call", json.dumps(block.get("input"), ensure_ascii=False), timestamp, block.get("name"))
            elif kind == "tool_result":
                tool_name = tool_names.get(block.get("toolUseID"))
                add("tool_result", render_tool_run(block.get("run") or {}), timestamp, tool_name)
            elif kind == "image" and block.get("sourcePath"):
                add("user", f"[image] {block['sourcePath']}", timestamp)
            elif kind == "summary":
                summary = block.get("summary")
                if isinstance(summary, dict):
                    summary = summary.get("summary")
                add("assistant", summary if isinstance(summary, str) else None, timestamp)
    return entries


def thread_session(thread: dict) -> common.FullSession:
    initial = (thread.get("env") or {}).get("initial") or {}
    trees = initial.get("trees") or []
    repository = next((tree["repository"] for tree in trees if (tree.get("repository") or {}).get("url")), {})
    models = [message["usage"]["model"] for message in thread["messages"] if (message.get("usage") or {}).get("model")]
    timestamps = [message["createdAt"] for message in thread["messages"] if message.get("createdAt")]
    started_at = datetime.fromtimestamp(thread["created"] / 1000, tz=UTC).isoformat()
    return common.FullSession(
        source=NAME,
        session_id=thread["id"],
        file_paths=[f"https://ampcode.com/threads/{thread['id']}"],
        cwd=file_uri_path(initial.get("workingDirectory") or (trees[0]["uri"] if trees else None)),
        git_branch=(repository.get("ref") or "").removeprefix("refs/heads/") or None,
        model=models[-1] if models else None,
        started_at=started_at,
        ended_at=max(timestamps) if timestamps else thread.get("updatedAt") or started_at,
        entries=thread_entries(thread["messages"]),
        repository_url=repository.get("url"),
    )


def parse_export_target(target: dict) -> common.FullSession | None:
    thread = run_amp("threads", "export", target["id"])
    if not isinstance(thread, dict) or thread.get("id") != target["id"]:
        raise AmpCLIError(f"amp threads export {target['id']} returned a different payload")
    session = thread_session(thread)
    return session if session.entries else None
