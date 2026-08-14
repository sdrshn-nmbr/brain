"""Legacy Cursor chat backend: composer conversations in globalStorage/state.vscdb.

Before ~/.cursor/chats/*/store.db (protobuf DAG) and the JSONL agent-transcripts,
Cursor stored every chat ("composer") in the shared SQLite state database at
~/Library/Application Support/Cursor/User/globalStorage/state.vscdb, table
`cursorDiskKV`:

  - `composerData:{uuid}` — one JSON blob per chat. Two generations:
      * inline: full message list under "conversation"
      * headers: "fullConversationHeadersOnly" lists bubble ids; each message
        lives in its own `bubbleId:{composerUuid}:{bubbleUuid}` row
  - message/bubble schema: type 1 = user, type 2 = assistant; assistant bubbles
    may carry "thinking" ({text, ...}) and "toolFormerData"
    ({name, rawArgs/params, result, status}) alongside/instead of "text".

Only composer-level timestamps exist (createdAt/lastUpdatedAt, epoch ms) — there
are no reliable per-message timestamps in this format.

The workspace a composer belongs to is recovered by scanning each
workspaceStorage/*/state.vscdb ItemTable key `composer.composerData`
("allComposers") and pairing it with that workspace's workspace.json folder.

This goes back to the very first Cursor use on this machine (Jan/Feb 2025),
long before either newer backend existed.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from . import common

CURSOR_USER_DIR = Path.home() / "Library" / "Application Support" / "Cursor" / "User"
GLOBAL_DB = CURSOR_USER_DIR / "globalStorage" / "state.vscdb"
WORKSPACE_STORAGE_DIR = CURSOR_USER_DIR / "workspaceStorage"


def store_exists() -> bool:
    return GLOBAL_DB.exists()


def _connect_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _iso_ms(ts_ms) -> str | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


@lru_cache(maxsize=1)
def _workspace_folder_map() -> dict[str, str]:
    """composerId -> workspace folder path, scanned from every workspaceStorage state.vscdb."""
    mapping: dict[str, str] = {}
    if not WORKSPACE_STORAGE_DIR.exists():
        return mapping
    for ws in WORKSPACE_STORAGE_DIR.iterdir():
        db_path = ws / "state.vscdb"
        if not db_path.is_file():
            continue
        folder = None
        wj = ws / "workspace.json"
        if wj.exists():
            try:
                folder = json.loads(wj.read_text()).get("folder")
            except (OSError, json.JSONDecodeError):
                folder = None
        if folder and folder.startswith("file://"):
            folder = folder[len("file://") :]
        try:
            con = _connect_ro(db_path)
            row = con.execute("SELECT value FROM ItemTable WHERE key='composer.composerData'").fetchone()
            con.close()
        except sqlite3.Error:
            continue
        if not row or not row[0]:
            continue
        try:
            data = json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            continue
        for comp in data.get("allComposers") or []:
            cid = comp.get("composerId")
            if cid and folder:
                mapping.setdefault(cid, folder)
    return mapping


def discover_composers() -> list[str]:
    """Composer uuids in the global state db that contain at least one message."""
    if not store_exists():
        return []
    ids: list[str] = []
    con = _connect_ro(GLOBAL_DB)
    try:
        for key, value in con.execute("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'"):
            if not value:
                continue
            try:
                d = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                continue
            if d.get("conversation") or d.get("fullConversationHeadersOnly"):
                ids.append(key.split(":", 1)[1])
    finally:
        con.close()
    return ids


def _bubble_entries(bubble: dict, add) -> None:
    btype = bubble.get("type")
    if btype == 1:
        text = bubble.get("text") or ""
        if text:
            add("user", text)
        return
    thinking = bubble.get("thinking")
    if isinstance(thinking, dict) and thinking.get("text"):
        add("thinking", thinking["text"])
    tfd = bubble.get("toolFormerData")
    if isinstance(tfd, dict):
        name = tfd.get("name") or f"tool_{tfd.get('tool')}"
        args = tfd.get("rawArgs") or tfd.get("params") or ""
        add("tool_call", str(args), tool_name=name)
        result = tfd.get("result")
        if result:
            add("tool_result", str(result), tool_name=name)
    text = bubble.get("text") or ""
    if text:
        add("assistant", text)


def parse_composer(composer_id: str) -> common.FullSession | None:
    if not store_exists():
        return None
    con = _connect_ro(GLOBAL_DB)
    try:
        row = con.execute("SELECT value FROM cursorDiskKV WHERE key=?", (f"composerData:{composer_id}",)).fetchone()
        if not row or not row[0]:
            return None
        try:
            data = json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return None

        entries: list[common.FullEntry] = []

        def add(role: str, text: str, tool_name: str | None = None) -> None:
            entries.append(
                common.FullEntry(index=len(entries), role=role, timestamp=None, text=text, tool_name=tool_name)
            )

        inline = data.get("conversation") or []
        headers = data.get("fullConversationHeadersOnly") or []
        if inline:
            for msg in inline:
                if isinstance(msg, dict):
                    _bubble_entries(msg, add)
        elif headers:
            for h in headers:
                bid = h.get("bubbleId")
                if not bid:
                    continue
                brow = con.execute(
                    "SELECT value FROM cursorDiskKV WHERE key=?", (f"bubbleId:{composer_id}:{bid}",)
                ).fetchone()
                if not brow or not brow[0]:
                    continue
                try:
                    bubble = json.loads(brow[0])
                except (json.JSONDecodeError, TypeError):
                    continue
                _bubble_entries(bubble, add)
    finally:
        con.close()

    if not entries:
        return None

    name = data.get("name")
    if name:
        entries.insert(0, common.FullEntry(index=0, role="user", timestamp=None, text=f"[chat title] {name}"))
        for i, e in enumerate(entries):
            entries[i] = common.FullEntry(
                index=i, role=e.role, timestamp=e.timestamp, text=e.text, tool_name=e.tool_name, raw=e.raw
            )

    return common.FullSession(
        source="cursor",
        session_id=composer_id,
        file_paths=[str(GLOBAL_DB)],
        cwd=_workspace_folder_map().get(composer_id),
        git_branch=None,
        model=None,
        started_at=_iso_ms(data.get("createdAt")),
        ended_at=_iso_ms(data.get("lastUpdatedAt")) or _iso_ms(data.get("createdAt")),
        entries=entries,
    )
