"""OpenAI Codex CLI conversation history adapter.

Reads ~/.codex/sessions/**/rollout-*.jsonl (+ archived_sessions), Codex Desktop side-chat
state from ~/.codex/.codex-global-state.json and ~/.codex/logs_2.sqlite,
~/.codex/history.jsonl (global prompt log), and
~/.codex/external_agent_session_imports.json (import markers).
Ported from the original search_codex_history.py.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import uuid
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from . import common

NAME = "codex"

CODEX_DIR = Path.home() / ".codex"
SESSIONS_DIR = CODEX_DIR / "sessions"
ARCHIVED_DIR = CODEX_DIR / "archived_sessions"
HISTORY_FILE = CODEX_DIR / "history.jsonl"
IMPORTS_FILE = CODEX_DIR / "external_agent_session_imports.json"
DESKTOP_STATE_FILE = CODEX_DIR / ".codex-global-state.json"
DESKTOP_LOGS_DB = CODEX_DIR / "logs_2.sqlite"
SIDE_CHAT_RECORDS_DIR = CODEX_DIR / "attachments" / "sidechats"

UUID_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.IGNORECASE)

# Rollout schema has two generations. Current: {"type": "response_item", "payload": {...}}. Older
# (pre-response_item, seen on 2025-08/09 rollouts): the same payload fields are inlined directly at
# the top level of the line, e.g. {"type": "message", "role": ..., "content": [...]} with no wrapper
# and no per-line "timestamp". Both shapes are handled everywhere a payload is read (see
# `_as_payload`) so older sessions aren't silently dropped.
_PAYLOAD_TYPES = {
    "message",
    "reasoning",
    "function_call",
    "custom_tool_call",
    "tool_search_call",
    "web_search_call",
    "function_call_output",
    "custom_tool_call_output",
    "tool_search_output",
}

_CWD_TAG_RE = re.compile(r"<cwd>(.*?)</cwd>")
_SIDE_CHAT_PREFIX = "sidechat:"
_SIDE_CHAT_ROUTE_PREFIX = "thread-tab-routes-v1:"
_LOG_CWD_RE = re.compile(r"\bcwd=([^\s}:]+)")
_LOG_USER_INPUT_RE = re.compile(
    r'Text \{ text: ("(?:\\.|[^"\\])*"), text_elements:',
    re.DOTALL,
)
_LOG_TOOL_CALL_RE = re.compile(
    r":handle_output_item_done: ToolCall: (\S+)\s+(.*?)(?:\n thread_id=|\Z)",
    re.DOTALL,
)


def _as_payload(obj: dict) -> tuple[dict, str | None] | None:
    """Normalize one decoded rollout line to (payload, timestamp) across both schema generations."""
    t = obj.get("type")
    if t == "response_item":
        return obj.get("payload") or {}, obj.get("timestamp") or None
    if t in _PAYLOAD_TYPES:
        return obj, obj.get("timestamp") or None
    return None


def store_exists() -> bool:
    return SESSIONS_DIR.exists() or DESKTOP_STATE_FILE.exists() or SIDE_CHAT_RECORDS_DIR.exists()


def _nested_strings(value) -> list[str]:
    strings: list[str] = []
    if isinstance(value, str):
        strings.append(value)
    elif isinstance(value, list):
        for item in value:
            strings.extend(_nested_strings(item))
    elif isinstance(value, dict):
        for item in value.values():
            strings.extend(_nested_strings(item))
    return strings


def _load_desktop_atom_state() -> dict:
    try:
        data = json.loads(DESKTOP_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    atom_state = data.get("electron-persisted-atom-state")
    return atom_state if isinstance(atom_state, dict) else {}


def _uuid7_timestamp(value: str) -> float | None:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        return None
    if parsed.version != 7:
        return None
    return (parsed.int >> 80) / 1000


def _parent_meta(parent_id: str) -> dict:
    if not parent_id:
        return {}
    for path in _discover_rollouts(include_archived=True):
        if parent_id not in path.name:
            continue
        return _load_session_meta(str(path)) or {}
    return {}


def _desktop_side_chat_fallback_records() -> list[dict]:
    """Build clean side-chat records from Desktop state plus selected live log events.

    Global state is authoritative for side-chat identity and submitted prompts. The log database
    supplies timestamps, inherited cwd when available, and completed tool calls. Raw diagnostic
    rows are never rendered because they are noisy, repetitive, and can contain process metadata.
    """

    atom_state = _load_desktop_atom_state()
    prompt_history = atom_state.get("prompt-history")
    if not isinstance(prompt_history, dict):
        prompt_history = {}

    parent_by_side_id: dict[str, str] = {}
    for key, route in atom_state.items():
        if not key.startswith(_SIDE_CHAT_ROUTE_PREFIX):
            continue
        parent_id = key.removeprefix(_SIDE_CHAT_ROUTE_PREFIX)
        for value in _nested_strings(route):
            if value.startswith(_SIDE_CHAT_PREFIX):
                parent_by_side_id[value.removeprefix(_SIDE_CHAT_PREFIX)] = parent_id

    if not parent_by_side_id:
        return []

    records: dict[str, dict] = {}
    for side_id, parent_id in parent_by_side_id.items():
        parent = _parent_meta(parent_id)
        raw_prompts = prompt_history.get(side_id)
        prompts = (
            [text for text in raw_prompts if isinstance(text, str) and text.strip()]
            if isinstance(raw_prompts, list)
            else []
        )
        records[side_id] = {
            "session_id": side_id,
            "parent_id": parent_id,
            "cwd": parent.get("cwd"),
            "originator": "Codex Desktop side chat",
            "first_timestamp": _uuid7_timestamp(side_id),
            "last_timestamp": _uuid7_timestamp(side_id),
            "prompts": prompts,
            "messages": [],
            "side_chat": True,
        }

    if DESKTOP_LOGS_DB.exists():
        placeholders = ",".join("?" for _ in records)
        query = f"""
            SELECT id, ts, ts_nanos, thread_id, target, feedback_log_body
            FROM logs
            WHERE thread_id IN ({placeholders})
              AND target IN ('codex_core::session::handlers', 'codex_core::stream_events_utils')
            ORDER BY ts, ts_nanos, id
        """
        try:
            connection = sqlite3.connect(
                f"file:{DESKTOP_LOGS_DB}?mode=ro",
                uri=True,
                timeout=1,
            )
            try:
                rows = connection.execute(query, tuple(records)).fetchall()
            finally:
                connection.close()
        except sqlite3.Error:
            rows = []

        for row_id, ts, ts_nanos, side_id, target, body in rows:
            record = records.get(side_id)
            if record is None or not isinstance(body, str):
                continue
            timestamp = float(ts) + float(ts_nanos or 0) / 1_000_000_000
            if record["first_timestamp"] is None or timestamp < record["first_timestamp"]:
                record["first_timestamp"] = timestamp
            if record["last_timestamp"] is None or timestamp > record["last_timestamp"]:
                record["last_timestamp"] = timestamp
            if not record.get("cwd"):
                cwd_match = _LOG_CWD_RE.search(body)
                if cwd_match:
                    record["cwd"] = cwd_match.group(1)

            if target == "codex_core::session::handlers":
                for match in _LOG_USER_INPUT_RE.finditer(body):
                    try:
                        text = json.loads(match.group(1)).strip()
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if text:
                        record["messages"].append(
                            {
                                "role": "user",
                                "text": text,
                                "timestamp": timestamp,
                                "order": row_id,
                            }
                        )
            elif target == "codex_core::stream_events_utils":
                tool_match = _LOG_TOOL_CALL_RE.search(body)
                if tool_match:
                    name, arguments = tool_match.groups()
                    record["messages"].append(
                        {
                            "role": "tool",
                            "text": f"[tool: {name}] {arguments.strip()}",
                            "timestamp": timestamp,
                            "order": row_id,
                            "is_tool": True,
                        }
                    )

    for record in records.values():
        logged_prompts = {message["text"] for message in record["messages"] if message["role"] == "user"}
        for offset, prompt in enumerate(record["prompts"], start=1):
            if prompt in logged_prompts:
                continue
            record["messages"].append(
                {
                    "role": "user",
                    "text": prompt,
                    "timestamp": record["first_timestamp"],
                    "order": offset - len(record["prompts"]) - 1,
                }
            )
        record["messages"].sort(key=lambda message: message["order"])
        for index, message in enumerate(record["messages"]):
            message["index"] = index
        record["total_messages"] = len(record["messages"])
    return list(records.values())


def _captured_timestamp(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return None


def _captured_user_text(params: dict) -> str:
    inputs = params.get("input")
    if not isinstance(inputs, list):
        return ""
    parts: list[str] = []
    for item in inputs:
        if not isinstance(item, dict):
            continue
        if item.get("type") in {"text", "input_text"} and item.get("text"):
            parts.append(str(item["text"]))
    return "\n".join(parts).strip()


def _captured_item_message(item: dict) -> tuple[str, str, bool] | None:
    item_type = item.get("type")
    if item_type in {"agentMessage", "plan"}:
        text = str(item.get("text") or "").strip()
        return ("assistant", text, False) if text else None
    if item_type == "userMessage":
        return None
    if not isinstance(item_type, str):
        return None
    rendered = json.dumps(item, ensure_ascii=False, sort_keys=True)
    return "tool", f"[tool: {item_type}] {rendered}", True


def _recorded_side_chat(path: Path) -> dict | None:
    meta: dict = {}
    messages: list[dict] = []
    completed_item_ids: set[str] = set()
    delta_by_item: dict[str, dict] = {}
    first_timestamp: float | None = None
    last_timestamp: float | None = None
    prompts: list[str] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                timestamp = _captured_timestamp(record.get("captured_at"))
                if timestamp is not None:
                    first_timestamp = timestamp if first_timestamp is None else min(first_timestamp, timestamp)
                    last_timestamp = timestamp if last_timestamp is None else max(last_timestamp, timestamp)
                if record.get("type") == "sidechat_meta":
                    meta.update(record)
                    continue
                if record.get("type") != "rpc":
                    continue
                message = record.get("message")
                if not isinstance(message, dict):
                    continue
                direction = record.get("direction")
                method = message.get("method")
                params = message.get("params")
                if not isinstance(params, dict):
                    params = {}
                order = int(record.get("sequence") or line_number)

                if direction == "client_to_server" and method in {
                    "turn/start",
                    "turn/steer",
                }:
                    text = _captured_user_text(params)
                    if text:
                        prompts.append(text)
                        messages.append(
                            {
                                "role": "user",
                                "text": text,
                                "timestamp": timestamp,
                                "order": order,
                            }
                        )
                    continue

                if direction != "server_to_client":
                    continue
                if method == "item/agentMessage/delta":
                    item_id = params.get("itemId")
                    delta = params.get("delta")
                    if isinstance(item_id, str) and isinstance(delta, str):
                        pending = delta_by_item.setdefault(
                            item_id,
                            {"text": [], "timestamp": timestamp, "order": order},
                        )
                        pending["text"].append(delta)
                    continue
                if method != "item/completed":
                    continue
                item = params.get("item")
                if not isinstance(item, dict):
                    continue
                item_id = item.get("id")
                if isinstance(item_id, str):
                    completed_item_ids.add(item_id)
                parsed = _captured_item_message(item)
                if parsed is None:
                    continue
                role, text, is_tool = parsed
                messages.append(
                    {
                        "role": role,
                        "text": text,
                        "timestamp": (
                            float(params["completedAtMs"]) / 1000
                            if isinstance(params.get("completedAtMs"), (int, float))
                            else timestamp
                        ),
                        "order": order,
                        "is_tool": is_tool,
                    }
                )
    except (OSError, UnicodeDecodeError):
        return None

    for item_id, pending in delta_by_item.items():
        if item_id in completed_item_ids:
            continue
        text = "".join(pending["text"]).strip()
        if text:
            messages.append(
                {
                    "role": "assistant",
                    "text": text,
                    "timestamp": pending["timestamp"],
                    "order": pending["order"],
                }
            )
    thread_id = meta.get("thread_id") or path.stem
    if not isinstance(thread_id, str) or not messages:
        return None
    messages.sort(key=lambda message: message["order"])
    for index, message in enumerate(messages):
        message["index"] = index
    return {
        "session_id": thread_id,
        "parent_id": meta.get("parent_thread_id"),
        "cwd": meta.get("cwd"),
        "model": meta.get("model"),
        "originator": "Codex Desktop side chat recorder",
        "first_timestamp": first_timestamp or _uuid7_timestamp(thread_id),
        "last_timestamp": last_timestamp or first_timestamp,
        "prompts": prompts,
        "messages": messages,
        "total_messages": len(messages),
        "side_chat": True,
        "record_path": str(path),
    }


def _recorded_side_chat_records() -> list[dict]:
    if not SIDE_CHAT_RECORDS_DIR.exists():
        return []
    records = []
    for path in sorted(SIDE_CHAT_RECORDS_DIR.glob("*.jsonl")):
        record = _recorded_side_chat(path)
        if record is not None:
            records.append(record)
    return records


def _desktop_side_chat_records() -> list[dict]:
    fallback_by_id = {record["session_id"]: record for record in _desktop_side_chat_fallback_records()}
    captured_by_id = {record["session_id"]: record for record in _recorded_side_chat_records()}
    records: list[dict] = []
    for session_id in sorted(fallback_by_id.keys() | captured_by_id.keys()):
        fallback = fallback_by_id.get(session_id)
        captured = captured_by_id.get(session_id)
        if captured is None:
            if fallback is not None:
                records.append(fallback)
            continue
        if fallback is None:
            records.append(captured)
            continue
        merged = dict(captured)
        for key in ("parent_id", "cwd"):
            if not merged.get(key) and fallback.get(key):
                merged[key] = fallback[key]
        seen = {(message.get("role"), message.get("text")) for message in merged["messages"]}
        for message in fallback.get("messages") or []:
            identity = (message.get("role"), message.get("text"))
            if identity not in seen:
                merged["messages"].append(message)
                seen.add(identity)
        merged["messages"].sort(
            key=lambda message: (
                message.get("timestamp") is None,
                message.get("timestamp") or 0,
                message.get("order") or 0,
            )
        )
        for index, message in enumerate(merged["messages"]):
            message["index"] = index
        merged["prompts"] = list(dict.fromkeys((captured.get("prompts") or []) + (fallback.get("prompts") or [])))
        merged["total_messages"] = len(merged["messages"])
        records.append(merged)
    return records


def _discover_rollouts(include_archived: bool = False) -> list[Path]:
    files: list[Path] = []
    if SESSIONS_DIR.exists():
        files.extend(SESSIONS_DIR.rglob("rollout-*.jsonl"))
    if include_archived and ARCHIVED_DIR.exists():
        files.extend(ARCHIVED_DIR.rglob("rollout-*.jsonl"))
    return files


@lru_cache(maxsize=4096)
def _load_session_meta(path_str: str) -> dict | None:
    try:
        with open(path_str, encoding="utf-8") as f:
            line = f.readline().strip()
        if not line:
            return None
        obj = json.loads(line)
        if obj.get("type") == "session_meta":
            payload = obj.get("payload", {}) or {}
        elif obj.get("type") is None and "id" in obj and ("git" in obj or "instructions" in obj):
            # Older rollout header (pre-session_meta wrapper): id/timestamp/git are inlined at the
            # top level. No "cwd" field exists in this generation — left None here; callers that
            # need it can recover it from the first message's <cwd> environment_context tag.
            payload = obj
        else:
            return None
        return {
            "id": payload.get("id"),
            "cwd": payload.get("cwd"),
            "ts": payload.get("timestamp") or obj.get("timestamp"),
            "originator": payload.get("originator"),
            "model_provider": payload.get("model_provider"),
        }
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _filter_by_scope(files: list[Path], *, cwd: str | None, project: str | None, all_projects: bool) -> list[Path]:
    if all_projects:
        return files
    cwd = cwd or os.getcwd()
    out: list[Path] = []
    for f in files:
        meta = _load_session_meta(str(f))
        if not meta:
            continue
        scwd = meta.get("cwd") or ""
        if project:
            if project.lower() in scwd.lower():
                out.append(f)
        elif scwd and common.is_path_under(scwd, cwd):
            out.append(f)
    return out


@lru_cache(maxsize=1)
def _load_imports() -> dict[str, dict]:
    if not IMPORTS_FILE.exists():
        return {}
    try:
        with open(IMPORTS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    records = data.get("records") if isinstance(data, dict) else data
    if not isinstance(records, list):
        return {}
    return {
        str(rec["imported_thread_id"]): rec
        for rec in records
        if isinstance(rec, dict) and rec.get("imported_thread_id")
    }


def _import_marker_for(session_id: str | None) -> str | None:
    if not session_id:
        return None
    rec = _load_imports().get(session_id)
    if not rec:
        return None
    src = common.collapse_home(rec.get("source_path", ""))
    when = common.format_ts(rec.get("imported_at"))
    return f"  [imported from {src} @ {when}]"


def _extract_text_parts(content) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    out = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") in ("input_text", "output_text", "text"):
            txt = block.get("text") or ""
            if txt:
                out.append(txt)
    return "\n".join(out)


def _parse_conversation(jsonl_path: Path, query: str | None = None, context_msgs: int = 2) -> dict | None:
    messages: list[dict] = []
    meta = _load_session_meta(str(jsonl_path)) or {}
    first_ts = meta.get("ts")

    query_terms, _ = common.parse_query_terms(query)
    query_pattern = common.build_highlight_pattern(query_terms)

    try:
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                parsed = _as_payload(obj)
                if parsed is None:
                    continue
                payload, ts = parsed
                ptype = payload.get("type")
                ts = ts or ""

                if ptype == "message":
                    role = payload.get("role")
                    if role not in ("user", "assistant"):
                        continue
                    text = common.strip_injected_noise(_extract_text_parts(payload.get("content"))).strip()
                    if not text:
                        continue
                    messages.append({"role": role, "text": text, "timestamp": ts, "index": len(messages)})
                elif ptype == "function_call":
                    name = payload.get("name") or "unknown"
                    # Arguments are where actions live (commands run, files written);
                    # without them tool calls are unsearchable.
                    arguments = str(payload.get("arguments") or "")[:2000]
                    messages.append(
                        {
                            "role": "tool",
                            "text": f"[tool: {name}] {arguments}".rstrip(),
                            "timestamp": ts,
                            "index": len(messages),
                            "is_tool": True,
                        }
                    )
    except (OSError, UnicodeDecodeError):
        return None

    if not messages:
        return None

    term_coverage, match_count = 0, 0
    if query_terms:
        matching_indices = {m["index"] for m in messages if common.query_matches_any(m.get("text", ""), query_terms)}
        if not matching_indices:
            return None
        term_coverage, match_count = common.score_terms([m.get("text", "") for m in messages], query_terms)

        included: set[int] = set()
        for idx in matching_indices:
            for offset in range(-context_msgs, context_msgs + 1):
                included.add(idx + offset)

        groups: list[list[str]] = []
        current: list[str] = []
        last_idx = -2
        for m in messages:
            if m["index"] in included:
                if m["index"] > last_idx + 1 and current:
                    groups.append(current)
                    current = []
                is_match = m["index"] in matching_indices
                current.append(_format_message(m, is_match, query_pattern))
                last_idx = m["index"]
        if current:
            groups.append(current)
        rendered = groups
    else:
        rendered = [[_format_message(m, False, None) for m in messages[:40]]]

    cwd = meta.get("cwd")
    if not cwd:
        for m in messages:
            match = _CWD_TAG_RE.search(m.get("text", ""))
            if match:
                cwd = match.group(1).strip()
                break

    return {
        "session_id": meta.get("id") or jsonl_path.stem,
        "file": jsonl_path,
        "cwd": cwd,
        "originator": meta.get("originator"),
        "first_timestamp": first_ts,
        "total_messages": len(messages),
        "groups": rendered,
        "term_coverage": term_coverage,
        "match_count": match_count,
    }


def _parse_side_chat(record: dict, query: str | None, context_msgs: int) -> dict | None:
    messages = record.get("messages") or []
    if not messages:
        return None

    query_terms, _ = common.parse_query_terms(query)
    query_pattern = common.build_highlight_pattern(query_terms)
    term_coverage, match_count = 0, 0
    if query_terms:
        term_coverage, match_count = common.score_terms(
            [message.get("text", "") for message in messages],
            query_terms,
        )
        if term_coverage != len(query_terms):
            return None
        matching_indices = {
            message["index"] for message in messages if common.query_matches_any(message.get("text", ""), query_terms)
        }
        included: set[int] = set()
        for index in matching_indices:
            included.update(range(index - context_msgs, index + context_msgs + 1))
        groups: list[list[str]] = []
        current: list[str] = []
        last_index = -2
        for message in messages:
            index = message["index"]
            if index not in included:
                continue
            if index > last_index + 1 and current:
                groups.append(current)
                current = []
            current.append(
                _format_message(
                    message,
                    index in matching_indices,
                    query_pattern,
                )
            )
            last_index = index
        if current:
            groups.append(current)
    else:
        groups = [[_format_message(message, False, None) for message in messages[:40]]]

    return {
        **record,
        "groups": groups,
        "term_coverage": term_coverage,
        "match_count": match_count,
    }


def _side_chat_in_scope(
    record: dict,
    *,
    cwd: str | None,
    project: str | None,
    all_projects: bool,
) -> bool:
    if all_projects:
        return True
    record_cwd = record.get("cwd") or ""
    if project:
        return project.lower() in record_cwd.lower()
    return bool(record_cwd and common.is_path_under(record_cwd, cwd or os.getcwd()))


def _format_message(msg: dict, is_match: bool, pattern: re.Pattern | None) -> str:
    ts = common.format_ts(msg.get("timestamp"))
    text = common.truncate_text(msg.get("text", ""))
    text = common.highlight(text, pattern) if is_match else text
    marker = " ***" if is_match else ""
    if msg.get("is_tool"):
        return f"[{ts}] {text}"
    return f"[{ts}] {msg['role'].upper()}{marker}\n{text}"


def _to_normalized(conv: dict) -> common.NormalizedConversation:
    sid = conv.get("session_id") or ""
    sid_prefix = sid[:8] if sid else "????????"
    kind = "Side chat" if conv.get("side_chat") else "Session"
    header = f"--- {kind}: {sid_prefix} ---"
    header += f"  cwd: {common.collapse_home(conv.get('cwd') or '?')}"
    if conv.get("originator"):
        header += f"  via: {conv['originator']}"
    if conv.get("first_timestamp"):
        header += f"  started: {common.format_ts(conv['first_timestamp'])}"
    header += f"  ({conv['total_messages']} msgs)"

    extra_lines = []
    marker = _import_marker_for(sid)
    if marker:
        extra_lines.append(marker)
    if conv.get("side_chat") and conv.get("parent_id"):
        extra_lines.append(f"  [side chat of {conv['parent_id'][:8]}]")

    return common.NormalizedConversation(
        source=NAME,
        session_id=sid,
        sort_ts=common.parse_sort_ts(conv.get("first_timestamp")),
        header=header,
        groups=conv["groups"],
        extra_lines=extra_lines,
        term_coverage=conv.get("term_coverage", 0),
        match_count=conv.get("match_count", 0),
        total_messages=conv.get("total_messages", 0),
    )


def search(
    query: str,
    *,
    cwd: str | None,
    project: str | None,
    all_projects: bool,
    since: str | None,
    max_results: int,
    context: int,
    include_archived: bool = False,
    include_subagents: bool = False,
) -> list[common.NormalizedConversation]:
    results: list[common.NormalizedConversation] = []
    all_files = _discover_rollouts(include_archived=include_archived)
    scoped = _filter_by_scope(
        all_files,
        cwd=cwd,
        project=project,
        all_projects=all_projects,
    )
    candidates = common.ripgrep_files(query, scoped) if scoped else []
    if since:
        since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC)
        candidates = [f for f in candidates if datetime.fromtimestamp(f.stat().st_mtime, tz=UTC) >= since_dt]

    if len(candidates) > common.MAX_PARSE_FILES:
        print(
            f"[codex] {len(candidates)} files match; scoring the {common.MAX_PARSE_FILES} most query-dense — "
            "narrow the query for full coverage",
            file=sys.stderr,
        )
        candidates = candidates[: common.MAX_PARSE_FILES]

    parsed_bytes = 0
    for f in candidates:
        try:
            size = f.stat().st_size
        except OSError:
            continue
        parsed_bytes += size
        if parsed_bytes > common.MAX_PARSE_BYTES:
            # Skip, do not break: a smaller file later in density order may still fit.
            parsed_bytes -= size
            continue
        conv = _parse_conversation(f, query=query, context_msgs=context)
        if conv:
            results.append(_to_normalized(conv))

    since_ts = 0.0
    if since:
        since_ts = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
    for record in _desktop_side_chat_records():
        if not _side_chat_in_scope(
            record,
            cwd=cwd,
            project=project,
            all_projects=all_projects,
        ):
            continue
        if since_ts and common.parse_sort_ts(record.get("last_timestamp")) < since_ts:
            continue
        conversation = _parse_side_chat(record, query, context)
        if conversation:
            results.append(_to_normalized(conversation))
    return results


def browse(
    *,
    cwd: str | None,
    project: str | None,
    all_projects: bool,
    max_results: int,
    include_archived: bool = False,
    include_subagents: bool = False,
) -> list[common.NormalizedBrowseRow]:
    all_files = _discover_rollouts(include_archived=include_archived)
    scoped = _filter_by_scope(all_files, cwd=cwd, project=project, all_projects=all_projects)
    if not scoped:
        return []

    enriched = []
    for f in scoped:
        meta = _load_session_meta(str(f))
        if meta:
            enriched.append((meta, f))
    enriched.sort(key=lambda mf: mf[0].get("ts") or "", reverse=True)
    enriched = enriched[: max_results or 15]

    rows: list[common.NormalizedBrowseRow] = []
    for meta, f in enriched:
        sid = meta.get("id") or f.stem
        sid_prefix = sid[:8]
        ts = common.format_ts(meta.get("ts"))
        row_cwd = meta.get("cwd")
        origin = meta.get("originator") or "?"

        first_user = ""
        msg_count = 0
        try:
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    if '"type":"response_item"' not in line and not any(
                        f'"type":"{t}"' in line for t in _PAYLOAD_TYPES
                    ):
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    parsed = _as_payload(obj)
                    if parsed is None:
                        continue
                    p, _ts = parsed
                    msg_count += 1
                    if first_user:
                        continue
                    if p.get("type") == "message" and p.get("role") == "user":
                        text = _extract_text_parts(p.get("content"))
                        if text:
                            first_user = text.strip().replace("\n", " ")[:100]
                            if not row_cwd:
                                m = _CWD_TAG_RE.search(text)
                                if m:
                                    row_cwd = m.group(1).strip()
        except OSError:
            pass

        cwd_d = common.cwd_tail(row_cwd)

        marker = _import_marker_for(sid)
        line = f"{ts:<16}  {sid_prefix}  ({msg_count:>3} msgs)  cwd: {cwd_d}  via: {origin}"
        if marker:
            line += marker
        rows.append(
            common.NormalizedBrowseRow(
                source=NAME,
                sort_ts=common.parse_sort_ts(meta.get("ts")),
                line=line,
                preview=first_user or None,
            )
        )

    for record in _desktop_side_chat_records():
        if not _side_chat_in_scope(
            record,
            cwd=cwd,
            project=project,
            all_projects=all_projects,
        ):
            continue
        prompts = record.get("prompts") or []
        preview = prompts[0].strip().replace("\n", " ")[:100] if prompts else None
        timestamp = record.get("first_timestamp")
        line = (
            f"{common.format_ts(timestamp):<16}  {record['session_id'][:8]}  "
            f"({record['total_messages']:>3} msgs)  "
            f"cwd: {common.cwd_tail(record.get('cwd'))}  via: Codex Desktop side chat"
        )
        rows.append(
            common.NormalizedBrowseRow(
                source=NAME,
                sort_ts=common.parse_sort_ts(timestamp),
                line=line,
                preview=preview,
            )
        )
    return rows


def _extract_reasoning_text(payload: dict) -> str:
    """Reasoning blocks store text in `summary` (list of {type, text}) and/or `content` (same shape).
    When both are empty (only `encrypted_content` present), the raw text simply isn't recoverable."""
    parts: list[str] = []
    for key in ("summary", "content"):
        blocks = payload.get(key)
        if isinstance(blocks, list):
            for b in blocks:
                if isinstance(b, dict) and b.get("text"):
                    parts.append(b["text"])
    if not parts and payload.get("encrypted_content"):
        return "[reasoning content is encrypted by the model provider; not recoverable from the rollout file]"
    return "\n".join(parts)


def _resolve_rollout_matches(prefix: str) -> list[Path]:
    prefix = prefix.lower()
    matches: list[Path] = []
    for f in _discover_rollouts(include_archived=True):
        m = UUID_RE.search(f.name)
        if m and m.group(1).lower().startswith(prefix):
            matches.append(f)

    if not matches:
        for f in _discover_rollouts(include_archived=True):
            meta = _load_session_meta(str(f))
            mid = (meta or {}).get("id") or ""
            if mid.lower().startswith(prefix):
                matches.append(f)
                break
    return matches


def _parse_full_session(jsonl_path: Path) -> common.FullSession | None:
    """Untruncated, full-fidelity parse for the `read` skill: message/reasoning/function_call/
    function_call_output (and other *_call/*_call_output/*_output payload types) each become their
    own FullEntry, in file order, nothing dropped or shortened."""
    meta = _load_session_meta(str(jsonl_path)) or {}
    entries: list[common.FullEntry] = []
    call_names: dict[str, str] = {}
    ended_at: str | None = None
    model: str | None = None

    def add(role: str, timestamp: str | None, text: str, tool_name: str | None = None, raw: dict | None = None) -> None:
        entries.append(
            common.FullEntry(
                index=len(entries), role=role, timestamp=timestamp, text=text, tool_name=tool_name, raw=raw
            )
        )

    try:
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if obj.get("type") == "turn_context":
                    if not model:
                        model = (obj.get("payload") or {}).get("model")
                    continue

                parsed = _as_payload(obj)
                if parsed is None:
                    continue
                payload, ts = parsed
                ptype = payload.get("type")
                if ts:
                    ended_at = ts

                if ptype == "message":
                    role = payload.get("role") or "unknown"
                    text = _extract_text_parts(payload.get("content"))
                    if text:
                        add(role, ts, text, raw=payload)
                elif ptype == "reasoning":
                    text = _extract_reasoning_text(payload)
                    if text:
                        add("reasoning", ts, text, raw=payload)
                elif ptype in ("function_call", "custom_tool_call", "tool_search_call", "web_search_call"):
                    name = payload.get("name") or ptype
                    call_id = payload.get("call_id") or payload.get("id")
                    if call_id:
                        call_names[call_id] = name
                    text = payload.get("arguments")
                    if text is None:
                        text = json.dumps(
                            payload.get("input") or payload.get("action") or {}, indent=2, ensure_ascii=False
                        )
                    add("tool_call", ts, str(text), tool_name=name, raw=payload)
                elif ptype in ("function_call_output", "custom_tool_call_output", "tool_search_output"):
                    call_id = payload.get("call_id")
                    name = call_names.get(call_id)
                    text = payload.get("output")
                    if text is None:
                        text = json.dumps(payload.get("tools") or {}, indent=2, ensure_ascii=False)
                    add("tool_result", ts, str(text), tool_name=name, raw=payload)
    except (OSError, UnicodeDecodeError):
        return None

    if not entries:
        return None

    cwd = meta.get("cwd")
    if not cwd:
        for e in entries:
            m = _CWD_TAG_RE.search(e.text)
            if m:
                cwd = m.group(1).strip()
                break

    return common.FullSession(
        source=NAME,
        session_id=meta.get("id") or jsonl_path.stem,
        file_paths=[str(jsonl_path)],
        cwd=cwd,
        git_branch=None,
        model=model or meta.get("model_provider"),
        started_at=meta.get("ts"),
        ended_at=ended_at or meta.get("ts"),
        entries=entries,
    )


def _side_chat_full_session(record: dict) -> common.FullSession:
    entries = [
        common.FullEntry(
            index=index,
            role=message["role"],
            timestamp=(
                datetime.fromtimestamp(message["timestamp"], tz=UTC).isoformat()
                if message.get("timestamp") is not None
                else None
            ),
            text=message["text"],
            tool_name=(message["text"].split("]", 1)[0].removeprefix("[tool: ") if message.get("is_tool") else None),
        )
        for index, message in enumerate(record.get("messages") or [])
    ]
    return common.FullSession(
        source=NAME,
        session_id=record["session_id"],
        file_paths=[
            path
            for path in (
                record.get("record_path"),
                str(DESKTOP_STATE_FILE),
                str(DESKTOP_LOGS_DB),
            )
            if path
        ],
        cwd=record.get("cwd"),
        git_branch=None,
        model=record.get("model"),
        started_at=(
            datetime.fromtimestamp(record["first_timestamp"], tz=UTC).isoformat()
            if record.get("first_timestamp") is not None
            else None
        ),
        ended_at=(
            datetime.fromtimestamp(record["last_timestamp"], tz=UTC).isoformat()
            if record.get("last_timestamp") is not None
            else None
        ),
        entries=entries,
    )


def _matching_side_chats(prefix: str) -> list[dict]:
    lowered = prefix.lower()
    return [record for record in _desktop_side_chat_records() if record["session_id"].lower().startswith(lowered)]


def discover_export_targets(include_archived: bool = True) -> list[Path]:
    """All Codex rollout JSONL files on disk, for the `export` skill's bulk export."""
    if not store_exists():
        return []
    return _discover_rollouts(include_archived=include_archived)


def parse_export_target(path: Path) -> common.FullSession | None:
    return _parse_full_session(path)


def discover_side_chat_export_targets() -> list[dict]:
    """Codex Desktop side chats that are still represented in Desktop state."""
    return _desktop_side_chat_records()


def parse_side_chat_export_target(record: dict) -> common.FullSession:
    return _side_chat_full_session(record)


def resolve_full_session(prefix: str) -> common.FullSessionResult:
    if not store_exists():
        return common.FullSessionResult()

    matches = _resolve_rollout_matches(prefix)
    side_matches = _matching_side_chats(prefix)
    if not matches and not side_matches:
        return common.FullSessionResult()
    if len(matches) + len(side_matches) > 1:
        ambiguous = [str(match) for match in matches]
        ambiguous.extend(f"sidechat:{record['session_id']}" for record in side_matches)
        return common.FullSessionResult(ambiguous=ambiguous[:10])

    if side_matches:
        return common.FullSessionResult(session=_side_chat_full_session(side_matches[0]))

    session = _parse_full_session(matches[0])
    if not session:
        return common.FullSessionResult()
    return common.FullSessionResult(session=session)


def get_session(prefix: str, query: str | None, context: int) -> common.SessionLookup:
    if not store_exists():
        return common.SessionLookup()

    matches = _resolve_rollout_matches(prefix)
    side_matches = _matching_side_chats(prefix)

    if not matches and not side_matches:
        return common.SessionLookup()
    if len(matches) + len(side_matches) > 1:
        ambiguous = [str(match) for match in matches]
        ambiguous.extend(f"sidechat:{record['session_id']}" for record in side_matches)
        return common.SessionLookup(ambiguous=ambiguous[:10])

    if side_matches:
        conversation = _parse_side_chat(
            side_matches[0],
            query,
            context if query else 10_000,
        )
        if conversation is None and query:
            conversation = _parse_side_chat(side_matches[0], None, 10_000)
        return common.SessionLookup(conversation=_to_normalized(conversation) if conversation else None)

    conv = _parse_conversation(matches[0], query=query, context_msgs=context if query else 10_000)
    if conv is None and query:
        # The session exists but the query matched nothing in it — show the
        # session rather than falsely reporting it missing.
        conv = _parse_conversation(matches[0], query=None, context_msgs=10_000)
    if not conv:
        return common.SessionLookup()
    return common.SessionLookup(conversation=_to_normalized(conv))


def list_projects() -> list[tuple[str, int]]:
    if not store_exists():
        return []
    files = _discover_rollouts(include_archived=True)
    counts: dict[str, int] = {}
    for f in files:
        meta = _load_session_meta(str(f))
        if not meta:
            continue
        cwd = common.collapse_home(meta.get("cwd") or "(unknown)")
        counts[cwd] = counts.get(cwd, 0) + 1
    for record in _desktop_side_chat_records():
        cwd = common.collapse_home(record.get("cwd") or "(unknown)")
        counts[cwd] = counts.get(cwd, 0) + 1
    return list(counts.items())


def search_prompts(query: str | None, project: str | None, max_results: int) -> list[common.PromptRow]:
    query_terms, _ = common.parse_query_terms(query)
    sid_to_cwd: dict[str, str] = {}

    def resolve_cwd(session_id: str) -> str:
        if not session_id:
            return ""
        if session_id in sid_to_cwd:
            return sid_to_cwd[session_id]
        for f in _discover_rollouts(include_archived=True):
            if session_id in f.name:
                meta = _load_session_meta(str(f))
                if meta:
                    sid_to_cwd[session_id] = meta.get("cwd") or ""
                    return sid_to_cwd[session_id]
        sid_to_cwd[session_id] = ""
        return ""

    rows: list[common.PromptRow] = []
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, encoding="utf-8") as fh:
            for line in fh:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                text = obj.get("text") or ""
                if query_terms and not common.query_matches_all(text, query_terms):
                    continue
                sid = obj.get("session_id") or ""
                cwd = resolve_cwd(sid) if project else ""
                if project and project.lower() not in cwd.lower():
                    continue

                rows.append(
                    common.PromptRow(
                        source=NAME,
                        sort_ts=common.parse_sort_ts(obj.get("ts")),
                        ts_display=common.format_ts(obj.get("ts")),
                        session=sid[:8],
                        project=common.cwd_tail(common.collapse_home(cwd), 2) if cwd else "",
                        prompt=text,
                    )
                )

    for record in _desktop_side_chat_records():
        cwd = record.get("cwd") or ""
        if project and project.lower() not in cwd.lower():
            continue
        timestamp = record.get("last_timestamp") or record.get("first_timestamp")
        for prompt in record.get("prompts") or []:
            if query_terms and not common.query_matches_all(prompt, query_terms):
                continue
            rows.append(
                common.PromptRow(
                    source=NAME,
                    sort_ts=common.parse_sort_ts(timestamp),
                    ts_display=common.format_ts(timestamp),
                    session=record["session_id"][:8],
                    project=common.cwd_tail(common.collapse_home(cwd), 2) if cwd else "",
                    prompt=prompt,
                )
            )

    rows.sort(key=lambda r: r.sort_ts, reverse=True)
    return rows[: max_results or 30]


def list_imports(project: str | None, max_results: int) -> list[dict]:
    """Codex-specific: list external-agent session imports (no equivalent in other sources)."""
    records = _load_imports()
    if not records:
        return []

    rows = []
    for tid, rec in records.items():
        cwd = ""
        for f in _discover_rollouts(include_archived=True):
            if tid in f.name:
                meta = _load_session_meta(str(f))
                if meta:
                    cwd = meta.get("cwd") or ""
                break
        rows.append(
            {
                "imported_at": rec.get("imported_at"),
                "thread_id": tid,
                "source": rec.get("source_path") or "",
                "cwd": cwd,
            }
        )

    if project:
        rows = [r for r in rows if project.lower() in str(r.get("cwd") or "").lower()]

    rows.sort(key=lambda r: r["imported_at"] or 0, reverse=True)
    return rows[: max_results or 50]


if __name__ == "__main__":
    print("This module is a source adapter; invoke ../search_history.py instead.", file=sys.stderr)
    sys.exit(1)
