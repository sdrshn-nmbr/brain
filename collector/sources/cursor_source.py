"""Cursor IDE agent conversation history adapter.

Cursor has used three on-disk formats over time, all scoped by this adapter:
  - oldest: composer chats in globalStorage/state.vscdb cursorDiskKV (see cursor_state_db.py),
    reaching back to first Cursor use (Jan/Feb 2025); export/read only
  - legacy JSONL transcripts under ~/.cursor/projects/**/agent-transcripts/**/*.jsonl
  - newer protobuf/SQLite blob-DAG chats under ~/.cursor/chats/*/*/store.db (see cursor_store_db.py)

`search()` and `browse()` always query both backends and merge the results — the
old --source jsonl|db toggle from the standalone cursor-past skill is intentionally
not exposed here, since which backend a given chat happens to live in is exactly the
kind of engine detail this unified skill is meant to abstract away.

`get_session()`, `list_projects()`, and `search_prompts()` only cover the legacy
JSONL backend (store.db chats have no stable UUID-prefix session lookup or global
prompt log equivalent), matching the original cursor-past skill's scope.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from . import common
from . import cursor_state_db as csdb
from . import cursor_store_db as cdb

NAME = "cursor"

CURSOR_PROJECTS_DIR = Path.home() / ".cursor" / "projects"
CURSOR_CHATS_DIR = Path.home() / ".cursor" / "chats"

TIMESTAMP_RE = re.compile(r"<timestamp>(.*?)</timestamp>", re.DOTALL)
USER_QUERY_RE = re.compile(r"<user_query>(.*?)</user_query>", re.DOTALL)


def store_exists() -> bool:
    return CURSOR_PROJECTS_DIR.exists() or CURSOR_CHATS_DIR.exists()


# --------------------------------------------------------------------------
# legacy JSONL backend
# --------------------------------------------------------------------------
def _dir_name_to_project_path(dir_name: str) -> str:
    if dir_name in {"empty-window", "tmp-reports-24h-notebooks"}:
        return f"(cursor:{dir_name})"
    if dir_name.isdigit():
        return f"(cursor workspace {dir_name})"
    if not dir_name.startswith("Users-"):
        return dir_name

    candidate = "/" + dir_name.replace("-", "/")
    if os.path.exists(candidate):
        return candidate

    parts = dir_name.split("-")
    for i in range(len(parts), 1, -1):
        prefix = "/" + "/".join(parts[:i])
        suffix_parts = parts[i:]
        if suffix_parts:
            for j in range(len(suffix_parts), 0, -1):
                candidate = prefix + "/" + "-".join(suffix_parts[:j])
                if j < len(suffix_parts):
                    candidate += "/" + "-".join(suffix_parts[j:])
                if os.path.exists(candidate):
                    return candidate
        elif os.path.exists(prefix):
            return prefix
    return "/" + dir_name.replace("-", "/")


@lru_cache(maxsize=512)
def _project_path_for_dir(dir_name: str) -> str:
    return _dir_name_to_project_path(dir_name)


def _discover_project_dirs() -> dict[str, Path]:
    if not CURSOR_PROJECTS_DIR.exists():
        return {}
    projects: dict[str, Path] = {}
    for project_dir in CURSOR_PROJECTS_DIR.iterdir():
        if not project_dir.is_dir():
            continue
        transcript_dir = project_dir / "agent-transcripts"
        if not transcript_dir.is_dir() or not any(transcript_dir.glob("*/*.jsonl")):
            continue
        projects[_project_path_for_dir(project_dir.name)] = project_dir
    return projects


def _get_project_dirs_for_cwd(cwd: str, all_projects: dict[str, Path]) -> list[Path]:
    cwd_normalized = os.path.realpath(cwd).rstrip("/")
    prefix = cwd_normalized + "/"
    matching: list[Path] = []
    for path, project_dir in all_projects.items():
        if path.startswith("(cursor"):
            continue
        path_normalized = path.rstrip("/")
        if path_normalized == cwd_normalized or path_normalized.startswith(prefix):
            matching.append(project_dir)
    return matching


def _find_session_files(project_dir: Path, include_subagents: bool = False) -> list[Path]:
    files = list((project_dir / "agent-transcripts").glob("*/*.jsonl"))
    if not include_subagents:
        files = [f for f in files if "subagents" not in f.parts]
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _discover_all_sessions(include_subagents: bool = False) -> list[Path]:
    files: list[Path] = []
    if not CURSOR_PROJECTS_DIR.exists():
        return files
    for project_dir in CURSOR_PROJECTS_DIR.iterdir():
        if project_dir.is_dir():
            files.extend(_find_session_files(project_dir, include_subagents=include_subagents))
    return files


def _session_meta(jsonl_path: Path) -> dict:
    project_dir = jsonl_path.parents[2]
    project_path = _project_path_for_dir(project_dir.name)
    session_id = jsonl_path.parent.name if jsonl_path.parent.name != "subagents" else jsonl_path.stem
    if jsonl_path.parent.name == "subagents":
        session_id = f"{jsonl_path.parents[1].name}/subagent/{jsonl_path.stem}"
    return {
        "session_id": session_id,
        "project_path": project_path,
        "is_subagent": "subagents" in jsonl_path.parts,
    }


def _resolve_search_dirs(
    *, cwd: str | None, project: str | None, all_projects: bool, projects: dict[str, Path]
) -> list[Path]:
    if project:
        needle = project.lower()
        return [d for path, d in projects.items() if needle in path.lower() or needle in d.name.lower()]
    if all_projects:
        return list(projects.values())
    dirs = _get_project_dirs_for_cwd(cwd or os.getcwd(), projects)
    return dirs if dirs else list(projects.values())


def _collect_session_files(search_dirs: list[Path], include_subagents: bool) -> list[Path]:
    files: list[Path] = []
    for project_dir in search_dirs:
        files.extend(_find_session_files(project_dir, include_subagents=include_subagents))
    return files


def _clean_user_text(raw: str) -> tuple[str, str | None]:
    timestamp = None
    ts_match = TIMESTAMP_RE.search(raw)
    if ts_match:
        timestamp = ts_match.group(1).strip()
        raw = raw[: ts_match.start()] + raw[ts_match.end() :]

    query_match = USER_QUERY_RE.search(raw)
    text = query_match.group(1).strip() if query_match else raw.strip()
    text = re.sub(r"\[Image(?: #[0-9]+)?\]", "", text).strip()
    return text, timestamp


def _extract_message_text(content) -> tuple[str, list[str]]:
    if isinstance(content, str):
        return content, []
    if not isinstance(content, list):
        return "", []
    text_parts: list[str] = []
    tools: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = re.sub(r"\n*\[REDACTED\]\s*", "\n", (block.get("text") or "").strip()).strip()
            if text:
                text_parts.append(text)
        elif block.get("type") == "tool_use":
            tools.append(block.get("name") or "unknown")
    return "\n".join(text_parts), tools


def _parse_conversation(jsonl_path: Path, query: str | None = None, context_msgs: int = 2) -> dict | None:
    meta = _session_meta(jsonl_path)
    messages: list[dict] = []
    first_timestamp: str | None = None

    query_terms, _ = common.parse_query_terms(query)
    query_pattern = common.build_highlight_pattern(query_terms)

    try:
        with open(jsonl_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                role = obj.get("role")
                if role not in ("user", "assistant"):
                    continue

                content = (obj.get("message") or {}).get("content", [])
                text, tools = _extract_message_text(content)
                timestamp = None
                if role == "user":
                    text, timestamp = _clean_user_text(text)
                text = common.strip_injected_noise(text).strip()
                if not text and not tools:
                    continue
                if timestamp and not first_timestamp:
                    first_timestamp = timestamp

                entry: dict = {"role": role, "text": text, "timestamp": timestamp, "index": len(messages)}
                if tools:
                    entry["tools"] = tools
                messages.append(entry)
    except (OSError, UnicodeDecodeError):
        return None

    if not first_timestamp or common.parse_sort_ts(first_timestamp) == 0.0:
        # Legacy timestamps can be year-less human text ('Thursday, Jul 9,
        # 11:58 AM') — unparseable, which used to sort these sessions as 1970.
        # File mtime is an honest approximation for both sorting and display.
        first_timestamp = datetime.fromtimestamp(jsonl_path.stat().st_mtime, tz=UTC).isoformat()
    if not messages:
        return None

    term_coverage, match_count = 0, 0
    if query_terms:
        matching_indices = {
            m["index"]
            for m in messages
            if common.query_matches_any(m.get("text", ""), query_terms)
            or any(common.query_matches_any(t, query_terms) for t in m.get("tools", []))
        }
        if not matching_indices:
            return None
        term_coverage, match_count = common.score_terms(
            ["\n".join([m.get("text", ""), *m.get("tools", [])]) for m in messages], query_terms
        )

        included: set[int] = set()
        for idx in matching_indices:
            for offset in range(-context_msgs, context_msgs + 1):
                included.add(idx + offset)

        groups: list[list[str]] = []
        current: list[str] = []
        last_idx = -2
        for msg in messages:
            if msg["index"] in included:
                if msg["index"] > last_idx + 1 and current:
                    groups.append(current)
                    current = []
                is_match = msg["index"] in matching_indices
                current.append(_format_message(msg, is_match, query_pattern))
                last_idx = msg["index"]
        if current:
            groups.append(current)
        rendered = groups
    else:
        rendered = [[_format_message(m, False, None) for m in messages[:40]]]

    return {
        "session_id": meta["session_id"],
        "file": jsonl_path,
        "project_path": meta["project_path"],
        "is_subagent": meta["is_subagent"],
        "first_timestamp": first_timestamp,
        "total_messages": len(messages),
        "groups": rendered,
        "term_coverage": term_coverage,
        "match_count": match_count,
    }


def _format_message(msg: dict, is_match: bool, pattern: re.Pattern | None) -> str:
    ts = common.format_ts(msg.get("timestamp"))
    text = common.truncate_text(msg.get("text", ""))
    if is_match and text:
        text = common.highlight(text, pattern)

    tools = msg.get("tools", [])
    tool_lines = [f"[tool: {name}]" for name in tools]
    if tool_lines and not text:
        body = "\n".join(tool_lines)
    elif tool_lines:
        body = text + "\n" + "\n".join(tool_lines)
    else:
        body = text

    marker = " ***" if is_match else ""
    prefix = f"[{ts}] " if ts else ""
    return f"{prefix}{msg['role'].upper()}{marker}\n{body}"


def _to_normalized(conv: dict) -> common.NormalizedConversation:
    sid = conv.get("session_id") or "?"
    sid_prefix = sid.split("/")[0][:8]
    header = f"--- Session: {sid_prefix} ---"
    header += f"  project: {common.collapse_home(conv.get('project_path') or '?')}"
    if conv.get("is_subagent"):
        header += "  [subagent]"
    if conv.get("first_timestamp"):
        header += f"  started: {common.format_ts(conv['first_timestamp'])}"
    header += f"  ({conv['total_messages']} msgs)"
    return common.NormalizedConversation(
        source=NAME,
        session_id=sid,
        sort_ts=common.parse_sort_ts(conv.get("first_timestamp")),
        header=header,
        groups=conv["groups"],
        term_coverage=conv.get("term_coverage", 0),
        match_count=conv.get("match_count", 0),
        total_messages=conv.get("total_messages", 0),
    )


# --------------------------------------------------------------------------
# store.db backend
# --------------------------------------------------------------------------
def _db_result_to_normalized(r: dict, pattern: re.Pattern | None) -> common.NormalizedConversation:
    sid_prefix = Path(r["db_path"]).parent.name[:8]
    header = f"--- Chat: {r['name'] or '(unnamed)'}  [{sid_prefix}] ---"
    if r.get("model"):
        header += f"  model: {r['model']}"
    if r.get("workspace"):
        header += f"  ws: {common.collapse_home(r['workspace'])}"
    header += f"  created: {common.format_ts(r.get('created_at', 0))}  ({r['total_messages']} msgs)"

    groups: list[list[str]] = []
    for group in r["groups"]:
        lines = []
        for (_idx, role, text), is_match in group:
            text = common.truncate_text(cdb.sanitize(text or "").strip())
            if is_match:
                text = common.highlight(text, pattern)
            marker = " ***" if is_match else ""
            lines.append(f"[{role}]{marker} {text}")
        groups.append(lines)

    session_id = Path(r["db_path"]).parent.name
    return common.NormalizedConversation(
        source=NAME,
        session_id=session_id,
        sort_ts=common.parse_sort_ts(r.get("created_at", 0)),
        term_coverage=r.get("term_coverage", 0),
        match_count=r.get("match_count", 0),
        total_messages=r.get("total_messages", 0),
        header=header,
        groups=groups,
    )


def _db_scope(*, cwd: str | None, project: str | None, all_projects: bool) -> list[str]:
    if not CURSOR_CHATS_DIR.exists():
        return []
    scoped = cdb.scope_dbs(cwd=cwd or os.getcwd(), project=project, all_projects=all_projects)
    return [db for db, _ in scoped]


def _db_browse_rows(
    *, cwd: str | None, project: str | None, all_projects: bool, max_results: int
) -> list[common.NormalizedBrowseRow]:
    dbs = _db_scope(cwd=cwd, project=project, all_projects=all_projects)
    if not dbs:
        return []

    scored: list[tuple[float, common.NormalizedBrowseRow]] = []
    for db_path in dbs:
        meta, msgs = cdb.db_messages(db_path)
        if not meta or not msgs:
            continue
        first_user = next(
            (cdb.sanitize(text).strip().replace("\n", " ")[:100] for _, role, text in msgs if role == "user"), None
        )
        ts = common.format_ts(meta.get("created_at", 0))
        name = meta.get("name") or "(unnamed)"
        agent_dir = Path(db_path).parent.name[:8]
        line = f"{ts:<16}  {agent_dir}  ({len(msgs):>3} msgs)  {name}"
        sort_ts = common.parse_sort_ts(meta.get("created_at", 0))
        scored.append(
            (sort_ts, common.NormalizedBrowseRow(source=NAME, sort_ts=sort_ts, line=line, preview=first_user))
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    return [row for _, row in scored[: max_results or 15]]


# --------------------------------------------------------------------------
# public adapter surface
# --------------------------------------------------------------------------
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

    if CURSOR_PROJECTS_DIR.exists():
        projects = _discover_project_dirs()
        search_dirs = _resolve_search_dirs(cwd=cwd, project=project, all_projects=all_projects, projects=projects)
        files = _collect_session_files(search_dirs, include_subagents=include_subagents)
        candidates = common.ripgrep_files(query, files)

        if since:
            since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC)
            candidates = [f for f in candidates if datetime.fromtimestamp(f.stat().st_mtime, tz=UTC) >= since_dt]

        if len(candidates) > common.MAX_PARSE_FILES:
            print(
                f"[cursor] {len(candidates)} files match; scoring the {common.MAX_PARSE_FILES} most query-dense — "
                "narrow the query for full coverage",
                file=sys.stderr,
            )
            candidates = candidates[: common.MAX_PARSE_FILES]

        parsed_bytes = 0
        for path in candidates:
            try:
                size = path.stat().st_size
            except OSError:
                continue
            parsed_bytes += size
            if parsed_bytes > common.MAX_PARSE_BYTES:
                # Skip, do not break: a smaller file later in density order may still fit.
                parsed_bytes -= size
                continue
            conv = _parse_conversation(path, query=query, context_msgs=context)
            if conv:
                results.append(_to_normalized(conv))

    dbs = _db_scope(cwd=cwd, project=project, all_projects=all_projects)
    if dbs:
        db_results = cdb.search_with_context(query, dbs, context=context, max_results=max_results or 10)
        if since:
            try:
                since_ms = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1000
                db_results = [r for r in db_results if r.get("created_at", 0) >= since_ms]
            except ValueError:
                pass
        pattern = common.build_highlight_pattern(common.parse_query_terms(query)[0])
        results.extend(_db_result_to_normalized(r, pattern) for r in db_results)

    # The same chat can surface multiple times: legacy JSONL + store.db copies,
    # the same transcript registered under several project dirs (symlink twins,
    # the empty-window catchall), or duplicate store.db dirs. Keep the richest
    # copy per session UUID or duplicates burn slots of the capped result list.
    best: dict[str, common.NormalizedConversation] = {}
    order: list[str] = []
    for c in results:
        key = c.session_id.split("/")[0]
        held = best.get(key)
        if held is None:
            best[key] = c
            order.append(key)
        elif (c.match_count, c.total_messages) > (held.match_count, held.total_messages):
            best[key] = c
    return [best[k] for k in order]


def browse(
    *,
    cwd: str | None,
    project: str | None,
    all_projects: bool,
    max_results: int,
    include_archived: bool = False,
    include_subagents: bool = False,
) -> list[common.NormalizedBrowseRow]:
    rows: list[common.NormalizedBrowseRow] = []

    if CURSOR_PROJECTS_DIR.exists():
        projects = _discover_project_dirs()
        search_dirs = _resolve_search_dirs(cwd=cwd, project=project, all_projects=all_projects, projects=projects)
        all_files = _collect_session_files(search_dirs, include_subagents=include_subagents)
        all_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        all_files = all_files[: max_results or 15]

        for path in all_files:
            conv = _parse_conversation(path)
            if not conv:
                continue
            sid_prefix = conv["session_id"].split("/")[0][:8]
            ts = common.format_ts(conv.get("first_timestamp"))
            project_path = common.collapse_home(conv.get("project_path") or "?")
            project_tail = "/".join(project_path.split("/")[-2:]) if project_path else "?"
            subagent = "  [subagent]" if conv.get("is_subagent") else ""

            first_user = ""
            for group in conv["groups"]:
                for msg_text in group:
                    if msg_text.startswith("USER\n") or "\nUSER\n" in msg_text:
                        first_user = msg_text.split("\n", 1)[-1][:100].replace("\n", " ")
                        break
                if first_user:
                    break

            line = f"{ts:<16}  {sid_prefix}  ({conv['total_messages']:>3} msgs)  {project_tail}{subagent}"
            rows.append(
                common.NormalizedBrowseRow(
                    source=NAME,
                    sort_ts=common.parse_sort_ts(conv.get("first_timestamp")),
                    line=line,
                    preview=first_user or None,
                )
            )

    rows.extend(_db_browse_rows(cwd=cwd, project=project, all_projects=all_projects, max_results=max_results))
    return rows


def get_session(prefix: str, query: str | None, context: int) -> common.SessionLookup:
    prefix = prefix.lower()
    matches: list[Path] = []
    for path in _discover_all_sessions(include_subagents=True):
        if path.stem.lower().startswith(prefix) or path.parent.name.lower().startswith(prefix):
            matches.append(path)

    if not matches:
        return common.SessionLookup()
    if len(matches) > 1:
        return common.SessionLookup(ambiguous=[str(m) for m in matches[:10]])

    conv = _parse_conversation(matches[0], query=query, context_msgs=context if query else 10_000)
    if conv is None and query:
        # The session exists but the query matched nothing in it — show the
        # session rather than falsely reporting it missing.
        conv = _parse_conversation(matches[0], query=None, context_msgs=10_000)
    if not conv:
        return common.SessionLookup()
    return common.SessionLookup(conversation=_to_normalized(conv))


# --------------------------------------------------------------------------
# full-fidelity extraction (for the `read` skill)
# --------------------------------------------------------------------------
def _resolve_legacy_matches(prefix: str) -> list[Path]:
    matches: list[Path] = []
    for path in _discover_all_sessions(include_subagents=True):
        if path.stem.lower().startswith(prefix) or path.parent.name.lower().startswith(prefix):
            matches.append(path)
    return matches


def _resolve_db_matches(prefix: str) -> list[str]:
    if not CURSOR_CHATS_DIR.exists():
        return []
    matches: list[str] = []
    for outer in CURSOR_CHATS_DIR.iterdir():
        if not outer.is_dir():
            continue
        for inner in outer.iterdir():
            if inner.is_dir() and inner.name.lower().startswith(prefix) and (inner / "store.db").exists():
                matches.append(str(inner / "store.db"))
    return matches


def _uuid_for_legacy_path(path: Path) -> str:
    return path.parent.name if path.parent.name != "subagents" else path.stem


def _uuid_for_db_path(db_path: str) -> str:
    return Path(db_path).parent.name


def _parse_full_session_from_db(db_path: str) -> common.FullSession | None:
    meta, msgs = cdb.load_full_messages(db_path)
    if not meta or not msgs:
        return None

    entries = [
        common.FullEntry(
            index=m["index"],
            role="tool_call" if m["role"] == "tool" else m["role"],
            timestamp=None,
            text=(f"[{m['title']}]\n{m['text']}" if m.get("title") else m["text"]),
            tool_name=None,
            raw=m,
        )
        for m in msgs
    ]
    started_at = meta.get("created_at")
    return common.FullSession(
        source=NAME,
        session_id=_uuid_for_db_path(db_path),
        file_paths=[db_path],
        cwd=meta.get("workspace"),
        git_branch=None,
        model=meta.get("model"),
        started_at=started_at,
        ended_at=started_at,
        entries=entries,
    )


def _parse_full_session_from_legacy(jsonl_path: Path) -> common.FullSession | None:
    entries: list[common.FullEntry] = []
    first_timestamp: str | None = None
    last_timestamp: str | None = None

    try:
        with open(jsonl_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                role = obj.get("role")
                if role not in ("user", "assistant"):
                    continue
                content = (obj.get("message") or {}).get("content", [])
                if isinstance(content, str):
                    content = [{"type": "text", "text": content}]
                if not isinstance(content, list):
                    continue

                for block in content:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text":
                        text = block.get("text") or ""
                        timestamp = None
                        if role == "user":
                            text, timestamp = _clean_user_text(text)
                        text = re.sub(r"\n*\[REDACTED\]\s*", "\n", text).strip()
                        if timestamp and not first_timestamp:
                            first_timestamp = timestamp
                        if timestamp:
                            last_timestamp = timestamp
                        if text:
                            entries.append(
                                common.FullEntry(
                                    index=len(entries), role=role, timestamp=timestamp, text=text, raw=block
                                )
                            )
                    elif btype == "tool_use":
                        name = block.get("name") or "unknown"
                        text = json.dumps(block.get("input", {}), indent=2, ensure_ascii=False)
                        entries.append(
                            common.FullEntry(
                                index=len(entries),
                                role="tool_call",
                                timestamp=None,
                                text=text,
                                tool_name=name,
                                raw=block,
                            )
                        )
    except (OSError, UnicodeDecodeError):
        return None

    if not entries:
        return None
    if not first_timestamp:
        first_timestamp = datetime.fromtimestamp(jsonl_path.stat().st_mtime, tz=UTC).isoformat()

    meta = _session_meta(jsonl_path)
    return common.FullSession(
        source=NAME,
        session_id=meta["session_id"],
        file_paths=[str(jsonl_path)],
        cwd=meta["project_path"],
        git_branch=None,
        model=None,
        started_at=first_timestamp,
        ended_at=last_timestamp or first_timestamp,
        entries=entries,
    )


def discover_export_targets(include_subagents: bool = True) -> list[tuple[str, str, str]]:
    """Enumerate every Cursor session UUID across
    all three backends and prefers store.db > legacy JSONL > state.vscdb composer per UUID (same
    preference as resolve_full_session), returning (uuid, backend, path) tuples for parse_export_target().

    The state.vscdb backend (globalStorage composerData/bubbleId rows) is the oldest format and
    reaches back to the first Cursor use on this machine (Jan/Feb 2025); a composer id that also
    exists in a newer backend is skipped, since the newer copy is richer (per-message timestamps)."""
    legacy_files = _discover_all_sessions(include_subagents=include_subagents)
    db_paths = cdb.discover_dbs()

    uuids: dict[str, dict] = {}
    for p in legacy_files:
        uuids.setdefault(_uuid_for_legacy_path(p), {})["legacy"] = str(p)
    for d in db_paths:
        uuids.setdefault(_uuid_for_db_path(d), {})["db"] = d
    for cid in csdb.discover_composers():
        uuids.setdefault(cid, {})["statedb"] = cid

    targets: list[tuple[str, str, str]] = []
    for uid, refs in uuids.items():
        if "db" in refs:
            targets.append((uid, "db", refs["db"]))
        elif "legacy" in refs:
            targets.append((uid, "legacy", refs["legacy"]))
        else:
            targets.append((uid, "statedb", refs["statedb"]))
    return targets


def parse_export_target(target: tuple[str, str, str]) -> common.FullSession | None:
    _uid, backend, path = target
    if backend == "db":
        return _parse_full_session_from_db(path)
    if backend == "statedb":
        return csdb.parse_composer(path)
    return _parse_full_session_from_legacy(Path(path))


def resolve_full_session(prefix: str) -> common.FullSessionResult:
    prefix = prefix.lower()
    legacy_matches = _resolve_legacy_matches(prefix)
    db_matches = _resolve_db_matches(prefix)
    statedb_matches = [cid for cid in csdb.discover_composers() if cid.lower().startswith(prefix)]

    uuids: dict[str, dict] = {}
    for p in legacy_matches:
        uuids.setdefault(_uuid_for_legacy_path(p), {})["legacy"] = p
    for d in db_matches:
        uuids.setdefault(_uuid_for_db_path(d), {})["db"] = d
    for cid in statedb_matches:
        uuids.setdefault(cid, {})["statedb"] = cid

    if not uuids:
        return common.FullSessionResult()
    if len(uuids) > 1:
        labels = []
        for uid, refs in list(uuids.items())[:10]:
            backend = "store.db" if "db" in refs else ("legacy jsonl" if "legacy" in refs else "state.vscdb")
            labels.append(f"{uid} ({backend})")
        return common.FullSessionResult(ambiguous=labels)

    refs = next(iter(uuids.values()))
    session = None
    if "db" in refs:
        session = _parse_full_session_from_db(refs["db"])
    if session is None and "legacy" in refs:
        session = _parse_full_session_from_legacy(refs["legacy"])
    if session is None and "statedb" in refs:
        session = csdb.parse_composer(refs["statedb"])
    if not session:
        return common.FullSessionResult()
    return common.FullSessionResult(session=session)


def list_projects() -> list[tuple[str, int]]:
    projects = _discover_project_dirs()
    rows = []
    for path, project_dir in projects.items():
        count = len(_find_session_files(project_dir, include_subagents=True))
        if count:
            rows.append((path, count))
    return rows


def search_prompts(query: str | None, project: str | None, max_results: int) -> list[common.PromptRow]:
    query_terms, _ = common.parse_query_terms(query)

    projects = _discover_project_dirs()
    search_dirs = (
        _resolve_search_dirs(cwd=None, project=project, all_projects=not project, projects=projects)
        if project
        else list(projects.values())
    )
    files = _collect_session_files(search_dirs, include_subagents=False)

    rows: list[common.PromptRow] = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("role") != "user":
                        continue
                    content = (obj.get("message") or {}).get("content", [])
                    text, ts = _clean_user_text(_extract_message_text(content)[0])
                    if not text:
                        continue
                    if query_terms and not common.query_matches_all(text, query_terms):
                        continue
                    meta = _session_meta(path)
                    sort_ts = common.parse_sort_ts(ts) or path.stat().st_mtime
                    rows.append(
                        common.PromptRow(
                            source=NAME,
                            sort_ts=sort_ts,
                            ts_display=common.format_ts(ts) or common.format_ts(path.stat().st_mtime),
                            session=meta["session_id"][:8],
                            project=common.cwd_tail(common.collapse_home(meta["project_path"]), 2),
                            prompt=text,
                        )
                    )
                    break
        except OSError:
            continue

    rows.sort(key=lambda r: r.sort_ts, reverse=True)
    return rows[: max_results or 30]


if __name__ == "__main__":
    print("This module is a source adapter; invoke ../search_history.py instead.", file=sys.stderr)
    sys.exit(1)
