"""Claude Code conversation history adapter.

Reads ~/.claude/projects/*.jsonl (per-session transcripts) and ~/.claude/history.jsonl
(global prompt log). Ported from the original Claude-only search_history.py.

Task-tool subagent invocations get their own full transcript at
<project-dir>/<session-uuid>/subagents/agent-<hash>.jsonl — the parent session's own
.jsonl only gets a short "progress" summary of what the subagent did, not its turns.
These are treated like Cursor's subagent files: discoverable/searchable/exportable as
their own session, tagged "{parent_session_id}/subagent/{stem}" and labeled [subagent],
toggled via include_subagents (default off for search/browse, on for export).
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from . import common

NAME = "claude"

CLAUDE_DIR = Path.home() / ".claude"
PROJECTS_DIR = CLAUDE_DIR / "projects"
HISTORY_FILE = CLAUDE_DIR / "history.jsonl"


def store_exists() -> bool:
    return PROJECTS_DIR.exists()


def _resolve_project_path_from_jsonl(project_dir: Path) -> str | None:
    for f in project_dir.glob("*.jsonl"):
        try:
            with open(f) as fh:
                for line in fh:
                    obj = json.loads(line.strip())
                    cwd = obj.get("cwd")
                    if cwd and os.path.sep in cwd:
                        return cwd
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
    return None


def _dir_name_to_project_path(dir_name: str, project_dir: Path | None = None) -> str:
    if project_dir:
        resolved = _resolve_project_path_from_jsonl(project_dir)
        if resolved:
            return resolved

    if not dir_name.startswith("-"):
        return dir_name
    candidate = "/" + dir_name[1:].replace("-", "/")
    if os.path.exists(candidate):
        return candidate
    parts = dir_name[1:].split("-")
    for i in range(len(parts), 0, -1):
        candidate = "/" + "/".join(parts[:i])
        if i < len(parts):
            candidate += "/" + "-".join(parts[i:])
        if os.path.exists(candidate):
            return candidate
    return "/" + dir_name[1:].replace("-", "/")


def _discover_project_dirs() -> dict[str, Path]:
    if not PROJECTS_DIR.exists():
        return {}
    projects = {}
    for d in PROJECTS_DIR.iterdir():
        if d.is_dir() and d.name != ".":
            if not any(d.glob("*.jsonl")):
                continue
            projects[_dir_name_to_project_path(d.name, d)] = d
    return projects


def _get_project_dirs_for_cwd(cwd: str, all_projects: dict[str, Path]) -> list[Path]:
    cwd_normalized = os.path.realpath(cwd).rstrip("/")
    prefix = cwd_normalized + "/"
    matching = []
    for path, d in all_projects.items():
        path_normalized = path.rstrip("/")
        if path_normalized == cwd_normalized or path_normalized.startswith(prefix):
            matching.append(d)
    return matching


def _find_jsonl_files(project_dir: Path, include_subagents: bool = False) -> list[Path]:
    files = list(project_dir.glob("*.jsonl"))
    if include_subagents:
        files.extend(_find_subagent_files(project_dir))
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _find_subagent_files(project_dir: Path) -> list[Path]:
    """Subagent transcripts usually sit at <session>/subagents/agent-<hash>.jsonl, but
    multi-agent workflow orchestration (teammate-mode plugins) nests them deeper at
    <session>/subagents/workflows/<wf-id>/agent-<hash>.jsonl. journal.jsonl next to them is
    orchestration bookkeeping (`{"type": "started", ...}`), not a conversation transcript."""
    return [f for f in project_dir.glob("*/subagents/**/*.jsonl") if f.name != "journal.jsonl"]


def _is_subagent_file(path: Path) -> bool:
    return "subagents" in path.parts


def _subagent_session_id(path: Path) -> str:
    parts = path.parts
    parent_session_id = parts[parts.index("subagents") - 1]
    return f"{parent_session_id}/subagent/{path.stem}"


def _resolve_search_dirs(
    *, cwd: str | None, project: str | None, all_projects: bool, projects: dict[str, Path]
) -> list[Path]:
    if project:
        dirs = [
            d for path, d in projects.items() if project.lower() in path.lower() or project.lower() in d.name.lower()
        ]
        return dirs

    if all_projects:
        return list(projects.values())

    dirs = _get_project_dirs_for_cwd(cwd or os.getcwd(), projects)
    return dirs if dirs else list(projects.values())


def _parse_conversation(jsonl_path: Path, query: str | None = None, context_msgs: int = 2) -> dict | None:
    messages = []
    session_id = None
    git_branch = None
    first_timestamp = None
    last_timestamp = None

    query_terms, _ = common.parse_query_terms(query)
    query_pattern = common.build_highlight_pattern(query_terms)

    try:
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                msg_type = obj.get("type")
                timestamp = obj.get("timestamp", "")
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp / 1000, tz=UTC).isoformat()

                if not session_id:
                    session_id = obj.get("sessionId")
                if not git_branch:
                    git_branch = obj.get("gitBranch")
                if timestamp and not first_timestamp:
                    first_timestamp = timestamp
                if timestamp:
                    last_timestamp = timestamp

                if msg_type == "user":
                    content = obj.get("message", {}).get("content", "")
                    if isinstance(content, list):
                        text_parts = []
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "text":
                                text_parts.append(block["text"])
                            elif isinstance(block, str):
                                text_parts.append(block)
                        content = "\n".join(text_parts)
                    content = common.strip_injected_noise(content).strip()
                    if content:
                        messages.append(
                            {"role": "user", "text": content, "timestamp": timestamp, "index": len(messages)}
                        )

                elif msg_type == "assistant":
                    msg = obj.get("message", {})
                    content_blocks = msg.get("content", [])
                    text_parts = []
                    tools_used = []
                    for block in content_blocks:
                        if not isinstance(block, dict):
                            continue
                        if block.get("type") == "text":
                            text = block["text"].strip()
                            if text:
                                text_parts.append(text)
                        elif block.get("type") == "tool_use":
                            tools_used.append(block.get("name", "unknown"))
                            # Tool inputs are where actions live (commands run, files
                            # written); without this line they are unsearchable.
                            tool_input = block.get("input")
                            if tool_input:
                                compact = json.dumps(tool_input, default=str)[:2000]
                                text_parts.append(f"[tool: {block.get('name', 'unknown')}] {compact}")
                    combined_text = "\n".join(text_parts)
                    if combined_text or tools_used:
                        entry = {
                            "role": "assistant",
                            "text": combined_text,
                            "timestamp": timestamp,
                            "index": len(messages),
                        }
                        if tools_used:
                            entry["tools"] = tools_used
                        messages.append(entry)

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

        included = set()
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
        rendered = [[_format_message(m, False, None) for m in messages[:20]]]

    is_subagent = _is_subagent_file(jsonl_path)
    session_id = _subagent_session_id(jsonl_path) if is_subagent else (session_id or jsonl_path.stem)

    return {
        "session_id": session_id,
        "file": jsonl_path,
        "git_branch": git_branch,
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
        "total_messages": len(messages),
        "groups": rendered,
        "is_subagent": is_subagent,
        "term_coverage": term_coverage,
        "match_count": match_count,
    }


def _format_message(msg: dict, is_match: bool, pattern: re.Pattern | None) -> str:
    role = msg["role"].upper()
    ts = common.format_ts(msg.get("timestamp"))
    text = common.truncate_text(msg.get("text", ""))
    text = common.highlight(text, pattern) if is_match else text
    tools = msg.get("tools", [])
    tools_str = f"  [tools: {', '.join(tools)}]" if tools else ""
    marker = " ***" if is_match else ""
    return f"[{ts}] {role}{tools_str}{marker}\n{text}"


def _to_normalized(conv: dict) -> common.NormalizedConversation:
    header = f"--- Session: {conv['session_id']} ---"
    if conv.get("git_branch"):
        header += f"  branch: {conv['git_branch']}"
    if conv.get("first_timestamp"):
        header += f"  started: {common.format_ts(conv['first_timestamp'])}"
    header += f"  ({conv['total_messages']} messages)"
    if conv.get("is_subagent"):
        header += "  [subagent]"
    return common.NormalizedConversation(
        source=NAME,
        session_id=conv["session_id"],
        sort_ts=common.parse_sort_ts(conv.get("first_timestamp")),
        header=header,
        groups=conv["groups"],
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
    if not store_exists():
        return []

    projects = _discover_project_dirs()
    search_dirs = _resolve_search_dirs(cwd=cwd, project=project, all_projects=all_projects, projects=projects)

    files: list[Path] = []
    for d in search_dirs:
        files.extend(_find_jsonl_files(d, include_subagents=include_subagents))

    candidates = common.ripgrep_files(query, files)

    if since:
        since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC)
        candidates = [f for f in candidates if datetime.fromtimestamp(f.stat().st_mtime, tz=UTC) >= since_dt]

    if len(candidates) > common.MAX_PARSE_FILES:
        print(
            f"[claude] {len(candidates)} files match; scoring the {common.MAX_PARSE_FILES} most query-dense — "
            "narrow the query for full coverage",
            file=sys.stderr,
        )
        candidates = candidates[: common.MAX_PARSE_FILES]

    results: list[common.NormalizedConversation] = []
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
    if not store_exists():
        return []

    projects = _discover_project_dirs()
    target_dirs = _resolve_search_dirs(cwd=cwd, project=project, all_projects=all_projects, projects=projects)

    all_files: list[Path] = []
    for d in target_dirs:
        all_files.extend(_find_jsonl_files(d, include_subagents=include_subagents))
    all_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    all_files = all_files[: max_results or 15]

    rows: list[common.NormalizedBrowseRow] = []
    for f in all_files:
        conv = _parse_conversation(f, context_msgs=0)
        if not conv:
            continue
        first_user_msg = ""
        for group in conv["groups"]:
            for msg_text in group:
                if "] USER" in msg_text:
                    first_user_msg = msg_text.split("\n", 1)[-1][:100]
                    break
            if first_user_msg:
                break

        ts = common.format_ts(conv.get("first_timestamp"))
        branch = conv.get("git_branch", "")
        subagent_tag = "  [subagent]" if conv.get("is_subagent") else ""
        line = f"{ts:<16}  {conv['session_id']}  ({conv['total_messages']:>3} msgs)  [{branch}]{subagent_tag}"
        rows.append(
            common.NormalizedBrowseRow(
                source=NAME,
                sort_ts=common.parse_sort_ts(conv.get("first_timestamp")),
                line=line,
                preview=first_user_msg or None,
            )
        )
    return rows


def _parse_full_session(jsonl_path: Path) -> common.FullSession | None:
    """Untruncated, full-fidelity parse for the `read` skill: every text/thinking/tool_use/tool_result
    block becomes its own FullEntry, in file order, nothing dropped or shortened."""
    entries: list[common.FullEntry] = []
    tool_names: dict[str, str] = {}
    session_id: str | None = None
    git_branch: str | None = None
    cwd: str | None = None
    model: str | None = None
    started_at: str | None = None
    ended_at: str | None = None

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

                if not session_id:
                    session_id = obj.get("sessionId")
                if not git_branch:
                    git_branch = obj.get("gitBranch")
                if not cwd:
                    cwd = obj.get("cwd")

                timestamp = obj.get("timestamp", "")
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp / 1000, tz=UTC).isoformat()
                if timestamp:
                    if not started_at:
                        started_at = timestamp
                    ended_at = timestamp

                msg_type = obj.get("type")
                if msg_type == "user":
                    content = obj.get("message", {}).get("content", "")
                    if isinstance(content, str):
                        if content:
                            add("user", timestamp, content, raw=obj)
                    elif isinstance(content, list):
                        for block in content:
                            if not isinstance(block, dict):
                                continue
                            btype = block.get("type")
                            if btype == "text":
                                text = block.get("text", "")
                                if text:
                                    add("user", timestamp, text, raw=block)
                            elif btype == "tool_result":
                                tname = tool_names.get(block.get("tool_use_id"))
                                rc = block.get("content", "")
                                if isinstance(rc, list):
                                    rc = "\n".join(
                                        b.get("text", "") for b in rc if isinstance(b, dict) and b.get("type") == "text"
                                    ) or json.dumps(rc, ensure_ascii=False)
                                add("tool_result", timestamp, str(rc), tool_name=tname, raw=block)

                elif msg_type == "assistant":
                    msg = obj.get("message", {})
                    if not model:
                        model = msg.get("model")
                    for block in msg.get("content", []):
                        if not isinstance(block, dict):
                            continue
                        btype = block.get("type")
                        if btype == "text":
                            text = block.get("text", "")
                            if text:
                                add("assistant", timestamp, text, raw=block)
                        elif btype == "thinking":
                            text = block.get("thinking", "")
                            if text:
                                add("thinking", timestamp, text, raw=block)
                        elif btype == "tool_use":
                            name = block.get("name") or "unknown"
                            tool_use_id = block.get("id")
                            if tool_use_id:
                                tool_names[tool_use_id] = name
                            text = json.dumps(block.get("input", {}), indent=2, ensure_ascii=False)
                            add("tool_call", timestamp, text, tool_name=name, raw=block)
    except (OSError, UnicodeDecodeError):
        return None

    if not entries:
        return None

    is_subagent = _is_subagent_file(jsonl_path)
    session_id = _subagent_session_id(jsonl_path) if is_subagent else (session_id or jsonl_path.stem)

    return common.FullSession(
        source=NAME,
        session_id=session_id,
        file_paths=[str(jsonl_path)],
        cwd=cwd,
        git_branch=git_branch,
        model=model,
        started_at=started_at,
        ended_at=ended_at,
        entries=entries,
    )


def discover_export_targets(include_subagents: bool = True) -> list[Path]:
    """All Claude session JSONL files on disk, for the `export` skill's bulk export.

    Top-level session transcripts live at PROJECTS_DIR/<project-dir>/<uuid>.jsonl. Subagent
    transcripts (Task-tool invocations) live one level deeper at
    PROJECTS_DIR/<project-dir>/<uuid>/subagents/agent-<hash>.jsonl — not rglob'd wholesale,
    since other nesting under a project dir (e.g. skill-injection logs) is other tooling's
    data, not a Claude Code session.
    """
    if not store_exists():
        return []
    files: list[Path] = []
    for project_dir in PROJECTS_DIR.iterdir():
        if project_dir.is_dir():
            files.extend(project_dir.glob("*.jsonl"))
            if include_subagents:
                files.extend(_find_subagent_files(project_dir))
    return files


def parse_export_target(path: Path) -> common.FullSession | None:
    return _parse_full_session(path)


def resolve_full_session(prefix: str) -> common.FullSessionResult:
    if not store_exists():
        return common.FullSessionResult()

    prefix = prefix.lower()
    matches: list[Path] = []
    for f in PROJECTS_DIR.rglob("*.jsonl"):
        if f.stem.lower().startswith(prefix):
            matches.append(f)

    if not matches:
        return common.FullSessionResult()
    if len(matches) > 1:
        return common.FullSessionResult(ambiguous=[str(m) for m in matches[:10]])

    session = _parse_full_session(matches[0])
    if not session:
        return common.FullSessionResult()
    return common.FullSessionResult(session=session)


def get_session(prefix: str, query: str | None, context: int) -> common.SessionLookup:
    if not store_exists():
        return common.SessionLookup()

    prefix = prefix.lower()
    matches: list[Path] = []
    for f in PROJECTS_DIR.rglob("*.jsonl"):
        if f.stem.lower().startswith(prefix):
            matches.append(f)

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


def list_projects() -> list[tuple[str, int]]:
    if not store_exists():
        return []
    projects = _discover_project_dirs()
    rows = []
    for path, d in projects.items():
        count = len(list(d.glob("*.jsonl")))
        if count > 0:
            rows.append((path, count))
    return rows


def search_prompts(query: str | None, project: str | None, max_results: int) -> list[common.PromptRow]:
    if not HISTORY_FILE.exists():
        return []

    query_terms, _ = common.parse_query_terms(query)
    rows: list[common.PromptRow] = []
    with open(HISTORY_FILE) as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            display = obj.get("display", "")
            proj = obj.get("project", "")
            ts = obj.get("timestamp")

            if query_terms and not common.query_matches_all(display, query_terms):
                continue
            if project and project.lower() not in proj.lower():
                continue

            rows.append(
                common.PromptRow(
                    source=NAME,
                    sort_ts=common.parse_sort_ts(ts),
                    ts_display=common.format_ts(ts),
                    session="",
                    project=proj,
                    prompt=display,
                )
            )

    rows.sort(key=lambda r: r.sort_ts, reverse=True)
    return rows[: max_results or 30]


if __name__ == "__main__":
    print("This module is a source adapter; invoke ../search_history.py instead.", file=sys.stderr)
    sys.exit(1)
