"""Shared helpers for the past-conversation source adapters.

Every source adapter (claude_source, codex_source, cursor_source) renders its own
conversation headers (they carry different metadata: git branch, cwd, project path,
subagent flag, ...) but shares everything else: truncation limits, highlighting,
ripgrep batching, timestamp parsing, and the group/budget rendering loop.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

MAX_TEXT_BLOCK_LEN = 2000
MAX_OUTPUT_CHARS = 80_000
RIPGREP_BATCH_SIZE = 500
# Relevance ranking needs every candidate parsed and scored before any cut —
# a small mtime-ordered cap here would silently prune old-but-on-topic sessions.
# These bounds exist only to keep pathological broad queries from parsing forever;
# candidates are ordered by prefilter hit-density first, so what gets cut is the
# least-relevant tail, not the oldest. Bytes are bounded too: multi-term AND
# prefiltering selects for the largest session files, so a file-count cap alone
# still admits gigabytes.
MAX_PARSE_FILES = 300
MAX_PARSE_BYTES = 2_500_000_000


@dataclass
class NormalizedConversation:
    """One conversation, ready to render, tagged with the source that produced it.

    `term_coverage` (distinct query terms present) and `match_count` (messages
    containing any term) are relevance signals the dispatcher ranks by, with
    recency as the final tiebreak — so an old on-topic conversation outranks a
    recent one that mentions the terms in passing.
    """

    source: str
    session_id: str
    sort_ts: float
    header: str
    groups: list[list[str]] = field(default_factory=list)
    extra_lines: list[str] = field(default_factory=list)
    term_coverage: int = 0
    match_count: int = 0
    total_messages: int = 0
    rank_priority: int = 1

    @property
    def match_density(self) -> float:
        """Smoothed fraction of messages that hit — 'about the topic', not 'long'.

        Raw match_count is roughly proportional to session length for common
        terms, so ranking by it buries a focused 400-message conversation under
        every 10k-message mega-session that mentions the words in passing.
        The +10 pseudo-count (Laplace smoothing) keeps 1-message stub chats from
        trivially scoring 1.0 and crowding out real conversations.
        """
        if self.total_messages <= 0:
            return 0.0
        return self.match_count / (self.total_messages + 10)


@dataclass
class NormalizedBrowseRow:
    source: str
    sort_ts: float
    line: str
    preview: str | None = None


@dataclass
class SessionLookup:
    conversation: NormalizedConversation | None = None
    ambiguous: list[str] = field(default_factory=list)


@dataclass
class FullEntry:
    """One untruncated transcript entry, used by the `read` skill's full-fidelity extraction.

    `role` is one of: user, assistant, tool_call, tool_result, thinking, reasoning.
    `text` is never truncated. `raw` is the original source block/object (for --include-raw).
    """

    index: int
    role: str
    timestamp: str | None
    text: str
    tool_name: str | None = None
    raw: dict | None = None


@dataclass
class FullSession:
    """A full, untruncated session, ready for filtering/paging by the `read` skill."""

    source: str
    session_id: str
    file_paths: list[str] = field(default_factory=list)
    cwd: str | None = None
    git_branch: str | None = None
    model: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    entries: list[FullEntry] = field(default_factory=list)


@dataclass
class FullSessionResult:
    session: FullSession | None = None
    ambiguous: list[str] = field(default_factory=list)


@dataclass
class PromptRow:
    source: str
    sort_ts: float
    ts_display: str
    session: str
    project: str
    prompt: str


def collapse_home(path: str | None) -> str:
    if not path:
        return path or ""
    home = str(Path.home())
    if path.startswith(home):
        return "~" + path[len(home) :]
    return path


# Cursor legacy transcripts stamp human-readable timestamps like
# 'Friday, Jul 10, 2026, 11:59 AM (UTC-7)' — unparseable by fromisoformat, which
# used to sink yesterday's cursor sessions to 1970 in merged sort order.
_HUMAN_TS_RE = re.compile(
    r"^[A-Za-z]+, ([A-Za-z]{3}) (\d{1,2}), (\d{4}), (\d{1,2}):(\d{2}) (AM|PM)(?: \(UTC([+-]\d{1,2})(?::(\d{2}))?\))?"
)


def _parse_human_ts(s: str) -> datetime | None:
    m = _HUMAN_TS_RE.match(s.strip())
    if not m:
        return None
    month, day, year, hour, minute, meridiem, tz_hours, tz_minutes = m.groups()
    try:
        dt = datetime.strptime(f"{month} {day} {year} {hour}:{minute} {meridiem}", "%b %d %Y %I:%M %p")
    except ValueError:
        return None
    if tz_hours is not None:
        offset = timedelta(hours=int(tz_hours), minutes=int(tz_minutes or 0) * (1 if int(tz_hours) >= 0 else -1))
        return dt.replace(tzinfo=timezone(offset))
    return dt.replace(tzinfo=UTC)


def format_ts(value) -> str:
    """Format an ISO-8601 string, epoch seconds, or epoch milliseconds as local 'YYYY-MM-DD HH:MM'."""
    if value is None or value == "":
        return ""
    try:
        if isinstance(value, (int, float)):
            seconds = value / 1000 if value > 1e12 else value
            dt = datetime.fromtimestamp(seconds, tz=UTC).astimezone()
            return dt.strftime("%Y-%m-%d %H:%M")
        s = str(value).strip()
        if s.isdigit():
            v = float(s)
            seconds = v / 1000 if v > 1e12 else v
            dt = datetime.fromtimestamp(seconds, tz=UTC).astimezone()
            return dt.strftime("%Y-%m-%d %H:%M")
        human = _parse_human_ts(s)
        if human is not None:
            return human.astimezone().strftime("%Y-%m-%d %H:%M")
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, OSError):
        return str(value)[:16]


def parse_sort_ts(value) -> float:
    """Best-effort epoch-seconds for sorting; 0.0 when unparseable."""
    if value is None or value == "":
        return 0.0
    try:
        if isinstance(value, (int, float)):
            return value / 1000 if value > 1e12 else float(value)
        s = str(value).strip()
        if s.isdigit():
            v = float(s)
            return v / 1000 if v > 1e12 else v
        human = _parse_human_ts(s)
        if human is not None:
            return human.timestamp()
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except (ValueError, TypeError, OSError):
        return 0.0


def cwd_tail(path: str | None, n: int = 2) -> str:
    if not path:
        return "?"
    parts = [p for p in path.split("/") if p]
    return "/".join(parts[-n:]) if parts else "?"


def highlight(text: str, pattern: re.Pattern | None) -> str:
    if not text or not pattern:
        return text
    return pattern.sub(lambda m: f">>>{m.group(0)}<<<", text)


STOPWORDS = frozenset(
    (
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "but",
        "by",
        "do",
        "does",
        "did",
        "for",
        "from",
        "had",
        "has",
        "have",
        "if",
        "in",
        "into",
        "is",
        "it",
        "its",
        "me",
        "my",
        "no",
        "not",
        "of",
        "on",
        "or",
        "our",
        "out",
        "over",
        "per",
        "so",
        "than",
        "that",
        "the",
        "their",
        "them",
        "then",
        "there",
        "these",
        "this",
        "those",
        "to",
        "up",
        "via",
        "was",
        "we",
        "were",
        "what",
        "when",
        "where",
        "whether",
        "which",
        "who",
        "will",
        "with",
        "you",
        "your",
        "i",
    )
)


def parse_query_terms(query: str | None) -> tuple[list[str], bool]:
    """Split a search query into match terms.

    A query wrapped in double quotes is treated as one exact phrase (matched
    verbatim, spaces included). Otherwise it splits on whitespace into terms that
    must ALL appear in a conversation, independent of order or adjacency — the
    Google-style AND semantics natural-language queries expect.

    Function words (STOPWORDS) and bare single characters are dropped from an
    unquoted query so they don't flood message-level anchors or over-highlight
    (e.g. ``for`` inside "platform"). If every token is a stopword, the tokens are
    kept verbatim rather than yielding an empty query.
    """
    if not query:
        return [], False
    q = query.strip()
    if len(q) >= 2 and q[0] == '"' and q[-1] == '"':
        inner = q[1:-1].strip()
        return ([inner], True) if inner else ([], True)
    tokens = q.split()
    substantive = [t for t in tokens if len(t) > 1 and t.lower() not in STOPWORDS]
    return (substantive or tokens), False


def term_in(term: str, text: str, lowered_text: str) -> bool:
    """Smart-case containment (ripgrep -S semantics): a term with any uppercase
    letter matches case-sensitively, so acronyms like NAT stop matching inside
    'pagination'; all-lowercase terms stay case-insensitive."""
    if term.islower():
        return term in lowered_text
    return term in text


def query_matches_all(text: str, terms: list[str]) -> bool:
    """True if EVERY term is present in text (smart-case). Conversation relevance."""
    if not terms:
        return False
    lowered = text.lower()
    return all(term_in(t, text, lowered) for t in terms)


def query_matches_any(text: str, terms: list[str]) -> bool:
    """True if ANY term is present in text (smart-case). Message-level anchoring."""
    if not terms:
        return False
    lowered = text.lower()
    return any(term_in(t, text, lowered) for t in terms)


# Harness-injected wrappers, not things a human said: Claude Code stamps
# system-reminders (memory recall, CLAUDE.md) and slash-command bookkeeping into
# user messages; Codex injects AGENTS.md as <user_instructions> plus environment
# blocks. Leaving these matchable makes every recent session rank for any term
# that appears in a saved memory — drowning the conversation where the words
# were actually said.
_NOISE_TAGS = (
    "system-reminder",
    "user_instructions",
    "environment_context",
    "recommended_plugins",
    "local-command-caveat",
    "command-name",
    "command-message",
    "command-args",
    "local-command-stdout",
)
HARNESS_NOISE_RE = re.compile(
    "|".join(rf"<{tag}>.*?</{tag}>" for tag in _NOISE_TAGS),
    re.DOTALL,
)


def strip_injected_noise(text: str) -> str:
    """Remove harness-injected wrapper blocks before query matching."""
    if "<" not in text:
        return text
    return HARNESS_NOISE_RE.sub("", text)


def score_terms(texts: list[str], terms: list[str]) -> tuple[int, int]:
    """(term_coverage, match_count) over a conversation's matchable texts.

    Coverage counts distinct terms present anywhere; match_count counts texts
    containing at least one term. Two integers, no tuned weights — the dispatcher
    sorts lexicographically by (coverage, match_count, recency).
    """
    if not terms:
        return 0, 0
    lowered_texts = [t.lower() for t in texts]
    pairs = list(zip(texts, lowered_texts, strict=False))
    coverage = sum(1 for term in terms if any(term_in(term, t, lt) for t, lt in pairs))
    match_count = sum(1 for t, lt in pairs if any(term_in(term, t, lt) for term in terms))
    return coverage, match_count


def build_highlight_pattern(terms: list[str]) -> re.Pattern | None:
    """Alternation over all terms, longest-first so multi-word phrases win.

    Case sensitivity is scoped per term with inline (?i:) groups to mirror the
    smart-case matching in term_in — a global IGNORECASE would highlight
    'pagination' for the term NAT.
    """
    if not terms:
        return None
    ordered = sorted(terms, key=len, reverse=True)
    parts = [f"(?i:{re.escape(t)})" if t.islower() else re.escape(t) for t in ordered]
    return re.compile("|".join(parts))


def truncate_text(text: str, limit: int = MAX_TEXT_BLOCK_LEN) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"... [truncated, {len(text)} chars total]"


def is_path_under(child: str | None, parent: str) -> bool:
    """True if `child` is at or under `parent` after realpath resolution."""
    if not child:
        return False
    try:
        import os

        c = os.path.realpath(child).rstrip("/")
        p = os.path.realpath(parent).rstrip("/")
    except OSError:
        return False
    return c == p or c.startswith(p + "/")


def _ripgrep_term_counts(term: str, files: list[Path], batch_size: int) -> dict[Path, int] | None:
    """Per-file match counts for `term` (smart-case, fixed-string), batched for argv limits.

    None signals ripgrep is unavailable/timed out — callers fall back to parsing everything.
    """
    case_flag = "-i" if term.islower() else "-s"
    counts: dict[Path, int] = {}
    for i in range(0, len(files), batch_size):
        batch = files[i : i + batch_size]
        try:
            result = subprocess.run(
                ["rg", "-cF", case_flag, "--", term, *map(str, batch)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            for line in result.stdout.splitlines():
                if len(batch) == 1 and line.isdigit():
                    counts[batch[0]] = int(line)
                    continue
                path, sep, count = line.rpartition(":")
                if sep and count.isdigit():
                    counts[Path(path)] = int(count)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None
    return counts


def ripgrep_files(query: str, files: list[Path], batch_size: int = RIPGREP_BATCH_SIZE) -> list[Path]:
    """Narrow `files` to those containing ALL query terms, ordered most-relevant first.

    Whitespace splits the query into terms that must each appear somewhere in a
    file — order-independent AND — so natural-language queries no longer fail when
    the words aren't a contiguous phrase. A quoted `"..."` query is one exact term.
    Terms are matched as fixed strings so query punctuation (``config-v2``,
    ``aws_iam_user``) is never reinterpreted as a regex.

    Ordering is by raw-hit density (total term hits / file size): when a caller
    caps how many candidates it parses, the cut falls on the least query-dense
    files instead of the oldest — mtime-ordered capping silently pruned old
    on-topic sessions whenever injected noise made common terms match everywhere.
    """
    if not files:
        return []
    terms, _ = parse_query_terms(query)
    if not terms:
        return list(files)
    total_hits: dict[Path, int] = {}
    matching: set[Path] | None = None
    for term in terms:
        term_counts = _ripgrep_term_counts(term, files, batch_size)
        if term_counts is None:
            return list(files)
        matching = set(term_counts) if matching is None else (matching & set(term_counts))
        if not matching:
            return []
        for path, count in term_counts.items():
            total_hits[path] = total_hits.get(path, 0) + count

    def density(path: Path) -> float:
        try:
            size = path.stat().st_size or 1
        except OSError:
            size = 1
        return total_hits.get(path, 0) / size

    return sorted(matching or set(), key=density, reverse=True)


def render_groups(groups: list[list[str]]) -> list[str]:
    """Render a conversation's message groups with '[...]' separators between non-adjacent groups."""
    lines: list[str] = []
    for i, group in enumerate(groups):
        if i > 0:
            lines.append("  [...]")
        lines.extend(group)
    return lines


def render_conversations(
    conversations: list[NormalizedConversation],
    query: str | None,
    max_chars: int = MAX_OUTPUT_CHARS,
) -> str:
    """Render a merged, cross-source list of conversations with a single global char budget.

    Each conversation also gets a per-conversation budget: with relevance-first
    ranking, a heavily-matched mega-session lands first and would otherwise
    consume the entire global budget, hiding every other result. Truncated
    conversations say so and point at the `session` subcommand for the full view.
    """
    lines: list[str] = []
    if query:
        lines.append(f'=== Search results for: "{query}" ===')
        lines.append(f"Found {len(conversations)} matching conversation(s)\n")

    per_conv_chars = max(8_000, max_chars // max(len(conversations), 1))
    for conv in conversations:
        block: list[str] = [f"[{conv.source}] {conv.header}"]
        block.extend(conv.extra_lines)
        used = sum(len(line) for line in block)
        for line in render_groups(conv.groups):
            if used + len(line) > per_conv_chars:
                block.append(
                    f"  [... more matches in this conversation — run: session {conv.session_id.split('/')[0][:8]}]"
                )
                break
            block.append(line)
            used += len(line)
        block.append("")

        lines.extend(block)
        if sum(len(line) for line in lines) > max_chars:
            lines.append(f"[Output truncated at {max_chars} chars. Use --max-results to limit or narrow your query.]")
            break

    return "\n".join(lines)


def render_browse_rows(rows: list[NormalizedBrowseRow], scope_label: str) -> str:
    lines = [f"Recent conversations under {scope_label}:\n"]
    for row in rows:
        lines.append(f"  [{row.source}] {row.line}")
        if row.preview:
            lines.append(f"    > {row.preview}")
        lines.append("")
    return "\n".join(lines)


def render_projects(rows: list[tuple[str, str, int]]) -> str:
    """rows: (source, display_path, session_count)."""
    if not rows:
        return "No sessions found on any surface."
    width = max(20, min(64, max(len(path) for _, path, _ in rows)))
    lines = [f"{'Source':<8} {'Project Path':<{width}} {'Sessions':>8}", "-" * (width + 20)]
    for source, path, count in sorted(rows, key=lambda r: r[2], reverse=True):
        disp = path if len(path) <= width else "..." + path[-(width - 3) :]
        lines.append(f"{source:<8} {disp:<{width}} {count:>8}")
    return "\n".join(lines)


def render_full_session_header(session: FullSession, filtered_count: int, filters_active: bool) -> str:
    """Create a header block for one full-fidelity session."""
    lines = [f"=== {session.source} session {session.session_id} ==="]
    if session.cwd:
        lines.append(f"cwd: {collapse_home(session.cwd)}")
    if session.git_branch:
        lines.append(f"git branch: {session.git_branch}")
    if session.model:
        lines.append(f"model: {session.model}")
    if session.started_at:
        span = format_ts(session.started_at)
        if session.ended_at and session.ended_at != session.started_at:
            span += f"  ->  {format_ts(session.ended_at)}"
        lines.append(f"time range: {span}")
    total = len(session.entries)
    if filters_active and filtered_count != total:
        lines.append(f"entries: {filtered_count} shown (of {total} total in session, after filters)")
    else:
        lines.append(f"entries: {total} total")
    lines.append(f"source file(s): {', '.join(collapse_home(p) for p in session.file_paths)}")
    return "\n".join(lines)


def render_full_entries(entries: list[FullEntry]) -> str:
    """Render the flat '### [index] role=... tool=... ts=...' block style."""
    blocks = []
    for e in entries:
        tag = f"### [{e.index:04d}] role={e.role}"
        if e.tool_name:
            tag += f" tool={e.tool_name}"
        if e.timestamp:
            tag += f" ts={format_ts(e.timestamp)}"
        blocks.append(f"{tag}\n{e.text}")
    return "\n\n".join(blocks)


def render_prompts(rows: list[PromptRow]) -> str:
    if not rows:
        return "No prompts found."
    lines = [f"{'Timestamp':<18} {'Source':<8} {'Session':<10} {'Project':<28} Prompt", "-" * 110]
    for r in rows:
        project = r.project if len(r.project) <= 28 else "..." + r.project[-25:]
        prompt = r.prompt[:80].replace("\n", " ")
        lines.append(f"{r.ts_display or 'unknown':<18} {r.source:<8} {r.session:<10} {project:<28} {prompt}")
    return "\n".join(lines)
