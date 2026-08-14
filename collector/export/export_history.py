#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["rich", "zstandard>=0.22"]
# ///
"""
Bulk exporter for Claude Code, Codex CLI, and Cursor IDE conversation history.

Walks every session across all three surfaces, converts entries to zstd-compressed
NDJSON maps plus deduplicated content-addressed bodies, and packs the result into
a timestamped zip in ~/Downloads.

The manifest records stable session fingerprints so a remote Brain can negotiate
and ingest only missing sessions and bodies.

Every session must be attributable through Git metadata to a repository explicitly
selected with --repository. Subagents and archived sessions are included. Narrow
with --source/--project/--since/--until/--exclude-subagents/--exclude-archived.

Run via `brain-export` after installation, or from a checkout with uv.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from collector.archive import (
    FORMAT_VERSION,
    ArchiveEntry,
    object_member,
    session_fingerprint,
    sha256_bytes,
    stable_session_key,
    write_entries,
)
from collector.export.repository_scope import (
    RepositoryIdentity,
    normalize_repository_selector,
    resolve_repository,
)
from collector.sources import claude_source, codex_source, common, cursor_source

DOWNLOADS_DIR = Path.home() / "Downloads"
DEFAULT_WORKERS = 8
PARSE_BATCH_SIZE = 128
SOURCE_NAMES = ("claude", "codex", "cursor")


@dataclass
class Job:
    source: str
    target: Any
    backend: str


@dataclass
class Outcome:
    job: Job
    session: common.FullSession | None
    repository: RepositoryIdentity | None = None
    error: str | None = None


@dataclass
class Totals:
    discovered: int = 0
    exported: int = 0
    failed: int = 0
    skipped_by_filter: int = 0
    skipped_by_repository: int = 0


def slugify(value: str | None, max_len: int = 60) -> str:
    if not value:
        return "unknown"
    value = common.collapse_home(value)
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    value = re.sub(r"-{2,}", "-", value)
    return (value or "unknown")[:max_len]


def filename_timestamp(started_at: str | None) -> str:
    if not started_at:
        return "unknown-time"
    ts = common.parse_sort_ts(started_at)
    if not ts:
        return "unknown-time"
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y%m%d-%H%M")


def archive_id_slug(job: Job, session_id: str) -> str:
    session_slug = session_id.replace("/", "__")
    if job.source != "claude":
        return session_slug
    source_id = Path(job.target).stem
    if source_id == session_id:
        return session_slug
    return f"{session_slug}__source-{slugify(source_id, max_len=80)}"


def discover_jobs(sources: set[str], *, include_subagents: bool, include_archived: bool) -> dict[str, list[Job]]:
    jobs: dict[str, list[Job]] = {name: [] for name in sources}

    if "claude" in sources and claude_source.store_exists():
        jobs["claude"] = [
            Job(source="claude", target=p, backend="jsonl")
            for p in claude_source.discover_export_targets(include_subagents=include_subagents)
        ]

    if "codex" in sources and codex_source.store_exists():
        jobs["codex"] = [
            Job(source="codex", target=p, backend="jsonl")
            for p in codex_source.discover_export_targets(include_archived=include_archived)
        ]
        discover_side_chats = (
            getattr(codex_source, "discover_side_chat_export_targets", None) or codex_source._desktop_side_chat_records
        )
        jobs["codex"].extend(
            Job(source="codex", target=record, backend="desktop-side-chat") for record in discover_side_chats()
        )

    if "cursor" in sources and cursor_source.store_exists():
        jobs["cursor"] = [
            Job(source="cursor", target=t, backend=t[1])
            for t in cursor_source.discover_export_targets(include_subagents=include_subagents)
        ]

    return jobs


def parse_job(job: Job) -> Outcome:
    module = {"claude": claude_source, "codex": codex_source, "cursor": cursor_source}[job.source]
    try:
        if job.source == "codex" and job.backend == "desktop-side-chat":
            parse_side_chat = (
                getattr(codex_source, "parse_side_chat_export_target", None) or codex_source._side_chat_full_session
            )
            session = parse_side_chat(job.target)
        else:
            session = module.parse_export_target(job.target)
    except Exception as e:  # noqa: BLE001 - one bad session must never abort the export
        return Outcome(job=job, session=None, error=str(e))
    if session is None:
        return Outcome(
            job=job,
            session=None,
            error="parser returned no session (empty or unparseable file)",
        )
    return Outcome(job=job, session=session, repository=resolve_repository(session.cwd))


def _parse_time_bound(value: str | None) -> float | None:
    if not value:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        value = value + "T00:00:00Z"
    return common.parse_sort_ts(value) or None


def passes_filters(
    session: common.FullSession,
    *,
    project: str | None,
    since_ts: float | None,
    until_ts: float | None,
) -> bool:
    if project and (not session.cwd or project.lower() not in session.cwd.lower()):
        return False
    if since_ts is not None or until_ts is not None:
        if not session.started_at:
            return False
        start_ts = common.parse_sort_ts(session.started_at)
        end_ts = common.parse_sort_ts(session.ended_at) if session.ended_at else start_ts
        if since_ts is not None and (not end_ts or end_ts < since_ts):
            return False
        if until_ts is not None and (not start_ts or start_ts > until_ts):
            return False
    return True


def target_identifier(job: Job) -> str:
    if job.source == "codex" and job.backend == "desktop-side-chat":
        return f"sidechat:{job.target['session_id']}"
    if job.source == "cursor":
        uuid, backend, path = job.target
        return f"{uuid} ({backend}: {path})"
    return str(job.target)


def target_mtime(job: Job) -> float | None:
    if job.source == "codex" and job.backend == "desktop-side-chat":
        return job.target.get("last_timestamp") or job.target.get("first_timestamp")
    if job.source == "cursor":
        _uuid, backend, path = job.target
        if backend == "statedb":
            return None
        target = Path(path)
    else:
        target = Path(job.target)
    try:
        return target.stat().st_mtime
    except OSError:
        return None


def entry_header(entry: common.FullEntry) -> str:
    header = f"### [{entry.index:04d}] role={entry.role}"
    if entry.tool_name:
        header += f" tool={entry.tool_name}"
    if entry.timestamp:
        header += f" ts={common.format_ts(entry.timestamp)}"
    return header


def archive_entries(session: common.FullSession) -> list[ArchiveEntry]:
    return [
        ArchiveEntry(
            seq=entry.index,
            role=entry.role,
            timestamp=entry.timestamp,
            tool_name=entry.tool_name,
            header_line=entry_header(entry),
            body_sha256=sha256_bytes(entry.text.encode()),
        )
        for entry in session.entries
    ]


def build_progress() -> Progress:
    return Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )


def run_export(args: argparse.Namespace, console: Console) -> None:
    sources = set(SOURCE_NAMES) if args.source == "all" else {args.source}
    include_subagents = not args.exclude_subagents
    include_archived = not args.exclude_archived
    since_ts = _parse_time_bound(args.since)
    until_ts = _parse_time_bound(args.until)
    configured_repositories = args.repository or []
    normalized = [normalize_repository_selector(repository) for repository in configured_repositories]
    if not configured_repositories or any(repository is None for repository in normalized):
        raise ValueError("At least one valid --repository is required")
    allowed_repositories = {repository for repository in normalized if repository is not None}

    console.print("[bold]Discovering sessions...[/bold]")
    jobs_by_source = discover_jobs(sources, include_subagents=include_subagents, include_archived=include_archived)
    discovered_jobs = [j for jobs in jobs_by_source.values() for j in jobs]
    if not discovered_jobs:
        console.print("[yellow]No sessions found for the given scope.[/yellow]")
        sys.exit(1)

    for name in SOURCE_NAMES:
        if name in jobs_by_source:
            console.print(f"  {name}: {len(jobs_by_source[name]):,} sessions discovered")

    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    wrapper = f"agent-chats-export-{stamp}"
    zip_path = Path(args.output) if args.output else DOWNLOADS_DIR / f"{wrapper}.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    totals: dict[str, Totals] = {name: Totals(discovered=len(jobs)) for name, jobs in jobs_by_source.items()}
    all_jobs: list[Job] = []
    for job in discovered_jobs:
        mtime = target_mtime(job)
        if since_ts is not None and mtime is not None and mtime < since_ts:
            totals[job.source].skipped_by_filter += 1
            continue
        all_jobs.append(job)
    manifest_sessions: list[dict] = []
    active_counts = {name: sum(job.source == name for job in all_jobs) for name in jobs_by_source}

    progress = build_progress()
    written_objects: set[str] = set()
    compressor = zstd.ZstdCompressor(level=3)
    with progress, zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        source_tasks = {
            name: progress.add_task(name, total=active_counts[name])
            for name, jobs in jobs_by_source.items()
            if jobs and active_counts[name]
        }
        total_task = progress.add_task("total", total=len(all_jobs))

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for batch_start in range(0, len(all_jobs), PARSE_BATCH_SIZE):
                batch = all_jobs[batch_start : batch_start + PARSE_BATCH_SIZE]
                futures = {ex.submit(parse_job, job): job for job in batch}
                for fut in as_completed(futures):
                    job = futures[fut]
                    outcome = fut.result()
                    progress.advance(source_tasks[job.source])
                    progress.advance(total_task)

                    if outcome.session is None:
                        totals[job.source].failed += 1
                        console.print(
                            f"[red]FAILED[/red] [{job.source}] {target_identifier(job)}: {outcome.error}",
                            highlight=False,
                        )
                        continue

                    session = outcome.session
                    if outcome.repository is None or outcome.repository.slug not in allowed_repositories:
                        totals[job.source].skipped_by_repository += 1
                        continue
                    if not passes_filters(
                        session,
                        project=args.project,
                        since_ts=since_ts,
                        until_ts=until_ts,
                    ):
                        totals[job.source].skipped_by_filter += 1
                        continue

                    id_slug = archive_id_slug(job, session.session_id)
                    ts_slug = filename_timestamp(session.started_at)
                    project_slug = slugify(session.cwd)
                    entries_arc_name = f"{wrapper}/{job.source}/{id_slug}__{ts_slug}__{project_slug}.entries.ndjson.zst"
                    entries = archive_entries(session)
                    write_entries(zf, entries_arc_name, entries)
                    for entry, source_entry in zip(entries, session.entries, strict=True):
                        if entry.body_sha256 in written_objects:
                            continue
                        zf.writestr(
                            object_member(wrapper, entry.body_sha256),
                            compressor.compress(source_entry.text.encode()),
                        )
                        written_objects.add(entry.body_sha256)

                    totals[job.source].exported += 1
                    manifest_sessions.append(
                        {
                            "source": session.source,
                            "uuid": session.session_id,
                            "session_fallback": id_slug,
                            "session_key": stable_session_key(
                                session.source, session.session_id, session.started_at, id_slug
                            ),
                            "backend": job.backend,
                            "file_paths": session.file_paths,
                            "cwd": session.cwd,
                            "repository": outcome.repository.slug,
                            "repository_root": outcome.repository.root,
                            "git_branch": session.git_branch,
                            "model": session.model,
                            "started_at": session.started_at,
                            "ended_at": session.ended_at,
                            "entry_count": len(session.entries),
                            "roles": sorted({entry.role for entry in session.entries}),
                            "assistant_text_available": any(entry.role == "assistant" for entry in session.entries),
                            "char_count": sum(len(entry.text) for entry in session.entries),
                            "session_fingerprint": session_fingerprint(session.source, session.session_id, entries),
                            "export_path": entries_arc_name,
                            "entries_path": entries_arc_name,
                        }
                    )

        manifest = {
            "format_version": FORMAT_VERSION,
            "export_timestamp": datetime.now(UTC).isoformat(),
            "filters": {
                "source": args.source,
                "project": args.project,
                "repositories": sorted(allowed_repositories),
                "since": args.since,
                "until": args.until,
                "exclude_subagents": args.exclude_subagents,
                "exclude_archived": args.exclude_archived,
            },
            "totals": {
                name: {
                    "discovered": t.discovered,
                    "exported": t.exported,
                    "failed": t.failed,
                    "skipped_by_filter": t.skipped_by_filter,
                    "skipped_by_repository": t.skipped_by_repository,
                }
                for name, t in totals.items()
            },
            "sessions": manifest_sessions,
            "object_count": len(written_objects),
        }
        zf.writestr(
            f"{wrapper}/_manifest.json",
            json.dumps(manifest, indent=2, ensure_ascii=False),
        )

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    console.print()
    console.print(f"[bold green]Export complete:[/bold green] {zip_path}  ({size_mb:.1f} MB)")
    for name in SOURCE_NAMES:
        if name in totals:
            t = totals[name]
            console.print(
                f"  {name:<8} discovered={t.discovered:<5} exported={t.exported:<5} "
                f"failed={t.failed:<5} skipped_by_repository={t.skipped_by_repository:<5} "
                f"skipped_by_filter={t.skipped_by_filter}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export full Claude Code / Codex CLI / Cursor conversation history into a zstd NDJSON and CAS archive."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=("all", *SOURCE_NAMES),
        default="all",
        help="Only export one surface (default: all)",
    )
    parser.add_argument("--project", help="Only export sessions whose cwd contains this substring")
    parser.add_argument(
        "--repository",
        action="append",
        default=None,
        help=(
            "Repository to export as host/owner/repository, a Git remote URL, or GitHub owner/repository "
            "(repeatable and required)"
        ),
    )
    parser.add_argument(
        "--since",
        help="Only export sessions active at/after this time (YYYY-MM-DD or ISO-8601)",
    )
    parser.add_argument(
        "--until",
        help="Only export sessions active at/before this time (YYYY-MM-DD or ISO-8601)",
    )
    parser.add_argument(
        "--exclude-subagents",
        action="store_true",
        help="Skip Claude/Cursor subagent transcripts (included by default)",
    )
    parser.add_argument(
        "--exclude-archived",
        action="store_true",
        help="Skip Codex archived_sessions (included by default)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel parse workers (default {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--output",
        help="Override the output zip path (default: ~/Downloads/agent-chats-export-<timestamp>.zip)",
    )
    args = parser.parse_args()
    console = Console()
    run_export(args, console)


if __name__ == "__main__":
    main()
