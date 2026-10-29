from __future__ import annotations

import argparse
import json
import logging
import math
import random
import re
import sqlite3
import statistics
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from brain.config import load_config
from brain.corpus import Corpus, CorpusStore, SearchDeadlineExceeded
from brain.judge import Judge, judged_score
from brain.learning import Usage
from brain.semantic import Semantic

logger = logging.getLogger("brain.evaluate")

WORD = re.compile(r"[A-Za-z][A-Za-z0-9_-]{3,}")
COMMON = {
    *("about", "after", "again", "also", "because", "been", "before", "being", "cannot", "check", "code", "could"),
    *("does", "done", "each", "file", "files", "first", "from", "have", "here", "into", "just", "last", "like"),
    *("made", "make", "many", "more", "most", "much", "need", "needs", "next", "only", "other", "over", "same"),
    *("should", "some", "still", "such", "sure", "test", "tests", "than", "that", "their", "them", "then", "there"),
    *("they", "thing", "things", "this", "under", "used", "uses", "using", "very", "want", "were", "what", "when"),
    *("where", "which", "while", "will", "with", "work", "working", "would", "your"),
}
READS_SQL = """SELECT started_at, arguments_json FROM requests
               WHERE mcp_name = 'read_session' AND status_code = 200 ORDER BY started_at"""
UUID_SQL = "SELECT id FROM sessions WHERE uuid LIKE ? ORDER BY is_subagent, length(uuid), started_at DESC LIMIT 1"
DOCUMENT_FREQUENCY = (30, 30_000)


def words(text: str) -> list[str]:
    return [word.lower() for word in WORD.findall(text) if word.lower() not in COMMON]


def known_items(corpus: Corpus, count: int, seed: int) -> list[dict[str, Any]]:
    """Queries built from sampled sessions, each with the sessions that should be found.

    A message query takes three moderately rare words from one message. A topic query takes the session's three most
    characteristic words, each from a different message, the way someone remembers a topic but not a sentence. Both
    stay on this machine, so no conversation text goes to a model provider.
    """
    rng = random.Random(seed)
    provenance = corpus.provenance
    if provenance is None:
        raise RuntimeError("Known-item sampling needs the corpus provenance index")
    frequencies: dict[str, int] = {}
    with sqlite3.connect(f"file:{corpus.index_path}?mode=ro", uri=True) as index:

        def frequency(term: str) -> int:
            if term not in frequencies:
                frequencies[term] = index.execute(
                    "SELECT count(*) FROM blobs_fts WHERE blobs_fts MATCH ?", (f'"{term}"',)
                ).fetchone()[0]
            return frequencies[term]

        total = index.execute("SELECT count(*) FROM blobs").fetchone()[0]
        sessions = [
            row
            for row in index.execute(
                "SELECT id, person, source, repository FROM sessions WHERE is_subagent = 0 AND entry_count >= 8"
            )
            if not provenance.is_derived(row[0])
        ]
        strata: dict[tuple[str, str], list[tuple]] = defaultdict(list)
        for row in sessions:
            strata[(row[1], row[2])].append(row)
        picked: list[tuple] = []
        for rows in strata.values():
            picked += rng.sample(rows, min(len(rows), max(6, round(count * len(rows) / len(sessions)))))
        rng.shuffle(picked)
        items: list[dict[str, Any]] = []
        injected = provenance.snapshot.injected
        for position, (session_id, _person, _source, repository) in enumerate(picked[:count]):
            rows = index.execute(
                """SELECT b.id, b.hash FROM entries e JOIN blobs b ON b.id = e.blob_id
                   WHERE e.session_id = ? AND e.role IN ('user', 'assistant')
                     AND b.nbytes BETWEEN 200 AND 8000 AND b.nrefs <= 3
                   LIMIT 400""",
                (session_id,),
            ).fetchall()
            rows = [row for row in rows if row[0] not in injected]
            texts = {
                blob_id: body
                for (blob_id, _), body in zip(rows, corpus.load_bodies([digest for _, digest in rows]), strict=True)
                if not body.lstrip().startswith("<")
            }
            if len(texts) < 3:
                continue
            filters = (
                {"repository": repository}
                if repository and not repository.startswith("local:") and position % 2 == 0
                else {}
            )
            blob_id = rng.choice(list(texts))
            candidates = list(dict.fromkeys(words(texts[blob_id])))[:40]
            band = [term for term in candidates if DOCUMENT_FREQUENCY[0] <= frequency(term) <= DOCUMENT_FREQUENCY[1]]
            if len(band) >= 3:
                terms = sorted(rng.sample(band, 3), key=candidates.index)
                targets = {
                    row[0]
                    for row in index.execute("SELECT DISTINCT session_id FROM entries WHERE blob_id = ?", (blob_id,))
                }
                items.append({"kind": "message", "query": " ".join(terms), "filters": filters, "targets": targets})
            seen: Counter[str] = Counter()
            where: dict[str, set[int]] = defaultdict(set)
            for message, body in texts.items():
                for term in set(words(body)):
                    seen[term] += 1
                    where[term].add(message)
            weighted = sorted(
                (
                    (occurrences * math.log(total / frequency(term)), term)
                    for term, occurrences in seen.most_common(400)
                    if occurrences >= 2 and DOCUMENT_FREQUENCY[0] <= frequency(term) <= DOCUMENT_FREQUENCY[1]
                ),
                reverse=True,
            )
            chosen: list[str] = []
            used: set[int] = set()
            for _, term in weighted:
                remaining = where[term] - used
                if remaining:
                    chosen.append(term)
                    used.add(min(remaining))
                if len(chosen) == 3:
                    break
            if len(chosen) == 3:
                items.append({"kind": "topic", "query": " ".join(chosen), "filters": filters, "targets": {session_id}})
    return items


def score(
    search: Callable[[str, dict[str, Any]], list[int]],
    items: list[dict[str, Any]],
    k: int,
    derived: Callable[[int], bool],
    before_each: Callable[[dict[str, Any]], None] = lambda _item: None,
) -> dict[str, Any]:
    recall, reciprocal, first, latencies = [], [], [], []
    empty = timeouts = derived_slots = slots = 0
    for item in items:
        before_each(item)
        started = time.perf_counter()
        try:
            ranked = search(item["query"], item["filters"])[:k]
        except ValueError:
            ranked = []
        except SearchDeadlineExceeded:
            ranked = []
            timeouts += 1
        latencies.append((time.perf_counter() - started) * 1_000)
        empty += not ranked
        slots += len(ranked)
        derived_slots += sum(1 for session_id in ranked if derived(session_id))
        wanted = item["targets"]
        if wanted:
            recall.append(float(bool(wanted & set(ranked))))
            reciprocal.append(next((1 / (rank + 1) for rank, value in enumerate(ranked) if value in wanted), 0.0))
            first.append(float(bool(ranked) and ranked[0] in wanted))
    latencies.sort()
    mean = lambda values: round(statistics.fmean(values), 4) if values else None  # noqa: E731
    return {
        "queries": len(items),
        f"recallAt{k}": mean(recall),
        "mrr": mean(reciprocal),
        "hitAt1": mean(first),
        "emptyResults": round(empty / len(items), 4) if items else None,
        "timeouts": timeouts,
        "derivedShare": round(derived_slots / slots, 4) if slots else None,
        "latencyMs": {
            "p50": round(latencies[len(latencies) // 2], 1),
            "p95": round(latencies[int(len(latencies) * 0.95)], 1),
        }
        if latencies
        else None,
    }


def logged_reads(data_dir: Path, index_path: Path) -> list[tuple[datetime, int]]:
    reads: list[tuple[datetime, int]] = []
    with (
        sqlite3.connect(f"file:{data_dir / 'requests.sqlite'}?mode=ro", uri=True) as requests,
        sqlite3.connect(f"file:{index_path}?mode=ro", uri=True) as index,
    ):
        for started_at, arguments in requests.execute(READS_SQL):
            values = json.loads(arguments or "{}")
            session_id = values.get("sessionId")
            if session_id is None and values.get("uuid"):
                row = index.execute(UUID_SQL, (f"{values['uuid']}%",)).fetchone()
                session_id = row[0] if row else None
            if session_id is not None:
                reads.append((datetime.fromisoformat(started_at), int(session_id)))
    return reads


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Score search against logged searches whose results were read, and against known-item queries"
    )
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--known-items", type=int, default=300, help="Sessions to sample for known-item queries")
    parser.add_argument("--seed", type=int, default=20261028)
    parser.add_argument("--keyword-only", action="store_true", help="Score keyword ranking without vectors")
    parser.add_argument(
        "--relabel", action="store_true", help="Rebuild useful results from the request log before scoring"
    )
    parser.add_argument(
        "--usage-dir", type=Path, help="Use usage.sqlite from this directory, such as a copy for trying relabeling"
    )
    parser.add_argument(
        "--judge-model",
        help="Grade results for sampled logged queries with this model at OPENAI_BASE_URL; sends excerpts to it",
    )
    parser.add_argument("--judge-queries", type=int, default=150, help="Logged queries to sample for grading")
    parser.add_argument("--judgments", type=Path, help="Grade cache shared across runs (default: beside usage.sqlite)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    config = load_config()
    semantic = None
    if config.embedding_model and not args.keyword_only:
        semantic = Semantic(config.data_dir, config.embedding_model)
    usage = Usage(args.usage_dir or config.data_dir)
    store = CorpusStore(config.data_dir, semantic=semantic, usage=usage)
    try:
        corpus = store.read()
        provenance = store.provenance
        provenance.refresh(corpus.load_bodies)
        report: dict[str, Any] = {"model": None if semantic is None else semantic.model_name}
        if args.relabel:
            backup = (
                args.usage_dir or config.data_dir
            ) / f"usage-before-relabel-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.sqlite"
            with sqlite3.connect(backup) as copy:
                usage.db.backup(copy)
            report["relabel"] = {
                **usage.relabel(logged_reads(config.data_dir, corpus.index_path)),
                "backup": str(backup),
            }
        logged = usage.logged_searches()
        for item in logged:
            item["targets"] |= {
                session for target in item["targets"] if (session := provenance.canonical(target)) is not None
            }
        live_priors = corpus.priors

        def honest_priors(item: dict[str, Any]) -> None:
            priors = usage.priors(before=item["at"], excluding=frozenset(item["searchIds"]))
            corpus.priors = lambda: priors

        def search(query: str, filters: dict[str, Any]) -> list[int]:
            return [result["sessionId"] for result in corpus.search(query, **filters, limit=args.k)]

        report["logged"] = score(
            search, [item for item in logged if item["targets"]], args.k, provenance.is_derived, honest_priors
        )
        report["allLogged"] = score(search, logged, args.k, provenance.is_derived, honest_priors)
        if args.judge_model:
            judge = Judge(args.judge_model, args.judgments or (args.usage_dir or config.data_dir) / "judgments.json")

            def results(item: dict[str, Any]) -> list[dict[str, Any]]:
                honest_priors(item)
                try:
                    return corpus.search(item["query"], **item["filters"], limit=args.k)
                except (ValueError, SearchDeadlineExceeded):
                    return []

            report["judged"] = judged_score(results, logged, judge, args.k, args.judge_queries, args.seed)
        corpus.priors = live_priors
        if args.known_items:
            items = known_items(corpus, args.known_items, args.seed)
            for kind in ("message", "topic"):
                report[f"known{kind.title()}"] = score(
                    search, [item for item in items if item["kind"] == kind], args.k, provenance.is_derived
                )
        print(json.dumps(report, indent=2))
    finally:
        store.close()


if __name__ == "__main__":
    main()
