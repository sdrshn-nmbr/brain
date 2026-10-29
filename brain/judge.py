from __future__ import annotations

import concurrent.futures
import json
import logging
import math
import os
import random
import re
import statistics
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger("brain.judge")

HIT_CHARS = 1_200
HITS_PER_RESULT = 3
ATTEMPTS = 3
PROMPT = """You grade results from a search engine over one developer's past AI coding-agent conversations.

Search query: {query}
Filters: {filters}

Each candidate is one past conversation, shown by the excerpts that matched. Grade how well each candidate is what
the searcher wants to find:
2 = clearly the conversation the query is looking for, or one that directly answers it
1 = related and partly useful, but not the main match
0 = not relevant

Return only JSON: {{"grades": {{"<candidate id>": 0, ...}}}}

{candidates}"""

Results = list[dict[str, Any]]


def query_key(query: str, filters: dict[str, Any]) -> str:
    return json.dumps([query, filters], sort_keys=True)


class Judge:
    """Grades search results with a model behind an OpenAI-compatible endpoint.

    Grades are cached by query, filters, and session, so rankings scored later reuse earlier grades and a pooled
    ideal ordering covers every session any ranking returned. The cache holds grades only, never excerpt text.
    """

    def __init__(self, model: str, path: Path) -> None:
        base, key = os.environ.get("OPENAI_BASE_URL"), os.environ.get("OPENAI_API_KEY")
        if not base or not key:
            raise RuntimeError("[brain-eval] --judge-model needs OPENAI_BASE_URL and OPENAI_API_KEY")
        self.model, self.path, self._base, self._key = model, path, base.rstrip("/"), key
        self.grades: dict[str, dict[str, int]] = json.loads(path.read_text()) if path.exists() else {}
        self.failures = 0
        self._lock = threading.Lock()

    def save(self) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.grades, sort_keys=True))
        temporary.replace(self.path)

    def _ask(self, query: str, filters: dict[str, Any], results: Results) -> dict[str, int]:
        candidates = "\n\n".join(
            f"Candidate {result['sessionId']} ({result['source']}, {result['repository']}, {result['startedAt']}):\n"
            + "\n---\n".join(hit["text"][:HIT_CHARS] for hit in result["hits"][:HITS_PER_RESULT])
            for result in results
        )
        body = json.dumps(
            {
                "model": self.model,
                "temperature": 0,
                "messages": [
                    {
                        "role": "user",
                        "content": PROMPT.format(query=query, filters=json.dumps(filters), candidates=candidates),
                    }
                ],
            }
        ).encode()
        request = urllib.request.Request(
            f"{self._base}/chat/completions",
            data=body,
            headers={"Authorization": f"Bearer {self._key}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=180) as response:
            content = json.load(response)["choices"][0]["message"]["content"]
        match = re.search(r"\{.*\}", content, re.S)
        if not match:
            raise ValueError("judge reply had no JSON object")
        grades = json.loads(match.group(0))["grades"]
        wanted = {str(result["sessionId"]) for result in results}
        return {str(session): max(0, min(2, int(grade))) for session, grade in grades.items() if str(session) in wanted}

    def grade(self, query: str, filters: dict[str, Any], results: Results) -> dict[int, int]:
        key = query_key(query, filters)
        with self._lock:
            known = dict(self.grades.get(key, {}))
        missing = [result for result in results if str(result["sessionId"]) not in known]
        if missing:
            for attempt in range(ATTEMPTS):
                try:
                    known |= self._ask(query, filters, missing)
                    break
                except (urllib.error.URLError, TimeoutError, ValueError, KeyError) as error:
                    logger.warning(
                        json.dumps({"event": "judge_failed", "attempt": attempt + 1, "error": type(error).__name__})
                    )
            else:
                with self._lock:
                    self.failures += 1
            with self._lock:
                self.grades.setdefault(key, {}).update(known)
        return {int(session): grade for session, grade in known.items()}


def judged_score(
    search: Callable[[dict[str, Any]], Results],
    items: list[dict[str, Any]],
    judge: Judge,
    k: int,
    sample: int,
    seed: int,
) -> dict[str, Any]:
    """nDCG@k and precision against model grades for a seeded sample of logged queries."""
    chosen = random.Random(seed).sample(items, min(sample, len(items)))
    ranked = [(item, search(item)[:k]) for item in chosen]
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        graded = list(pool.map(lambda pair: judge.grade(pair[0]["query"], pair[0]["filters"], pair[1]), ranked))
    judge.save()
    ndcg, precision5, precision10, top = [], [], [], []
    for (item, results), grades in zip(ranked, graded, strict=True):
        pooled = sorted(judge.grades.get(query_key(item["query"], item["filters"]), {}).values(), reverse=True)[:k]
        gains = [grades.get(result["sessionId"], 0) for result in results]
        ideal = sum((2**grade - 1) / math.log2(rank + 2) for rank, grade in enumerate(pooled))
        actual = sum((2**grade - 1) / math.log2(rank + 2) for rank, grade in enumerate(gains))
        if ideal:
            ndcg.append(actual / ideal)
        precision5.append(sum(1 for grade in gains[:5] if grade) / 5)
        precision10.append(sum(1 for grade in gains[:k] if grade) / k)
        top.append(float(bool(gains) and gains[0] == 2))
    mean = lambda values: round(statistics.fmean(values), 4) if values else None  # noqa: E731
    return {
        "model": judge.model,
        "queries": len(chosen),
        "queriesWithRelevantResults": len(ndcg),
        f"ndcgAt{k}": mean(ndcg),
        "precisionAt5": mean(precision5),
        f"precisionAt{k}": mean(precision10),
        "topResultFullyRelevant": mean(top),
        "judgeFailures": judge.failures,
    }
