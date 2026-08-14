import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import brain.corpus as corpus_module
from brain.corpus import SEARCH_SQL, Corpus, CorpusStore, SearchCancelled, SearchDeadlineExceeded

D_FLASH_SENTENCE = "Vector index compaction directly demonstrates the relevant storage tradeoff"


def test_search_preserves_repository_identity(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        results = corpus.search("detection boundary", repository="github.com/acme/widget")
        assert len(results) == 1
        assert results[0]["person"] == "alice"
        assert results[0]["repository"] == "github.com/acme/widget"
        assert results[0]["hitCount"] == 1
    finally:
        corpus.close()


def test_filters_repository_and_person(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        assert corpus.search("detection", repository="gitlab.com/acme/model") == []
        assert len(corpus.search("detection", person="alice")) == 1
        assert corpus.search("detection", person="bob") == []
    finally:
        corpus.close()


def test_role_defaults_exclude_tool_traffic(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        assert len(corpus.search("detection")) == 1
        assert len(corpus.search("detection", roles=[])) == 2
    finally:
        corpus.close()


def test_loads_packed_compressed_body(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        results = corpus.search("detection")
        assert results[0]["hits"][0]["text"] == "warehouse detection boundary"
    finally:
        corpus.close()


def test_reads_session_by_uuid_and_id(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        by_uuid = corpus.read_session(uuid="11111111-1111-1111-1111-111111111111", limit=10)
        by_id = corpus.read_session(session_id=1, limit=10)
        assert by_uuid["returnedEntries"] == 1
        assert by_id["session"]["sessionId"] == 1
    finally:
        corpus.close()


def test_search_finds_compaction_regression_queries(corpus_dir: Path) -> None:
    corpus = Corpus(corpus_dir)
    try:
        exact = corpus.search(f'"{D_FLASH_SENTENCE}"')
        combined = corpus.search("vector / compaction")
        broad = corpus.search("deployment")
        assert exact[0]["uuid"] == "33333333-3333-3333-3333-333333333333"
        assert D_FLASH_SENTENCE in exact[0]["hits"][0]["text"]
        assert combined[0]["uuid"] == "33333333-3333-3333-3333-333333333333"
        assert broad[0]["uuid"] == "44444444-4444-4444-4444-444444444444"
    finally:
        corpus.close()


def test_search_plan_materializes_fts_before_indexed_entry_lookup(corpus_dir: Path) -> None:
    db = sqlite3.connect(corpus_dir / "index.sqlite")
    try:
        plan = [row[3] for row in db.execute(f"EXPLAIN QUERY PLAN {SEARCH_SQL}", ("compaction",))]
    finally:
        db.close()
    assert any("MATERIALIZE matched_blobs" in step for step in plan)
    assert any("SCAN blobs_fts VIRTUAL TABLE INDEX" in step for step in plan)
    assert any("SEARCH e USING INDEX idx_entries_blob (blob_id=?)" in step for step in plan)


def test_search_honors_preexisting_cancellation(corpus_dir: Path) -> None:
    cancelled = threading.Event()
    cancelled.set()
    corpus = Corpus(corpus_dir)
    try:
        with pytest.raises(SearchCancelled):
            corpus.search("deployment", cancel_event=cancelled)
    finally:
        corpus.close()


def test_search_interrupts_sqlite_at_deadline(corpus_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(corpus_module, "SQLITE_PROGRESS_STEPS", 1)
    corpus = Corpus(corpus_dir)
    try:
        with pytest.raises(SearchDeadlineExceeded):
            corpus.search("deployment", deadline_seconds=1e-9)
    finally:
        corpus.close()


def test_concurrent_searches_do_not_share_a_corpus_lock(corpus_dir: Path) -> None:
    entered_loader = threading.Barrier(2)

    def load_body(_digest: str) -> str:
        entered_loader.wait(timeout=2)
        return "warehouse detection boundary"

    corpus = Corpus(corpus_dir, load_body=load_body)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            searches = [executor.submit(corpus.search, "detection") for _ in range(2)]
            assert [future.result(timeout=3)[0]["hitCount"] for future in searches] == [1, 1]
    finally:
        corpus.close()


def test_store_migrates_delete_mode_databases_to_wal_before_serving(corpus_dir: Path) -> None:
    for name in ("index.sqlite", "objects.sqlite"):
        with sqlite3.connect(corpus_dir / name) as connection:
            assert connection.execute("PRAGMA journal_mode = DELETE").fetchone() == ("delete",)
    store = CorpusStore(corpus_dir)
    try:
        for name in ("index.sqlite", "objects.sqlite"):
            with sqlite3.connect(corpus_dir / name) as connection:
                assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    finally:
        store.close()
