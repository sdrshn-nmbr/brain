from datetime import timedelta
from pathlib import Path

import pytest

import brain.learning as learning
from brain.learning import Usage


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch):
    now = [learning.datetime(2026, 10, 28, 12, tzinfo=learning.UTC)]
    monkeypatch.setattr(learning, "utc_now", lambda: now[0])
    return now


def test_a_read_credits_only_the_latest_search_that_returned_it(tmp_path: Path, clock) -> None:
    usage = Usage(tmp_path)
    try:
        usage.record_search("deploy brain", {}, [7, 9])
        clock[0] += timedelta(minutes=1)
        usage.record_search("brain deploy mac", {}, [9])
        clock[0] += timedelta(minutes=1)
        usage.record_read(9)
        usage.record_read(11)

        assert usage.db.execute("SELECT search_id, session_id FROM useful").fetchall() == [(2, 9)]
        assert set(usage.priors()) == {9}
        assert usage.priors(before=usage.db.execute("SELECT at FROM searches WHERE id = 2").fetchone()[0]) == {}
        assert [item["targets"] for item in usage.logged_searches()] == [set(), {9}]
    finally:
        usage.close()


def test_reads_outside_the_window_are_not_credited(tmp_path: Path, clock) -> None:
    usage = Usage(tmp_path)
    try:
        usage.record_search("deploy brain", {}, [9])
        clock[0] += timedelta(minutes=16)
        usage.record_read(9)
        assert usage.priors() == {}
    finally:
        usage.close()


def test_relabel_rebuilds_useful_results_from_reads(tmp_path: Path, clock) -> None:
    usage = Usage(tmp_path)
    try:
        start = clock[0]
        usage.record_search("deploy brain", {}, [7, 9])
        clock[0] += timedelta(minutes=1)
        usage.record_search("brain deploy mac", {}, [9])
        usage.db.execute("INSERT INTO useful VALUES (1, 9, ?)", (start.isoformat(),))
        usage.db.commit()

        report = usage.relabel([(start + timedelta(minutes=2), 9), (start + timedelta(minutes=3), 7)])

        assert report == {"previousUseful": 1, "useful": 2}
        assert sorted(usage.db.execute("SELECT search_id, session_id FROM useful").fetchall()) == [(1, 7), (2, 9)]
    finally:
        usage.close()
