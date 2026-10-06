from pathlib import Path

from brain.learning import Usage


def test_reads_after_a_search_become_useful_results_and_an_eval_set(tmp_path: Path) -> None:
    usage = Usage(tmp_path)
    try:
        usage.record_search("deploy brain", {"person": None}, [7, 9])
        usage.record_read(9)
        usage.record_read(11)

        assert set(usage.priors()) == {9}
        assert usage.evaluate(lambda _query, _filters: [9, 7]) == {"queries": 1, "recallAt10": 1.0, "mrr": 1.0}
        assert usage.evaluate(lambda _query, _filters: [7, 9]) == {"queries": 1, "recallAt10": 1.0, "mrr": 0.5}
    finally:
        usage.close()
