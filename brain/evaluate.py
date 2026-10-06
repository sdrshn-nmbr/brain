from __future__ import annotations

import argparse
import json
import logging

from brain.config import load_config
from brain.corpus import CorpusStore
from brain.learning import Usage
from brain.semantic import Semantic


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay searches whose results were later read, and score the current ranking against them"
    )
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--keyword-only", action="store_true", help="Score keyword ranking without vectors")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    config = load_config()
    semantic = None
    if config.embedding_model and not args.keyword_only:
        semantic = Semantic(config.data_dir, config.embedding_model)
    usage = Usage(config.data_dir)
    store = CorpusStore(config.data_dir, semantic=semantic, usage=usage)
    try:
        corpus = store.read()
        report = usage.evaluate(
            lambda query, filters: [result["sessionId"] for result in corpus.search(query, **filters, limit=args.k)],
            k=args.k,
        )
        print(json.dumps({"model": None if semantic is None else semantic.model_name, **report}))
    finally:
        store.close()


if __name__ == "__main__":
    main()
