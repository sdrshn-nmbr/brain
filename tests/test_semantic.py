import sqlite3
from pathlib import Path

import numpy as np
import pytest

import brain.semantic as semantic_module
from brain.corpus import Corpus
from brain.semantic import Semantic


class WordModel:
    dim = 4
    vocabulary = ("warehouse", "vector", "deployment", "boundary")

    def encode(self, texts: list[str]) -> np.ndarray:
        return np.array(
            [[float(word in text.lower()) + 0.01 for word in self.vocabulary] for text in texts], dtype=np.float32
        )


@pytest.fixture
def word_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(semantic_module.StaticModel, "from_pretrained", lambda _name: WordModel())


def vector_rows(data_dir: Path) -> set[int]:
    semantic = Semantic(data_dir, "word-model")
    with semantic._connection() as db:
        return {row[0] for row in db.execute("SELECT rowid FROM blob_vectors")}


def test_indexes_only_conversation_bodies_once(corpus_dir: Path, word_model: None) -> None:
    semantic = Semantic(corpus_dir, "word-model")
    corpus = Corpus(corpus_dir)

    assert semantic.index_missing(corpus.load_bodies) == 3
    assert semantic.index_missing(corpus.load_bodies) == 0
    assert vector_rows(corpus_dir) == {1, 3, 4}
    assert semantic.nearest("deployment", 1) == [4]


def test_changing_the_model_rebuilds_the_index(corpus_dir: Path, word_model: None) -> None:
    corpus = Corpus(corpus_dir)
    Semantic(corpus_dir, "word-model").index_missing(corpus.load_bodies)

    rebuilt = Semantic(corpus_dir, "another-model")
    with sqlite3.connect(corpus_dir / "vectors.sqlite") as db:
        assert db.execute("SELECT value FROM meta WHERE key = 'model'").fetchone() == ("another-model",)
    assert rebuilt.index_missing(corpus.load_bodies) == 3
