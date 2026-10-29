from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import sqlite_vec
from model2vec import StaticModel

logger = logging.getLogger("brain.semantic")

EMBEDDED_ROLES = ("user", "assistant")
MAX_CHARS = 2_000
BATCH_SIZE = 512

BodyLoader = Callable[[list[str]], list[str]]


def unit_rows(matrix: np.ndarray) -> np.ndarray:
    return matrix / (np.linalg.norm(matrix, axis=1, keepdims=True) + 1e-12)


class Semantic:
    """Vector index over unique user and assistant bodies.

    vectors.sqlite beside the corpus stores the embeddings. Searches score a normalized copy held in memory, which is
    exact and takes a few milliseconds where a sqlite-vec scan took hundreds.
    """

    def __init__(self, data_dir: Path, model_name: str) -> None:
        self.index_path = data_dir / "index.sqlite"
        self.path = data_dir / "vectors.sqlite"
        self.model_name = model_name
        self.model = StaticModel.from_pretrained(model_name)
        self._index_lock = threading.Lock()
        with self._connection() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            row = db.execute("SELECT value FROM meta WHERE key = 'model'").fetchone()
            if row and row[0] != model_name:
                logger.warning(json.dumps({"event": "embedding_model_changed", "from": row[0], "to": model_name}))
                db.execute("DROP TABLE IF EXISTS blob_vectors")
            db.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS blob_vectors USING "
                f"vec0(embedding float[{self.model.dim}] distance_metric=cosine)"
            )
            db.execute("INSERT OR REPLACE INTO meta(key, value) VALUES ('model', ?)", (model_name,))
        self._ids, self._matrix = self._load()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.enable_load_extension(True)
        sqlite_vec.load(connection)
        connection.enable_load_extension(False)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _load(self) -> tuple[np.ndarray, np.ndarray]:
        started = time.perf_counter()
        with self._connection() as db:
            rows = db.execute("SELECT rowid, embedding FROM blob_vectors").fetchall()
        ids = np.fromiter((row[0] for row in rows), dtype=np.int64, count=len(rows))
        matrix = np.frombuffer(b"".join(row[1] for row in rows), dtype=np.float32).reshape(len(rows), self.model.dim)
        logger.info(
            json.dumps(
                {"event": "semantic_loaded", "vectors": len(rows), "seconds": round(time.perf_counter() - started, 1)}
            )
        )
        return ids, unit_rows(matrix)

    def embed(self, texts: list[str]) -> np.ndarray:
        return np.asarray(self.model.encode([text[:MAX_CHARS] for text in texts]), dtype=np.float32)

    def nearest(self, query: str, k: int) -> list[int]:
        ids, matrix = self._ids, self._matrix
        if not len(ids):
            return []
        scores = matrix @ unit_rows(self.embed([query]))[0]
        k = min(k, len(ids))
        top = np.argpartition(-scores, k - 1)[:k]
        return ids[top[np.argsort(-scores[top])]].tolist()

    def index_missing(self, load_bodies: BodyLoader) -> int:
        """Embed every user/assistant body that has no vector yet. Safe to call after each ingest."""
        if not self._index_lock.acquire(blocking=False):
            return 0
        try:
            started = time.perf_counter()
            with sqlite3.connect(f"file:{self.index_path}?mode=ro", uri=True) as index:
                candidates = index.execute(
                    f"""SELECT DISTINCT b.id, b.hash FROM entries e JOIN blobs b ON b.id = e.blob_id
                        WHERE e.role IN ({",".join("?" for _ in EMBEDDED_ROLES)})""",
                    EMBEDDED_ROLES,
                ).fetchall()
            present = set(self._ids.tolist())
            missing = [(blob_id, digest) for blob_id, digest in candidates if blob_id not in present]
            for offset in range(0, len(missing), BATCH_SIZE):
                batch = missing[offset : offset + BATCH_SIZE]
                vectors = self.embed(load_bodies([digest for _, digest in batch]))
                with self._connection() as db:
                    db.executemany(
                        "INSERT INTO blob_vectors(rowid, embedding) VALUES (?, ?)",
                        ((blob_id, vector.tobytes()) for (blob_id, _), vector in zip(batch, vectors, strict=True)),
                    )
                self._ids = np.concatenate([self._ids, np.array([blob_id for blob_id, _ in batch], dtype=np.int64)])
                self._matrix = np.vstack([self._matrix, unit_rows(vectors)])
            logger.info(
                json.dumps(
                    {
                        "event": "semantic_index",
                        "model": self.model_name,
                        "embedded": len(missing),
                        "total": len(present) + len(missing),
                        "seconds": round(time.perf_counter() - started, 1),
                    }
                )
            )
            return len(missing)
        finally:
            self._index_lock.release()
