from pathlib import Path

from brain.config import load_config
from brain.server import create_app


def test_new_data_directory_bootstraps_an_empty_searchable_corpus(tmp_path: Path) -> None:
    data_dir = tmp_path / "new-data"
    config = load_config(
        {
            "BRAIN_AUTH_MODE": "none",
            "BRAIN_ALLOWED_REPOSITORIES": "github.com/acme/widget",
            "BRAIN_DATA_DIR": str(data_dir),
        }
    )
    app = create_app(config)
    try:
        assert app.state.corpus.read().stats() == {
            "sessions": [],
            "blobs": {"blobs": 0, "unique_bytes": 0, "logical_bytes": 0},
        }
        assert {path.name for path in data_dir.iterdir()} >= {
            "index.sqlite",
            "objects.sqlite",
            "uploads.sqlite",
            "requests.sqlite",
        }
    finally:
        app.state.uploads.close()
        app.state.request_log.close()
        app.state.corpus.close()
