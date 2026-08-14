set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

install:
    uv sync --frozen

format:
    uv run ruff format .
    uv run ruff check . --fix

check:
    uv run ruff format --check .
    uv run ruff check .
    uv run ty check
    uv run pytest -q
    uv build

serve:
    uv run brain

smoke:
    uv run python scripts/mcp_smoke.py

smoke-commit:
    BRAIN_SMOKE_COMMIT=1 uv run python scripts/mcp_smoke.py

container:
    docker build --tag brain:local .

compose-up:
    docker compose up --build --detach

compose-down:
    docker compose down
