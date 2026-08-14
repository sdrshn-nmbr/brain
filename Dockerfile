FROM ghcr.io/astral-sh/uv:0.7.19 AS uv

FROM python:3.13-slim-bookworm AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY brain ./brain
COPY collector ./collector
RUN uv sync --frozen --no-dev

FROM python:3.13-slim-bookworm
ENV BRAIN_DATA_DIR=/data \
    BRAIN_HOST=0.0.0.0 \
    BRAIN_PORT=8788 \
    PATH=/app/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN groupadd --gid 1000 brain \
    && useradd --uid 1000 --gid brain --create-home brain \
    && install -d --owner=brain --group=brain /data
WORKDIR /app
COPY --from=builder --chown=brain:brain /app /app
USER brain
EXPOSE 8788
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8788/healthz', timeout=2)"]
ENTRYPOINT ["python", "-m", "brain"]
