# Deployment

Brain needs Python 3.11 or newer, a writable data directory, and an HTTPS or private-network front door.

## Required settings

```text
BRAIN_ALLOWED_REPOSITORIES=github.com/acme/widget,gitlab.example.com/platform/model
BRAIN_AUTH_MODE=token
BRAIN_TOKENS_FILE=/run/secrets/brain/tokens.json
BRAIN_PUBLIC_HOSTS=brain.example.com
BRAIN_VISIBILITY=Acme engineering
```

`BRAIN_PUBLIC_HOSTS` controls host and origin checks. `BRAIN_VISIBILITY` is displayed during local preview and must match
the confirmed upload scope.

## Docker Compose

Copy `.env.example` to `.env`, set the repository list, create `tokens.json` with `brain-token`, then run
`docker compose up -d`. The default port mapping is loopback-only. Put Caddy, nginx, Envoy, Tailscale Serve, or another
authenticated HTTPS proxy in front of it.

## Kubernetes

The example manifest uses one replica and a ReadWriteOnce volume. Create `brain-auth` separately as shown in
[`deploy/kubernetes/README.md`](../deploy/kubernetes/README.md), edit the placeholders, add an ingress, then apply it.

Do not scale the Deployment above one replica. The persistent SQLite writer is intentionally single-owner. CPU and
memory limits should be load-tested against the largest allowed archive; the default archive parser has bounded
decompression, but ingestion still needs working memory for a session's entry map.

## Backups

Take crash-consistent snapshots of the entire data directory. Keep `index.sqlite`, `objects.sqlite`, `uploads.sqlite`, and
their WAL files together. Stop the process or use a volume snapshot mechanism that preserves a point-in-time filesystem
view.

## Resource controls

- `BRAIN_MAX_UPLOAD_BYTES`
- `BRAIN_MAX_PENDING_BYTES_PER_OWNER`
- `BRAIN_UPLOAD_TTL_SECONDS`
- `BRAIN_UPLOAD_RECEIVE_TIMEOUT_SECONDS`
- `BRAIN_MAX_MCP_REQUEST_BYTES`
- `BRAIN_SEARCH_TIMEOUT_SECONDS`
- `BRAIN_REQUEST_LOG_RETENTION`
