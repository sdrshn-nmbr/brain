# Deployment

Brain needs one writable data directory and a Tailscale Serve front door. The application remains cloud-neutral.

```text
BRAIN_ALLOWED_REPOSITORIES=github.com/acme/widget,gitlab.example.com/platform/model
BRAIN_AUTH_MODE=tailscale
BRAIN_PUBLIC_HOSTS=brain.your-tailnet.ts.net
BRAIN_TAILSCALE_ADMIN_USERS=alice@example.com
BRAIN_VISIBILITY=Acme engineering
```

`BRAIN_PUBLIC_HOSTS` is the Tailnet hostname accepted by host and origin checks. `BRAIN_VISIBILITY` is shown in the local
upload preview.

## Docker Compose

Copy `.env.example` to `.env`, edit it, and run `docker compose up -d`. The port is loopback-only. Then run Tailscale
Serve on the host:

```bash
tailscale serve --accept-app-caps=example.com/cap/brain 8788
```

## Kubernetes

The example runs Brain and Tailscale in one pod with one replica and a ReadWriteOnce volume. It has no Kubernetes
Service, so the Tailscale sidecar is the front door. Edit the placeholders and follow
[`deploy/kubernetes/README.md`](../deploy/kubernetes/README.md).

Do not scale above one replica. Searches use independent read-only SQLite connections, while ingestion performs one
atomic write transaction.

## Backups

Snapshot the entire data directory at one point in time. Keep `index.sqlite`, `objects.sqlite`, `uploads.sqlite`, and
their WAL files together.

## Resource controls

- `BRAIN_MAX_UPLOAD_BYTES`
- `BRAIN_MAX_PENDING_BYTES_PER_OWNER`
- `BRAIN_UPLOAD_TTL_SECONDS`
- `BRAIN_UPLOAD_RECEIVE_TIMEOUT_SECONDS`
- `BRAIN_MAX_MCP_REQUEST_BYTES`
- `BRAIN_SEARCH_TIMEOUT_SECONDS`
- `BRAIN_REQUEST_LOG_RETENTION`
