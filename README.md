# Brain

Brain is a self-hosted search service for agent session history. It exposes a stateless MCP endpoint, collects local
sessions from Claude Code, Codex, and Cursor, and lets each person decide exactly what to share.

Brain is built for a team, company, research group, or any other trusted group. It does not depend on one Git host,
identity provider, cloud, or private network.

## What it does

- Searches full session text with SQLite FTS5.
- Preserves user and assistant messages, reasoning records, tool calls, tool results, timestamps, source metadata, and
  Git provenance when the local agent stored them.
- Scopes exports by the Git `origin` of each session's saved working directory.
- Deduplicates repeated content with SHA-256 content-addressed storage.
- Sends only sessions and bodies the server does not already have.
- Gives callers read, append, or admin access.
- Lets append users publish only to their own corpus identity.
- Records bounded, redacted request metadata for administrators without retaining raw search text or upload bodies.

Brain does not upload arbitrary Codex attachment files. They are not reliably tied to a repository. Codex Desktop side
chats are included only when their recorded working directory resolves to an allowed repository. Brain reads the local
Codex diagnostic store and also understands richer recorder output at `~/.codex/attachments/sidechats/*.jsonl`, but it
does not install or inject a recorder into Codex. Side chats can therefore be partial when Codex did not persist assistant
response text.

## Quick start

Install [uv](https://docs.astral.sh/uv/), clone the repository, then create an admin token:

```bash
uv sync --frozen
uv run brain-token --principal you@example.com --access admin --output tokens.json
```

The command stores only the token digest and prints the token once. Keep the printed token private.

Start Brain for one repository:

```bash
export BRAIN_AUTH_MODE=token
export BRAIN_TOKENS_FILE="$PWD/tokens.json"
export BRAIN_ALLOWED_REPOSITORIES=github.com/example/widget
export BRAIN_VISIBILITY="example team"
uv run brain
```

In another shell, preview a local export. GitHub `owner/repository` is accepted as shorthand; other hosts use
`host/owner/repository`.

```bash
export BRAIN_TOKEN='<the token printed above>'
uv run brain-sync \
  --endpoint http://127.0.0.1:8788/mcp \
  --repository example/widget \
  --visibility "example team" \
  --dry-run
```

The preview writes a local archive and sends no transcript data. Review the repository list, sources, time bounds,
session counts, skipped sessions, and archive path. Then publish that exact archive:

```bash
uv run brain-sync \
  --endpoint http://127.0.0.1:8788/mcp \
  --archive "$HOME/Downloads/agent-chats-export-<timestamp>.zip" \
  --visibility "example team"
```

Brain asks for confirmation before upload. Use `--yes` only in an already approved automation flow.

## MCP client

Point any stateless MCP client at `https://your-brain.example/mcp` and send:

```text
Authorization: Bearer <token>
```

Read tools:

- `access`
- `search`
- `browse`
- `read_session`
- `stats`

Append tools:

- `plan_upload`
- `missing_blobs`
- `prepare_upload`
- `commit_upload`
- `upload_status`
- `list_my_uploads`
- `cancel_upload`

Admin tools:

- `admin_requests`
- `admin_request_stats`

## Repository scope

Repository scope is enforced three times:

1. The collector resolves each saved session working directory with Git and reads its `origin`.
2. The upload server compares the confirmed archive scope with `BRAIN_ALLOWED_REPOSITORIES`.
3. The ingester checks every session again before changing the searchable index.

Canonical repository IDs include the host, for example `github.com/acme/api` or
`gitlab.example.com/platform/models/recommender`. Local folder names are never used as repository identity.

## Authentication

The secure default is bearer-token authentication. Brain also supports trusted identity headers and Tailscale Serve.
Unauthenticated mode is restricted to a loopback bind and exists only for local development.

See [authentication](docs/authentication.md) for configuration and threat boundaries.

## Deployment

- [Docker Compose](compose.yaml) binds Brain to host loopback by default.
- [Kubernetes](deploy/kubernetes/README.md) uses one persistent volume and a separately created token Secret.
- The container is published for AMD64 and ARM64 at `ghcr.io/sdrshn-nmbr/brain`.

Put HTTPS or a private network proxy in front of Brain before remote use. SQLite requires one writable replica and a
ReadWriteOnce volume. Searches use independent read-only connections, so reads remain available during atomic ingests.

See [deployment](docs/deployment.md) and [architecture](docs/architecture.md).

## Verify

```bash
just check
```

Run a non-persistent remote transport test:

```bash
BRAIN_MCP_URL=https://your-brain.example/mcp \
BRAIN_TOKEN='<admin or append token>' \
uv run python scripts/mcp_smoke.py
```

Run the full upload, ingest, search, and read-back test only against a disposable corpus:

```bash
BRAIN_SMOKE_COMMIT=1 \
BRAIN_MCP_URL=http://127.0.0.1:8788/mcp \
BRAIN_TOKEN='<admin or append token>' \
uv run python scripts/mcp_smoke.py
```

## Limits

- The service is designed for one writable process over local persistent storage, not active-active replicas.
- SQLite FTS is lexical search. Brain does not add embeddings or semantic ranking.
- Transcript formats are private implementation details of agent tools and can change. Parser regressions are isolated per
  session and reported in export totals.
- Session supersession is narrow: a newer saved generation of the same canonical session can replace the older searchable
  generation. Different sessions and different users cannot replace each other.

## License

Apache-2.0. See [LICENSE](LICENSE).
