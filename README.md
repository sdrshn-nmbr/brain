# Brain

Your team already wrote the missing manual. It is sitting in agent chats.

Brain turns Claude Code, Codex, and Cursor sessions into a shared, searchable memory. A new teammate can ask why a
system works this way. An agent can find the old investigation instead of repeating it. The useful context can span the
whole history of a Git organization, including decisions that never made it into a document.

Deploy Brain once. Put it on your Tailnet. Point any stateless MCP client at `/mcp`. Search needs no Brain package,
token, browser login, or custom client. It works well from Code Mode because the MCP tools are small data operations and
the agent can combine them in a REPL.

Brain is open source and cloud-neutral. AWS, another cloud, or one machine all work.

## What feels different

- Tailscale is the default front door. People use the identity they already have on the private network.
- Search and reading are remote, stateless MCP calls. There is nothing to install on each reader's machine.
- Sharing stays deliberate. The local exporter shows the repository, source, dates, and session count before upload.
- Repository scope comes from the saved session directory's Git `origin`, not its folder name.
- Claude Code, Codex, Cursor, subagents, archived Codex sessions, and recorded Codex Desktop side chats are supported.
- Repeated text is stored once with SHA-256 content-addressed storage. Uploads send only missing sessions and bodies.
- Admins can inspect bounded, redacted request records without storing raw query text or upload bodies.

## Run it

[uv](https://docs.astral.sh/uv/) is the default way to build, test, and run Brain.

```bash
git clone https://github.com/sdrshn-nmbr/brain
cd brain
cp .env.example .env
# Edit the repository, Tailnet host, admin login, and app-capability domain.
docker compose up -d
tailscale serve --accept-app-caps=example.com/cap/brain 8788
```

Then point a remote MCP client at:

```text
https://brain.your-tailnet.ts.net/mcp
```

Tailscale Serve adds the caller identity and strips spoofed identity headers. Keep Brain's published port on host
loopback and do not expose its plain HTTP port. See [authentication](docs/authentication.md) and
[deployment](docs/deployment.md).

## Search without a client install

The server exposes `search`, `browse`, `read_session`, and `stats` as normal MCP tools. Add the URL to Claude Code,
Codex, Cursor, OpenCode, or another remote MCP client. Code Mode can call those tools from its own REPL; it does not need
the Brain Python package.

Publishing is the one local step. The server cannot read `~/.claude`, `~/.codex`, or Cursor's local databases. Run the
publisher once through uv without installing it:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain@v1.1.0 brain-sync \
  --endpoint https://brain.your-tailnet.ts.net/mcp \
  --repository github.com/example/widget \
  --visibility "Example engineering" \
  --dry-run
```

The dry run writes a local archive but sends no transcript data. Read the printed scope, then publish that exact archive:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain@v1.1.0 brain-sync \
  --endpoint https://brain.your-tailnet.ts.net/mcp \
  --archive "$HOME/Downloads/agent-chats-export-<timestamp>.zip"
```

Brain asks before upload. Use `--yes` only after a person or approved automation has accepted that exact scope.

## Codex Desktop side chats

Codex does not always put complete Desktop side chats in its normal session files. Brain includes a small opt-in proxy
that records future side-chat prompts, assistant replies, and tool events under
`~/.codex/attachments/sidechats/*.jsonl`:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain@v1.1.0 brain-sidechat-recorder --install-hook
export CODEX_CLI_PATH="$HOME/.codex/bin/brain-sidechat-recorder"
```

Start Codex Desktop from an environment containing that variable. The hook is local and contains only Python standard
library code. Recording failure never blocks Codex. These files can contain sensitive tool data. Brain uploads a
recorded side chat only when its saved working directory resolves to an allowed repository and the user confirms the
archive. Arbitrary files from `~/.codex/attachments` are never uploaded.

## Scope and access

Scope is checked by the exporter, upload server, and ingester. Repository IDs include the Git host, such as
`github.com/acme/api` or `gitlab.example.com/platform/models/recommender`.

Tailnet users get append access by default. Named admins get observability tools. Tagged workloads need a Tailscale app
capability and stay read-only. The included [policy templates](deploy/tailscale/) show read, append, and admin grants for
fine-grained access. Bearer tokens and trusted proxy headers remain explicit alternatives.

## Why SQLite

The current bottlenecks were query shape, archive packaging, and connection reuse. After those fixes, local SQLite FTS
search is fast and the single-writer design stays easy to operate. Brain keeps reads online during atomic ingestion and
uses a separate append-only object database for deduplicated bodies.

Possible future directions:

- A Turbopuffer backend for lexical search, vector search, and other indexes, with S3 for session bodies.
- Hybrid semantic and full-text ranking.
- Postgres or Turso when a deployment needs several writers or a managed database.
- Go, Rust, async pipelines, or SIMD JSON parsing if profiles show CPU parsing is the next real limit.

These are options, not required complexity.

## Develop

```bash
uv sync --frozen
just check
```

`just check` runs Ruff, ty, the full test suite, and package build. Brain uses Pydantic models at its MCP boundary and
checks them with ty's Pydantic-aware type analysis.

The service uses one writable process and one persistent data directory. See [architecture](docs/architecture.md) for
the storage and consistency rules.

Apache-2.0. See [LICENSE](LICENSE).
