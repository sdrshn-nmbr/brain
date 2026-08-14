# Authentication

Tailscale Serve is the default. Every accepted request becomes one identity with `read`, `append`, or `admin` access.

## Tailscale Serve

Keep Brain's plain HTTP port reachable only from the local Tailscale proxy. Let Tailscale Serve terminate HTTPS and
forward to port 8788:

```bash
tailscale serve --accept-app-caps=example.com/cap/brain 8788
```

Set:

```text
BRAIN_AUTH_MODE=tailscale
BRAIN_PUBLIC_HOSTS=brain.your-tailnet.ts.net
BRAIN_TAILSCALE_ADMIN_USERS=alice@example.com
BRAIN_TAILSCALE_APP_CAPABILITY=example.com/cap/brain
```

Human Tailnet users get append access. `BRAIN_TAILSCALE_ALLOWED_USERS` can restrict them, and
`BRAIN_TAILSCALE_ADMIN_USERS` names administrators.

For fine-grained access, set `BRAIN_TAILSCALE_REQUIRE_CAPABILITY=true` and grant the configured app capability with an
`access` value of `read`, `append`, or `admin`. See the [Serve and policy templates](../deploy/tailscale/). Use a
capability name under a domain you control.

Tagged devices do not get human identity headers. They must receive an app capability. Brain always reduces tagged
workloads to read-only, even if a policy grants append or admin. Shared workload identity is not safe ownership for
uploads.

Tailscale Serve removes inbound copies of its identity headers before adding its own. Direct access to Brain would let a
client forge them, so do not expose the backend port.

## Bearer tokens

Tokens are an explicit fallback for deployments without Tailscale. Set `BRAIN_AUTH_MODE=token` and provide
`BRAIN_TOKENS_FILE` or `BRAIN_TOKENS_JSON`:

```json
[
  {
    "tokenSha256": "<64 lowercase hex characters>",
    "principal": "alice@example.com",
    "access": "admin",
    "name": "Alice"
  }
]
```

Use `uv run brain-token` to create the digest file. Remote token endpoints must use HTTPS.

## Trusted headers

`BRAIN_AUTH_MODE=trusted-header` accepts `X-Brain-Principal`, `X-Brain-Access`, and `X-Brain-Name` from an existing
identity proxy. Use it only when clients cannot reach Brain directly and the proxy removes inbound copies.

## Local development

`BRAIN_AUTH_MODE=none` creates one local admin identity. Startup rejects a non-loopback bind in this mode.
