# Authentication

Every accepted request becomes an identity with a stable principal and one access level:

- `read`: search and read shared history.
- `append`: read plus publish sessions under that principal's derived corpus label.
- `admin`: append plus inspect request observability.

The server exposes the same MCP tool list to every client, but authorization is checked inside each tool. This keeps MCP
discovery simple without relying on the client to hide forbidden operations.

## Bearer tokens

Set `BRAIN_AUTH_MODE=token` and provide `BRAIN_TOKENS_FILE` or `BRAIN_TOKENS_JSON`. The value is a JSON array:

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

Use `brain-token` to generate the token and digest file. Brain never needs the plaintext token at rest. Clients send the
plaintext value in `Authorization: Bearer ...`, so remote endpoints must use HTTPS.

## Trusted headers

Set `BRAIN_AUTH_MODE=trusted-header` when a reverse proxy already authenticates users. The defaults are:

```text
X-Brain-Principal: alice@example.com
X-Brain-Access: append
X-Brain-Name: Alice
```

Header names are configurable with `BRAIN_TRUSTED_PRINCIPAL_HEADER`, `BRAIN_TRUSTED_ACCESS_HEADER`, and
`BRAIN_TRUSTED_NAME_HEADER`.

This mode is safe only when clients cannot reach Brain directly and the proxy removes inbound copies of these headers
before adding its own. Keep Brain on loopback, a private Unix/network namespace, or an internal Kubernetes Service.

## Tailscale Serve

Set `BRAIN_AUTH_MODE=tailscale`. Human devices use the identity headers injected by Tailscale Serve. Configure optional
allowlists with `BRAIN_TAILSCALE_ALLOWED_USERS` and administrators with `BRAIN_TAILSCALE_ADMIN_USERS`.

Tagged devices do not receive human identity headers. Brain can accept a read-only Tailscale app capability configured by
`BRAIN_TAILSCALE_APP_CAPABILITY`, which defaults to `brain.dev/cap/read`. The Serve handler must list the capability in
`AcceptAppCaps`, and the tailnet policy must grant it to the intended workload tag. Brain intentionally keeps this path
read-only because a shared workload identity is not safe upload ownership.

## Development mode

`BRAIN_AUTH_MODE=none` creates one local admin identity. Startup fails if `BRAIN_HOST` is not loopback. Do not put a proxy
in front of this mode.
