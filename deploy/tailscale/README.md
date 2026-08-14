# Tailscale templates

Replace `example.com/cap/brain` with a capability under a domain you control.

- `serve.json` is the Serve config used by the Kubernetes sidecar. It forwards the selected app capability and keeps
  Funnel off.
- `policy.hujson` shows append access for people, admin access for Brain operators, and read-only access for tagged
  workloads.

Set `BRAIN_TAILSCALE_REQUIRE_CAPABILITY=true` when every human caller should be authorized by these grants. Brain always
keeps tagged workload identities read-only.
