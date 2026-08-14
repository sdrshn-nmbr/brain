# Kubernetes

The manifest runs Brain beside a userspace Tailscale node. There is no Kubernetes Service. Tailnet HTTPS is the only
front door.

1. Replace the repository, Tailnet hostname, visibility, admin login, and app capability in `brain.yaml`.
2. Replace `example.com/cap/brain` with a capability under a domain you control.
3. Apply the matching grants from [`../tailscale/policy.hujson`](../tailscale/policy.hujson) to the tailnet policy.
4. Create a reusable Tailscale auth key that can use `tag:brain`.

```bash
kubectl create namespace brain --dry-run=client -o yaml | kubectl apply -f -
kubectl -n brain create secret generic brain-tailscale-auth --from-literal=authkey='tskey-auth-...'
kubectl apply -f deploy/kubernetes/brain.yaml
```

For human identity without app-capability enforcement, leave `BRAIN_TAILSCALE_REQUIRE_CAPABILITY=false`. Set it to
`true` when the policy grants every human caller `read`, `append`, or `admin` through the configured capability.

Keep one replica. The persistent SQLite writer is single-owner.
