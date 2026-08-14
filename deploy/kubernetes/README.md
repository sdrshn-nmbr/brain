# Kubernetes

Create token credentials before applying the deployment. The file is a JSON array documented in
[`docs/authentication.md`](../../docs/authentication.md).

```bash
kubectl create namespace brain --dry-run=client -o yaml | kubectl apply -f -
kubectl -n brain create secret generic brain-auth --from-file=tokens.json=./tokens.json
kubectl apply -f deploy/kubernetes/brain.yaml
```

Edit the image, repository allowlist, public host, visibility label, storage size, and ingress before production use.
The service is plain HTTP inside the cluster. Put an HTTPS ingress or private-network proxy in front of it.
