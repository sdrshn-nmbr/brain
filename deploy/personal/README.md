# Personal Brain

One Linux machine on your tailnet holds every agent session from every machine you use. Each machine uploads its own
sessions on a timer, and any agent searches through `https://<host>.<tailnet>.ts.net/mcp`. Tailscale is the only door:
Brain trusts the login that Tailscale Serve attaches to each request, and `BRAIN_TAILSCALE_ALLOWED_USERS` admits only
you.

## Server

On the host, with Tailscale and [uv](https://docs.astral.sh/uv/) installed:

```bash
sudo install -d -o "$USER" -m 700 /var/lib/brain
sudo cp brain@.service /etc/systemd/system/
sudo cp brain.env /etc/brain.env    # set your login and tailnet host
sudo systemctl enable --now "brain@$USER"
sudo tailscale serve --bg 8788
```

The service runs the latest `main` through `uvx`, so a restart is an upgrade. The first start downloads the embedding
model and embeds existing messages in the background; search works throughout.

## Backups

[Litestream](https://litestream.io) streams `index.sqlite`, `objects.sqlite` and `usage.sqlite` to any S3-compatible
bucket within seconds of each write. `vectors.sqlite` is rebuilt from the index, and `uploads.sqlite` only tracks uploads
in flight.

```bash
sudo cp litestream.yml /etc/litestream.yml     # set the bucket and endpoint
sudo cp litestream.env /etc/litestream.env     # set the bucket's access key
sudo systemctl enable --now litestream
```

To restore onto a new host, stop Brain and run `litestream restore -config /etc/litestream.yml <path>` for each
database, `objects.sqlite` before `index.sqlite`.

## Machines

Every machine, including the server, uploads its own sessions every five minutes:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain brain-sync --endpoint https://<host>.<tailnet>.ts.net/mcp --machine <label>
```

Run it from a systemd user timer on Linux (`brain-sync.service`, `brain-sync.timer`) or a LaunchAgent on macOS
(`brain-sync.plist`). Edit the endpoint and label in each, then enable it.

## Moving a chat

`brain-handoff` copies a Codex chat to or from another machine so it continues there with `codex resume`:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain brain-handoff pull <session-id> <host>
```

Only the conversation moves. Push code first, and close the chat where it was running.
