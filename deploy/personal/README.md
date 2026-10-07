# Personal Brain

One Linux machine on your tailnet holds every agent session from every machine you use. Each machine uploads its own
sessions on a timer, and any agent searches through `https://<host>.<tailnet>.ts.net/mcp`. Tailscale is the only door:
Brain trusts the login or app capability that Tailscale Serve attaches to each request. Set
`BRAIN_TAILSCALE_ALLOWED_USERS` to your login, and grant your tagged VMs the same access through an app capability.

## Server

On the host, with Tailscale and [uv](https://docs.astral.sh/uv/) installed:

```bash
sudo install -d -o "$USER" -m 700 /var/lib/brain
sudo cp brain@.service /etc/systemd/system/
sudo cp brain.env /etc/brain.env    # set your login and tailnet host
sudo systemctl enable --now "brain@$USER"
sudo tailscale serve --bg --accept-app-caps=example.com/cap/brain 8788
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

For a final upload on Linux shutdown, copy `sync.env` to `~/.config/brain/sync.env` and set the uvx path, endpoint
and machine label. Install `brain-final-sync` at `/usr/local/bin/brain-final-sync` and `brain-sync-shutdown.service`
in `/etc/systemd/system/`. Set its `User` and `Before=user@<uid>.service` to this machine's user, then run
`sudo systemctl enable --now brain-sync-shutdown.service`. This orders the upload after the user manager stops
and before Tailscale and the network stop. A failed shutdown upload is logged; Linux still completes shutdown.

Freestyle pause freezes the guest without running shutdown hooks. Use the lifecycle helper for pause or deletion:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain brain-vm pause <vm>
uvx --from git+https://github.com/sdrshn-nmbr/brain brain-vm delete <vm>
```

The helper requires the guest's configured `brain-final-sync`. It starts paused or stopped VMs, uploads every saved
chat, waits for ingestion, and cancels the lifecycle action if export or upload fails. Deletion refuses while Codex,
Claude or Cursor agent processes are running; close them first. Pause keeps running processes, so writes made after
the export remain in the paused VM and upload after resume. VM failures and dashboard or direct API actions bypass
the helper. Periodic uploads remain necessary for those cases. Brain stores transcripts; commit and push code separately.

## Moving a chat

`brain-handoff` copies a Codex chat to or from another machine so it continues there with `codex resume`:

```bash
uvx --from git+https://github.com/sdrshn-nmbr/brain brain-handoff pull <session-id> <host>
```

Only the conversation moves. Push code first, and close the chat where it was running.
