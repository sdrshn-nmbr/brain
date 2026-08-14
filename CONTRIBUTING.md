# Contributing

Use Python 3.11 or newer and uv.

```bash
uv sync --frozen
just check
```

Keep changes small and add a regression test for behavior changes. Do not commit real transcripts, credentials, private
hostnames, internal repository names, or production database files. Test fixtures must use synthetic people,
repositories, UUIDs, and message text.

Parser changes need tests for discovery, malformed records, incomplete records, repository attribution, and entry order.
Authorization changes need tests for read, append, admin, cross-owner access, malformed credentials, and request logging.

By submitting a contribution, you agree that it is licensed under Apache-2.0.
