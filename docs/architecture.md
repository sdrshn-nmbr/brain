# Architecture

Brain has four boundaries:

1. The collector parses local Claude Code, Codex, and Cursor stores. It resolves repository identity from each session's
   saved working directory and Git `origin`.
2. The sync client shows the exact scope, gets confirmation, asks the server which session and body hashes are missing,
   and uploads only the needed archive data.
3. The ingester validates the archive, writes compressed bodies to an append-only content-addressed object database, and
   atomically commits session maps and the FTS index.
4. The stateless MCP server opens request-local read-only SQLite snapshots for search, browse, session reads, and stats.

## Storage

`index.sqlite` contains session metadata, entry order, body references, and a contentless FTS5 index. `objects.sqlite`
contains one zstd-compressed body per SHA-256 digest. `uploads.sqlite` tracks upload ownership and state.

Both index databases use WAL mode and full synchronous commits. An object can survive a failed index transaction as an
unreferenced CAS object; a retry safely reuses it. Readers continue using their old snapshot until the new index commit is
visible.

## Deduplication and replacement

The client computes one fingerprint from a session's source, UUID, ordered entry metadata, and body hashes. Bodies have
their own SHA-256 identities. The server negotiates both sets before upload.

A stable session key includes source, UUID, start time, and a source-file fallback. A later generation replaces an older
searchable generation only when its end time or entry count advances. A stale archive cannot roll a session backward.
Ownership is part of every uniqueness boundary.

## Operational model

Run one writer with persistent local storage. Search work uses independent SQLite connections and includes execution
deadlines and cancellation. Uploads are bounded by archive size, pending bytes per owner, receive time, record count,
decompressed entry-map size, and decompressed body size.
