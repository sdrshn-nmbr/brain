from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from zipfile import ZipFile

import zstandard as zstd

FORMAT_VERSION = 2
MAX_ENTRY_LINE_BYTES = 1024 * 1024
MAX_ENTRY_MAP_BYTES = 32 * 1024 * 1024
MAX_ENTRIES_PER_SESSION = 100_000


@dataclass(frozen=True)
class ArchiveEntry:
    seq: int
    role: str
    timestamp: str | None
    tool_name: str | None
    header_line: str
    body_sha256: str


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def session_fingerprint(source: str, uuid: str, entries: Iterable[ArchiveEntry]) -> str:
    digest = hashlib.sha256()
    digest.update(b'{"entries":[')
    for index, entry in enumerate(entries):
        if index:
            digest.update(b",")
        digest.update(
            json.dumps(
                {
                    "seq": entry.seq,
                    "role": entry.role,
                    "ts": entry.timestamp,
                    "header_line": entry.header_line,
                    "body_sha256": entry.body_sha256,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
    digest.update(b'],"source":')
    digest.update(json.dumps(source, ensure_ascii=False, separators=(",", ":")).encode())
    digest.update(b',"uuid":')
    digest.update(json.dumps(uuid, ensure_ascii=False, separators=(",", ":")).encode())
    digest.update(b"}")
    return digest.hexdigest()


def stable_session_key(source: str, uuid: str, started_at: str | None, fallback: str) -> str:
    payload = {
        "source": source,
        "uuid": uuid,
        "started_at": started_at,
        "fallback": fallback,
    }
    return sha256_bytes(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())


def write_entries(zipped: ZipFile, path: str, entries: Iterable[ArchiveEntry]) -> None:
    compressor = zstd.ZstdCompressor(level=3)
    with zipped.open(path, "w") as raw_output, compressor.stream_writer(raw_output, closefd=False) as output:
        for entry in entries:
            record = {
                "seq": entry.seq,
                "role": entry.role,
                "timestamp": entry.timestamp,
                "tool_name": entry.tool_name,
                "header_line": entry.header_line,
                "body_sha256": entry.body_sha256,
            }
            output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode() + b"\n")


def read_entries(zipped: ZipFile, path: str) -> list[ArchiveEntry]:
    decompressor = zstd.ZstdDecompressor()
    entries: list[ArchiveEntry] = []
    with (
        zipped.open(path) as compressed,
        decompressor.stream_reader(compressed) as reader,
        io.BufferedReader(reader) as buffered,
    ):
        total_bytes = 0
        line_number = 0
        while True:
            raw_line = buffered.readline(MAX_ENTRY_LINE_BYTES + 1)
            if not raw_line:
                break
            line_number += 1
            total_bytes += len(raw_line)
            if len(raw_line) > MAX_ENTRY_LINE_BYTES:
                raise ValueError(f"entry line exceeds 1 MiB at {path}:{line_number}")
            if total_bytes > MAX_ENTRY_MAP_BYTES:
                raise ValueError(f"entry map exceeds 32 MiB: {path}")
            if line_number > MAX_ENTRIES_PER_SESSION:
                raise ValueError(f"entry map exceeds 100000 records: {path}")
            if not raw_line.strip():
                continue
            record = json.loads(raw_line)
            try:
                entry = ArchiveEntry(
                    seq=int(record["seq"]),
                    role=str(record["role"]),
                    timestamp=str(record["timestamp"]) if record.get("timestamp") else None,
                    tool_name=str(record["tool_name"]) if record.get("tool_name") else None,
                    header_line=str(record["header_line"]),
                    body_sha256=str(record["body_sha256"]),
                )
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(f"invalid entry at {path}:{line_number}") from error
            if len(entry.body_sha256) != 64:
                raise ValueError(f"invalid body SHA-256 at {path}:{line_number}")
            entries.append(entry)
    sequence_numbers = [entry.seq for entry in entries]
    if sequence_numbers != sorted(set(sequence_numbers)):
        raise ValueError(f"entries have duplicate or unordered sequence numbers: {path}")
    return entries


def object_member(wrapper: str, digest: str) -> str:
    return f"{wrapper}/objects/{digest}.zst"


def iter_session_hashes(zipped: ZipFile, sessions: Iterable[dict]) -> Iterator[str]:
    for session in sessions:
        for entry in read_entries(zipped, str(session["entries_path"])):
            yield entry.body_sha256
