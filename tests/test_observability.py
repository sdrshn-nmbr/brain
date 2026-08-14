from __future__ import annotations

from pathlib import Path

from brain.observability import RequestLog, observable_arguments


def event(index: int, *, actor: str = "alice@example.com", name: str = "search", status: int = 200) -> dict:
    return {
        "requestId": f"request-{index}",
        "startedAt": f"2026-08-11T00:00:{index:02d}+00:00",
        "completedAt": f"2026-08-11T00:00:{index:02d}+00:00",
        "actor": actor,
        "identityKind": "user",
        "accessLevel": "append",
        "path": "/mcp",
        "httpMethod": "POST",
        "mcpMethod": "tools/call",
        "mcpName": name,
        "arguments": {"query": "structured request"},
        "client": {"info": {"name": "probe", "version": "1"}},
        "statusCode": status,
        "durationMs": float(index + 1),
        "requestBytes": 100,
        "responseBytes": 200,
        "userAgent": "probe/1",
        "errorCode": None if status < 400 else "request_failed",
    }


def test_records_filters_paginates_and_summarizes_requests(tmp_path: Path) -> None:
    log = RequestLog(tmp_path, 100)
    try:
        log.record(event(1))
        log.record(event(2, actor="workload:cursor-cloud", name="stats"))
        log.record(event(3, status=403))

        records = log.list_records(limit=2)
        assert [record["requestId"] for record in records] == ["request-3", "request-2"]
        assert records[0]["arguments"] == {"query": "structured request"}
        assert log.list_records(limit=10, before_id=records[-1]["id"])[0]["requestId"] == "request-1"
        assert [record["requestId"] for record in log.list_records(limit=10, actor="workload:cursor-cloud")] == [
            "request-2"
        ]

        stats = log.stats()
        assert stats["requestCount"] == 3
        assert stats["byActor"] == {"alice@example.com": 2, "workload:cursor-cloud": 1}
        assert stats["byIdentityKind"] == {"user": 3}
        assert stats["byStatus"] == {"200": 2, "403": 1}
        assert stats["byError"] == {"unknown": 2, "request_failed": 1}
        assert stats["latencyMs"] == {"p50": 3.0, "p95": 3.0, "max": 4.0}
    finally:
        log.close()


def test_summarizes_large_cas_negotiation_arguments() -> None:
    assert observable_arguments("plan_upload", {"sessionFingerprints": ["a" * 64, "b" * 64]}) == {
        "sessionFingerprintCount": 2
    }
    assert observable_arguments("missing_blobs", {"blobHashes": ["a" * 64]}) == {"blobHashCount": 1}
    assert observable_arguments("search", {"query": "index compaction"}) == {
        "queryChars": 16,
        "querySha256": "6e2c0e93a4ac69bd9b0ebd49d10004483eb4bb955deb7021a00e6987ec19371e",
    }


def test_never_records_raw_unknown_or_oversized_arguments(tmp_path: Path) -> None:
    assert observable_arguments("unknown", {"api_key": "secret", "payload": "private"}) == {
        "argumentNames": ["api_key", "payload"]
    }
    log = RequestLog(tmp_path, 100)
    try:
        oversized = event(1)
        oversized["arguments"] = {"api_key": "secret" * 1_000_000}
        log.record(oversized)
        arguments = log.list_records(limit=1)[0]["arguments"]
        assert arguments["omitted"] is True
        assert arguments["jsonBytes"] > 5_000_000
        assert "secret" not in str(arguments)
    finally:
        log.close()
