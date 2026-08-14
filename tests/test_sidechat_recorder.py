from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

from brain.sidechat_recorder import install_hook
from collector.sources.codex_source import _recorded_side_chat

SIDE_ID = "019fee4d-f92d-7221-825e-c1c26e8a9ab5"
PARENT_ID = "019fef42-5451-7fe0-8241-43a1bc59a31b"


def fake_codex(path: Path) -> Path:
    path.write_text(
        textwrap.dedent(
            f"""\
            #!{sys.executable}
            import json
            import sys

            if "app-server" not in sys.argv[1:]:
                print("codex-cli test")
                raise SystemExit(0)

            for line in sys.stdin:
                message = json.loads(line)
                if message.get("method") == "thread/fork":
                    print(json.dumps({{
                        "id": message["id"],
                        "result": {{
                            "thread": {{"id": "{SIDE_ID}"}},
                            "cwd": "/workspace/example",
                            "model": "test-model"
                        }}
                    }}), flush=True)
                elif message.get("method") == "turn/start":
                    print(json.dumps({{"id": message["id"], "result": {{"turn": {{"id": "turn-1"}}}}}}), flush=True)
                    print(json.dumps({{
                        "method": "item/completed",
                        "params": {{
                            "threadId": "{SIDE_ID}",
                            "turnId": "turn-1",
                            "item": {{"id": "item-1", "type": "agentMessage", "text": "captured assistant response"}}
                        }}
                    }}), flush=True)
            """
        )
    )
    path.chmod(0o700)
    return path


def test_forwards_protocol_and_records_complete_side_chat(tmp_path: Path) -> None:
    recorder = install_hook(tmp_path / "brain-sidechat-recorder")
    records = tmp_path / "records"
    real_codex = fake_codex(tmp_path / "fake-codex")
    requests = [
        {
            "id": 1,
            "method": "thread/fork",
            "params": {"threadId": PARENT_ID, "ephemeral": True, "excludeTurns": True},
        },
        {
            "id": 2,
            "method": "turn/start",
            "params": {"threadId": SIDE_ID, "input": [{"type": "text", "text": "capture this"}]},
        },
    ]
    process = subprocess.Popen(
        [str(recorder), "app-server"],
        env={
            **os.environ,
            "CODEX_SIDECHAT_REAL_CLI": str(real_codex),
            "CODEX_SIDECHAT_RECORD_DIR": str(records),
        },
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    process.stdin.write(json.dumps(requests[0]).encode() + b"\n")
    process.stdin.flush()
    first_response = process.stdout.readline()
    process.stdin.write(json.dumps(requests[1]).encode() + b"\n")
    process.stdin.flush()
    process.stdin.close()
    remaining_output = process.stdout.read()
    stderr = process.stderr.read() if process.stderr is not None else b""
    assert process.wait() == 0, stderr.decode()

    output = [json.loads(line) for line in (first_response + remaining_output).splitlines()]
    assert [message.get("id") for message in output[:2]] == [1, 2]
    assert output[2]["params"]["item"]["text"] == "captured assistant response"

    path = records / f"{SIDE_ID}.jsonl"
    captured = [json.loads(line) for line in path.read_text().splitlines()]
    assert captured[0]["type"] == "sidechat_meta"
    assert captured[0]["cwd"] == "/workspace/example"
    rpc_messages = [record["message"] for record in captured if record["type"] == "rpc"]
    assert any(message.get("method") == "turn/start" for message in rpc_messages)
    assert any(message.get("method") == "item/completed" for message in rpc_messages)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(recorder.stat().st_mode) == 0o700
    parsed = _recorded_side_chat(path)
    assert parsed is not None
    assert [(message["role"], message["text"]) for message in parsed["messages"]] == [
        ("user", "capture this"),
        ("assistant", "captured assistant response"),
    ]


def test_non_app_server_calls_exec_real_cli(tmp_path: Path) -> None:
    recorder = install_hook(tmp_path / "brain-sidechat-recorder")
    real_codex = fake_codex(tmp_path / "fake-codex")
    completed = subprocess.run(
        [str(recorder), "--version"],
        env={**os.environ, "CODEX_SIDECHAT_REAL_CLI": str(real_codex)},
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == "codex-cli test"
