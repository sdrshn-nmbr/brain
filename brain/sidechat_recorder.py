#!/usr/bin/env python3
from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
import selectors
import signal
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

DEFAULT_CODEX = "/Applications/Codex.app/Contents/Resources/codex"
SCHEMA_VERSION = 1
THREAD_ID_RE = re.compile(r"^[0-9A-Za-z_-]{8,128}$")


def _request_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _contains(value: Any, target: str) -> bool:
    if value == target:
        return True
    if isinstance(value, dict):
        return any(_contains(item, target) for item in value.values())
    if isinstance(value, list):
        return any(_contains(item, target) for item in value)
    return False


def _timestamp() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


class SideChatRecorder:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._enabled = True
        try:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.chmod(self.root, 0o700)
        except OSError:
            self._enabled = False
        self._sequence = 0
        self._side_threads: set[str] = set()
        self._pending_forks: dict[str, tuple[dict[str, Any], bytes, str]] = {}
        self._pending_client_requests: dict[str, str] = {}
        self._pending_server_requests: dict[str, str] = {}

    def observe(self, direction: str, wire: bytes) -> None:
        try:
            message = json.loads(wire)
            if isinstance(message, dict):
                self._observe(direction, message, wire.rstrip(b"\r\n"))
        except Exception as error:
            self._log_error(error)

    def _observe(self, direction: str, message: dict[str, Any], wire: bytes) -> None:
        request_id = message.get("id")
        request_key = _request_key(request_id) if request_id is not None else None

        if direction == "client_to_server":
            params = message.get("params")
            if not isinstance(params, dict):
                params = {}
            if (
                message.get("method") == "thread/fork"
                and request_key is not None
                and params.get("ephemeral") is True
                and params.get("excludeTurns") is True
            ):
                self._pending_forks[request_key] = (message, wire, _timestamp())
                return
            side_id = self._side_id_for(message)
            if side_id is None and request_key is not None:
                side_id = self._pending_server_requests.pop(request_key, None)
            if side_id is None:
                return
            self._append(side_id, direction, message, wire)
            if request_key is not None and message.get("method") is not None:
                self._pending_client_requests[request_key] = side_id
            return

        if request_key is not None and request_key in self._pending_forks:
            fork_message, fork_wire, fork_timestamp = self._pending_forks.pop(request_key)
            result = message.get("result")
            thread = result.get("thread") if isinstance(result, dict) else None
            side_id = thread.get("id") if isinstance(thread, dict) else None
            if isinstance(side_id, str) and THREAD_ID_RE.fullmatch(side_id):
                params = fork_message.get("params") or {}
                self._side_threads.add(side_id)
                self._append_meta(
                    side_id=side_id,
                    parent_thread_id=params.get("threadId"),
                    cwd=(result or {}).get("cwd") or params.get("cwd"),
                    model=(result or {}).get("model"),
                )
                self._append(side_id, "client_to_server", fork_message, fork_wire, captured_at=fork_timestamp)
                self._append(side_id, direction, message, wire)
            return

        side_id = self._side_id_for(message)
        if side_id is None and request_key is not None:
            side_id = self._pending_client_requests.pop(request_key, None)
        if side_id is None:
            return
        self._append(side_id, direction, message, wire)
        if request_key is not None and message.get("method") is not None:
            self._pending_server_requests[request_key] = side_id

    def _side_id_for(self, message: dict[str, Any]) -> str | None:
        return next((side_id for side_id in self._side_threads if _contains(message, side_id)), None)

    def _path(self, side_id: str) -> Path:
        if not THREAD_ID_RE.fullmatch(side_id):
            raise ValueError("invalid side-chat thread id")
        return self.root / f"{side_id}.jsonl"

    def _append_meta(self, *, side_id: str, parent_thread_id: Any, cwd: Any, model: Any) -> None:
        self._write(
            side_id,
            {
                "type": "sidechat_meta",
                "schema_version": SCHEMA_VERSION,
                "captured_at": _timestamp(),
                "thread_id": side_id,
                "parent_thread_id": parent_thread_id,
                "cwd": cwd,
                "model": model,
            },
        )

    def _append(
        self,
        side_id: str,
        direction: str,
        message: dict[str, Any],
        wire: bytes,
        *,
        captured_at: str | None = None,
    ) -> None:
        if not self._should_capture(direction, message):
            return
        self._sequence += 1
        self._write(
            side_id,
            {
                "type": "rpc",
                "schema_version": SCHEMA_VERSION,
                "captured_at": captured_at or _timestamp(),
                "sequence": self._sequence,
                "direction": direction,
                "wire_sha256": hashlib.sha256(wire).hexdigest(),
                "message": message,
            },
        )

    @staticmethod
    def _should_capture(direction: str, message: dict[str, Any]) -> bool:
        if direction == "client_to_server":
            return True
        method = message.get("method")
        if method is None:
            return True
        return (
            method
            in {
                "error",
                "item/completed",
                "item/started",
                "serverRequest/resolved",
                "turn/completed",
                "turn/started",
            }
            or str(method).startswith("thread/")
            or str(method).endswith("/requestApproval")
        )

    def _write(self, side_id: str, record: dict[str, Any]) -> None:
        if not self._enabled:
            return
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        descriptor = os.open(self._path(side_id), os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            _write_all(descriptor, payload)
        finally:
            os.close(descriptor)

    def _log_error(self, error: Exception) -> None:
        try:
            payload = (
                json.dumps(
                    {"captured_at": _timestamp(), "error_type": type(error).__name__, "error": str(error)},
                    separators=(",", ":"),
                ).encode()
                + b"\n"
            )
            descriptor = os.open(self.root / "recorder-errors.jsonl", os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                _write_all(descriptor, payload)
            finally:
                os.close(descriptor)
        except Exception:
            pass


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written == 0:
            raise BrokenPipeError("zero-byte pipe write")
        view = view[written:]


def _proxy(child: subprocess.Popen[bytes], recorder: SideChatRecorder) -> None:
    if child.stdin is None or child.stdout is None:
        raise RuntimeError("app-server pipes are unavailable")
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin.buffer, selectors.EVENT_READ, (child.stdin.fileno(), "client_to_server"))
    selector.register(child.stdout, selectors.EVENT_READ, (sys.stdout.buffer.fileno(), "server_to_client"))
    buffers = {"client_to_server": b"", "server_to_client": b""}
    stdout_open = True
    while stdout_open:
        for key, _mask in selector.select():
            destination, direction = key.data
            try:
                chunk = os.read(key.fd, 65536)
            except OSError:
                chunk = b""
            if not chunk:
                selector.unregister(key.fileobj)
                if direction == "client_to_server":
                    child.stdin.close()
                else:
                    stdout_open = False
                continue
            try:
                _write_all(destination, chunk)
            except (BrokenPipeError, OSError):
                if direction == "client_to_server":
                    selector.unregister(key.fileobj)
                else:
                    child.terminate()
                    stdout_open = False
                continue
            pending = buffers[direction] + chunk
            lines = pending.split(b"\n")
            buffers[direction] = lines.pop()
            for line in lines:
                recorder.observe(direction, line)
    selector.close()


def install_hook(target: Path | None = None) -> Path:
    destination = target or Path.home() / ".codex" / "bin" / "brain-sidechat-recorder"
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    temporary.write_bytes(Path(__file__).read_bytes())
    temporary.chmod(0o700)
    temporary.replace(destination)
    return destination


def main() -> int:
    arguments = sys.argv[1:]
    if arguments == ["--install-hook"]:
        destination = install_hook()
        print(f"Installed {destination}")
        print("Start Codex Desktop with:")
        print(f'export CODEX_CLI_PATH="{destination}"')
        return 0

    real_codex = os.environ.get("CODEX_SIDECHAT_REAL_CLI", DEFAULT_CODEX)
    if os.path.realpath(real_codex) == os.path.realpath(sys.argv[0]):
        print("side-chat recorder points to itself", file=sys.stderr)
        return 126
    if "app-server" not in arguments or os.environ.get("CODEX_SIDECHAT_DISABLE") == "1":
        os.execv(real_codex, [real_codex, *arguments])

    child_environment = os.environ.copy()
    child_environment.pop("CODEX_CLI_PATH", None)
    child_environment.pop("CODEX_SIDECHAT_REAL_CLI", None)
    record_root = Path(
        os.environ.get("CODEX_SIDECHAT_RECORD_DIR", str(Path.home() / ".codex" / "attachments" / "sidechats"))
    )
    recorder = SideChatRecorder(record_root)
    child = subprocess.Popen(
        [real_codex, *arguments],
        env=child_environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=None,
        bufsize=0,
    )

    def forward_signal(signum: int, _frame: Any) -> None:
        with suppress(ProcessLookupError):
            child.send_signal(signum)

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, forward_signal)
    _proxy(child, recorder)
    return child.wait()


if __name__ == "__main__":
    raise SystemExit(main())
