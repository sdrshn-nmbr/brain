"""Exercise lifecycle ordering at the process boundary; guest/systemd behavior needs a live VM."""

from __future__ import annotations

import json
import subprocess

import pytest

from collector import vm


class FreestyleProcess:
    def __init__(self, state: str, fail_sync: bool = False):
        self.state = state
        self.fail_sync = fail_sync
        self.synced = False

    def __call__(self, command, **kwargs):
        if "get" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"id": "vm-test", "state": self.state}))
        if "start" in command:
            self.state = "running"
        elif "exec" in command:
            if int(command[command.index("--timeout-ms") + 1]) > 300_000:
                raise subprocess.CalledProcessError(1, command)
            if self.fail_sync:
                raise subprocess.CalledProcessError(1, command)
            self.synced = True
        elif "pause" in command or "delete" in command:
            assert self.synced
            self.state = "paused" if "pause" in command else "deleted"
        return subprocess.CompletedProcess(command, 0, "")


@pytest.mark.parametrize("state", ["running", "paused", "stopped"])
@pytest.mark.parametrize("action", ["pause", "delete"])
@pytest.mark.parametrize("fail_sync", [False, True])
def test_sync_failure_never_pauses_or_deletes(state, action, fail_sync) -> None:
    provider = FreestyleProcess(state, fail_sync)
    if fail_sync:
        with pytest.raises(subprocess.CalledProcessError):
            vm.operate(action, "test-vm", provider)
        assert provider.state == "running"
    else:
        vm.operate(action, "test-vm", provider)
        assert provider.state == ("paused" if action == "pause" else "deleted")


def test_unknown_transition_does_not_mutate_vm() -> None:
    provider = FreestyleProcess("pausing")
    with pytest.raises(RuntimeError, match="pausing"):
        vm.operate("delete", "test-vm", provider)
    assert provider.state == "pausing"
