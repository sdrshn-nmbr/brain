from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable

FINAL_SYNC = """set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/$(id -u)
if [ "$1" = delete ] && pgrep -u "$(id -u)" -f '(^|/)(codex|claude|cursor-agent)( |$)' >/dev/null; then
  echo '[brain-vm] FAILED active agent: close it before deleting this VM' >&2
  exit 1
fi
systemctl --user stop brain-sync.timer
trap 'systemctl --user start brain-sync.timer' EXIT
systemctl --user stop brain-sync.service
/usr/local/bin/brain-final-sync
"""


def operate(action: str, machine: str, run: Callable = subprocess.run) -> None:
    command = ["npx", "-y", "freestyle@latest"]
    result = run([*command, "--output", "json", "vm", "get", machine], check=True, capture_output=True, text=True)
    data = json.loads(result.stdout)
    state = data["state"]
    vm_id = data["id"]
    if state not in {"running", "paused", "stopped"}:
        raise RuntimeError(f"VM {machine} is {state}; wait for its lifecycle transition before retrying")
    if state != "running":
        print(f"[brain-vm] starting {machine} to upload its last chats", file=sys.stderr)
        run([*command, "vm", "start", vm_id], check=True)
    print(f"[brain-vm] final upload from {machine}; {action} requires success", file=sys.stderr)
    run(
        [*command, "vm", "exec", vm_id, "--timeout-ms", "900000", "--", "bash", "-lc", FINAL_SYNC, "brain-vm", action],
        check=True,
    )
    run([*command, "vm", action, vm_id], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload saved VM chats before pausing or deleting a Freestyle VM")
    parser.add_argument("action", choices=("pause", "delete"))
    parser.add_argument("vm", help="Freestyle VM slug or ID")
    args = parser.parse_args()
    try:
        operate(args.action, args.vm)
    except (subprocess.CalledProcessError, RuntimeError) as error:
        raise SystemExit(f"[brain-vm] FAILED {args.action} cancelled; VM retained: {error}") from error


if __name__ == "__main__":
    main()
