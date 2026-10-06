from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SESSIONS = ".codex/sessions"


def ssh(host: str, command: str) -> str:
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, command],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise SystemExit(f"[brain-handoff] FAILED ssh {host}: {result.stderr.strip()}")
    return result.stdout.strip()


def find_command(session_id: str) -> str:
    return f"ls ~/{SESSIONS}/*/*/*/rollout-*{session_id}*.jsonl 2>/dev/null | head -1"


def local_session(session_id: str) -> Path | None:
    matches = sorted((Path.home() / SESSIONS).glob(f"*/*/*/rollout-*{session_id}*.jsonl"))
    return matches[0] if matches else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move a Codex chat to or from another machine so it can continue there with codex resume"
    )
    parser.add_argument(
        "direction", choices=("pull", "push"), help="pull copies host to here; push copies here to host"
    )
    parser.add_argument("session_id", help="Codex session UUID or a unique prefix")
    parser.add_argument("host", help="SSH host, such as a tailnet machine name")
    args = parser.parse_args()

    if args.direction == "pull":
        source = ssh(args.host, find_command(args.session_id))
        if not source:
            raise SystemExit(f"[brain-handoff] FAILED no Codex session {args.session_id} on {args.host}")
        relative = source.split(f"/{SESSIONS}/", 1)[1]
        destination = Path.home() / SESSIONS / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy = ["scp", "-q", "-o", "BatchMode=yes", f"{args.host}:{source}", str(destination)]
    else:
        source_path = local_session(args.session_id)
        if source_path is None:
            raise SystemExit(f"[brain-handoff] FAILED no local Codex session {args.session_id}")
        relative = str(source_path.relative_to(Path.home() / SESSIONS))
        ssh(args.host, f"mkdir -p ~/{SESSIONS}/{Path(relative).parent}")
        copy = ["scp", "-q", "-o", "BatchMode=yes", str(source_path), f"{args.host}:{SESSIONS}/{relative}"]
    if subprocess.run(copy, check=False).returncode:
        raise SystemExit(f"[brain-handoff] FAILED copying {relative}")
    where = "here" if args.direction == "pull" else f"on {args.host}"
    print(f"Copied {relative}. Continue {where} with: codex resume {args.session_id}")
    print("Close the chat where it was running first; a chat open on two machines cannot resume.", file=sys.stderr)


if __name__ == "__main__":
    main()
