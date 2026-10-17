from __future__ import annotations

import io
import json
import os
import stat
from argparse import Namespace
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pytest
from rich.console import Console

from collector.export import export_history
from collector.sources import amp_source

ORB_THREAD: dict[str, Any] = {
    "id": "T-orb",
    "created": 1792017240808,
    "updatedAt": "2026-10-15T03:31:11.738Z",
    "env": {
        "initial": {
            "workingDirectory": "file:///home/user/workspace/repo",
            "trees": [
                {
                    "uri": "file:///home/user/workspace/repo",
                    "repository": {"url": "https://github.com/Action-Intelligence/labs", "ref": "refs/heads/main"},
                }
            ],
        }
    },
    "messages": [
        {"role": "user", "createdAt": "2026-10-14T22:34:02Z", "content": [{"type": "text", "text": "check routing"}]},
        {
            "role": "assistant",
            "createdAt": "2026-10-14T22:34:05Z",
            "usage": {"model": "gpt-6-astra"},
            "content": [
                {"type": "thinking", "thinking": "look at placement"},
                {"type": "tool_use", "id": "tool-1", "name": "Bash", "input": {"cmd": "rg PlacementBook"}},
            ],
        },
        {
            "role": "user",
            "createdAt": "2026-10-14T22:34:07Z",
            "content": [{"type": "tool_result", "toolUseID": "tool-1", "run": {"status": "done", "result": "found"}}],
        },
        {"role": "assistant", "createdAt": "2026-10-14T22:35:00Z", "content": [{"type": "text", "text": "fixed"}]},
    ],
}


def install_fake_amp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, script: str) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    amp = bin_dir / "amp"
    amp.write_text("#!/usr/bin/env python3\nimport json, sys\n" + script)
    amp.chmod(amp.stat().st_mode | stat.S_IEXEC)
    accounts = tmp_path / "home" / ".local" / "share" / "amp" / "accounts.json"
    accounts.parent.mkdir(parents=True)
    accounts.write_text("{}")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setattr(amp_source.Path, "home", lambda: tmp_path / "home")


def export_args(output: Path, **overrides) -> Namespace:
    return Namespace(
        **{
            "source": "all",
            "exclude_subagents": False,
            "exclude_archived": False,
            "since": None,
            "until": None,
            "repository": [],
            "every_folder": True,
            "project": None,
            "output": str(output),
            "workers": 2,
            **overrides,
        }
    )


def manifest_of(output: Path) -> dict:
    with ZipFile(output) as archive:
        return json.loads(archive.read(next(name for name in archive.namelist() if name.endswith("_manifest.json"))))


def test_orb_thread_exports_under_its_git_remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    thread = json.dumps(ORB_THREAD)
    install_fake_amp(
        tmp_path,
        monkeypatch,
        "if sys.argv[1:3] == ['threads', 'list']:\n"
        "    print(json.dumps([{'id': 'T-orb', 'updated': '2026-10-15T03:31:11.738Z'}]))\n"
        f"elif sys.argv[1:4] == ['threads', 'export', 'T-orb']:\n    print({thread!r})\n",
    )
    output = tmp_path / "export.zip"

    export_history.run_export(export_args(output, source="amp"), Console(file=io.StringIO()))

    session = manifest_of(output)["sessions"][0]
    assert (session["source"], session["uuid"]) == ("amp", "T-orb")
    assert session["repository"] == "github.com/action-intelligence/labs"
    assert (session["cwd"], session["git_branch"], session["model"]) == (
        "/home/user/workspace/repo",
        "main",
        "gpt-6-astra",
    )
    assert session["roles"] == ["assistant", "thinking", "tool_call", "tool_result", "user"]
    entries = amp_source.thread_entries(ORB_THREAD["messages"])
    assert [(entry.role, entry.tool_name, entry.text) for entry in entries if entry.role == "tool_result"] == [
        ("tool_result", "Bash", "found")
    ]


def test_all_sources_means_local_stores_without_amp(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_amp(tmp_path, monkeypatch, "raise SystemExit('amp must not run')\n")
    for source in ("claude", "codex", "cursor"):
        monkeypatch.setattr(getattr(export_history, f"{source}_source"), "store_exists", lambda: False)
    output = tmp_path / "export.zip"

    export_history.run_export(export_args(output), Console(file=io.StringIO()))

    assert "amp" not in manifest_of(output)["totals"]


def test_signed_out_amp_cli_fails_loudly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_amp(
        tmp_path, monkeypatch, "print('Error: Invalid or missing API key. Run amp login to authenticate.')\n"
    )

    with pytest.raises(amp_source.AmpCLIError, match="Invalid or missing API key"):
        amp_source.discover_export_targets()
