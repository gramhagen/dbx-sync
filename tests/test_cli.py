from __future__ import annotations

from pathlib import Path
from typing import Any

import dbx_sync.cli as cli


def test_main_uses_explicit_flags(monkeypatch: Any, tmp_path: Path) -> None:
    calls: dict[str, Any] = {}

    def fake_run_sync(**kwargs: Any) -> int:
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    exit_code = cli.main(
        [
            str(tmp_path),
            "/Shared/example",
            "--profile",
            "WORKSPACE",
            "--poll-interval",
            "5",
            "--log-level",
            "DEBUG",
            "--dry-run",
            "--watch",
            "--force",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "local_dir": tmp_path.resolve(),
        "profile": "WORKSPACE",
        "remote_path": "/Shared/example",
        "poll_interval_seconds": 5,
        "log_level": "DEBUG",
        "dry_run": True,
        "watch": True,
        "force": True,
    }


def test_main_uses_defaults_for_optional_flags(monkeypatch: Any, tmp_path: Path) -> None:
    calls: dict[str, Any] = {}

    def fake_run_sync(**kwargs: Any) -> int:
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    exit_code = cli.main([str(tmp_path), "/Users/demo"])

    assert exit_code == 0
    assert calls == {
        "local_dir": tmp_path.resolve(),
        "profile": "DEFAULT",
        "remote_path": "/Users/demo",
        "poll_interval_seconds": 1,
        "log_level": "INFO",
        "dry_run": False,
        "watch": False,
        "force": False,
    }


def test_main_requires_local_and_workspace_arguments(monkeypatch: Any) -> None:
    def fake_run_sync(**kwargs: Any) -> int:
        del kwargs
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    try:
        cli.main([])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected argparse to require positional arguments")


def test_main_enforces_min_poll_interval(monkeypatch: Any, tmp_path: Path) -> None:
    calls: dict[str, Any] = {}

    def fake_run_sync(**kwargs: Any) -> int:
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    exit_code = cli.main([str(tmp_path), "/Users/demo", "--poll-interval", "0"])

    assert exit_code == 0
    assert calls["poll_interval_seconds"] == 1


def test_main_handles_invalid_refresh_token_error(
    monkeypatch: Any, tmp_path: Path, capsys: Any
) -> None:
    def fake_run_sync(**kwargs: Any) -> int:
        del kwargs
        raise RuntimeError(
            "Error: A new access token could not be retrieved because the refresh token is invalid."
        )

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    exit_code = cli.main([str(tmp_path), "/Users/demo"])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert output == (
        "Databricks authentication failed for profile 'DEFAULT'.\n"
        "Reauthenticate with: databricks auth login --profile DEFAULT\n"
    )


def test_main_handles_generic_runtime_error(monkeypatch: Any, tmp_path: Path) -> None:
    def fake_run_sync(**kwargs: Any) -> int:
        del kwargs
        raise RuntimeError("Command failed (1): databricks workspace get-status")

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    try:
        cli.main([str(tmp_path), "/Users/demo"])
    except RuntimeError as exc:
        assert str(exc) == "Command failed (1): databricks workspace get-status"
    else:
        raise AssertionError("expected RuntimeError to be re-raised for non-reauth errors")
