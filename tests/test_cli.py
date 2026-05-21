from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import dbx_sync.cli as cli
from dbx_sync.sync import ForceType


def test_main_supports_version_flag(capsys: Any) -> None:
    try:
        cli.main(["--version"])
    except SystemExit as exc:
        assert exc.code == 0
    else:
        raise AssertionError("expected argparse version action to exit")

    output = capsys.readouterr().out
    assert output == f"dbx-sync {cli.__version__}\n"


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
            "--force-download",
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
        "force_type": ForceType.DOWNLOAD,
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
        "force_type": None,
    }


def test_main_single_force_flag_no_warning(monkeypatch: Any, tmp_path: Path) -> None:
    calls: dict[str, Any] = {}

    def fake_run_sync(**kwargs: Any) -> int:
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    for flag, expected_type in [
        ("--force", ForceType.CLEAR),
        ("--force-upload", ForceType.UPLOAD),
        ("--force-download", ForceType.DOWNLOAD),
    ]:
        calls.clear()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cli.main([str(tmp_path), "/Users/demo", flag])

        assert len(caught) == 0, f"unexpected warning for {flag}: {caught}"
        assert calls["force_type"] == expected_type


def test_main_multiple_force_flags_are_rejected(monkeypatch: Any, tmp_path: Path) -> None:
    def fake_run_sync(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    try:
        cli.main([str(tmp_path), "/Users/demo", "--force", "--force-upload"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected argparse to reject multiple force flags")


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
    def fake_run_sync(**kwargs: Any) -> int:
        del kwargs
        return 0

    monkeypatch.setattr(cli, "run_sync", fake_run_sync)

    try:
        cli.main([str(tmp_path), "/Users/demo", "--poll-interval", "0"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected argparse to reject a non-positive poll interval")


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
