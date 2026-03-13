from __future__ import annotations

import json
import runpy
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import dbx_sync.sync as sync


def test_language_extensions_cover_databricks_notebook_languages() -> None:
    assert sync.LANGUAGE_EXTENSIONS == {
        "PYTHON": ".py",
        "R": ".r",
        "SCALA": ".scala",
        "SQL": ".sql",
        "JUPYTER": ".ipynb",
    }

    assert sync.EXTENSION_LANGUAGES == {
        ".py": "PYTHON",
        ".r": "R",
        ".scala": "SCALA",
        ".sql": "SQL",
        ".ipynb": "JUPYTER",
    }


def test_iso_from_ms_none() -> None:
    assert sync.iso_from_ms(None) == "missing"


def test_iso_from_ms_epoch() -> None:
    assert "19" in sync.iso_from_ms(0)


def test_configure_logging_does_not_raise() -> None:
    sync.configure_logging("DEBUG")
    sync.configure_logging("INFO")
    sync.configure_logging("WARNING")


def test_config_path_for_builds_expected_path(tmp_path: Path) -> None:
    assert sync.config_path_for(tmp_path) == tmp_path / ".databricks" / "dbx-sync" / "config.json"


def test_load_saved_config_missing_returns_none(tmp_path: Path) -> None:
    assert sync.load_saved_config(tmp_path / "missing.json") is None


def test_load_saved_config_reads_json_object(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"profile": "DEFAULT"}), encoding="utf-8")

    assert sync.load_saved_config(config_path) == {"profile": "DEFAULT"}


def test_load_saved_config_rejects_non_object(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("[]", encoding="utf-8")

    try:
        sync.load_saved_config(config_path)
    except RuntimeError as exc:
        assert "JSON object" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for non-object config")


@patch("dbx_sync.sync.subprocess.run")
def test_run_cli_success(mock_run: MagicMock) -> None:
    mock_run.return_value = MagicMock(returncode=0, stdout="hello\n", stderr="")

    result = sync.run_cli(["echo", "hello"])

    assert result == "hello\n"
    mock_run.assert_called_once_with(
        ["echo", "hello"],
        capture_output=True,
        text=True,
        timeout=sync.DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    )


@patch("dbx_sync.sync.subprocess.run")
def test_run_cli_failure_raises(mock_run: MagicMock) -> None:
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="bad command")

    try:
        sync.run_cli(["bad"])
    except RuntimeError as exc:
        assert "Command failed" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for failed subprocess")


@patch("dbx_sync.sync.run_cli")
def test_list_workspace_parses_supported_items(mock_run_cli: MagicMock) -> None:
    mock_run_cli.return_value = json.dumps(
        [
            {
                "path": "/repos/nb1",
                "object_type": "NOTEBOOK",
                "language": "PYTHON",
                "modified_at": 1000,
            },
            {
                "path": "/repos/data.csv",
                "object_type": "FILE",
                "modified_at": 2000,
            },
            {"path": "/repos/dir", "object_type": "DIRECTORY"},
        ]
    )

    items = sync.list_workspace("/repos", "DEFAULT", None)

    assert [(item.path, item.object_type) for item in items] == [
        ("/repos/nb1", "NOTEBOOK"),
        ("/repos/data.csv", "FILE"),
    ]


@patch("dbx_sync.sync.run_cli")
def test_list_workspace_includes_modified_after_flag(mock_run_cli: MagicMock) -> None:
    mock_run_cli.return_value = "[]"

    sync.list_workspace("/repos", "DEFAULT", 5000)

    cmd = mock_run_cli.call_args[0][0]
    assert "--notebooks-modified-after" in cmd
    assert "5000" in cmd


@patch("dbx_sync.sync.run_cli")
def test_list_workspace_bad_json_raises(mock_run_cli: MagicMock) -> None:
    mock_run_cli.return_value = "not json"

    try:
        sync.list_workspace("/repos", "DEFAULT", None)
    except RuntimeError as exc:
        assert "Failed to parse JSON" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for invalid JSON")


@patch("dbx_sync.sync.run_cli")
def test_get_status_success(mock_run_cli: MagicMock) -> None:
    mock_run_cli.return_value = json.dumps({"object_type": "NOTEBOOK", "language": "SQL"})

    result = sync.get_status("/repos/nb", "DEFAULT")

    assert result == {"object_type": "NOTEBOOK", "language": "SQL"}


@patch("dbx_sync.sync.run_cli")
def test_get_status_missing_returns_none(mock_run_cli: MagicMock) -> None:
    mock_run_cli.side_effect = RuntimeError("RESOURCE_DOES_NOT_EXIST")

    assert sync.get_status("/repos/missing", "DEFAULT") is None


@patch("dbx_sync.sync.run_cli")
def test_get_status_other_error_reraises(mock_run_cli: MagicMock) -> None:
    mock_run_cli.side_effect = RuntimeError("something else went wrong")

    try:
        sync.get_status("/repos/missing", "DEFAULT")
    except RuntimeError as exc:
        assert "something else" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for non-missing error")


@patch("dbx_sync.sync.run_cli")
def test_download_workspace_item_notebook_source(mock_run_cli: MagicMock, tmp_path: Path) -> None:
    item = sync.WorkspaceItem("/repos/nb", "NOTEBOOK", "PYTHON", 1000)

    sync.download_workspace_item(item, tmp_path / "nb.py", "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "SOURCE" in cmd


@patch("dbx_sync.sync.run_cli")
def test_download_workspace_item_file_uses_auto(mock_run_cli: MagicMock, tmp_path: Path) -> None:
    item = sync.WorkspaceItem("/repos/data.csv", "FILE", None, 1000)

    sync.download_workspace_item(item, tmp_path / "data.csv", "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "AUTO" in cmd


@patch("dbx_sync.sync.run_cli")
def test_download_workspace_item_jupyter_uses_jupyter_format(
    mock_run_cli: MagicMock, tmp_path: Path
) -> None:
    item = sync.WorkspaceItem("/repos/nb", "NOTEBOOK", "JUPYTER", 1000)

    sync.download_workspace_item(item, tmp_path / "nb.ipynb", "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "JUPYTER" in cmd


@patch("dbx_sync.sync.run_cli")
def test_upload_workspace_item_adds_language_from_extension(
    mock_run_cli: MagicMock, tmp_path: Path
) -> None:
    local_path = tmp_path / "nb.sql"
    local_path.write_text("SELECT 1", encoding="utf-8")
    item = sync.WorkspaceItem("/repos/nb", "NOTEBOOK", None, 1000)

    sync.upload_workspace_item(item, local_path, "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "SOURCE" in cmd
    assert "--language" in cmd
    assert "SQL" in cmd


@patch("dbx_sync.sync.run_cli")
def test_upload_workspace_item_jupyter_uses_jupyter_format(
    mock_run_cli: MagicMock, tmp_path: Path
) -> None:
    local_path = tmp_path / "nb.ipynb"
    local_path.write_text("{}", encoding="utf-8")
    item = sync.WorkspaceItem("/repos/nb", "NOTEBOOK", "JUPYTER", 1000)

    sync.upload_workspace_item(item, local_path, "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "JUPYTER" in cmd
    assert "--language" not in cmd


@patch("dbx_sync.sync.run_cli")
def test_upload_workspace_item_file_uses_auto(mock_run_cli: MagicMock, tmp_path: Path) -> None:
    local_path = tmp_path / "data.csv"
    local_path.write_text("a,b\n1,2", encoding="utf-8")
    item = sync.WorkspaceItem("/repos/data.csv", "FILE", None, 1000)

    sync.upload_workspace_item(item, local_path, "DEFAULT")

    cmd = mock_run_cli.call_args[0][0]
    assert "AUTO" in cmd


def test_tracked_local_files_filters_to_supported_extensions(tmp_path: Path) -> None:
    (tmp_path / "notebook.py").write_text("# python", encoding="utf-8")
    (tmp_path / "query.sql").write_text("SELECT 1", encoding="utf-8")
    (tmp_path / "script.scala").write_text("println(1)", encoding="utf-8")
    (tmp_path / "readme.md").write_text("# readme", encoding="utf-8")
    (tmp_path / "data.csv").write_text("a,b", encoding="utf-8")
    (tmp_path / "nb.ipynb").write_text("{}", encoding="utf-8")
    (tmp_path / "subdir").mkdir()

    result = sync.tracked_local_files(tmp_path)

    assert sorted(file_path.name for file_path in result) == [
        "nb.ipynb",
        "notebook.py",
        "query.sql",
        "script.scala",
    ]


def test_tracked_local_files_missing_directory_returns_empty(tmp_path: Path) -> None:
    assert sync.tracked_local_files(tmp_path / "missing") == []


def test_default_file_state_has_all_expected_keys() -> None:
    state = sync._default_file_state()

    assert state == {
        "local_path": None,
        "object_type": None,
        "language": None,
        "last_synced_remote_modified_ms": None,
        "last_synced_local_modified_ms": None,
        "last_action": None,
    }


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_new_local_only(mock_get_status: MagicMock, tmp_path: Path) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# new", encoding="utf-8")
    state = sync._default_file_state()
    state["local_path"] = str(local_file)
    state["object_type"] = "NOTEBOOK"
    state["language"] = "PYTHON"
    files_state = {"/workspace/nb": state}

    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb", files_state, {}, "/workspace", tmp_path, "DEFAULT"
    )

    assert action == "upload"
    mock_get_status.assert_called_once()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_new_remote_only(mock_get_status: MagicMock, tmp_path: Path) -> None:
    remote_item = sync.WorkspaceItem("/workspace/nb", "NOTEBOOK", "PYTHON", 5000)

    action, _, item, path = sync._resolve_file_action(
        "/workspace/nb",
        {},
        {"/workspace/nb": remote_item},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "download"
    assert item.path == "/workspace/nb"
    assert path == tmp_path / "nb.py"
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_both_gone(mock_get_status: MagicMock, tmp_path: Path) -> None:
    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb",
        {"/workspace/nb": sync._default_file_state()},
        {},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "remove"
    mock_get_status.assert_called_once()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_skip_when_unchanged(
    mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# existing", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    remote_item = sync.WorkspaceItem("/workspace/nb", "NOTEBOOK", "PYTHON", 5000)
    files_state = {
        "/workspace/nb": {
            **sync._default_file_state(),
            "local_path": str(local_file),
            "last_synced_remote_modified_ms": 5000,
            "last_synced_local_modified_ms": local_mtime_ms,
        }
    }

    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb",
        files_state,
        {"/workspace/nb": remote_item},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "skip"
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status")
def test_resolve_file_action_uses_cached_remote_state_when_not_listed(
    mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# existing", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    files_state = {
        "/workspace/nb": {
            **sync._default_file_state(),
            "local_path": str(local_file),
            "object_type": "NOTEBOOK",
            "language": "PYTHON",
            "last_synced_remote_modified_ms": 5000,
            "last_synced_local_modified_ms": local_mtime_ms,
        }
    }

    action, _, item, _ = sync._resolve_file_action(
        "/workspace/nb",
        files_state,
        {},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "skip"
    assert item.modified_at == 5000
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_conflict_when_both_changed(
    mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# changed", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    remote_item = sync.WorkspaceItem("/workspace/nb", "NOTEBOOK", "PYTHON", local_mtime_ms + 2000)
    files_state = {
        "/workspace/nb": {
            **sync._default_file_state(),
            "local_path": str(local_file),
            "last_synced_remote_modified_ms": local_mtime_ms - 2000,
            "last_synced_local_modified_ms": local_mtime_ms - 1000,
        }
    }

    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb",
        files_state,
        {"/workspace/nb": remote_item},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "conflict"
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_local_change_prefers_upload(
    mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# updated locally", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    remote_item = sync.WorkspaceItem("/workspace/nb", "NOTEBOOK", "PYTHON", local_mtime_ms)
    files_state = {
        "/workspace/nb": {
            **sync._default_file_state(),
            "local_path": str(local_file),
            "last_synced_remote_modified_ms": local_mtime_ms,
            "last_synced_local_modified_ms": local_mtime_ms - 1000,
        }
    }

    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb",
        files_state,
        {"/workspace/nb": remote_item},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "upload"
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
def test_resolve_file_action_remote_change_prefers_download(
    mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "nb.py"
    local_file.write_text("# unchanged locally", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    remote_item = sync.WorkspaceItem("/workspace/nb", "NOTEBOOK", "PYTHON", local_mtime_ms + 2000)
    files_state = {
        "/workspace/nb": {
            **sync._default_file_state(),
            "local_path": str(local_file),
            "last_synced_remote_modified_ms": local_mtime_ms,
            "last_synced_local_modified_ms": local_mtime_ms,
        }
    }

    action, _, _, _ = sync._resolve_file_action(
        "/workspace/nb",
        files_state,
        {"/workspace/nb": remote_item},
        "/workspace",
        tmp_path,
        "DEFAULT",
    )

    assert action == "download"
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_empty(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    result = sync.run_sync_pass(config, config_path, dry_run=True)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 0,
        "removed": 0,
        "skipped": 0,
    }
    mock_list_workspace.assert_called_once()
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_local_file_dry_run_uploads(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    (tmp_path / "test.py").write_text("# test", encoding="utf-8")
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    result = sync.run_sync_pass(config, config_path, dry_run=True)

    assert result["uploaded"] == 1
    mock_list_workspace.assert_called_once()
    mock_get_status.assert_called_once()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_first_sync_does_not_filter_remote_listing(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    (tmp_path / "test.py").write_text("# test", encoding="utf-8")
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    sync.run_sync_pass(config, config_path, dry_run=True)

    assert mock_list_workspace.call_args.args == ("/workspace", "DEFAULT", None)
    mock_get_status.assert_called_once()


@patch("dbx_sync.sync.upload_workspace_item")
@patch("dbx_sync.sync.get_status", return_value={"object_type": "NOTEBOOK", "modified_at": 9000})
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_upload_persists_state(
    mock_list_workspace: MagicMock,
    mock_get_status: MagicMock,
    mock_upload: MagicMock,
    tmp_path: Path,
) -> None:
    (tmp_path / "test.py").write_text("# upload me", encoding="utf-8")
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    result = sync.run_sync_pass(config, config_path, dry_run=False)

    assert result["uploaded"] == 1
    assert config_path.exists()
    mock_upload.assert_called_once()
    mock_list_workspace.assert_called_once()
    assert mock_get_status.call_count >= 1


@patch("dbx_sync.sync.download_workspace_item")
@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace")
def test_run_sync_pass_downloads_remote_file(
    mock_list_workspace: MagicMock,
    mock_get_status: MagicMock,
    mock_download: MagicMock,
    tmp_path: Path,
) -> None:
    remote_item = sync.WorkspaceItem("/workspace/notebook", "NOTEBOOK", "PYTHON", 5000)
    mock_list_workspace.return_value = [remote_item]
    mock_download.side_effect = lambda item, path, profile: path.write_text(
        "# downloaded", encoding="utf-8"
    )
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    result = sync.run_sync_pass(config, config_path, dry_run=False)

    assert result["downloaded"] == 1
    mock_download.assert_called_once()
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_dry_run_skip_counts_skipped(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "test.py"
    local_file.write_text("# synced", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {
            "/workspace/test": {
                **sync._default_file_state(),
                "local_path": str(local_file),
                "object_type": "NOTEBOOK",
                "language": "PYTHON",
                "last_synced_remote_modified_ms": local_mtime_ms,
                "last_synced_local_modified_ms": local_mtime_ms,
            }
        },
    }

    result = sync.run_sync_pass(config, config_path, dry_run=True)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 0,
        "removed": 0,
        "skipped": 1,
    }
    assert config["files"]["/workspace/test"]["last_action"] == "skip"
    mock_list_workspace.assert_called_once()
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_removes_stale_entries(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {"/workspace/missing": sync._default_file_state()},
    }

    result = sync.run_sync_pass(config, config_path, dry_run=False)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 0,
        "removed": 1,
        "skipped": 0,
    }
    assert "/workspace/missing" not in config["files"]
    mock_list_workspace.assert_called_once()
    mock_get_status.assert_called_once_with("/workspace/missing", "DEFAULT")


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace")
def test_run_sync_pass_persists_conflict_state(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "test.py"
    local_file.write_text("# changed locally", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    mock_list_workspace.return_value = [
        sync.WorkspaceItem("/workspace/test", "NOTEBOOK", "PYTHON", local_mtime_ms + 2000)
    ]
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {
            "/workspace/test": {
                **sync._default_file_state(),
                "local_path": str(local_file),
                "object_type": "NOTEBOOK",
                "language": "PYTHON",
                "last_synced_remote_modified_ms": local_mtime_ms - 2000,
                "last_synced_local_modified_ms": local_mtime_ms - 1000,
            }
        },
    }

    result = sync.run_sync_pass(config, config_path, dry_run=False)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 1,
        "removed": 0,
        "skipped": 0,
    }
    assert config["files"]["/workspace/test"]["last_action"] == "conflict"
    mock_get_status.assert_not_called()
    mock_list_workspace.assert_called_once()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.list_workspace", return_value=[])
def test_run_sync_pass_persists_skip_state(
    mock_list_workspace: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    local_file = tmp_path / "test.py"
    local_file.write_text("# unchanged", encoding="utf-8")
    local_mtime_ms = int(local_file.stat().st_mtime * 1000)
    config_path = tmp_path / ".databricks" / "dbx-sync" / "config.json"
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {
            "/workspace/test": {
                **sync._default_file_state(),
                "local_path": str(local_file),
                "object_type": "NOTEBOOK",
                "language": "PYTHON",
                "last_synced_remote_modified_ms": local_mtime_ms,
                "last_synced_local_modified_ms": local_mtime_ms,
            }
        },
    }

    result = sync.run_sync_pass(config, config_path, dry_run=False)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 0,
        "removed": 0,
        "skipped": 1,
    }
    file_state = config["files"]["/workspace/test"]
    assert file_state["last_action"] == "skip"
    assert file_state["last_synced_remote_modified_ms"] == local_mtime_ms
    assert file_state["last_synced_local_modified_ms"] == local_mtime_ms
    mock_list_workspace.assert_called_once()
    mock_get_status.assert_not_called()


@patch("dbx_sync.sync.time.sleep", side_effect=KeyboardInterrupt)
@patch(
    "dbx_sync.sync.run_sync_pass",
    return_value={"downloaded": 0, "uploaded": 0, "conflicts": 0, "removed": 0, "skipped": 0},
)
def test_run_forever_stops_on_keyboard_interrupt(
    mock_run_sync_pass: MagicMock, mock_sleep: MagicMock, tmp_path: Path
) -> None:
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "poll_interval_seconds": 1,
    }

    result = sync.run_forever(config, tmp_path / "config.json", dry_run=False)

    assert result == 0
    mock_run_sync_pass.assert_called_once()
    mock_sleep.assert_called_once_with(1)


@patch("dbx_sync.sync.time.sleep", side_effect=[None, KeyboardInterrupt])
@patch(
    "dbx_sync.sync.run_sync_pass",
    side_effect=[
        RuntimeError("boom"),
        {"downloaded": 0, "uploaded": 0, "conflicts": 0, "removed": 0, "skipped": 0},
    ],
)
def test_run_forever_retries_after_sync_pass_failure(
    mock_run_sync_pass: MagicMock, mock_sleep: MagicMock, tmp_path: Path
) -> None:
    config = {
        "local_dir": str(tmp_path),
        "remote_path": "/workspace",
        "poll_interval_seconds": 1,
    }

    result = sync.run_forever(config, tmp_path / "config.json", dry_run=False)

    assert result == 0
    assert mock_run_sync_pass.call_count == 2
    assert mock_sleep.call_count == 2


@patch(
    "dbx_sync.sync.run_sync_pass",
    return_value={"downloaded": 0, "uploaded": 0, "conflicts": 0, "removed": 0, "skipped": 0},
)
@patch("dbx_sync.sync.get_status", return_value={"object_type": "DIRECTORY"})
@patch("dbx_sync.sync.load_saved_config", return_value=None)
def test_run_sync_single_pass_success(
    mock_load_saved_config: MagicMock,
    mock_get_status: MagicMock,
    mock_run_sync_pass: MagicMock,
    tmp_path: Path,
) -> None:
    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=1,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=False,
    )

    assert result == 0
    mock_load_saved_config.assert_called_once()
    mock_get_status.assert_called_once_with("/workspace", "DEFAULT")
    mock_run_sync_pass.assert_called_once()


@patch("dbx_sync.sync.get_status", return_value=None)
@patch("dbx_sync.sync.load_saved_config", return_value=None)
def test_run_sync_missing_remote_parent_returns_one(
    mock_load_saved_config: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=1,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=False,
    )

    assert result == 1
    mock_load_saved_config.assert_called_once()
    mock_get_status.assert_called_once_with("/workspace", "DEFAULT")


@patch("dbx_sync.sync.get_status", return_value={"object_type": "NOTEBOOK"})
@patch("dbx_sync.sync.load_saved_config", return_value=None)
def test_run_sync_non_directory_remote_parent_returns_one(
    mock_load_saved_config: MagicMock, mock_get_status: MagicMock, tmp_path: Path
) -> None:
    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=1,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=False,
    )

    assert result == 1
    mock_load_saved_config.assert_called_once()
    mock_get_status.assert_called_once_with("/workspace", "DEFAULT")


@patch("dbx_sync.sync.run_forever", return_value=0)
@patch("dbx_sync.sync.get_status", return_value={"object_type": "DIRECTORY"})
@patch("dbx_sync.sync.load_saved_config", return_value=None)
def test_run_sync_watch_mode_delegates_to_run_forever(
    mock_load_saved_config: MagicMock,
    mock_get_status: MagicMock,
    mock_run_forever: MagicMock,
    tmp_path: Path,
) -> None:
    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=7,
        log_level="DEBUG",
        dry_run=True,
        watch=True,
        force=False,
    )

    assert result == 0
    mock_run_forever.assert_called_once()
    mock_load_saved_config.assert_called_once()
    mock_get_status.assert_called_once_with("/workspace", "DEFAULT")


@patch(
    "dbx_sync.sync.run_sync_pass",
    return_value={"downloaded": 0, "uploaded": 0, "conflicts": 0, "removed": 0, "skipped": 0},
)
@patch("dbx_sync.sync.get_status", return_value={"object_type": "DIRECTORY"})
def test_run_sync_force_removes_existing_config(
    mock_get_status: MagicMock, mock_run_sync_pass: MagicMock, tmp_path: Path
) -> None:
    config_path = sync.config_path_for(tmp_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"files": {}}), encoding="utf-8")

    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=1,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=True,
    )

    assert result == 0
    assert not config_path.exists()
    mock_get_status.assert_called_once_with("/workspace", "DEFAULT")
    mock_run_sync_pass.assert_called_once()


@patch("dbx_sync.sync.list_workspace", return_value=[])
@patch("dbx_sync.sync.get_status", return_value=None)
def test_run_sync_allows_missing_local_directory(
    mock_get_status: MagicMock, mock_list_workspace: MagicMock, tmp_path: Path
) -> None:
    missing_dir = tmp_path / "missing"
    config = {
        "local_dir": str(missing_dir),
        "remote_path": "/workspace",
        "profile": "DEFAULT",
        "files": {},
    }

    result = sync.run_sync_pass(config, tmp_path / "config.json", dry_run=True)

    assert result == {
        "downloaded": 0,
        "uploaded": 0,
        "conflicts": 0,
        "removed": 0,
        "skipped": 0,
    }
    mock_list_workspace.assert_called_once_with("/workspace", "DEFAULT", None)
    mock_get_status.assert_not_called()


def test_run_sync_rejects_non_directory_local_path(tmp_path: Path) -> None:
    local_file = tmp_path / "local.py"
    local_file.write_text("print('hi')\n", encoding="utf-8")

    result = sync.run_sync(
        local_dir=local_file,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=1,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=False,
    )

    assert result == 1


def test_run_sync_rejects_non_positive_poll_interval(tmp_path: Path) -> None:
    result = sync.run_sync(
        local_dir=tmp_path,
        remote_path="/workspace/test",
        profile="DEFAULT",
        poll_interval_seconds=0,
        log_level="INFO",
        dry_run=False,
        watch=False,
        force=False,
    )

    assert result == 1


def test_workspace_item_dataclass() -> None:
    item = sync.WorkspaceItem("/path", "NOTEBOOK", "PYTHON", 1234)

    assert item.path == "/path"
    assert item.object_type == "NOTEBOOK"
    assert item.language == "PYTHON"
    assert item.modified_at == 1234


def test_package_main_exits_with_cli_status(monkeypatch: Any) -> None:
    monkeypatch.setattr("dbx_sync.cli.main", lambda: 7)

    try:
        runpy.run_module("dbx_sync.__main__", run_name="__main__")
    except SystemExit as exc:
        assert exc.code == 7
    else:
        raise AssertionError("expected package entrypoint to exit")
