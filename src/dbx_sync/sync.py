from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

DEFAULT_SUBPROCESS_TIMEOUT_SECONDS = 30
SYNC_FOLDER = Path(".databricks") / "dbx-sync"
CONFIG_FILE_NAME = "config.json"

LANGUAGE_EXTENSIONS = {
    "PYTHON": ".py",
    "R": ".r",
    "SCALA": ".scala",
    "SQL": ".sql",
}

EXTENSION_LANGUAGES = {
    ".ipynb": "PYTHON",
    **{v: k for k, v in LANGUAGE_EXTENSIONS.items()},
}

LOGGER = logging.getLogger(__name__)


def remote_parent_path(remote_path: str) -> str:
    """Return the parent workspace path for a workspace object path."""
    return remote_path.rstrip("/").rsplit("/", 1)[0] or "/"


def remote_name_for_local_file(local_path: Path) -> str:
    """Build a Databricks workspace object name from a local file path."""
    if local_path.suffix.lower() in EXTENSION_LANGUAGES:
        return local_path.stem
    return local_path.name


class ForceType(str, Enum):
    """Specifies how to override the normal per-file sync decision.

    Attributes:
        CLEAR: Clear saved sync state before running (equivalent to old --force).
        UPLOAD: Force upload of all local files, ignoring sync state.
        DOWNLOAD: Force download of all remote files, ignoring sync state.
    """

    CLEAR = "clear"
    UPLOAD = "upload"
    DOWNLOAD = "download"


@dataclass
class WorkspaceItem:
    """Metadata for a Databricks workspace object used during sync decisions.

    Attributes:
        path: Full Databricks workspace path.
        object_type: Workspace object type, such as NOTEBOOK or FILE.
        language: Optional Databricks notebook language.
        modified_at: Optional last-modified timestamp in milliseconds.
    """

    path: str
    object_type: str
    language: str | None
    modified_at: int | None


def iso_from_ms(timestamp_ms: int | None) -> str:
    """Convert a millisecond timestamp to an ISO 8601 string.

    Args:
        timestamp_ms: Millisecond timestamp or None.

    Returns:
        str: ISO 8601 timestamp text, or "missing" when no timestamp is available.
    """
    if timestamp_ms is None:
        return "missing"
    return datetime.fromtimestamp(timestamp_ms / 1000.0).isoformat(timespec="seconds")


def configure_logging(log_level: str) -> None:
    """Configure process-wide logging for the sync tool.

    Args:
        log_level: Desired log level name.

    Returns:
        None: This function mutates logging configuration in place.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[logging.StreamHandler()],
        force=True,
    )


def config_path_for(local_dir: Path) -> Path:
    """Build the sync state file path for a local directory.

    Args:
        local_dir: Local project directory being synchronized.

    Returns:
        Path: Path to the persisted sync state file.
    """
    return local_dir / SYNC_FOLDER / CONFIG_FILE_NAME


def load_saved_config(config_path: Path) -> dict[str, Any] | None:
    """Load a saved sync configuration from disk.

    Args:
        config_path: Path to the persisted sync state file.

    Returns:
        dict[str, Any] | None: Parsed configuration object, or None when no file exists.

    Raises:
        RuntimeError: If the saved JSON does not contain an object.
    """
    if not config_path.exists():
        return None
    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError(f"Config file must contain a JSON object: {config_path}")
    return data


def run_cli(args: list[str]) -> str:
    """Run a Databricks CLI command and capture standard output.

    Args:
        args: Full command argument list.

    Returns:
        str: Standard output from the CLI process.

    Raises:
        RuntimeError: If the CLI exits with a non-zero status.
    """
    LOGGER.debug("Running CLI command: %s", " ".join(args))
    timeout_text = os.environ.get("DBX_SYNC_SUBPROCESS_TIMEOUT_SECONDS")
    try:
        timeout_seconds = (
            float(timeout_text) if timeout_text is not None else DEFAULT_SUBPROCESS_TIMEOUT_SECONDS
        )
    except ValueError as exc:
        raise RuntimeError(
            "DBX_SYNC_SUBPROCESS_TIMEOUT_SECONDS must be a number of seconds."
        ) from exc
    if timeout_seconds <= 0:
        raise RuntimeError("DBX_SYNC_SUBPROCESS_TIMEOUT_SECONDS must be greater than zero.")
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {timeout_seconds:g}s: {' '.join(args)}"
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(args)}\n{result.stderr.strip()}"
        )
    return result.stdout


def list_workspace(remote_path: str, profile: str) -> list[WorkspaceItem]:
    """List notebook and file objects under a workspace path.

    Args:
        remote_path: Workspace folder to enumerate.
        profile: Databricks CLI profile name.

    Returns:
        list[WorkspaceItem]: Supported workspace objects returned by the CLI.

    Raises:
        RuntimeError: If the CLI response cannot be parsed as expected JSON.
    """
    cmd = ["databricks", "--profile", profile, "workspace", "list"]
    cmd += ["--output", "json", remote_path]
    raw = run_cli(cmd)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse JSON from command: {' '.join(cmd)}\nOutput: {raw[:500]}"
        ) from exc
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected payload from workspace list for {remote_path}")
    items: list[WorkspaceItem] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        object_type = entry.get("object_type")
        path = entry.get("path")
        if object_type not in {"NOTEBOOK", "FILE"} or not isinstance(path, str):
            continue
        language = entry.get("language")
        modified_at = entry.get("modified_at")
        items.append(
            WorkspaceItem(
                path=path,
                object_type=object_type,
                language=language if isinstance(language, str) else None,
                modified_at=modified_at if isinstance(modified_at, int) else None,
            )
        )
    return items


def get_status(remote_path: str, profile: str) -> dict[str, Any] | None:
    """Fetch metadata for a single workspace object.

    Args:
        remote_path: Full workspace path to inspect.
        profile: Databricks CLI profile name.

    Returns:
        dict[str, Any] | None: Status payload for the workspace object, or None when missing.

    Raises:
        RuntimeError: If the CLI fails for a reason other than a missing object,
            or if JSON parsing fails.
    """
    cmd = [
        "databricks",
        "--profile",
        profile,
        "workspace",
        "get-status",
        "--output",
        "json",
        remote_path,
    ]
    try:
        raw = run_cli(cmd)
    except RuntimeError as exc:
        message = str(exc)
        if (
            "RESOURCE_DOES_NOT_EXIST" in message
            or "doesn't exist" in message
            or "does not exist" in message
        ):
            return None
        raise
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse JSON from command: {' '.join(cmd)}\nOutput: {raw[:500]}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected payload from workspace status for {remote_path}")
    return payload


def download_workspace_item(item: WorkspaceItem, local_path: Path, profile: str) -> None:
    """Download a workspace object to a local file.

    Args:
        item: Workspace object to export.
        local_path: Destination path on the local filesystem.
        profile: Databricks CLI profile name.

    Returns:
        None: The file is written to disk as a side effect.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "databricks",
        "--profile",
        profile,
        "workspace",
        "export",
        item.path,
        "--file",
        str(local_path),
    ]
    if item.object_type == "NOTEBOOK":
        if local_path.suffix.lower() == ".ipynb":
            cmd += ["--format", "JUPYTER"]
        else:
            cmd += ["--format", "SOURCE"]
    else:
        cmd += ["--format", "AUTO"]
    run_cli(cmd)


def upload_workspace_item(item: WorkspaceItem, local_path: Path, profile: str) -> None:
    """Upload a local file into the Databricks workspace.

    Args:
        item: Target workspace object metadata.
        local_path: Local file to import.
        profile: Databricks CLI profile name.

    Returns:
        None: The workspace is updated as a side effect.
    """
    cmd = [
        "databricks",
        "--profile",
        profile,
        "workspace",
        "import",
        item.path,
        "--file",
        str(local_path),
        "--overwrite",
    ]
    if item.object_type == "NOTEBOOK":
        if local_path.suffix.lower() == ".ipynb":
            cmd += ["--format", "JUPYTER"]
        else:
            language = item.language or EXTENSION_LANGUAGES.get(local_path.suffix.lower(), "PYTHON")
            cmd += ["--format", "SOURCE", "--language", language]
    else:
        cmd += ["--format", "AUTO"]
    run_cli(cmd)


def tracked_local_files(local_dir: Path) -> list[Path]:
    """List local files that the sync engine treats as notebook candidates.

    Args:
        local_dir: Local directory being synchronized.

    Returns:
        list[Path]: Files with supported notebook extensions in the top-level directory.
    """
    if not local_dir.exists():
        return []
    return [
        file_path
        for file_path in local_dir.iterdir()
        if file_path.is_file() and file_path.suffix.lower() in EXTENSION_LANGUAGES
    ]


def local_path_for_workspace_item(item: WorkspaceItem, remote_root: str, local_dir: Path) -> Path:
    """Build the local path for a workspace item under a local sync root."""
    relative_name = item.path[len(remote_root.rstrip("/")) :].lstrip("/")
    if item.object_type == "NOTEBOOK" and item.language:
        extension = LANGUAGE_EXTENSIONS.get(item.language, ".py")
        return local_dir / f"{relative_name}{extension}"
    return local_dir / relative_name


def _default_file_state() -> dict[str, Any]:
    """Create an empty persisted state record for one synced path.

    Returns:
        dict[str, Any]: Default state structure used in the sync config file.
    """
    return {
        "local_path": None,
        "object_type": None,
        "language": None,
        "last_synced_remote_modified_ms": None,
        "last_synced_local_modified_ms": None,
        "last_action": None,
    }


def _resolve_file_action(
    remote_path: str,
    files_state: dict[str, Any],
    remote_candidates: dict[str, WorkspaceItem],
    remote_root: str,
    local_dir: Path,
    profile: str,
) -> tuple[str, dict[str, Any], WorkspaceItem, Path]:
    """Determine the next sync action for one workspace path.

    Args:
        remote_path: Workspace path under evaluation.
        files_state: Persisted sync state keyed by workspace path.
        remote_candidates: Freshly listed remote objects keyed by workspace path.
        remote_root: Root workspace folder being synchronized.
        local_dir: Local directory paired with the workspace root.
        profile: Databricks CLI profile name.

    Returns:
        tuple[str, dict[str, Any], WorkspaceItem, Path]: Action name, mutable state entry,
        resolved workspace item, and resolved local path.
    """
    # Persisted state supplies local path and object metadata, but remote existence must be fresh.
    file_state = files_state.get(remote_path)
    if not isinstance(file_state, dict):
        file_state = _default_file_state()
        files_state[remote_path] = file_state

    remote_item = remote_candidates.get(remote_path)
    last_synced_remote_ms = file_state.get("last_synced_remote_modified_ms")
    saved_language = file_state.get("language")
    file_state_language = saved_language if isinstance(saved_language, str) else None
    if remote_item is None:
        status = get_status(remote_path, profile)
        if status is not None:
            status_language = status.get("language")
            status_modified_at = status.get("modified_at")
            remote_item = WorkspaceItem(
                path=remote_path,
                object_type=str(
                    status.get("object_type") or file_state.get("object_type") or "FILE"
                ),
                language=status_language if isinstance(status_language, str) else None,
                modified_at=status_modified_at if isinstance(status_modified_at, int) else None,
            )

    if remote_item is None:
        remote_item = WorkspaceItem(
            path=remote_path,
            object_type=str(file_state.get("object_type") or "FILE"),
            language=file_state_language,
            modified_at=None,
        )

    # Reuse the saved local path when available; otherwise rebuild it from the remote item.
    configured_local_path = file_state.get("local_path")
    if isinstance(configured_local_path, str) and configured_local_path:
        local_path = Path(configured_local_path)
    else:
        local_path = local_path_for_workspace_item(remote_item, remote_root, local_dir)

    local_exists = local_path.exists()
    local_mtime_ms = int(local_path.stat().st_mtime * 1000) if local_exists else None
    remote_mtime_ms = remote_item.modified_at

    if not local_exists:
        file_state["last_synced_local_modified_ms"] = None
    if remote_mtime_ms is None:
        file_state["last_synced_remote_modified_ms"] = None

    # If neither side still exists, drop the stale file-state entry on the next sync pass.
    if not local_exists and remote_mtime_ms is None:
        return "remove", file_state, remote_item, local_path
    if local_exists and remote_mtime_ms is None:
        return "upload", file_state, remote_item, local_path

    last_synced_local_ms = file_state.get("last_synced_local_modified_ms")

    local_changed = (
        local_mtime_ms is not None
        and isinstance(last_synced_local_ms, int)
        and local_mtime_ms > last_synced_local_ms
    )
    remote_changed = (
        remote_mtime_ms is not None
        and isinstance(last_synced_remote_ms, int)
        and remote_mtime_ms > last_synced_remote_ms
    )

    if (
        remote_changed
        and file_state.get("last_action") == "upload"
        and local_mtime_ms == last_synced_local_ms
    ):
        remote_changed = False
    if (
        local_changed
        and file_state.get("last_action") == "download"
        and remote_mtime_ms == last_synced_remote_ms
    ):
        local_changed = False

    # On first sync, choose a source of truth from the available timestamps.
    if last_synced_remote_ms is None and last_synced_local_ms is None:
        if remote_mtime_ms is None and local_mtime_ms is not None:
            action = "upload"
        elif local_mtime_ms is None and remote_mtime_ms is not None:
            action = "download"
        elif local_mtime_ms is not None and remote_mtime_ms is not None:
            action = "upload" if local_mtime_ms > remote_mtime_ms else "download"
        else:
            action = "skip"
    # After the first sync, newer content wins unless both sides changed.
    elif local_changed and remote_changed:
        action = "conflict"
    elif local_changed:
        action = "upload"
    elif remote_changed:
        action = "download"
    else:
        action = "skip"

    return action, file_state, remote_item, local_path


def run_sync_pass(
    config: dict[str, Any],
    config_path: Path,
    dry_run: bool = False,
    force_type: ForceType | None = None,
) -> dict[str, int]:
    """Run one synchronization pass between local and remote content.

    Args:
        config: Sync configuration and persisted state.
        config_path: Path where sync state should be written.
        dry_run: Whether to compute actions without applying them.
        force_type: Optional override for the sync decision. UPLOAD forces all
            local files to be uploaded; DOWNLOAD forces all remote files to be
            downloaded. CLEAR is handled before this function is called (it
            removes saved state), so it has no additional effect here.

    Returns:
        dict[str, int]: Counters for downloaded, uploaded, conflicts, removed, and skipped items.

    Raises:
        RuntimeError: If the saved file-state payload is malformed.
    """
    local_dir = Path(str(config["local_dir"]))
    remote_root = str(config["remote_path"])
    profile = str(config["profile"])
    configured_local_file = config.get("local_file_path")
    local_file = Path(configured_local_file) if isinstance(configured_local_file, str) else None
    configured_remote_file = config.get("remote_file_path")
    remote_file = configured_remote_file if isinstance(configured_remote_file, str) else None
    files_state = config.setdefault("files", {})
    if not isinstance(files_state, dict):
        raise RuntimeError("Config 'files' entry must be a JSON object.")

    if local_file is not None:
        local_files = [local_file] if local_file.exists() else []
    elif remote_file is not None:
        local_files = []
    else:
        local_files = tracked_local_files(local_dir)

    if remote_file is not None:
        remote_status = get_status(remote_file, profile)
        remote_candidates = {}
        if remote_status is not None:
            remote_language = remote_status.get("language")
            remote_modified_at = remote_status.get("modified_at")
            remote_candidates[remote_file] = WorkspaceItem(
                path=remote_file,
                object_type=str(remote_status.get("object_type") or "FILE"),
                language=remote_language if isinstance(remote_language, str) else None,
                modified_at=remote_modified_at if isinstance(remote_modified_at, int) else None,
            )
    else:
        remote_candidates = {item.path: item for item in list_workspace(remote_root, profile)}

    for local_path in local_files:
        remote_path = (
            remote_file or f"{remote_root.rstrip('/')}/{remote_name_for_local_file(local_path)}"
        )
        if remote_path not in files_state:
            state = _default_file_state()
            state["local_path"] = str(local_path)
            local_language = EXTENSION_LANGUAGES.get(local_path.suffix.lower())
            state["object_type"] = "NOTEBOOK" if local_language is not None else "FILE"
            state["language"] = local_language
            files_state[remote_path] = state

    downloaded = 0
    uploaded = 0
    conflicts = 0
    removed = 0
    skipped = 0

    if remote_file is not None:
        # Single-file mode: only consider the target path, ignoring stale state
        # entries left over from prior directory syncs sharing the same config.
        candidate_paths = {remote_file}
    else:
        candidate_paths = set(remote_candidates) | set(files_state)

    for remote_path in sorted(candidate_paths):
        action, file_state, remote_item, local_path = _resolve_file_action(
            remote_path, files_state, remote_candidates, remote_root, local_dir, profile
        )

        if action == "remove":
            LOGGER.info("REMOVE: %s (missing locally and remotely)", remote_path)
            if dry_run:
                file_state["last_action"] = "remove"
            else:
                files_state.pop(remote_path, None)
            removed += 1
            continue

        local_mtime_ms = int(local_path.stat().st_mtime * 1000) if local_path.exists() else None
        remote_mtime_ms = remote_item.modified_at

        # Apply force overrides before logging or executing the action.
        if force_type is ForceType.DOWNLOAD and remote_mtime_ms is not None:
            action = "download"
        elif force_type is ForceType.UPLOAD and local_mtime_ms is not None:
            action = "upload"

        if action == "skip":
            LOGGER.debug("SKIP: %s", local_path.name)
        else:
            LOGGER.info("%s: %s", action.upper(), local_path.name)

        LOGGER.debug(
            "%s <=> %s, (remote %s, local %s, last-sync-remote %s, last-sync-local %s)",
            remote_path,
            local_path,
            iso_from_ms(remote_mtime_ms),
            iso_from_ms(local_mtime_ms),
            iso_from_ms(file_state.get("last_synced_remote_modified_ms")),
            iso_from_ms(file_state.get("last_synced_local_modified_ms")),
        )

        file_state["local_path"] = str(local_path)
        file_state["object_type"] = remote_item.object_type
        file_state["language"] = remote_item.language

        if dry_run:
            if action == "download":
                downloaded += 1
            elif action == "upload":
                uploaded += 1
            elif action == "conflict":
                conflicts += 1
            else:
                skipped += 1
            file_state["last_action"] = action
            continue

        if action == "download" and remote_mtime_ms is not None:
            download_workspace_item(remote_item, local_path, profile)
            downloaded_local_mtime_ms = int(local_path.stat().st_mtime * 1000)
            sync_watermark_ms = max(remote_mtime_ms, downloaded_local_mtime_ms)
            file_state["last_synced_remote_modified_ms"] = sync_watermark_ms
            file_state["last_synced_local_modified_ms"] = sync_watermark_ms
            file_state["last_action"] = "download"
            downloaded += 1
        elif action == "upload" and local_mtime_ms is not None:
            upload_workspace_item(remote_item, local_path, profile)
            status = get_status(remote_path, profile)
            status_modified_at = status.get("modified_at") if isinstance(status, dict) else None
            uploaded_remote_mtime_ms = (
                status_modified_at if isinstance(status_modified_at, int) else local_mtime_ms
            )
            sync_watermark_ms = max(local_mtime_ms, uploaded_remote_mtime_ms)
            file_state["last_synced_remote_modified_ms"] = sync_watermark_ms
            file_state["last_synced_local_modified_ms"] = sync_watermark_ms
            file_state["last_action"] = "upload"
            uploaded += 1
        elif action == "conflict":
            file_state["last_action"] = "conflict"
            conflicts += 1
        else:
            file_state["last_action"] = "skip"
            if remote_mtime_ms is not None:
                file_state["last_synced_remote_modified_ms"] = remote_mtime_ms
            if local_mtime_ms is not None:
                file_state["last_synced_local_modified_ms"] = local_mtime_ms
            skipped += 1

    if not dry_run:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with config_path.open("w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=2)

    return {
        "downloaded": downloaded,
        "uploaded": uploaded,
        "conflicts": conflicts,
        "removed": removed,
        "skipped": skipped,
    }


def run_forever(
    config: dict[str, Any],
    config_path: Path,
    dry_run: bool,
) -> int:
    """Run synchronization continuously until interrupted.

    Args:
        config: Sync configuration and persisted state.
        config_path: Path where sync state should be written.
        dry_run: Whether to compute actions without applying them.

    Returns:
        int: Process exit code, returning zero on a clean interrupt.
    """
    LOGGER.info(
        "Starting sync loop: local_dir=%s remote_path=%s interval=%ss dry_run=%s",
        config["local_dir"],
        config["remote_path"],
        config["poll_interval_seconds"],
        dry_run,
    )
    try:
        while True:
            try:
                result = run_sync_pass(
                    config,
                    config_path,
                    dry_run=dry_run,
                )
            except Exception:
                LOGGER.exception("Sync pass failed")
                result = None
            if result is not None:
                LOGGER.debug(
                    (
                        "Sync pass complete: downloaded=%s, uploaded=%s, conflicts=%s, "
                        "removed=%s, skipped=%s"
                    ),
                    result["downloaded"],
                    result["uploaded"],
                    result["conflicts"],
                    result["removed"],
                    result["skipped"],
                )
                if result["conflicts"] > 0:
                    conflicted_paths = [
                        path
                        for path, state in config.get("files", {}).items()
                        if state.get("last_action") == "conflict"
                    ]
                    LOGGER.error(
                        "Stopping watch mode: %d conflict(s) detected. "
                        "Conflicting file(s): %s. "
                        "Run with --force to clear sync state and retry.",
                        result["conflicts"],
                        ", ".join(conflicted_paths),
                    )
                    return 1

            time.sleep(int(config["poll_interval_seconds"]))
    except KeyboardInterrupt:
        LOGGER.info("Sync loop stopped by user.")
        return 0


def run_sync(
    local_dir: Path,
    remote_path: str,
    profile: str,
    poll_interval_seconds: int,
    log_level: str,
    dry_run: bool,
    watch: bool,
    force_type: ForceType | None = None,
) -> int:
    """Run a sync operation using fully resolved CLI parameters.

    Args:
        local_dir: Local file or directory to synchronize.
        remote_path: Databricks workspace file or folder to synchronize.
        profile: Databricks CLI profile name.
        poll_interval_seconds: Watch-loop interval in seconds.
        log_level: Desired process log level.
        dry_run: Whether to compute actions without applying them.
        watch: Whether to continue syncing in a loop.
        force_type: Optional force override. CLEAR removes saved sync state before
            starting; UPLOAD forces all local files to be uploaded; DOWNLOAD forces
            all remote files to be downloaded. Cannot be combined with watch mode.

    Returns:
        int: Process exit code.
    """
    resolved_local_dir = local_dir.expanduser().resolve()
    resolved_log_level = log_level.upper()
    resolved_remote_path = remote_path.rstrip("/")

    configure_logging(resolved_log_level)

    if poll_interval_seconds < 1:
        LOGGER.error("Poll interval must be at least 1 second.")
        return 1

    if watch:
        if force_type is not None:
            LOGGER.error(
                "Force options (--force, --force-upload, --force-download) can only be used for a "
                "single sync pass and cannot be combined with --watch."
            )
            return 1
        if dry_run:
            LOGGER.error("--dry-run cannot be used with --watch mode.")
            return 1

    if resolved_local_dir.exists() and not (
        resolved_local_dir.is_dir() or resolved_local_dir.is_file()
    ):
        LOGGER.error("Local sync path is not a file or directory: %s", resolved_local_dir)
        return 1

    remote_status = get_status(resolved_remote_path, profile)
    remote_object_type = remote_status.get("object_type") if remote_status is not None else None

    local_file_path = None
    if resolved_local_dir.is_file() or (
        not resolved_local_dir.exists()
        and resolved_local_dir.suffix
        and remote_object_type in {"NOTEBOOK", "FILE"}
    ):
        local_file_path = resolved_local_dir

    local_sync_dir = (
        resolved_local_dir.parent if local_file_path is not None else resolved_local_dir
    )

    config_path = config_path_for(local_sync_dir)

    if force_type is ForceType.CLEAR and config_path.exists():
        config_path.unlink()
        LOGGER.info("Removed saved sync state for forced refresh: %s", config_path)

    existing_config = load_saved_config(config_path)
    configured_files_state = (
        existing_config.get("files") if isinstance(existing_config, dict) else None
    )
    files_state = configured_files_state if isinstance(configured_files_state, dict) else {}
    remote_file_path = None
    remote_sync_root = resolved_remote_path

    if remote_object_type == "DIRECTORY":
        if local_file_path is not None:
            remote_file_path = (
                f"{resolved_remote_path.rstrip('/')}/{remote_name_for_local_file(local_file_path)}"
            )
    elif remote_object_type in {"NOTEBOOK", "FILE"} or local_file_path is not None:
        remote_file_path = resolved_remote_path
        remote_sync_root = remote_parent_path(resolved_remote_path)

    if remote_file_path is not None and local_file_path is None and not local_sync_dir.exists():
        local_sync_dir = resolved_local_dir
        config_path = config_path_for(local_sync_dir)

    config = {
        "version": "v1",
        "profile": profile,
        "poll_interval_seconds": poll_interval_seconds,
        "log_level": resolved_log_level,
        "remote_path": remote_sync_root,
        "local_dir": str(local_sync_dir),
        "files": files_state,
    }
    if local_file_path is not None:
        config["local_file_path"] = str(local_file_path)
    if remote_file_path is not None:
        config["remote_file_path"] = remote_file_path

    remote_parent = remote_parent_path(remote_file_path or remote_sync_root)
    remote_parent_status = get_status(remote_parent, profile)
    if remote_parent_status is None:
        LOGGER.error("Remote parent workspace folder does not exist: %s", remote_parent)
        return 1
    if remote_parent_status.get("object_type") != "DIRECTORY":
        LOGGER.error("Remote parent workspace path is not a directory: %s", remote_parent)
        return 1

    if watch:
        return run_forever(
            config,
            config_path,
            dry_run=dry_run,
        )

    result = run_sync_pass(
        config,
        config_path,
        dry_run=dry_run,
        force_type=force_type,
    )
    LOGGER.info(
        "Sync pass complete: downloaded=%s, uploaded=%s, conflicts=%s, removed=%s, skipped=%s",
        result["downloaded"],
        result["uploaded"],
        result["conflicts"],
        result["removed"],
        result["skipped"],
    )
    if result["conflicts"] > 0:
        configured_files_state = config.get("files")
        conflicted_paths = []
        if isinstance(configured_files_state, dict):
            conflicted_paths = [
                path
                for path, state in configured_files_state.items()
                if isinstance(state, dict) and state.get("last_action") == "conflict"
            ]
        LOGGER.error("Conflict(s) detected. Conflicting file(s): %s", ", ".join(conflicted_paths))
        return 1
    return 0
