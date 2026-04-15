from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from dbx_sync.sync import run_sync

DEFAULT_POLL_INTERVAL_SECONDS = 1
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def positive_int(value: str) -> int:
    """Parse a positive integer argument for argparse."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and run a sync operation.

    Args:
        argv: Optional argument vector used instead of sys.argv.

    Returns:
        int: Process exit code from the sync operation.
    """
    parser = argparse.ArgumentParser(
        prog="dbx-sync",
        description="Synchronize Databricks workspace files to a local directory.",
    )
    parser.add_argument("local_dir", help="Local directory to sync")
    parser.add_argument("workspace", help="Databricks workspace folder to sync")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile name")
    parser.add_argument(
        "-p",
        "--poll-interval",
        type=positive_int,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="Polling interval in seconds when running in watch mode",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        choices=LOG_LEVELS,
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help="Plan the sync without applying changes.",
    )
    parser.add_argument(
        "-w",
        "--watch",
        action="store_true",
        help="Watch for changes and sync continuously",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force a refresh by clearing saved sync state before running",
    )
    args = parser.parse_args(argv)

    try:
        return run_sync(
            local_dir=Path(args.local_dir).expanduser().resolve(),
            remote_path=args.workspace,
            profile=args.profile,
            poll_interval_seconds=args.poll_interval,
            log_level=args.log_level,
            dry_run=args.dry_run,
            watch=args.watch,
            force=args.force,
        )
    except RuntimeError as exc:
        # print user-friendly instructions if error is reauthentication-related, otherwise re-raise
        reauth = "refresh token is invalid" in str(exc).strip().lower()
        if reauth and args.log_level != "DEBUG":
            print(f"Databricks authentication failed for profile '{args.profile}'.")
            print(f"Reauthenticate with: databricks auth login --profile {args.profile}")
            return 1

        raise
