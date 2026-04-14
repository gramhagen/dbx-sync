from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from dbx_sync.sync import run_sync

DEFAULT_POLL_INTERVAL_SECONDS = 1
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOGGER = logging.getLogger(__name__)


def positive_int(value: str) -> int:
    """Parse a strictly positive integer argument.

    Args:
        value: Raw command-line argument text.

    Returns:
        int: Parsed positive integer value.

    Raises:
        argparse.ArgumentTypeError: If the value is not a positive integer.
    """
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the sync tool."""
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and run a sync operation.

    Args:
        argv: Optional argument vector used instead of sys.argv.

    Returns:
        int: Process exit code from the sync operation.
    """
    parser = build_parser()
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
        message = str(exc).strip()
        if "refresh token is invalid" in message.lower():
            LOGGER.error(
                "Databricks authentication failed for profile '%s'. \nReauthenticate with: "
                "databricks auth login --profile %s",
                args.profile,
                args.profile,
            )
            return 1

        LOGGER.error(message)
        return 1
