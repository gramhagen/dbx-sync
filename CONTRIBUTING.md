# Contributing

## Local Setup

Local development targets Python 3.12.

```bash
uv sync --dev
```

The Databricks CLI is required for running the tool against a real workspace.

- Install or update the Databricks CLI: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install>
- Configure a Databricks CLI profile: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/reference/configure-commands#create-a-configuration-profile>

## Development Workflow

- Implement changes inside `src/dbx_sync/`.
- Add or update tests in `tests/` for behavior changes.
- Keep changes typed, readable, and small enough to review comfortably.
- Use `uv run` for project commands so the local environment and lockfile stay authoritative.

## Validation

Run the full local validation suite before finishing non-trivial changes.

```bash
uv run ruff format .
uv run ruff check .
uv run ty check
uv run pytest
```

Generate a line-by-line HTML coverage report when needed.

```bash
uv run pytest --cov=dbx_sync --cov-report term-missing --cov-report html
```

The HTML coverage report is written to `htmlcov/`.

## CLI Notes

- Required positional arguments: local directory, workspace path.
- Optional flags: `--profile`, `--poll-interval`, `--log-level`, `--dry-run`, `--watch`, `--force`.
- The current sync scope is a single folder level only; local discovery is not recursive.
- The current local tracking scope is Databricks notebook files with supported notebook extensions.
- Use `--force` to clear saved sync state and trigger a fresh comparison.

## Packaging And Release

Build and publish with uv.

```bash
uv build
uv publish
```

If you want to test the packaged CLI experience locally, install it as a uv tool:

```bash
uv tool install .
```

## License

This repository is released under the MIT license. See `LICENSE` for the full text.

## Agent Guidance

- Shared coding-agent instructions live in `AGENTS.md`.
- Keep repository-specific conventions there rather than creating overlapping instruction files.