# Agent Workflow Notes

This repository uses a single shared instruction file for coding agents.

## Default workflow

1. Sync dependencies with `uv sync --dev`.
2. Implement code inside `src/dbx_sync/` using typed functions and small modules.
3. Add or update tests in `tests/` for each user-visible change.
4. Run `uv run ruff format .`, `uv run ruff check .`, `uv run ty check`, and `uv run pytest` before handing work back.
5. Use `uv build` and `uv publish` for packaging and release operations.

## Guardrails

- Keep configuration centralized in `pyproject.toml` where practical.
- Prefer extending the package API instead of adding loose scripts at repository root.
- Evolve behavior incrementally with tests rather than broad rewrites.
- Keep agent-specific workflow guidance here instead of splitting it across multiple files.

## Python Conventions

- Write readable, maintainable Python with descriptive names and explicit type hints.
- Break down complex logic into smaller helper functions instead of growing large multi-purpose functions.
- Use docstrings for non-trivial public functions and classes, following PEP 257 and concise Google-style sections when helpful.
- Prefer clear exception handling and cover edge cases such as missing inputs, invalid data, and empty results.
- Keep comments focused on intent or non-obvious design decisions; avoid narrating obvious code.
- Use logging module formatting for log messages instead of f-strings in logging calls.

## Python Style

- Follow PEP 8 with 4-space indentation.
- Keep code consistent with the repository toolchain: Ruff formatting, Ty type checking, and the line-length configured in `pyproject.toml`.
- Use modern built-in generics and standard typing features appropriate for Python 3.10+.
- Place docstrings immediately after function, class, or module declarations.
- Prefer straightforward, idiomatic Python over clever or overly abstract patterns.

## Testing Expectations

- Add unit tests for critical paths and behavior changes.
- Include edge-case coverage for empty inputs, invalid state, and error handling when those paths matter.
- Keep tests readable and focused on behavior rather than implementation detail.
- When adapting logic from another codebase, translate the relevant tests into this repo's current API instead of copying obsolete cases unchanged.
