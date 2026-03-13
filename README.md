# dbx-sync

```text
         __ __
    ,___/ // /____   __      _____ __  ______  _____
   / __  // __ \\ \ / /_____/ ___// / / / __ \/ ___/
  / /_/ // /_/ //  X  \____(__  )/ /_/ / / / / /__
 /_____//_____//__/ \__\  /____/ \__, /_/ /_/\___/
                                 /____/
```

Are you tired of bouncing between the Databricks workspace UI and your local editor, copying changes by hand, and pretending that counts as a workflow? Well now there's `dbx-sync`.

`dbx-sync` keeps a single Databricks workspace folder and a single local directory in sync so you can work with your favorite tools and still stay aligned with what is running in Databricks.

Build locally, run in Databricks, tweak it there, then jump back to local coding. Skip the usual copy-paste ritual or one-way imports to weird folders.

Great for AI coding-agent workflows, including GitHub Copilot and Claude-based setups that work best against a real local folder.

Worried about losing files? `dbx-sync` does not delete files locally or remotely, but it can overwrite content if both sides changed while you were not syncing. Use version control locally and Databricks revision history remotely when you need rollback.

Current scope notes:

- Sync is limited to a single local folder and a single Databricks workspace folder.
- File and folder discovery is not recursive.
- Local tracking currently covers notebook files with Databricks notebook extensions: `.py`, `.sql`, `.scala`, `.r`, and `.ipynb`.

## Prerequisites

- Databricks CLI 0.205 or newer
  - With a configured Databricks CLI profile

## Install

### Recommended: install as a uv tool

Install `dbx-sync` as a tool so you can run it directly from your shell:

```bash
uv tool install dbx-sync
```

### Alternative: install with pip

If you prefer a standard virtual environment workflow, install the package with `pip`:

```bash
python -m pip install dbx-sync
```

### Alternative: run from a local checkout

If you are developing on the project itself, install the local environment and run it with `uv run`:

```bash
uv sync --dev
uv run dbx-sync ./local-project /Workspace/Users/me/project
```

## Usage

Sync a single workspace folder with a single local folder (one-time):

```bash
dbx-sync ./local-project /Workspace/Users/me/project
```

Preview actions without applying them:

```bash
dbx-sync ./local-project /Workspace/Users/me/project --dry-run
```

Continuously watch and resync (default polling happens every second):

```bash
dbx-sync ./local-project /Workspace/Users/me/project --watch
```

Override optional settings when needed:

```bash
dbx-sync ./local-project /Workspace/Users/me/project \
	--profile WORKSPACE \
	--poll-interval 5 \
	--log-level DEBUG \
	--force
```

Use `--force` to clear saved sync state before a fresh pass.

The local directory may start empty or not exist yet. On a non-dry-run sync, the tool creates what it needs under that directory when files or sync state are written.

## Alternatives
Yes, I recognize there are a variety of official ways to do something close to this, but none of them fit my desired workflow well. So here are some references for alternatives.

- Databricks CLI workspace commands (`import`, `import-dir`, `export`, `export-dir`, `sync`, and related commands): <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/commands/>
- Databricks extension for Visual Studio Code: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext/>
- Databricks Asset Bundles documentation: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/>
- Databricks Git folders: <https://learn.microsoft.com/en-us/azure/databricks/repos/>

## Development

See [CONTRIBUTING.md](/home/scgraham/repos/dbx-sync/CONTRIBUTING.md) for local development, testing, release, and repository workflow details.

## License

MIT. See [LICENSE](/home/scgraham/repos/dbx-sync/LICENSE).
