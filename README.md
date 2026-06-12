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

`dbx-sync` keeps a Databricks workspace folder or file and a local directory or file in sync so you can work with your favorite tools and still stay aligned with what is running in Databricks.

Build locally, run in Databricks, tweak it there, then jump back to local coding. Skip the usual copy-paste ritual or one-way imports to weird folders.

Great for AI coding-agent workflows, including GitHub Copilot and Claude-based setups that work best against a real local folder.

Worried about losing files? `dbx-sync` does not delete files locally or remotely, but it can overwrite content if both sides changed while you were not syncing. Use version control locally and Databricks revision history remotely when you need rollback.

Current scope notes:

- Sync is limited to a single local folder/workspace folder pair or one local/workspace file pair.
- File and folder discovery is not recursive.
- Local tracking currently covers notebook files with Databricks notebook extensions: `.py`, `.sql`, `.scala`, `.r`, and `.ipynb`.
- When syncing a single local file, notebook extensions are imported as notebooks and other files are imported as workspace files.

## Prerequisites

- Databricks CLI 0.205 or newer
  - [Install the Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install)
  - [Configure a DEFAULT profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/authentication#oauth-user-to-machine-u2m-authentication)

## Install

### Recommended: install as a uv tool

Install `dbx-sync` as a tool so you can run it directly from your shell:

```bash
uv tool install dbx-sync
```

#### Update tool

```bash
uv tool upgrade dbx-sync
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

The command takes two positional arguments: the **first** is always the **local** path (file or directory) and the **second** is always the **remote** Databricks workspace path (file or folder):

```bash
dbx-sync <local-path> <remote-workspace-path>
```

Sync a single workspace folder with a single local folder (one-time):

```bash
dbx-sync ./local-project /Workspace/Users/me/project
```

Sync a single local file to a workspace folder, using the source filename for the target object:

```bash
dbx-sync ./local-project/notebook.py /Workspace/Users/me/project
```

Sync a single workspace file or notebook to a local folder, using the source filename locally:

```bash
dbx-sync ./local-project /Workspace/Users/me/project/notebook
```

Sync explicit local and workspace file paths:

```bash
dbx-sync ./local-project/notebook.py /Workspace/Users/me/project/notebook
```

Preview actions without applying them:

```bash
dbx-sync ./local-project /Workspace/Users/me/project --dry-run
```

Continuously watch and resync (default polling happens every second):

```bash
dbx-sync ./local-project /Workspace/Users/me/project --watch
```

Use `--force` to clear saved sync state before a fresh pass. This can be useful to handle conflicts.

Pro-tip: add `--dry-run` to check force behavior before running it for real.

Force options are mutually exclusive and only apply to a single sync pass:

- `--force` clears saved sync state before comparing local and remote files.
- `--force-upload` uploads matching local files even when saved sync state would otherwise skip them.
- `--force-download` downloads matching remote files even when saved sync state would otherwise skip them.

```bash
dbx-sync ./local-project /Workspace/Users/me/project --force
```

Override optional settings when needed:

```bash
dbx-sync ./local-project /Workspace/Users/me/project \
	--profile WORKSPACE \
	--poll-interval 5 \
	--log-level DEBUG \
```

Watch mode cannot be combined with force options or dry-run mode. Use `--watch` for continuous syncing, or use `--dry-run`, `--force`, `--force-upload`, and `--force-download` for one-time sync passes.

If your local directory does not exist, the tool will attempt to create it for you (when not in dry-run mode).

## Notes on Jupyter Notebooks

 Jupyter notebooks are represented the same as other notebooks when using Databricks CLI `databricks workspace list`. For cases where there is not a matching local `.ipynb` file, we export those files as `.py`.

 You can manually export them as `.ipynb` first if you wish to avoid this, using `databricks workspace export <FILE> --format JUPYTER --file <FILE>.ipynb`.

## Alternatives
Yes, I recognize there are a variety of official ways to do something close to this, but none of them fit my desired workflow well. So here are some references for alternatives.

- Databricks CLI workspace commands (`import`, `import-dir`, `export`, `export-dir`, `sync`, and related commands): <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/commands/>
- Databricks extension for Visual Studio Code: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext/>
- Databricks Asset Bundles documentation: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/>
- Databricks Git folders: <https://learn.microsoft.com/en-us/azure/databricks/repos/>

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for local development, testing, release, and repository workflow details.

## License

MIT. See [LICENSE](./LICENSE).
