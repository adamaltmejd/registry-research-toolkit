# Registry Research Toolkit

Tools for working with Swedish registry microdata on [SCB
MONA](https://www.scb.se/mona).

  | Package                             | Description                                                             |
  | ----------------------------------- | ----------------------------------------------------------------------- |
  | [`reg_meta`](reg_meta/)             | Search and query SCB registry metadata (CLI `reg-meta`)                 |
  | [`reg_meta_build`](reg_meta_build/) | Build the `reg_meta` metadata DBs from agency exports (maintainer-only) |
  | [`reg_schema`](reg_schema/)         | `project_data.json` schema and structural validator                     |
  | [`reg_webapp`](reg_webapp/)         | Web app (FastAPI + Svelte): catalog browse + project authoring          |

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for how the packages fit together.

## Prerequisites

**Python 3.14+** and **uv** (Python package manager).

macOS:

```sh
brew install python   # or download from python.org
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Windows:

```powershell
winget install Python.Python.3.14   # or download from python.org
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

See [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/) for
other methods.

## Install

### Agent plugin (recommended)

The toolkit ships as the `microdata-tools-se` plugin. In Claude Code:

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

In Codex:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit
```

then install `microdata-tools-se` from the plugin marketplace UI.

This bundles the `/microdata-tools-se:register-metadata-search` skill and keeps it
updated through the plugin host. The skill uses the underlying CLI below; install it
once per machine.

### CLIs

```bash
uv tool install reg-meta
reg-meta update            # download metadata DB (~400 MB compressed)
```

## Quick start

### reg_meta

```bash
reg-meta update                              # download metadata DB
reg-meta search --query "kommun"                      # search variables
reg-meta get register LISA                            # register overview
reg-meta get schema --register LISA --years 2020      # columns for a year
reg-meta docs search "disponibel inkomst"             # search documentation
```

See the [reg_meta README](reg_meta/README.md) for details.

## Development

Use isolated branches and reviewed pull requests. Run the relevant lint and test
commands in [AGENTS.md](AGENTS.md); pull requests require green CI and the maintainer's
review before merge. Coordinate changes to main so only one integration advances it at a
time.

## License

[MIT](LICENSE)
