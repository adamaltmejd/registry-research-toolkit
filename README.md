# Registry Research Toolkit

Tools for working with Swedish registry microdata on
[SCB MONA](https://www.scb.se/mona).

| Package | Description |
|---|---|
| [`reg_meta`](reg_meta/) | Search and query SCB registry metadata |
| [`mock_data_wizard`](mock_data_wizard/) | Generate mock CSV data from MONA projects without exporting personal data |

## Prerequisites

**Python 3.13+** and **uv** (Python package manager).

| | macOS | Windows |
|---|---|---|
| Python | `brew install python` or [python.org](https://www.python.org/downloads/) | [python.org](https://www.python.org/downloads/) or `winget install Python.Python.3.13` |
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` | `powershell -c "irm https://astral.sh/uv/install.ps1 \| iex"` |

See [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/)
for other methods.

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

This bundles two skills —
`/microdata-tools-se:register-metadata-search` and
`/microdata-tools-se:init-mona-project` — and keeps them updated through the
plugin host. The skills use the underlying CLIs below; install those once per
machine.

### CLIs

```bash
uv tool install reg-meta
reg-meta update            # download metadata DB (~400 MB compressed)

uv tool install mock-data-wizard   # depends on reg_meta
```

Both CLIs check for updates on startup and ship explicit upgrade paths
(`reg-meta update`, `mock-data-wizard update`).

## Quick start

### reg_meta

```bash
reg-meta update                              # download metadata DB
reg-meta search --query "kommun"                      # search variables
reg-meta get register LISA                            # register overview
reg-meta get schema --register LISA --years 2020      # columns for a year
reg-meta docs search "disponibel inkomst"             # search documentation
```

### mock-data-wizard

```bash
mock-data-wizard build-bundle                        # build mdw_runner.py
# Upload the bundle to MONA, edit configure(), run `python mdw_runner.py`,
# download mdw_step3_stats.json
mock-data-wizard generate --stats mdw_step3_stats.json --seed 42   # mock CSVs
```

See per-package READMEs for details:
[reg_meta](reg_meta/README.md) |
[mock_data_wizard](mock_data_wizard/README.md)

## License

[MIT](LICENSE)
