# Registry Research Toolkit

Tools for working with Swedish registry microdata on
[SCB MONA](https://www.scb.se/mona).

| Package | Description |
|---|---|
| [`regmeta`](regmeta/) | Search and query SCB registry metadata |
| [`mock_data_wizard`](mock_data_wizard/) | Generate mock CSV data from MONA projects without exporting personal data |

## Prerequisites

**Python 3.11+** and **uv** (Python package manager).

| | macOS | Windows |
|---|---|---|
| Python | `brew install python` or [python.org](https://www.python.org/downloads/) | [python.org](https://www.python.org/downloads/) or `winget install Python.Python.3.12` |
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` | `powershell -c "irm https://astral.sh/uv/install.ps1 \| iex"` |

See [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/)
for other methods.

## Install

### For AI agents

Point your agent at the
[registry-metadata-search skill](https://github.com/adamaltmejd/registry-research-toolkit/tree/main/.claude/skills/registry-metadata-search)
and ask it to install regmeta and use it. The skill contains install
instructions, the full command reference, and common query workflows.

### Manual

```bash
# regmeta (metadata queries)
uv tool install regmeta
regmeta maintain update

# mock-data-wizard (mock data generation, depends on regmeta)
uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

## Quick start

### regmeta

```bash
regmeta maintain update                              # download metadata DB
regmeta search --query "kommun"                      # search variables
regmeta get register LISA                            # register overview
regmeta get schema --register LISA --years 2020      # columns for a year
regmeta docs search "disponibel inkomst"             # search documentation
```

### mock-data-wizard

```bash
mock-data-wizard generate-script -p P1405            # R script for MONA
# Run on MONA, download stats.json
mock-data-wizard generate --stats stats.json --seed 42   # mock CSVs
```

See per-package READMEs for details:
[regmeta](regmeta/README.md) |
[mock_data_wizard](mock_data_wizard/README.md)

## License

[MIT](LICENSE)
