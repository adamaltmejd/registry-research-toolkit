# Registry Research Toolkit

Tools for working with Swedish registry microdata on [SCB MONA](https://www.scb.se/mona).

## Packages

| Package | Description |
|---|---|
| [`regmeta`](regmeta/) | Search and query SCB registry metadata |
| [`mock_data_wizard`](mock_data_wizard/) | Generate mock CSV data from MONA projects without exporting personal data |

## Install

### For AI agents

Point your agent at the [registry-metadata-search skill](https://github.com/adamaltmejd/registry-research-toolkit/tree/main/.claude/skills/registry-metadata-search) and ask it to install regmeta and use it. The skill contains install instructions, the full command reference, and common query workflows.

### Manual

```bash
# regmeta (metadata queries)
uv tool install "regmeta @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=regmeta"
regmeta maintain download

# mock-data-wizard (mock data generation, depends on regmeta)
uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

## Quick start

### regmeta

```bash
# Download the pre-built metadata database (~400 MB download, ~1.6 GB on disk)
regmeta maintain download

# Search for variables
regmeta search --query "kommun" --datacolumn

# Get register overview
regmeta get register LISA

# Get schema for specific years
regmeta get schema --register LISA --years 2020-2023
```

### mock-data-wizard

```bash
# Generate an R script to extract aggregate statistics on MONA
mock-data-wizard generate-script -p P1405

# Upload and run the script on MONA, then download stats.json
# Generate mock CSV files locally
mock-data-wizard generate --stats stats.json --seed 42
```

See per-package READMEs for full documentation:
- [regmeta/README.md](regmeta/README.md)
- [mock_data_wizard/README.md](mock_data_wizard/README.md)

## License

[MIT](LICENSE)
