# Registry Research Toolkit

Tools for working with Swedish registry microdata on [SCB MONA](https://www.scb.se/mona).

## Packages

| Package | Description |
|---|---|
| [`regmeta`](regmeta/) | Search and query SCB registry metadata |
| [`mock_data_wizard`](mock_data_wizard/) | Generate mock CSV data from MONA projects without exporting personal data |

## Install

```bash
# regmeta (metadata queries)
uv add "regmeta @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=regmeta"

# mock-data-wizard (mock data generation, depends on regmeta)
uv add "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
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

## Development

```bash
git clone https://github.com/adamaltmejd/registry-research-toolkit.git
cd registry-research-toolkit
uv sync --group dev
```

### Testing

```bash
uv run python -m pytest                            # unit tests only
uv run python -m pytest --run-integration           # include Docker integration tests
```

Expensive test suites are gated behind `--run-<name>` flags. To add a new category, add an entry to `OPTIONAL_MARKERS` in `conftest.py` and decorate tests with `@pytest.mark.<name>`.

## License

[MIT](LICENSE)
