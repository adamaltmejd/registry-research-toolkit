---
name: register-metadata-search
description: Query SCB register metadata using the regmeta CLI. Use when answering questions about Swedish register data — variable definitions, value codes, register schemas, column names, or how data is structured across registers and years.
---

# regmeta — Register Metadata Queries

You have access to `regmeta`, a CLI tool for querying SCB (Statistics Sweden)
register metadata. The database contains structural metadata about registers
— not microdata.

## Install

If `regmeta` is not yet installed:

```bash
uv tool install regmeta
regmeta maintain update --yes
```

## Learning the tool

Run these to understand what regmeta can do and how to use it:

```bash
regmeta --examples              # usage examples and workflows
regmeta --help                  # full command reference with syntax
regmeta <command> --help        # detailed help for a specific command
regmeta <command> --examples    # examples for a specific command
```

Use `--format json` when you need structured output for further processing.

## Troubleshooting

If a command fails unexpectedly or flags seem wrong:

1. Run `regmeta --help` to see the current command reference.
2. Run `regmeta <command> --help` for current flags and examples.
3. If the behavior still does not match these instructions, trust the CLI
   help output and file an issue at
   <https://github.com/adamaltmejd/registry-research-toolkit/issues>.
