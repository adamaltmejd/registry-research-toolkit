---
name: register-metadata-search
description: Query SCB register metadata using the reg-meta CLI. Use when answering questions about Swedish register data — variable definitions, value codes, register schemas, column names, or how data is structured across registers and years.
---

# reg-meta — Register Metadata Queries

You have access to `reg-meta`, a CLI tool for querying SCB (Statistics Sweden)
register metadata. The database contains structural metadata about registers
— not microdata.

## Install

If `reg-meta` is not yet installed:

```bash
uv tool install reg-meta
reg-meta update --yes
```

## Learning the tool

Run these to understand what reg-meta can do and how to use it:

```bash
reg-meta --examples              # usage examples and workflows
reg-meta --help                  # full command reference with syntax
reg-meta <command> --help        # detailed help for a specific command
reg-meta <command> --examples    # examples for a specific command
```

Use `--format json` when you need structured output for further processing.

## Troubleshooting

If a command fails unexpectedly or flags seem wrong:

1. Run `reg-meta --help` to see the current command reference.
2. Run `reg-meta <command> --help` for current flags and examples.
3. If the behavior still does not match these instructions, trust the CLI
   help output and file an issue at
   <https://github.com/adamaltmejd/registry-research-toolkit/issues>.
