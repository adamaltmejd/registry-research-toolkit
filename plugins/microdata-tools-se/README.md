# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). Bundles two skills:

| Skill | Purpose |
|---|---|
| `init-mona-project` | Scaffold a local R environment for an existing SCB MONA research project (mock data, templates, guardrails). |
| `register-metadata-search` | Query register metadata (variables, value codes, schemas) via the `regmeta` CLI. |

## Prerequisites

The skills wrap two Python CLIs. Install them before enabling the plugin:

```bash
uv tool install regmeta
uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
regmeta maintain update --yes   # pull the latest metadata DB
```

## Install

### Claude Code

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

Skills are then available as `/microdata-tools-se:init-mona-project` and
`/microdata-tools-se:register-metadata-search`.

### Codex

Add the marketplace from the public GitHub repo:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit@plugin-restructure
```

Then open the Codex plugin marketplace, find `microdata-tools-se` under
`registry-research-toolkit`, and install it.

The branch ref is only needed for this prerelease build. After the plugin is
merged, the `@plugin-restructure` suffix should no longer be needed.

## Scope

The toolkit targets Swedish register-based work generally — research, report
writing, statistics production — not only MONA. `init-mona-project` is the
MONA-specific piece; `register-metadata-search` works with any register whose
schema is in the `regmeta` DB.

## Personal data

MONA contains personal data. The skills never export row-level data; only
aggregate statistics.

## Support

Source code and issue tracker:
[adamaltmejd/registry-research-toolkit](https://github.com/adamaltmejd/registry-research-toolkit)

If the plugin behaves unexpectedly or the documentation is unclear, please
file an issue.
