# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). Bundles two skills:

| Skill | Purpose |
|---|---|
| `init-mona-project` | Scaffold a local R environment for an existing SCB MONA research project (mock data, templates, guardrails). |
| `registry-metadata-search` | Query register metadata (variables, value codes, schemas) via the `regmeta` CLI. |

## Prerequisites

The skills wrap two Python CLIs. Install them before enabling the plugin:

```bash
uv tool install regmeta
uv tool install mock-data-wizard
regmeta maintain update --yes   # pull the latest metadata DB
```

## Install

### Claude Code

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

Skills are then available as `/microdata-tools-se:init-mona-project` and
`/microdata-tools-se:registry-metadata-search`.

### Codex

For workspace-local testing, open this repo in Codex. The workspace
marketplace entry lives in `.agents/plugins/marketplace.json` and exposes
`./plugins/microdata-tools-se` as a local plugin.

If Codex does not pick up marketplace changes immediately, restart it and
then install `microdata-tools-se` from the workspace marketplace.

## Scope

The toolkit targets Swedish register-based work generally — research, report
writing, statistics production — not only MONA. `init-mona-project` is the
MONA-specific piece; `registry-metadata-search` works with any register whose
schema is in the `regmeta` DB.

## Personal data

MONA contains personal data. The skills never export row-level data; only
aggregate statistics. See the main repo `CLAUDE.md` for full safety rules.
