# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). Bundles one skill:

  | Skill                      | Purpose                                                                           |
  | -------------------------- | --------------------------------------------------------------------------------- |
  | `register-metadata-search` | Query register metadata (variables, value codes, schemas) via the `reg-meta` CLI. |

## Prerequisites

The skill wraps the `reg-meta` CLI. Install it before enabling the plugin:

```bash
uv tool install reg-meta
reg-meta update --yes   # pull the latest metadata DB
```

The CLI checks for updates on startup. Upgrade explicitly with `reg-meta update`
(package + DB).

## Install

### Claude Code

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

The skill is then available as `/microdata-tools-se:register-metadata-search`.

### Codex

Add the marketplace from the public GitHub repo:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit
```

Then open the Codex plugin marketplace, find `microdata-tools-se` under
`registry-research-toolkit`, and install it.

## Scope

The toolkit targets Swedish register-based work generally — research, report writing,
statistics production — not only MONA. `register-metadata-search` works with any
register whose schema is in the `reg_meta` DB.

## Personal data

MONA contains personal data. The skill never exports row-level data; only aggregate
statistics.

## Support

Source code and issue tracker:
[adamaltmejd/registry-research-toolkit](https://github.com/adamaltmejd/registry-research-toolkit)

If the plugin behaves unexpectedly or the documentation is unclear, please file an
issue.
