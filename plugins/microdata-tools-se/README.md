# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). It declares one MCP server and bundles one skill:

  | Component                  | Purpose                                                                        |
  | -------------------------- | ------------------------------------------------------------------------------ |
  | `catalog` MCP server       | The hosted register metadata catalog at `https://catalog.swecov.se/mcp`.       |
  | `register-metadata-search` | Skill: how to query variables, value codes and schemas with the catalog tools. |

Nothing to install beyond the plugin: the tools run on the hosted server.

## Install

### Claude Code

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

The tools appear as `mcp__plugin_microdata-tools-se_catalog__<tool>`, and the skill as
`/microdata-tools-se:register-metadata-search`.

### Codex

Add the marketplace from the public GitHub repo:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit
```

Then open the Codex plugin marketplace, find `microdata-tools-se` under
`registry-research-toolkit`, and install it.

## Local catalog

To query the catalog without the hosted server, run it locally over stdio with
`reg-meta mcp --db <dir>`, where `<dir>` holds an unpacked `reg_meta.db` (and optionally
`reg_meta_docs.db`). The `reg-meta` binary for macOS arm64 and Linux x86_64 and the
catalog DBs ship on each `reg_meta/v*` GitHub release; the download, SHA-256 check and
`zstd` unpack recipe is in the [repository README's Local server
section](https://github.com/adamaltmejd/registry-research-toolkit#local-server).

Then register the local server with your agent host, for example:

```bash
claude mcp add catalog-local -- /path/to/reg-meta mcp --db /path/to/catalog
codex mcp add catalog-local -- /path/to/reg-meta mcp --db /path/to/catalog
```

The local server answers the same tools from your machine; no query is sent to the
hosted server.

## Scope

The toolkit targets Swedish register-based work generally — research, report writing,
statistics production — not only MONA. The catalog holds structural metadata (registers,
variables, value codes, classifications, documentation), not microdata.

## Personal data

MONA contains personal data. The plugin never handles row-level data, and the catalog
holds none. Queries are sent to the hosted server; see [PRIVACY.md](PRIVACY.md).

## Support

Source code and issue tracker:
[adamaltmejd/registry-research-toolkit](https://github.com/adamaltmejd/registry-research-toolkit)

If the plugin behaves unexpectedly or the documentation is unclear, please file an
issue.
