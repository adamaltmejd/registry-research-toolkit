# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). It declares the hosted catalog MCP server and
bundles four skills:

  | Component                    | Purpose                                                                                                                                             |
  | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `catalog` MCP server         | The hosted register metadata catalog: the global catalog `https://reg-meta.vqv.uk/mcp` by default, or a data steward's catalog (below).             |
  | `register-metadata-search`   | Skill: how to query variables, value codes and schemas with the catalog tools.                                                                      |
  | `register-project`           | Skill: drafting and validating a `project_data.json` data order, and answering questions about a project.                                           |
  | `register-mock-data`         | Skill: local mock data shaped like the project's delivery, with `reg-mock`.                                                                         |
  | `register-research-scaffold` | Skill: scaffolding a batch-based register research project with mock validation, restricted execution (MONA or another server) and the export gate. |

The catalog tools need nothing beyond the plugin. Mock data needs the `reg-mock` binary
(the [repository README's Mock data
section](https://github.com/adamaltmejd/registry-research-toolkit#mock-data)), and a
project's export gate needs R where the pipeline runs.

## Install

### Claude Code

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

The tools appear as `mcp__plugin_microdata-tools-se_catalog__<tool>`, and the skills as
`/microdata-tools-se:<skill>`.

### Codex

Add the marketplace from the public GitHub repo:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit
```

Then open the Codex plugin marketplace, find `microdata-tools-se` under
`registry-research-toolkit`, and install it.

## Data stewards

The default catalog is the global one, `https://reg-meta.vqv.uk/mcp`: what the data
holders deliver. A data steward holds an extract of that data in MONA and runs its own
deployment of the catalog at `https://<steward-host>/mcp`. Its catalog adds the
steward's holdings (the tables, years and columns it holds, under its own file names)
and writes the MONA order file for a project (`materialize_order`). Point the plugin at
a steward's catalog when your project's data comes from that steward.

The simplest way is to tell the agent, for example "use the plugin with the
`<steward-host>` steward". The `register-metadata-search` skill's
[data-stewards reference](skills/register-metadata-search/references/data-stewards.md)
has the steps it follows:

1. Check that `https://<steward-host>/api/context` answers JSON naming the steward
   (`data.steward_config.presentation`) and its release (`data.reg_meta_version`).
2. Set the plugin option `catalog_url` to `https://<steward-host>/mcp`: in Claude Code,
   `/plugin configure microdata-tools-se@microdata-tools-se`, then restart. In Codex,
   add the URL as your own server in `~/.codex/config.toml`
   (`[mcp_servers.steward_catalog]`, `url = "https://<steward-host>/mcp"`).
3. Confirm: a catalog tool's `meta.catalog_version` matches the release `/api/context`
   reported, and `materialize_order` no longer answers `holdings_unavailable`.

A steward runs the toolkit release it pins, which can be older than the global
catalog's; the project, `reg-mock`, `extract.R` and the export gate follow that release.

Some stewards sit behind Cloudflare Access: `/api/context` then redirects to a sign-in
page or answers `403`. The plugin option cannot reach those. Ask the steward for a
service token and add the catalog as your own MCP server with its headers (the reference
has the command).

## Scope

The toolkit targets Swedish register-based work generally — research, report writing,
statistics production — not only MONA. The catalog holds structural metadata (registers,
variables, value codes, documentation), not microdata.

## Personal data

MONA contains personal data. No agent runs on MONA, and row-level data never leaves it:
only aggregates that pass the export gate and a human's review. The plugin never handles
row-level data, and the catalog holds none. Queries go to the catalog server you set;
see [PRIVACY.md](PRIVACY.md).

## Support

Source code and issue tracker:
[adamaltmejd/registry-research-toolkit](https://github.com/adamaltmejd/registry-research-toolkit)

If the plugin behaves unexpectedly or the documentation is unclear, please file an
issue.
