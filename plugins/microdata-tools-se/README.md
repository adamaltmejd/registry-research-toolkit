# microdata-tools-se

Agent plugin for working with Swedish administrative register microdata (SCB,
Socialstyrelsen, and other holders). It declares the hosted catalog MCP server and
bundles four skills:

  | Component                    | Purpose                                                                                                                                             |
  | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `catalog` MCP server         | The hosted register metadata catalog: the global catalog `https://reg-meta.vqv.uk/mcp` by default, or a steward's catalog (below).                  |
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

## Steward catalogs

The global catalog documents what the data holders (SCB, Socialstyrelsen and others)
deliver. A steward, such as SWECOV, holds an extract of that data in MONA and runs its
own catalog: `https://data.swecov.se/mcp` for SWECOV. Point the plugin at your steward's
catalog when your project's data comes from the steward's holdings. Only a steward's
catalog:

- answers from the steward's holdings: the tables, years and columns it holds, under its
  own file and column names;
- writes the MONA order file (`order` with `operation: "materialize_order"`; the global
  catalog answers `holdings_unavailable`), which the steward's extract runner reads;
- gives `reg-mock` the delivery's own file names (`register-mock-data` skill).

A steward's catalog runs the toolkit release the steward pins, which can be older than
the global catalog's. The skills read the release from `meta.catalog_version` in each
tool response; use that release for the project's `reg_meta_version`, `reg-mock`,
`extract.R` and the export gate.

**Claude Code.** The catalog URL is the plugin option `catalog_url`. Set it when the
install dialog asks, or later with `/plugin configure microdata-tools-se@microdata-tools-se`
(or the plugin's Configure entry on the Installed tab of `/plugin`), then restart Claude
Code. Claude Code stores it in your user settings under
`pluginConfigs["microdata-tools-se@microdata-tools-se"].options.catalog_url`.

**Codex.** The Codex plugin always connects to the global catalog. Add the steward's
catalog as your own MCP server in `~/.codex/config.toml`, under a different name:

```toml
[mcp_servers.swecov_catalog]
url = "https://data.swecov.se/mcp"
```

Its tools are the same; tell the agent which server to use.

Some steward catalogs are not public and sit behind a sign-in. A URL setting alone does
not reach those; ask your steward how to connect.

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
