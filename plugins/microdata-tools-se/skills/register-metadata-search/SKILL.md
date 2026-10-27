---
name: register-metadata-search
description: Query Swedish register metadata (SCB, Socialstyrelsen and other holders)
  through the catalog MCP tools. Use when answering questions about Swedish register
  data — variable definitions, value codes, register schemas, column names, or how data
  is structured across registers and years.
---

# Register metadata queries

The plugin's `catalog` MCP server answers questions about Swedish administrative
registers. It serves the published global catalog (`https://reg-meta.vqv.uk/mcp`);
`show` with no `ref` lists its providers. It holds structural metadata — registers,
tables, variables, value codes, documentation — not microdata. Classifications are not
served yet.

## Tools

Each tool's parameters are in its MCP schema; this is what each one is for.

  | Tool       | Use it for                                                                                                                                                         |
  | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `search`   | Free-text search over registers, tables, variables, codes and documents. Start here.                                                                               |
  | `show`     | The summary of any ref: a provider, register, table or variable. No `ref` lists the providers.                                                                     |
  | `segments` | A variable's segments over time: table, period, spelling, type, value set and `state` (`clear`, `ambiguous` or `preliminary`). `evidence=true` adds occurrences.   |
  | `values`   | The codes of a value set, by its `value_set` hash (from a segment), or the value sets holding a `code` and `label` (from a `code` search hit).                     |
  | `coverage` | The years a register, table or variable is delivered, with gaps.                                                                                                   |
  | `schema`   | `operation`: `schema` (a register's, table's or variable's delivered columns) or `diff` (columns between two periods). Coded variables: `search` with type `code`. |
  | `resolve`  | Delivered column names (data-file headers) to the variables that deliver them.                                                                                     |
  | `graph`    | A ref's relations, narrowed by `kind`: `continues`, `derived_from`, `equivalent` or `label_of`. Lineage is `kind=derived_from`.                                    |
  | `docs`     | `operation`: `docs_search`, `docs_get` (one documentation entry) or `docs_related` (the documents of a register, table or variable).                               |
  | `order`    | `operation`: `validate` or `order` a `project_data.json` document (`project` argument); see the `register-project` skill.                                          |

## Conventions

- **Refs.** A `ref` is an FQID (`scb/lisa`, `scb/lisa/kon`) or a bare name: a register,
  table or variable name, matched case-insensitively; a slug matches only where it is
  also the name. A bare name that matches several entities fails with `ambiguous_ref`;
  repeat the call with one of the candidates' FQIDs.
- **Periods.** `period` takes `2019`, `2015..2019`, `LA2019`, `2019-03` or
  `2019-01-01..2019-06-30`. `diff` takes `from` and `to` in the same grammar.
- **Paging.** Open-ended lists return `{"items": [...], "next_cursor": ...}`. Pass
  `next_cursor` back as `cursor`; `limit` defaults to 50 (max 200).

## Responses

A successful call returns `{data, meta}`. `data` is the result; `meta` names the
`contract_version`, the catalog `generation` and the `catalog_version`.

A failed call is an MCP tool error carrying `{error, meta}`, where `error` is
`{code, class, message, remediation, fields}`. Act on `code` and follow `remediation`:

- `ambiguous_ref`: `fields.candidates` lists the FQIDs to choose from.
- `not_found`: search for the entity and use its FQID.
- `retired_ref`: the id was retired in `fields.retired_in`; search for the entity again.
- `table_is_register`: the ref is a register with named tables; pick one from
  `fields.tables`.
- `order_blocked`: `fields.findings` lists every blocking issue of the order.
- `invalid_parameter`, `invalid_ref`, `invalid_period`, `project_invalid`: fix the named
  argument or the project.
- `invalid_cursor`, `stale_cursor`: restart the listing without `cursor`.
- `docs_unavailable`: the catalog has no documents.
- `rate_limited`: wait before retrying.

## Workflow

1. `search` for the concept (Swedish terms usually match best, e.g. `inkomst`).
2. `show` the variable or register FQID from the hit.
3. `segments`, `values` and `coverage` for segments, codes and years.

If a tool behaves differently from this page, trust its MCP schema and file an issue at
<https://github.com/adamaltmejd/registry-research-toolkit/issues>.
