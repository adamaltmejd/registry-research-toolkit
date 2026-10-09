---
name: register-metadata-search
description: Query Swedish register metadata (SCB, Socialstyrelsen and other holders)
  through the catalog MCP tools. Use when answering questions about Swedish register
  data — variable definitions, value codes, register schemas, column names, or how data
  is structured across registers and years.
---

# Register metadata queries

The plugin's `catalog` MCP server (hosted at `https://catalog.swecov.se/mcp`) answers
questions about Swedish administrative registers. It holds structural metadata —
registers, variables, value codes, classifications, documentation — not microdata.

## Tools

Each tool's parameters are in its MCP schema; this is what each one is for.

  | Tool       | Use it for                                                                                                                     |
  | ---------- | ------------------------------------------------------------------------------------------------------------------------------ |
  | `search`   | Free-text search over registers, variables, classifications and codes. Start here.                                             |
  | `show`     | The summary of any ref: a provider, register, variable, classification or group. No `ref` gives the catalog root.              |
  | `states`   | A variable's delivered representations over time: variant, period bounds, column, coding, data warnings.                       |
  | `values`   | A classification's codes, or the value set of one variable state (a `state_id` from `states`).                                 |
  | `coverage` | The years a register or variable is delivered, with gaps.                                                                      |
  | `schema`   | `operation`: `schema` (a register's or variable's delivered columns), `diff` (columns between two periods), `coded_variables`. |
  | `resolve`  | Delivered column names (data-file headers) to the variables that deliver them.                                                 |
  | `graph`    | `operation`: `graph` (succession graph) or `lineage` (source states feeding a variable).                                       |
  | `docs`     | `operation`: `docs_search`, `docs_get` (one documentation entry) or `docs_related` (a register's rehosted PDFs).               |
  | `order`    | `operation`: `validate` or `order` a `project_data.json` document (`project` argument).                                        |

## Conventions

- **Refs.** A `ref` is an FQID (`scb/lisa`, `scb/lisa/kon`, `class/sun1996`) or a bare
  name. A bare name that matches several entities fails with `ambiguous_ref`; repeat the
  call with one of the candidates' FQIDs.
- **Periods.** `period` takes `2019`, `2015..2019`, `LA2019`, `2019-03` or
  `2019-01-01..2019-06-30`. `diff` takes `from` and `to` in the same grammar.
- **Paging.** Open-ended lists return `{"items": [...], "next_cursor": ...}`. Pass
  `next_cursor` back as `cursor`; `limit` defaults to 50 (max 200).
- **Scope.** Leave `scope` unset. The hosted catalog serves `reference`; `holdings`
  needs a steward catalog and fails with `scope_unavailable` there.

## Responses

A successful call returns `{data, meta}`. `data` is the result; `meta` names the
`contract_version`, the catalog `generation` and the `scope` that answered.

A failed call is an MCP tool error carrying `{error, meta}`, where `error` is
`{code, class, message, remediation, fields}`. Act on `code` and follow `remediation`:

- `ambiguous_ref`: `fields.candidates` lists the FQIDs to choose from.
- `not_found`: search for the entity and use its FQID.
- `invalid_parameter`, `invalid_ref`, `invalid_period`: fix the named argument.
- `invalid_cursor`, `stale_cursor`: restart the listing without `cursor`.
- `rate_limited`: wait before retrying.

## Workflow

1. `search` for the concept (Swedish terms usually match best, e.g. `inkomst`).
2. `show` the variable or register FQID from the hit.
3. `states`, `values` and `coverage` for representations, codes and years.

If a tool behaves differently from this page, trust its MCP schema and file an issue at
<https://github.com/adamaltmejd/registry-research-toolkit/issues>.
