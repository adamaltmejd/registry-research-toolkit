---
name: register-project
description: Draft a project_data.json register data order with the catalog MCP tools,
  or answer questions about an existing project file.
---

# Register projects

A `project_data.json` lists the register columns a study orders: each source is one
table over a period, and each binding is one variable delivered from it. The agent
drafts and checks the file. The document's schema is
[project_data.schema.json](project_data.schema.json), generated from the server's API
contract. Regenerate it with `uv run --no-project scripts/gen_project_schema.py` from
the repository root.

The catalog server's `search`, `show`, `segments` and `coverage` tools are described in
the `register-metadata-search` skill.

## Drafting a project

1. Set the document's `schema_version` to `"5.1.0"`. Set `reg_meta_version` (required)
   to the serving catalog's release tag, `reg_meta/vX.Y.Z`. Read `meta.catalog_version`
   from a catalog tool response and format its release version `X.Y.Z` as that tag. If
   it is not a release version (for example, `dev`), ask which catalog release to use.
   Never infer it from the local executable or from memory.
2. Find each variable with `search` and `show`, then `segments` for the table that
   delivers it over the study period. A source is a `table` and a `period`. `table` is
   the table's public id, `<provider>/<register>/<table>`, or `<provider>/<register>`
   for a register with only its implicit table. A binding names a variable by FQID and
   has a `type`, one of `id`, `categorical`, `numeric`, `date`, `datetime`, `opaque`. A
   panel lists source names. A source cannot select an edition or a qualifier; final
   data is served over preliminary where both exist.
3. Write periods in the document's own grammar, which is not the `period` argument of
   the read tools: `{"from": 2015, "to": 2019}` is a range, `[2015, 2019]` is two
   separate years, `2019` is one year. A range string such as `"2015..2019"` is refused
   with `invalid_period`.
4. Call `order` with `operation: "validate"` and the document as `project`. Fix every
   issue whose `level` is `error` and validate again until `ok` is true; report the
   warnings to the user.
5. Save the file as `project_data.json`.

## Questions about my project

1. Read the user's `project_data.json`. If its `schema_version` is not `"5.1.0"`, say so
   and stop: the server reads one version exactly.
2. Call `order` with `operation: "order"` and the document as `project`. It returns the
   logical order: per source, the table, the period and the entries, each with the
   spelling delivered. Ambiguous windows block it: the call fails with `order_blocked`
   and lists every issue, and `validate` lists the same issues. A window only a
   preliminary release covers does not block; it is warned (`preliminary_data`), so tell
   the user. There are no physical files yet; give the user the logical order and the
   project file. Answer what will be delivered from the order, not from the document.
3. For what a variable means or how it changed, use `show`, `segments` and `values` over
   the project's period.
