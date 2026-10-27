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
contract.

The catalog server's `search`, `show`, `states` and `coverage` tools are described in
the `register-metadata-search` skill.

## Drafting a project

1. Call `show` with no `ref`. `data.catalog` names the catalog this server serves: its
   `reg_meta_version` and `project_schema_version`. Set the document's
   `reg_meta_version` to `reg_meta_version` and `schema_version` to
   `project_schema_version`. Never write these from memory.
2. Find each variable with `search` and `show`, then `states` for the table that
   delivers it over the study period. A source's `table` is the table's public id,
   `<provider>/<register>/<table>`, or `<provider>/<register>` for a register with one
   table. A binding's `variable` is the variable's FQID; its `type` is one of `id`,
   `categorical`, `numeric`, `date`, `datetime`, `opaque`. A source may also set
   `edition` (`final`, `unstated` or `preliminary`) and `qualifier`.
3. Write periods in the document's own grammar, which is not the `period` argument of
   the read tools: `{"from": 2015, "to": 2019}` is a range, `[2015, 2019]` is two
   separate years, `2019` is one year. A range string such as `"2015..2019"` is refused
   with `invalid_period`.
4. Call `order` with `operation: "validate"` and the document as `project`, on the same
   server whose `show` root gave the versions. Fix every issue whose `level` is `error`
   and validate again until `ok` is true; report the warnings to the user.
5. Save the file as `project_data.json`.

## Questions about my project

1. Read the user's `project_data.json`. If its `schema_version` differs from this
   server's `project_schema_version`, say so and stop: the server reads one version
   exactly.
2. Call `order` with `operation: "order"` and the document as `project`. The manifest
   lists the table and column delivering each binding, and clips what is not available.
   Answer what will be delivered from it, not from the document.
3. For what a variable means or how it changed, use `show`, `states` and `values` over
   the project's period, and report the data warnings that `states` carries.
