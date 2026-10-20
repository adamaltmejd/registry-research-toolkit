---
name: register-project
description: Draft and check a project's register data (project_data.json) with the
  catalog MCP tools, or answer questions about an existing project file.
---

# Register projects

A `project_data.json` lists the register data a study uses: each source is one register
variant over a period, and each binding is one variable delivered from it. The agent
drafts and checks the file. The document's schema is
[project_data.schema.json](project_data.schema.json), generated from the server's API
contract.

The catalog server's `search`, `show`, `states` and `coverage` tools are described in
the `register-metadata-search` skill.

## Drafting a project

1. Call `show` with no `ref`. `data.catalog` names the catalog this server serves: its
   `id`, `reg_meta_version` and `project_schema_version`. Set the document's `steward`
   to `id`, `reg_meta_version` to `reg_meta_version` and `schema_version` to
   `project_schema_version`. Never write these from memory.
2. Find each variable with `search` and `show`, then `states` for the register variant
   that delivers it over the study period. A source's `register_variant` is the
   three-part `<provider>/<register>/<variant>`; a binding's `variable` is the
   variable's FQID; its `type` is one of `id`, `categorical`, `numeric`, `date`,
   `datetime`, `opaque`.
3. Write periods in the document's own grammar, which is not the `period` argument of
   the read tools: `{"from": 2015, "to": 2019}` is a range, `[2015, 2019]` is two
   separate years, `2019` is one year. A range string such as `"2015..2019"` is refused
   with `invalid_period`.
4. Call `validate` with the document as `project`, on the same server whose `show` root
   gave the `steward`. Each catalog validates against its own holdings; another server
   refuses the document with `steward_mismatch`. Fix every issue whose `level` is
   `error` and validate again until `ok` is true; report the warnings to the user.
5. Save the file as `project_data.json`.

## Questions about my project

1. Read the user's `project_data.json`. Its `steward` names the catalog it belongs to;
   if this server's `show` root reports a different `id`, say so and stop, because every
   answer below would come from the wrong holdings.
2. Call `validate` with the document as `project`. Its issues say which bindings the
   catalog cannot deliver over the source's period; answer from them, not from the
   document alone.
3. For what a variable means or how it changed, use `show`, `states` and `values` over
   the project's period, and report the data warnings that `states` carries.
