---
name: register-project
description: Draft a project_data.json register data order with the catalog MCP tools,
  or answer questions about an existing project file.
---

# Register projects

A `project_data.json` lists the register columns a study orders: each source is one
register variant over a period, and each binding is one variable delivered from it. The
agent drafts and checks the file. The document's schema is
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
   three-part `<provider>/<register>/<variant>`. For a dated period it reaches the
   variant's whole succession family (`variant_family` in `states`): name the family's
   newest variant, and each year is ordered from the member that delivers it, so one
   source covers e.g. LISA 16+ and 15+ years. `"_default"` reaches the named variant
   only. A binding's `variable` is the variable's FQID; its `type` is one of `id`,
   `categorical`, `numeric`, `date`, `datetime`, `opaque`.
3. Write periods in the document's own grammar, which is not the `period` argument of
   the read tools: `{"from": 2015, "to": 2019}` is a range, `[2015, 2019]` is two
   separate years, `2019` is one year. A range string such as `"2015..2019"` is refused
   with `invalid_period`. Set `extract_format` only when the user names a file format
   for the extract: `parquet` or `csv`. Absent means Stata `.dta`.
4. Call `order` with `operation: "validate"` and the document as `project`, on the same
   server whose `show` root gave the `steward`. Each catalog validates against its own
   holdings; another server refuses the document with `steward_mismatch`. Fix every
   issue whose `level` is `error` and validate again until `ok` is true; report the
   warnings to the user.
5. Save the file as `project_data.json`.

## Questions about my project

1. Read the user's `project_data.json`. Its `steward` names the catalog it belongs to;
   if this server's `show` root reports a different `id`, say so and stop, because every
   answer below would come from the wrong holdings.
2. Call `order` with `operation: "order"` and the document as `project`. The manifest
   lists the table and column delivering each binding, and clips what is not available.
   Answer what will be delivered from it, not from the document.
3. For what a variable means or how it changed, use `show`, `states` and `values` over
   the project's period, and report the data warnings that `states` carries.
