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
contract. Regenerate it with
`REG_META_BLESS=1 cargo test -p xtask --test project_schema_snapshot` from the
repository root.

The catalog server's `search`, `show`, `segments` and `coverage` tools are described in
the `register-metadata-search` skill.

## Drafting a project

1. Set the document's `schema_version` to `"6.2.0"`. Set `reg_meta_version` (required)
   to the serving catalog's release tag, `reg_meta/vX.Y.Z`. Read `meta.catalog_version`
   from a catalog tool response and format its release version `X.Y.Z` as that tag. If
   it is not a release version (for example, `dev`), ask which catalog release to use.
   Never infer it from the local executable or from memory.
2. Find each variable with `search` and `show`, then `segments` for the table that
   delivers it over the study period. To list the variables on one table, page
   `schema` (`ref` = the table, `period`, `limit`, `cursor`); `show` on a large register
   returns every table and variable at once. A source is a `table` and a `period`.
   `table` is the table's public id, `<provider>/<register>/<table>`, or
   `<provider>/<register>` for a register with only its implicit table. A binding names a variable by FQID; its
   type is the catalog's (`segments`). Set the optional `type` only when the delivered
   column is known to differ from the catalog, in the catalog's type names (`integer`,
   `decimal`, `text`, `date`, `datetime`, `boolean`, `geometry`, `uuid`, `binary`,
   `identifier`). It covers the whole binding: if only some years differ, split them
   into their own source by period. A `binding_type_differs` warning from `validate`
   then names both types; report it to the user, as the catalog may need fixing. A panel
   lists source names. A source cannot select an edition or a qualifier; final data is
   served over preliminary where both exist.
3. Write periods in the document's own grammar, which is not the `period` argument of
   the read tools: `{"from": 2015, "to": 2019}` is a range, `[2015, 2019]` is two
   separate years, `2019` is one year. A range string such as `"2015..2019"` is refused
   with `invalid_period`. `"_default"` is the period of a timeless table, one that is
   delivered once and not per year (`show` on the table says `timeless`). On a dated
   table it is the error `period_default_on_dated_table`, also for registers that are in
   effect a snapshot (RTB, Flergenreg): each year there is a separate delivery of the
   whole table, so pick the year whose state the study needs, usually the latest year
   `coverage` lists, and ask the user when it matters.
4. "Database" means a catalog database, not a data holder: the global catalog is one
   database, and a steward's catalog may hold several. Sources from several global
   providers (SCB, Pliktverket, Riksarkivet) sit in one project without `linkage`. A
   steward whose data sits in several databases, each with its own person key, needs
   `"linkage": {"database": "<id>"}`: `validate` names the databases in a
   `database_required` error, and `segments` gives each held row its `space`, the id of
   the database the column is in. One project draws from one of a steward's databases. A
   variable that database lacks is a `not_in_database` error naming the databases that
   have it. Never merge two databases into one project: make a separate project for each
   database. The researcher joins the extracts later, outside this toolkit. A steward
   with one database needs no `linkage`.
5. Data the catalog does not document can be entered by hand, but only after `resolve`,
   `search` and `show` find no variable or table for it: a catalog binding is checked
   and extracted, a manual one is neither. A column the delivery has that a documented
   table lacks is a binding `{"manual": {"column": "<delivery name>", "type": "<type>"}}`
   in that table's source; the source keeps at least one catalog binding. A file or
   years the catalog has no table for is a source with
   `"manual": {"provider": "<name>", "register": "<name>"}` in place of `table`, holding
   manual bindings only. Optional `codes` lists the column's codes
   (`[{"code": "HT", "label": "Hösttermin"}]`, `"sentinel": true` for a missing-value
   code), taken from documentation, never from values read on MONA, and never on an
   `identifier` column. `validate` reports each such source with a `manual_item` info;
   tell the user that the extract runner does not deliver these columns, so the data
   manager adds them by hand.
6. Call `order` with `operation: "validate"` and the document as `project`. Fix every
   issue whose `level` is `error` and validate again until `ok` is true; report the
   warnings to the user. A `classification_version_ambiguous` warning means the variable
   carries several versions of a code list (municipality codes, say) over the period and
   the catalog cannot tell which one the delivered codes follow. Pin one with the
   binding's `classification`, a version id `class/<family>/<version>` (`show class`
   lists the families and versions). A pin delivers only the years where the variable
   carries that version and clips the rest (`period_clipped`); a version it has no codes
   from over the period is a `classification_incompatible` error. To cover several
   versions, split the source by period and pin each. Ignore the warning when the column
   is only carried along, not used to join or recode across years.
7. Save the file as `project_data.json`.

Every ordered table also delivers its row-key columns, including the person key,
without a binding: `order` lists them per source and period in `row_keys`, under the
register's own spelling (`PersonNr` or `personnr` on SCB's registers, `LopNr` on a
steward's catalog). No binding is needed. The Flergenreg reused-personnummer flags are
ordinary variables: `scb/flergenreg/ateranvant-personnr` and the per-parent
`aterpnrfar`, `aterpnrmor` and adoptive variants, code `1` for a reused personnummer and
`0` for not. Bind them when needed. `SenPNr` is only in `sos/lova`.

A minimal document, checked against catalog 0.84.1 (set `reg_meta_version` as in step
1). `sources` is a list, each source holds its own `bindings`, and `period` is a range in
one source and a year list in the other:

```json
{
  "schema_version": "6.2.0",
  "reg_meta_version": "reg_meta/v0.84.1",
  "name": "example",
  "sources": [
    {
      "name": "deaths",
      "table": "scb/rtb/doda-kvartal",
      "period": {"from": 2015, "to": 2019},
      "bindings": [
        {"variable": "scb/rtb/alderslut"},
        {"variable": "scb/rtb/civilstand"}
      ]
    },
    {
      "name": "enlistment",
      "table": "riksarkivet/inskrivning",
      "period": [1990, 1995],
      "bindings": [
        {"variable": "riksarkivet/inskrivning/ar-for-inskrivning"},
        {"variable": "riksarkivet/inskrivning/forsvarsgren"}
      ]
    }
  ]
}
```

## Questions about my project

1. Read the user's `project_data.json`. If its `schema_version` is not `"6.2.0"`, say so
   and stop: the server reads one version exactly.
2. Call `order` with `operation: "order"` and the document as `project`. It returns the
   logical order: per source, the table, the period and the entries, each with the
   spelling delivered, and the `manual` list of columns entered by hand. Ambiguous
   windows block it: the call fails with `order_blocked` and lists every issue, and
   `validate` lists the same issues. A window only a preliminary release covers does not
   block; it is warned (`preliminary_data`), so tell the user. There are no physical files yet; give the user the logical order and the
   project file. Answer what will be delivered from the order, not from the document.
3. For what a variable means or how it changed, use `show`, `segments` and `values` over
   the project's period.
