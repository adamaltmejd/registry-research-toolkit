---
name: project-data
description: Write and maintain a register project's project_data.json with the catalog
  MCP tools. It is the data order, and the record of what a project holds. Use to draft
  or change an order, to describe data that is already delivered (from a data summary
  made on the server), or to answer questions about a project file.
---

# Register projects

A `project_data.json` lists the register columns a study uses: each source is one
table over a period, and each binding is one variable delivered from it. A project that
waits for data sends it as its order; a project whose data is delivered writes the same
file from what it holds ("Delivered data" below). The agent drafts and checks the file. The document's schema is
[project_data.schema.json](project_data.schema.json), generated from the server's API
contract.

The catalog server's `search`, `show`, `segments` and `coverage` tools are described in
the `catalog-search` skill.

## Drafting a project

1. Set the document's `schema_version` to `meta.project_schema_version` from a response
   of the catalog you order from. Order from the catalog the data comes from: in a steward's
   project, the project's `steward-catalog` server, for the steward's holdings
   (`catalog-search` skill, `references/data-stewards.md`); otherwise the
   plugin's `catalog`. Data found only in the global catalog cannot go in a steward's
   order. Set `catalog` (required) to that catalog's id: `meta.steward.owner` from one of
   its responses, or `global` when `meta` has no `steward`. `validate`, `order` and
   `materialize_order` refuse the project on any other catalog's server
   (`catalog_mismatch`). Set `reg_meta_version` (required) to the serving
   catalog's release tag, `reg_meta/vX.Y.Z`. Read `meta.catalog_version` from a catalog
   tool response and format its release version `X.Y.Z` as that tag. If it is not a
   release version (for example, `dev`), ask which catalog release to use. Never infer it
   from the local executable or from memory.
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
   served over preliminary where both exist. A few tables were delivered in releases
   whose years overlap (`show` on the table lists them under `shared_release_days`). A
   source whose period meets such a year blocks with `release_ambiguous` until it names
   the releases to use in `releases` (`["2003 - 2004"]`); each named release is
   delivered on its own, and its entries say which release they come from. Ask the user
   which release they want, or pick a period outside the shared years.
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
   `search` and `show` find no variable or table for it (a steward's table that `show`
   marks `not_orderable` is documented: tell the user to ask the steward, never enter it
   by hand): a catalog binding is checked
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
6. Set `extract_format` (required), the format each delivered file is written in:
   `parquet`, `dta` (Stata) or `csv`. Write `"extract_format": "parquet"` unless
   the analysis runs in Stata or the user asks for another format; the `project-setup`
   pipeline reads Parquet.
7. Call `order` with `operation: "validate"` and the document as `project`. Fix every
   issue whose `level` is `error` and validate again until `ok` is true; report the
   warnings to the user. A variable whose code list (municipality codes, say) changes
   version over the years gives no warning: each order entry names the version of its
   years in `classification`, and a `form_change` info says when the codes change. A
   `classification_version_ambiguous` warning means the variable carries more than one
   version on the same days, in the period the warning names (several releases of one
   table ordered for a year they share); each entry still names its own version. The
   binding's
   `classification`, a version id `class/<family>/<version>` (`show class` lists the
   families and versions), keeps one version only: it delivers the years where the
   variable carries that version and clips the rest (`period_clipped`), so do not pin to
   silence a warning. A version the variable has no codes from over the period is a
   `classification_incompatible` error.
8. Save the file as `project_data.json` in the project folder.
9. On a steward's catalog, `order` with `operation: "materialize_order"` returns the MONA
   order file the steward's extract runner reads: one file per table and delivery, its
   columns and row filter. Show it to the user to confirm what the extract will hold. It
   submits nothing; the steward writes the order it runs. It needs a non-blank `name`
   (`project_unnamed`) and at least one catalog source (`order_manual_only`).

A large table the study needs only part of (prescriptions by ATC group, diagnoses by
chapter) can be ordered by code: give one binding of the source a `filter` with exact
`codes` and/or `prefixes`, and only rows whose value is one of them are delivered:
`{"variable": "sos/lmed/atc", "filter": {"prefixes": ["N06A", "R03"]}}`. Use it whenever
the study needs a known subset of a coded variable: it is the main way to keep an
extract small. One filtered binding per source, codes or prefixes only (no ranges, no
exclusions), printable ASCII, at most 64 characters each. On a column the data holder
delivers as several codes in one string (PAR's `DIAGNOS`), an entry matches any of the
codes once the steward's holdings mark the column packed. Find the codes with `values`; `validate` refuses a code or
prefix the variable does not have (`filter_code_unknown`, `filter_prefix_unknown`), a
prefix on an integer variable, and a filter on a variable that is not coded or is an
identifier. `filter_codes_unchecked` means the variable has no code list in the catalog:
check the codes against its documentation. Tables sharing a file keep each other's
codes (`code_filter_union`), and a shared file with an unfiltered table is not filtered
(`code_filter_dropped`): tell the user. Mock data draws only the filtered codes.

Every ordered table also delivers its row-key columns, the ids that identify a row
(the person key in a person table), without a binding: `order` lists them per source
and period in `row_keys`, under the register's own spelling (`PersonNr` or `personnr`
on SCB's registers, `LopNr` on a steward's catalog). No binding is needed. Other
identifiers (a parent's personnummer, a name, an address) are never row keys: bind them
if the study needs them. A table with no stated row key gets a `row_key_undeclared`
info; bind the column that identifies its rows. The Flergenreg reused-personnummer flags are
ordinary variables: `scb/flergenreg/ateranvant-personnr` and the per-parent
`aterpnrfar`, `aterpnrmor` and adoptive variants, code `1` for a reused personnummer and
`0` for not. Bind them when needed. `SenPNr` is only in `sos/lova`.

A minimal document on the global catalog (set `schema_version`, `reg_meta_version` and
`catalog` as in step 1). `sources` is a list, each source holds its own `bindings`, and
`period` is a range in one source and a year list in the other:

```json
{
  "schema_version": "8.1.0",
  "reg_meta_version": "reg_meta/vX.Y.Z",
  "catalog": "global",
  "name": "example",
  "extract_format": "parquet",
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

## Delivered data

When the files are already on the server, start from a data summary: which files exist,
their columns, types and row counts. It holds no observed value. File and column names
are assumed not to be personal data.

1. Copy [scripts/data_summary.R](scripts/data_summary.R) to the project's
   `.regmeta/data_summary.R` and set `DATA_DIR` to the delivered files' absolute folder,
   with forward slashes. Ask for the full path (on MONA
   `//micro.intra/projekt/P<nnnn>$/P<nnnn>_Data/<subfolder>` by default, to confirm) and
   never write a relative one: the script may run from another folder. The user must not
   need to edit it.
2. The user uploads it and submits it as it is (on MONA, to the batch runner). It writes
   `data_summary.csv` to the folder it runs from. The user sends the file
   out through the server's own export, which they review as any export, and saves it as
   `.regmeta/data_summary.csv`. It needs no export gate: it has names, types and counts
   only, with folders written relative to the data folder.
3. Match it to the catalog. Each file is a table and a year: find the table with
   `resolve` and `search` on the file and column names, and `schema` or `segments` for
   its columns. On a steward's catalog `segments` lists the steward's own file names.
   Ask the user about a file or column the catalog does not explain; never guess.
4. Write `project_data.json` as in "Drafting a project": one source per table and
   period, one binding per column the project uses, a manual binding or source for what
   the catalog lacks (step 5 there). Validate until `ok` is true.
5. Update the mock data (`mock-data` skill): save the answers again, set the delivered
   file names (`rename`) and columns in the rules, compile and generate. Then check
   `SOURCES` in `src/inputs.R` against the summary's file and column names.

Run the same five steps when an ordered delivery arrives: the summary shows where the
delivery differs from the order. The script reads files only (Parquet, `.dta`,
`.sas7bdat`, delimited text); data delivered in SQL Server is not covered yet. Never ask
the user to read or paste values, codes or distributions from the server: a column's
codes come from the catalog or from documentation.

## Questions about my project

1. Read the user's `project_data.json`. Each server reads one `schema_version` exactly;
   if `validate` returns `unsupported_schema_version`, say so and stop. Its `catalog`
   names the server to ask: `global` the plugin's `catalog`, a steward's id the project's
   `steward-catalog` server. `catalog_mismatch` means the call went to the other one:
   switch servers. Moving a project to another catalog is the user's decision, made by
   editing `catalog`; never change it to make a check pass.
2. Call `order` with `operation: "order"` and the document as `project`. It returns the
   logical order: per source, the table, the period and the entries, each with the
   spelling delivered, and the `manual` list of columns entered by hand. Ambiguous
   windows block it: the call fails with `order_blocked` and lists every issue, and
   `validate` lists the same issues. A window only a preliminary release covers does not
   block; it is warned (`preliminary_data`), so tell the user. There are no physical files yet; give the user the logical order and the
   project file. Answer what will be delivered from the order, not from the document.
3. For what a variable means or how it changed, use `show`, `segments` and `values` over
   the project's period.
