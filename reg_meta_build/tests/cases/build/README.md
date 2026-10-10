# Build cases

Each directory here is one boundary claim about `build_catalog`. The claim is stated as
data:

- the source deliveries,
- the curation tree a curator would commit,
- the build options,
- the expected result: a status, a refusal, report-ledger rows or built-catalog rows.

`test_build_cases.py` runs every case and `_build_case_runner.py` implements this
format. A case fails when the build stops matching its `expected.json`.

## Layout

```text
cases/build/
  _sources/<name>.json          shared source specs, referenced by name
  <surface>-<behavior>/
    request.json                sources and build options
    curation/                   the curation tree (registers/**, relations.toml, ...)
    expected.json               the oracle
```

A case that needs more than one build in one process has numbered step directories
instead (`1-<label>/`, `2-<label>/`). Each step has the same three parts, and the steps
run in order in one test.

## Naming

Name a case `<surface>-<behavior>`: the curation or source surface it exercises, then
the behavior in plain words. For example,
`coding-choice-stale-when-competing-list-changes` or
`split-partition-native-stale-when-a-reviewed-column-is-missing`.

  | Surface prefix         | Covers                                                                                                                                                                           |
  | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `coding-`              | `[[coding.choice]]`, `[[coding.warning]]`, `[[coding.uncoded]]`, `[[coding.omit]]`, `[[coding.extend]]`, `[[coding.documented]]`, `[[coding.sentinel]]` and `[[coding.support]]` |
  | `support-errata-`      | `[[errata.support]]` source-support decisions                                                                                                                                    |
  | `split-sos-`           | SOS `[[identity.split]]` and `[[identity.rename]]`                                                                                                                               |
  | `split-partition-`     | SCB `[[identity.partition]]`, `[[identity.column_owner]]` and `[[identity.unassigned]]`, and slices that reference them                                                          |
  | `errata-delivered-`    | `[[errata.delivered]]` additions                                                                                                                                                 |
  | `errata-column-`       | `[[errata.column]]` placements, flags, prose, classification references and steward storage types, and `[[errata.version]]` declared editions                                    |
  | `errata-sos-`          | SOS `[[errata.data_type]]` and `[[errata.classification_reference]]`                                                                                                             |
  | `errata-field-`        | `[[errata.field]]` checked corrections of one source occurrence                                                                                                                  |
  | `errata-period-`       | `[[errata.occurrence_period]]` checked period corrections                                                                                                                        |
  | `checked-corrections-` | several checked corrections in one build, one register per claim                                                                                                                 |
  | `enrichment-`          | `[[enrichment.description]]` and `[[enrichment.alias]]`                                                                                                                          |
  | `route-sos-`           | SOS `[[identity.route]]` and styrtabell lookup subsets                                                                                                                           |
  | `topology-sos-`        | SOS variants formed from delivered subset names                                                                                                                                  |
  | `thin-`                | authored thin-provider registers (`Forsakringskassan/`, `scb_canonical/`)                                                                                                        |
  | `representation-`      | `[[representation.delivery_metadata]]`, `[[representation.parallel]]` and `[[representation.alias_window]]`                                                                      |
  | `matrix-`              | `[[representation.matrix]]` answer matrices                                                                                                                                      |
  | `siblings-`            | sibling grouping of co-delivered columns                                                                                                                                         |
  | `relations-`           | `relations.toml` edges                                                                                                                                                           |
  | `code-label-pair-`     | `[[code_label_pair]]`                                                                                                                                                            |
  | `search-pins-`         | `search_pins.toml`                                                                                                                                                               |
  | `acknowledge-`         | `[[acknowledge]]` entries matched against build issues                                                                                                                           |
  | `classification-`      | classification books, references and label bindings                                                                                                                              |
  | `scope-`               | register-scoped builds and checks, and references out of the slice                                                                                                               |
  | `period-family-`       | `[[representation.period_family]]` calendar-month families and the relations into them                                                                                           |
  | `source-relation-`     | literal source relationships, unbound code lists and source findings                                                                                                             |
  | `value-`               | source code lists bound by native member, member name or declared list name: item and row validity, list references, declared-identifier drops                                   |
  | `naming-`              | generated and authored slug pins under the zone freeze states                                                                                                                    |
  | `edition-`             | SCB preliminary/final editions, `[[identity.edition_split]]` and `[[errata.edition_period]]`                                                                                     |
  | `dependency-`          | curated groups, tags, panel keys and relations whose catalog dependency is withheld                                                                                              |
  | `coverage-`            | delivery the formed variables owe the catalog, written or explicitly withdrawn                                                                                                   |
  | `source-rows-`         | physical source rows the build refuses when it loads a scope                                                                                                                     |
  | `warning-`             | the data warnings a build publishes: the register, variable and state each one attaches to, and where its text comes from                                                        |
  | `value-sets-`          | stored value sets: one per distinct membership, its id kept across builds, and the per-column state overlaps formation withholds                                                 |
  | `lineage-`             | source register attribution from a delivered source label, and the state lineage edges, lineage warnings and `[[acknowledge]]` entries it resolves                               |
  | `formation-`           | variables formed from delivered rows alone: variable facts versus state texts, unit pairs, and the Unika sensitivity and identifier flags                                        |
  | `occurrence-`          | overlapping occurrences resolved into states: exact period cuts, pooled editions, reconciled types, lengths, units, texts, steward storage, unplaced or columnless members       |
  | `summary-`             | SCB Unika and `Identifierare.csv` flag declarations bound to the delivered variables their literal key and version endpoints name                                                |

Later stages add their own prefixes to this table.

Refusals end in `-fails-curation-load` (the build refuses its curation),
`-fails-the-build` or `-refuses-...` (the build stops after its curation loaded), or
name the stale outcome (`-stale-when-...`). A case that shows the allowed outcome of a
guard sits beside its refusal twin.

## `request.json`

  | Key             | Meaning                                                                                                                                                    |
  | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `replaces`      | The Python test (`file::function[param]`) whose assertions the expected values were read from.                                                             |
  | `fails_if`      | Required: the product change that would make this case (or step) fail. The runner refuses a request without one.                                           |
  | `note`          | Optional prose: why this case exists.                                                                                                                      |
  | `sources`       | A source spec, inline or the name of `_sources/<name>.json`. The build reads it.                                                                           |
  | `authored_from` | Optional source spec that the curation placeholders read. Defaults to `sources`. A stale case authors from the reviewed source and builds the changed one. |
  | `registers`     | Optional register slice (scope names: a native register id, a register name or `<source>:<name>`). Defaults to every register.                             |
  | `diagnostic`    | Optional; defaults to `true`, so curation errors land in the ledger instead of aborting.                                                                   |
  | `mode`          | Optional: `build` (the default) runs `build_catalog`; `check` runs `check_curation`, which needs `registers` and writes no catalog.                        |

`fails_if` names a change to the product, not the behavior restated. It is the claim a
reviewer checks by applying that change and watching the case fail. For example:
`"the acknowledgement matcher accepts an entry whose period window contains, rather than equals, the dated issue's period"`.
A case that shows the allowed outcome beside a refusal names the opposite change, such
as a guard that refuses unchanged rows. If no product change can make a case fail, the
case cannot fail and does not belong here.

### Source spec

```json
{
  "description": "free text",
  "scb": {
    "registerinformation": [{"cvid": 1001, "var_id": 101, "colname": "VALUE"}],
    "vardemangder": ["keep|1|01|Label|1001|7001"],
    "unika": ["TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2010|2020|0|0|0"],
    "valid_dates": ["7001|2000-01-01|2030-12-31"]
  },
  "sos": [{"abbrev": "PAR", "title": "Patientregistret", "subsets": [], "variables": [], "code_lists": {}}],
  "files": {
    "Forsakringskassan/fk.toml": ["[[register]]", "key = \"remote\"", "..."],
    "classifications/a.csv": ["code,label", "1,One"]
  }
}
```

- `scb` is required, because every input bundle carries an SCB snapshot.
  - `registerinformation` rows are the keyword arguments of `_csv_fixtures.var_row`:
    `cvid`, `var_id`, `colname`, `year`, `regver_id`, `versionname`, `varname`,
    `vardef`, `vardesc`, `unit`, `varopdef`, `varsource`, `data_type`, `data_length`,
    and `register` as `[name, register_id, variant_id]`. Fields left out take that
    function's defaults: register `TESTREG` (native 1, variant 10), year 2020, type
    `int`. A row's optional `cells` maps a Registerinformation header to a raw cell
    value that replaces the generated one, for cells `var_row` has no argument for.
  - `vardemangder` rows are raw pipe-delimited Vardemangder lines.
  - `unika` defaults to one non-sensitive, non-identifier summary row per delivered
    column. `null` delivers no Unika file.
  - `valid_dates` defaults to every value item being valid from 2000 to 2030.
- `sos` lists Socialstyrelsen workbooks.
  - `subsets` are Deldatamängder rows: `name`, `label`, `description`, `data_from`,
    `data_to`, `aggregation_level`. An empty list writes no Deldatamängder sheet.
  - `variables` are Variabelnivå rows: `name`, `deldatamangd`, `label`, `description`,
    `data_type` (default `Sträng (text)`), `value_set`, `external_classification`,
    `data_from`, `data_to`.
  - `code_lists` maps a variable to its `Kodlista_<variable>` rows
    `[period, code, label]`. The sheet opens with a `Variabelnamn` row naming the
    variable, so the build binds the list to it.
  - `sheets` maps an extra sheet name to its raw rows, for example a recode table or a
    code list in another shape. A `Kodlista_` sheet here without a `Variabelnamn` row is
    a list the build cannot bind (`unresolved_list_reference`).
  - `blank_dataset: true` blanks the `Datamängd` cell of `Generell information`, so the
    workbook names no register.
  - Every workbook gets a delivered `Kopplingsvariabel` column. A blank cell is SOS's
    explicit "not a linkage variable" claim; a variable's optional `linkage` text fills
    its cell, which declares it a linkage (identifier) variable.
- `files` maps a path under the source directory to its text, for deliveries the other
  keys do not model: an authored `Forsakringskassan/fk.toml` or a
  `classifications/<slug>.csv` code list. The text is a string, or a list of lines
  joined with newlines.

The runner prepares each distinct spec once and caches it by the spec's content hash.
The cache is the reader fixture cache (see `conformance/README.md`, "Fixture cache"):
`$REG_FIXTURE_CACHE`, else the system temp directory, shared by sessions, worktrees and
xdist workers. Its generation is keyed by the builder sources, the installed
distributions, the test modules that prepare a spec and the Git version. CI sets no
`REG_FIXTURE_CACHE`, so each fresh runner starts cold.

## `curation/`

This is the curation tree exactly as a curator commits it, under
`registers/<provider>/<slug>.toml` plus optional `relations.toml`. The runner adds the
empty `classifications/` directory.

Values that a curator copies from the accepted prepared records are written as
placeholders. The runner fills them in from the `authored_from` source, using the public
helpers a curator uses:

```toml
expected_evidence_sha256 = {{coding_evidence_sha256 key=member:1001}}
```

  | Placeholder              | Renders                                                                                                                    |
  | ------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
  | `evidence_sha256`        | `acknowledgement_evidence_sha256` of the selected records                                                                  |
  | `coding_evidence_sha256` | the same, plus the bound physical code-list evidence of each record (`coding_source_sha256`)                               |
  | `expected_records`       | `capture_expectations(..., parents=True, coding=True)` of the selected records                                             |
  | `expected_fields`        | the captured fields of one record                                                                                          |
  | `period_text`            | one record's original period text                                                                                          |
  | `edition_scope`          | one record's edition scope; `end=` replaces its first interval's end                                                       |
  | `period_scope`           | one record's edition period scope                                                                                          |
  | `revision`               | one record's source revision                                                                                               |
  | `locators`               | the locators of the selected records                                                                                       |
  | `marker_bindings`        | one record's marker-binding fingerprints over `from=`..`to=`                                                               |
  | `raw_codings`            | the sorted distinct `coding_source_sha256` of the code-list claims bound to the selected records                           |
  | `association`            | the locator of the one source-list association behind member `code=`/`label=` (a blank label when omitted) of those claims |
  | `association_sha256`     | that association's `coding_source_sha256`                                                                                  |
  | `source_codings`         | `copied_coding_fingerprints` of those claims                                                                               |
  | `relationship_row`       | the physical row of the one literal relationship delivered in `table=`                                                     |
  | `relationship_sha256`    | the content hash of that relationship's declaration                                                                        |
  | `table_sha256`           | the content hash of the one prepared evidence table named `table=`                                                         |
  | `naming_id`              | `authored_naming_id(kind=, provider=, register_key=, member_key=)`, a thin or SOS native id                                |
  | `variant_key`            | one record's native variant key                                                                                            |

Record selectors are `key=value` arguments, and all of them must match. A
comma-separated value lists alternatives. An argument value cannot contain a space.

- `source` is a prefix of the source name, for example `scb-registerinformation` or
  `Socialstyrelsen/`.
- `key` is the last semantic record key part: SCB `member:<cvid>`, SOS
  `variable:<name>`.
- `column` is the delivered column name. An empty value selects a blank column.
- `member` is the delivered member name.
- `variable` is the native variable id: an SCB `var_id` or an SOS variable name.
- `edition` is the native edition id.

`fields=` picks the captured fields: `all` (the default), `prose` (name, definition,
description, operational definition), or a comma-separated list.

## `expected.json`

```json
{
  "status": "diagnostic_complete",
  "error": {"code": "register_entry_invalid", "message_contains": ["curation/registers/scb/sample.toml"]},
  "projections": [
    {"table": "state_codes", "where": {"column": "VALUE"}, "fields": ["code"], "match": "exact", "rows": [["01"]]}
  ]
}
```

Every key is optional, and only the keys that are present get checked.

- `status`: the build result's status.
- `result`: keys of the build or check result dict, nested. Only the named keys are
  compared; a missing key reads as `null`, and a list names members that must be
  present.
- `rebuilt_identical: true`: the runner builds the same step a second time, in a fresh
  interpreter with decision dumps on; the artifact, the report ledger and the dumped
  decisions must be byte-identical. The first build runs under the test process's hash
  seed (random unless `PYTHONHASHSEED` fixes it). The rebuild runs with
  `PYTHONHASHSEED=0`, or `1` when the test process already runs under `0`, so the two
  builds never share a fixed seed and an output that depends on set or dict iteration
  order over strings shows up as a difference. One distinct seed catches such an output
  only when the two seeds happen to order the strings differently. It costs a second
  build and an interpreter start, so one case per compile surface with no other
  byte-identity witness carries it: `coding-entries-apply-or-go-stale-per-register` for
  `[[coding.*]]`, and
  `classification-bindings-conform-extend-or-stay-unbound-per-register` (step 1) for
  classification bindings and their conformance, and
  `lineage-edges-need-accepted-identity-and-overlapping-states` (step 1) for state
  lineage. A rebuild reruns the same rows; source row order is a separate property,
  shown by a later step that builds the relevant rows reversed and expects the first
  step's projections (that case's step 2,
  `value-native-lists-bind-set-aside-or-withhold-per-register` step 2, and
  `errata-column-holdings-period-forms-one-pooled-state` step 2,
  `representation-parallel-shared-states-and-per-column-windows-resolve-per-register`
  step 2, and the reversed-order steps of
  `lineage-edges-need-accepted-identity-and-overlapping-states` and
  `lineage-source-variant-is-named-defaulted-or-left-unresolved`).
- `error`: the build or check must refuse. Only the keys present are compared:
  - `code` and `exit_code` are the located error the `build-db` and `check-curation`
    commands report. A `RegMetaError` keeps its own; a `ValueError`, `OSError` or
    `KeyError` from the pipeline is wrapped as `pipeline_build_failed`, exit code 10.
  - Each string in `message_contains` must appear in the error message.
- `projections`: the rows of one named table.
  - `where` filters rows before projecting. A scalar means equality. A list means
    membership. `{"contains": text}` or `{"contains": [text, ...]}` requires the text to
    appear in the value.
  - `fields` picks the columns that are compared.
  - `match` decides how rows are compared, always order-insensitively:
    - `exact` (the default): the rows match, duplicates included.
    - `set`: the distinct rows match.
    - `includes`: every expected row is present.
  - `note` is optional prose: which claim of the case this projection pins. The runner
    does not compare it.

  | Table                        | One row per                                                                    | Fields                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | ---------------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `issues`                     | report-ledger issue                                                            | `code`, `severity`, `subject`, `case_id`, `locator` (the `curation/...` entry its detail names), `detail`, `acknowledged_by`, `valid_from`, `valid_to`, `fields` (the source fields it names, a list), `withheld_output` (the outputs it withholds, a list)                                                                                                                                                                                                                                   |
  | `issue_refs`                 | source record a report-ledger issue cites                                      | every `issues` field, then `source`, `key` (the full semantic record key, a list) and `ref` (the two joined, in the `refs` string form)                                                                                                                                                                                                                                                                                                                                                       |
  | `cases`                      | report-ledger curation case                                                    | `case_id`, `status`                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
  | `uses`                       | ledger disposition of a prepared source record                                 | `source`, `native_variable`, `key`, `column_name`, `data_type`, `name`, `description`, `use`, `variable`                                                                                                                                                                                                                                                                                                                                                                                      |
  | `case_uses`                  | curation case a ledger disposition names                                       | `case_id`, `source`, `key`, `use`, `variable`                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
  | `states`                     | built variable state                                                           | `register`, `variable`, `variant` (slugs), `column`, `valid_from`, `valid_to`, `data_type`, `name` (the variable's), `state_name` (the state's), `provenance`, `pooled`, `data_length`, `definition`, `measurement_unit`, `description`, `operational_definition` and `source_register_text` (the state's)                                                                                                                                                                                    |
  | `state_codes`                | built state and value-set member (or one null-code row)                        | `register`, `variable`, `variant`, `column`, `valid_from`, `valid_to`, `code`, `label`                                                                                                                                                                                                                                                                                                                                                                                                        |
  | `variables`                  | built variable and delivery column (one null-column row if it has none)        | `register`, `variable`, `column`, `provider_key`, `name`, `definition`, `description`, `measurement_unit` and `operational_definition` (the variable's own), `is_identifier`, `is_sensitive`, `deprecated`, `source_register` (the attributed register's slug), `source_label`, `source_register_text` (the variable's)                                                                                                                                                                       |
  | `variants`                   | built register variant                                                         | `register`, `variant`, `name`, `panel_entity_key` (a slug, or a JSON list of slugs), `panel_time_key`, `panel_time_grain`, `description`, `display_group`                                                                                                                                                                                                                                                                                                                                     |
  | `registers`                  | built register                                                                 | `register` (its slug), `name`, `purpose`                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
  | `editions`                   | built register version                                                         | `register`, `variant` (slugs), `name`, `description`, `measurement_information`, `documentation_status`, `first_approved_at`, `last_approved_at`, `populations` (sorted `[name, definition, comment, date_range]`) and `object_types` (sorted `[name, definition]`)                                                                                                                                                                                                                           |
  | `tags`                       | built tag member (one null-member row for a tag without members)               | `slug`, `member` (`provider/register/variable`, or `provider/register` for a register member)                                                                                                                                                                                                                                                                                                                                                                                                 |
  | `aliases`                    | built search alias                                                             | `register`, `variable`, `variant`, `column`                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
  | `alias_windows`              | alias window of a delivery column                                              | `register`, `variable`, `variant`, `column`, `valid_from`, `valid_to`, `provenance` (a curated window's attribution; null for a source-derived window), `column_metadata` and `coding_metadata` (`shared` or `per_column`), the window's `data_type`, `data_length`, `definition`, `measurement_unit`, `name`, `description`, `operational_definition`, `source_register_text`, `codes` (sorted `[code, label]` of its per-column value set) and `classifications` (sorted slugs bound to it) |
  | `edges`                      | built variable relation                                                        | `type` (`same_as` or `replaced_by`), `a`, `b` (`provider/register/variable`; each `same_as` direction is its own row; `replaced_by` runs `a` to `b`)                                                                                                                                                                                                                                                                                                                                          |
  | `lineage`                    | built state lineage edge                                                       | `register`, `variable`, `variant`, `column` (the consumer state), `source_register`, `source_variable`, `source_variant`, `source_column` (the source state), `valid_from`, `valid_to` (the edge's intersection)                                                                                                                                                                                                                                                                              |
  | `lineage_warnings`           | built state lineage warning                                                    | `register`, `variable`, `variant`, `column`, `valid_from`, `valid_to` (the consumer state), `kind`, `message`                                                                                                                                                                                                                                                                                                                                                                                 |
  | `concept_groups`             | built concept group                                                            | `variables` (its member slugs, sorted)                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | `warnings`                   | built data warning                                                             | `register`, `variable`, `variant` (slugs), `column`, `valid_from`, `valid_to`, `code`, `severity`, `detail`, `summary`, `detail_hash_of`, `fields`, `refs`, `withheld_output`, `acknowledged_by`, `source_subject`, `case_id`                                                                                                                                                                                                                                                                 |
  | `search_pins`                | built search pin                                                               | `query` (the folded key), `type`, `position`, `entity`                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | `search_text`                | built variable's search text                                                   | `register`, `variable`, `name`, `definition`, `description`: the unfolded text `variable_fts` indexes, the variable's own or else its state and alias-window texts, distinct and sorted, one per line; `delivery_column_names`: the sorted list of its published spellings (`variable_alias`)                                                                                                                                                                                                 |
  | `manifest`                   | import-manifest entry                                                          | `key`, `value`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
  | `state_classifications`      | built state bound to a classification                                          | `register`, `variable` (slugs), `column`, `valid_from`, `valid_to`, `classification` (slug), `provenance` (the binding's)                                                                                                                                                                                                                                                                                                                                                                     |
  | `conformance`                | built state's or alias window's conformance to one bound book                  | `register`, `variable`, `column`, `valid_from`, `valid_to`, `window` (`state` or `alias`), `classification`, `status`, `checked`, `matched`, `nonconforming` (code counts), `overlap`                                                                                                                                                                                                                                                                                                         |
  | `conformance_codes`          | source member a state's or alias window's conformance records outside the book | `register`, `variable`, `column`, `valid_from`, `valid_to`, `window` (`state` or `alias`), `classification`, `code`, `label`, `member_kind`, `sentinel_meaning`, `scoped_windows`                                                                                                                                                                                                                                                                                                             |
  | `code_index`                 | code a built variable carries (the reader's code-search index)                 | `register`, `variable`, `code`, `label`, `mapping_count` (the variables carrying that code and label)                                                                                                                                                                                                                                                                                                                                                                                         |
  | `classifications`            | built classification                                                           | `slug`, `short_name`, `name`, `name_en`, `publisher`, `valid_from`, `valid_to`, `description`, `url`, `code_count`, `valid_code_count`, `supersedes` (the predecessor slug its single pointer projects, or null)                                                                                                                                                                                                                                                                              |
  | `classification_successions` | built classification succession edge                                           | `predecessor`, `successor` (slugs), `effective_year`, `note` (`curated:slug_toml` or `derived:vintage_chain`)                                                                                                                                                                                                                                                                                                                                                                                 |
  | `classification_codes`       | built classification's code                                                    | `slug` (the book's), `code`, `label`, `level`, `is_valid`                                                                                                                                                                                                                                                                                                                                                                                                                                     |
  | `relationships`              | built literal source relationship                                              | `kind`, `binding_status`, `source_dataset`, `owner` (variable slug), `endpoints` (bound variables)                                                                                                                                                                                                                                                                                                                                                                                            |
  | `evidence`                   | ledger disposition of prepared auxiliary evidence                              | `kind`, `disposition`                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
  | `source_issues`              | ledger support- and value-source issue (the evidence behind issues)            | `kind`, `severity`, `descriptor_key`, `physical_associations`, `refs`                                                                                                                                                                                                                                                                                                                                                                                                                         |
  | `value_sets`                 | stored value set                                                               | `id` (its `value_set_id` as text), `members` (sorted `[code, label]`), `variables` (sorted `provider/register/variable` of the states and alias windows carrying it)                                                                                                                                                                                                                                                                                                                          |

A `conformance_codes` row's `member_kind` is `nonstandard` or `sentinel`.
`scoped_windows` lists, as `[valid_from, valid_to]`, the scoped sentinel certificates
(`[[coding.sentinel]]`) that name the member; their fingerprints are not projected. An
alias window stores its conformance as evidence, not counts: its `conformance` row
counts it the way the reader does (distinct checked codes; `nonconforming` is the
distinct codes outside the book, sentinels included), and its `conformance_codes` rows
have no `sentinel_meaning`.

A `code_index` row is one `code_variable_map` entry: a code reaches a variable through a
state's value set or an alias window's per-column one.

A `warnings` row's coordinates (`register`, `variable`, `variant`, `column`,
`valid_from`, `valid_to`) are stored twice: in the `data_warning` columns the reader
filters on, and in the `warning_json` it returns. When the two agree the field projects
that value; when they disagree it projects `{"row": ..., "json": ...}`, so any case
naming the field fails. `detail_hash_of` says what the warning's
`diagnostic_detail_sha256` hashes: `issue` when it is the detail of a report-ledger
issue with the warning's `code` and `source_subject` as its subject, else `detail` when
it is the warning's own `detail`, else null. A case states where a warning's text came
from without writing a hash.

A `refs` value (on `warnings` and `source_issues`) lists source record refs as
`<source>#<semantic key parts joined by />`. An issue's refs are the `issue_refs` rows,
so a case can filter and project them one ref at a time.

A projection's `rows` may state a relation instead of literal values. With
`"fields": ["ref"]` and `"rows": {"refs_of": {"table": ..., "where": ...}}`, the
projected refs must equal the refs that the selected rows of another table cite (`ref`
on `issue_refs`, `refs` on `warnings` and `source_issues`), duplicates included. Both
sides are read from the same build. Use it when the claim is that two outputs agree on
their source records, so the case never has to write a semantic-record hash.

In a later step, `"rows": {"step": "<earlier step>"}` expects the rows the same
projection (its table, `where` and `fields`) reads from that earlier step's build, under
this projection's `match`. Use it when the claim is that two builds agree, in particular
on a value a case must never write as a literal, such as a `value_sets` `id`: the case
states that an id survives a change to the input without knowing the id.

Expected values are read from the test a case replaces, or from the source fixture or
the spec. Never copy them from a run of the code under test. A new table or placeholder
goes in `_build_case_runner.py` and in this README in the same change.
