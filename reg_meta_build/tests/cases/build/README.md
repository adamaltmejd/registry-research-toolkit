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

  | Surface prefix      | Covers                                                                                       |
  | ------------------- | -------------------------------------------------------------------------------------------- |
  | `coding-`           | `[[coding.choice]]`, `[[coding.warning]]` and `[[coding.documented]]` certificates           |
  | `support-errata-`   | `[[errata.support]]` source-support decisions                                                |
  | `split-sos-`        | SOS `[[identity.split]]` and `[[identity.rename]]`                                           |
  | `split-partition-`  | SCB `[[identity.partition]]` and `[[identity.column_owner]]`, and slices that reference them |
  | `errata-delivered-` | `[[errata.delivered]]` additions                                                             |
  | `errata-sos-`       | SOS `[[errata.data_type]]` and `[[errata.classification_reference]]`                         |
  | `enrichment-`       | `[[enrichment.description]]` and `[[enrichment.alias]]`                                      |
  | `route-sos-`        | SOS `[[identity.route]]` and styrtabell lookup subsets                                       |
  | `topology-sos-`     | SOS variants formed from delivered subset names                                              |
  | `thin-`             | authored thin-provider registers (`Forsakringskassan/`, `scb_canonical/`)                    |
  | `representation-`   | `[[representation.delivery_metadata]]` and `[[representation.parallel]]`                     |
  | `matrix-`           | `[[representation.matrix]]` answer matrices                                                  |
  | `siblings-`         | sibling grouping of co-delivered columns                                                     |
  | `relations-`        | `relations.toml` edges                                                                       |
  | `code-label-pair-`  | `[[code_label_pair]]`                                                                        |
  | `search-pins-`      | `search_pins.toml`                                                                           |

Later stages add their own prefixes to this table.

Refusals end in `-fails-curation-load` (the build refuses its curation) or name the
stale outcome (`-stale-when-...`). A case that shows the allowed outcome of a guard sits
beside its refusal twin.

## `request.json`

  | Key             | Meaning                                                                                                                                                    |
  | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `replaces`      | The Python test (`file::function[param]`) whose assertions the expected values were read from.                                                             |
  | `note`          | Optional prose: why this case exists.                                                                                                                      |
  | `sources`       | A source spec, inline or the name of `_sources/<name>.json`. The build reads it.                                                                           |
  | `authored_from` | Optional source spec that the curation placeholders read. Defaults to `sources`. A stale case authors from the reviewed source and builds the changed one. |
  | `registers`     | Optional register slice (native register ids). Defaults to every register.                                                                                 |
  | `diagnostic`    | Optional; defaults to `true`, so curation errors land in the ledger instead of aborting.                                                                   |

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
  "sos": [{"abbrev": "PAR", "title": "Patientregistret", "subsets": [], "variables": [], "code_lists": {}}]
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
    `[period, code, label]`.
  - Every workbook gets a blank delivered `Kopplingsvariabel` column. That column is
    SOS's explicit "not a linkage variable" claim.
- `files` maps a path under the source directory to its text, for deliveries the other
  keys do not model, such as an authored `Forsakringskassan/fk.toml`.

The runner prepares each distinct spec once per test session and caches it by the spec's
content hash.

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

  | Placeholder              | Renders                                                                                      |
  | ------------------------ | -------------------------------------------------------------------------------------------- |
  | `evidence_sha256`        | `acknowledgement_evidence_sha256` of the selected records                                    |
  | `coding_evidence_sha256` | the same, plus the bound physical code-list evidence of each record (`coding_source_sha256`) |
  | `expected_records`       | `capture_expectations(..., parents=True, coding=True)` of the selected records               |
  | `expected_fields`        | the captured fields of one record                                                            |
  | `period_text`            | one record's original period text                                                            |
  | `edition_scope`          | one record's edition scope; `end=` replaces its first interval's end                         |
  | `period_scope`           | one record's edition period scope                                                            |
  | `revision`               | one record's source revision                                                                 |
  | `locators`               | one record's locators                                                                        |
  | `variant_key`            | one record's native variant key                                                              |

Record selectors are `key=value` arguments, and all of them must match. A
comma-separated value lists alternatives.

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
  "error": {"code": "classification_curation_invalid", "message_contains": ["curation/registers/scb/sample.toml"]},
  "projections": [
    {"table": "state_codes", "where": {"column": "VALUE"}, "fields": ["code"], "match": "exact", "rows": [["01"]]}
  ]
}
```

Every key is optional, and only the keys that are present get checked.

- `status`: the build result's status.
- `error`: the build must refuse with this located error `code` and, when given,
  `exit_code`. Each string in `message_contains` must appear in the error message. A
  build that raises a `ValueError`, `OSError` or `KeyError` refuses the way the `build`
  command reports it: code `pipeline_build_failed`, exit code 10.
- `projections`: the rows of one named table.
  - `where` filters rows before projecting. A scalar means equality. A list means
    membership. `{"contains": text}` or `{"contains": [text, ...]}` requires the text to
    appear in the value.
  - `fields` picks the columns that are compared.
  - `match` decides how rows are compared, always order-insensitively:
    - `exact` (the default): the rows match, duplicates included.
    - `set`: the distinct rows match.
    - `includes`: every expected row is present.

  | Table            | One row per                                                             | Fields                                                                                                                                                                  |
  | ---------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `issues`         | report-ledger issue                                                     | `code`, `severity`, `subject`, `case_id`, `locator` (the `curation/...` entry its detail names), `detail`                                                               |
  | `issue_refs`     | source record a report-ledger issue cites                               | `code`, `case_id`, `source`, `key` (the full semantic record key, a list)                                                                                               |
  | `cases`          | report-ledger curation case                                             | `case_id`, `status`                                                                                                                                                     |
  | `uses`           | ledger disposition of a prepared source record                          | `source`, `native_variable`, `key`, `column_name`, `data_type`, `name`, `description`, `use`, `variable`                                                                |
  | `case_uses`      | curation case a ledger disposition names                                | `case_id`, `source`, `key`, `use`, `variable`                                                                                                                           |
  | `states`         | built variable state                                                    | `register`, `variable`, `variant` (slugs), `column`, `valid_from`, `valid_to`, `data_type`, `name` (the variable's), `state_name` (the state's), `provenance`, `pooled` |
  | `state_codes`    | built state and value-set member (or one null-code row)                 | `register`, `variable`, `variant`, `column`, `valid_from`, `valid_to`, `code`, `label`                                                                                  |
  | `variables`      | built variable and delivery column (one null-column row if it has none) | `register`, `variable`, `column`, `provider_key`, `description`, `is_identifier`, `is_sensitive`                                                                        |
  | `variants`       | built register variant                                                  | `register`, `variant`, `name`                                                                                                                                           |
  | `aliases`        | built search alias                                                      | `register`, `variable`, `variant`, `column`                                                                                                                             |
  | `same_as`        | built same-as edge, each direction its own row                          | `a`, `b` (variable FQIDs)                                                                                                                                               |
  | `concept_groups` | built concept group                                                     | `variables` (its member slugs, sorted)                                                                                                                                  |
  | `warnings`       | built data warning                                                      | `column`, `valid_from`, `valid_to`, `variable_fqid`, `variant`, `summary`, `fields`                                                                                     |
  | `search_pins`    | built search pin                                                        | `query` (the folded key), `type`, `position`, `entity`                                                                                                                  |
  | `manifest`       | import-manifest entry                                                   | `key`, `value`                                                                                                                                                          |

Expected values are read from the test a case replaces, or from the source fixture or
the spec. Never copy them from a run of the code under test. A new table or placeholder
goes in `_build_case_runner.py` and in this README in the same change.
