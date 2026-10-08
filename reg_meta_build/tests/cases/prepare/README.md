# Prepare cases

Each directory here is one boundary claim about preparing a provider delivery. The claim
is stated as data:

- the delivery, as the provider ships it (a workbook, a CSV, an SQL file or an SCB
  snapshot),
- the reader the prepare step dispatches that input role to,
- the expected result: projections of the prepared records and evidence, or the located
  refusal the prepare command reports.

`test_prepare_cases.py` runs every case and implements this format. A case fails when
its reader stops matching its `expected.json`.

The readers are the ones `prepared_catalog.prepare_catalog_sources` calls per input
role, run in process on the case's delivery. Prepare stores what they return unchanged,
so a case reads the prepared records without the bundle commit, acceptance and build a
full prepare costs. A claim about the build (formation, coding, curation) belongs in
`cases/build/`.

## Layout

```text
cases/prepare/
  _sources/<name>.json      shared workbook bases, referenced by name
  <surface>-<behavior>/
    source.json             the delivery
    expected.json           the reader and the oracle
```

## Naming

Name a case `<surface>-<behavior>`: the delivery surface, then the behavior in plain
words. A case whose reader refuses ends in `-refused`.

  | Surface prefix   | Covers                                                                        |
  | ---------------- | ----------------------------------------------------------------------------- |
  | `sos-`           | Socialstyrelsen metadata workbooks: variables, code sheets, subsets, metadata |
  | `sos-parse-`     | the parsed workbook as `parse-sos` reports it                                 |
  | `lisa-`          | the SCB LISA variable-availability workbook                                   |
  | `code-list-`     | classification code-list CSVs                                                 |
  | `scb-records-`   | SCB `Registerinformation.csv` observations                                    |
  | `scb-summary-`   | SCB `UnikaRegisterOchVariabler.csv` and `Identifierare.csv` declarations      |
  | `scb-values-`    | SCB `Vardemangder.csv` and `VardemangderValidDates.csv`                       |
  | `scb-events-`    | SCB `Timeseries.csv` events                                                   |
  | `scb-columns-`   | SCB `Tabelldefinitioner.sql` column types                                     |
  | `scb-join-keys-` | SCB `ID-kolumner.xlsx` join keys                                              |

## `source.json`

```json
{
  "description": "free text",
  "workbook": {"file": "Socialstyrelsen/Metadata Test (TST)_webb.xlsx", "from": "sos-source", "cells": {"Metadata - Variabelnivå!L6": "X"}},
  "files": {"classifications/codes.csv": "code,label\n1,One\n"},
  "scb": {"registerinformation": [{"cvid": 1001, "var_id": 101, "colname": "VALUE"}]},
  "variants": {"code-sheet-first": {"order": ["Kodlista_Target"]}}
}
```

Every key is optional; a case delivers what its reader reads.

- `workbook` is one `.xlsx` file, written from the workbook spec below to its `file`
  path.
- `files` maps a path to its text, written as UTF-8. `{"text": ..., "encoding": ...}`
  writes it in another encoding: `latin-1` turns each character U+0000 to U+00FF into
  that one byte, for bytes no text encoding produces.
- `scb` is an SCB snapshot. Each key names one file and lists its rows: a raw
  pipe-delimited line, or for `registerinformation` the keyword arguments of
  `_csv_fixtures.var_row` (as in `cases/build/README.md`, "Source spec"). The keys are
  `registerinformation`, `unika`, `identifierare`, `timeseries`, `vardemangder` and
  `valid_dates`; a file not named is not delivered. `replace_bytes` maps a file name to
  `{placeholder: replacement}`: after writing, each placeholder becomes the latin-1
  bytes of its replacement.
- `variants` names alternative deliveries of the same workbook. Each is a workbook spec
  applied on top of the case's own workbook. `expected.json` `agree` names the result
  paths that must not change (below).

Every bundle file is declared with the revision prepare gives it: its dataset is the
file's path in the bundle and its publisher the provider directory (the path's first
part), `upstream_revision` is `fixture`. The LISA reader declares dataset
`scb-lisa-variable-availability`. An SCB file's dataset is
`scb-<file stem in lower case>`.

### Workbook spec

```json
{
  "file": "Socialstyrelsen/Metadata Test (TST)_webb.xlsx",
  "from": "sos-source",
  "sheets": {"Kodlista_X": [["Tidsperiod", "Kod", "Beskrivning"], ["2020", {"value": 1, "number_format": "000"}, "Ett"]]},
  "cells": {"Metadata - Variabelnivå!E2": "1=ja; 0=nej", "Individ!E600": null}
}
```

- `file` is the workbook's path in the bundle. A Socialstyrelsen register is minted from
  the parenthesised code in the file name, so keep it there.
- `from` starts from a base: a `_sources/<name>.json` workbook, or `lisa`, the
  `_lisa_fixtures.write_lisa_workbook` fixture (its layout is `cases/lisa/layout.json`).
  A spec that changes nothing delivers the base's bytes unchanged.
- `remove` lists sheets to delete, and `rename` maps old sheet names to new ones.
- `sheets` maps a sheet to rows appended to it, in order. A sheet that does not exist is
  created at the end.
- `cells` maps `Sheet!A1` to one cell value. `null` empties the cell.
- `order` lists sheets to move to the front, in that order.
- `state` maps a sheet to `hidden` or `veryHidden`.
- `merge` maps a sheet to its merged ranges.
- `dimension` maps a sheet to the used range its XML declares, such as a stale `A1`.

A cell is a JSON scalar, or an object:

  | Key             | Meaning                                                                          |
  | --------------- | -------------------------------------------------------------------------------- |
  | `value`         | the cell value                                                                   |
  | `datetime`      | an ISO date-time, written as a calendar cell                                     |
  | `number_format` | the number format, such as `000` for a code stored as a number shown padded      |
  | `hyperlink`     | an external link target                                                          |
  | `location`      | an internal link, such as `'Kodlista_HDIA'!A1`                                   |
  | `bold`          | `true` makes the cell bold                                                       |
  | `cached`        | a formula cell's cached result as another producer delivers it (`t="str"`)       |
  | `stored`        | the number exactly as another producer stores it in the sheet XML, such as `7.0` |

## `expected.json`

```json
{
  "reader": "sos_workbook",
  "replaces": "test_sos_source_records.py::test_sos_workbook_flags_claim_identifier_from_kopplingsvariabel",
  "fails_if": "the SOS reader reads a delivered blank Kopplingsvariabel as an unknown identifier claim",
  "projections": [
    {
      "of": "records",
      "where": {"subject": {"member": {"name": "PARTIELL"}}},
      "rows": [{"fields": {"identifier": {"status": "value", "value": true, "raw_value": "X"}}}]
    }
  ]
}
```

  | Key           | Meaning                                                                                                            |
  | ------------- | ------------------------------------------------------------------------------------------------------------------ |
  | `reader`      | Required. The reader, by its name in the table below.                                                              |
  | `args`        | The reader's arguments, for the readers that take any.                                                             |
  | `replaces`    | The Python test (`file::function[param]`), or a list of them, whose assertions the expected values were read from. |
  | `fails_if`    | Required. The concrete product change that makes this case fail. The runner refuses a case without one.            |
  | `note`        | Optional prose: why this case exists, or why it is the hardest form of its rule.                                   |
  | `projections` | The reader returns; each projection claims part of the result (below).                                             |
  | `error`       | The reader refuses. Exactly one of `projections` and `error` is present.                                           |
  | `agree`       | Result paths every `variants` delivery must reproduce exactly, apart from its source revision.                     |

### `fails_if`

Name the product change, not the assertion: "the LISA reader defaults an undeclared
sensitivity to `true` on the company sheet", not "the sensitivity differs". A reviewer
reads it as the failure proof for the case, so it must be a change someone could
plausibly make.

### Projections

The reader's result is turned into JSON data as `cases/curation_toml/README.md`
describes (models by field name, dataclasses by field, sets sorted). Where the result
has a `values` mapping beside its `associations` (code lists and value sets), each
association also carries the value and the list it names, as `value` and `descriptor`.

  | Key     | Meaning                                                                                                             |
  | ------- | ------------------------------------------------------------------------------------------------------------------- |
  | `of`    | Required. The dotted path of a list or mapping in the result, such as `records`; `""` is the root.                  |
  | `where` | Optional. Selects the elements this pattern matches, compared as an `includes` projection.                          |
  | `rows`  | Required. The selected elements, in the reader's order: the list must have the same length.                         |
  | `match` | How each row is compared: `includes` (the default, since a prepared record is too large to state whole) or `exact`. |
  | `note`  | Optional prose: which claim of the case this projection pins.                                                       |

A mapping lists its items as `{"key": ..., "value": ...}`, in the reader's order.

Rows compare by `_case_projection.mismatch`, as `cases/curation_toml/README.md`
("`loads` projection") describes: `{"$any": true}` is an element that must be present
but is not claimed, `{"$exact": value}` compares one value exactly, and a bare `{}` is
refused under `includes`. A nested list is claimed by membership with
`{"$contains": [...]}` (each projection matches some element), `{"$once": [...]}` (each
matches exactly one) or `{"$lacks": [...]}` (none matches), beside `"$length": n` and
`"$last": projection`; `{"$absent": true}` claims that an object has no such key.

A content digest is never written as a literal. Within the selected rows, each distinct
40- or 64-digit hex digest (a record id, a value key, a revision) reads as `#1`, `#2`,
... in order of first appearance. A case claims that two records share an identity, or
do not, by writing the same or different aliases:
`"record_id": "sos-metadata:record:sha256:#1"`.

`agree` takes the same dotted paths as `of`. The selected parts of every variant's
result must equal the case's own, as JSON, once each source revision id is blanked: a
variant delivers other bytes, so its revision differs, and everything derived from the
content must not.

### `error`

  | Key                | Meaning                                                                                 |
  | ------------------ | --------------------------------------------------------------------------------------- |
  | `code`             | Required. The error code the command reports (below).                                   |
  | `exit_code`        | The exit code. Defaults to 10 (`EXIT_CONFIG`).                                          |
  | `type`             | Required. The exception class the reader raises.                                        |
  | `locator`          | Required. Text in the message that names where the refusal points: a cell range, a row. |
  | `message_contains` | Further texts that must appear in the message.                                          |

The prepare readers' refusals are reported as `prepare-sources` reports them: a located
`RegMetaError` keeps its own code, a `ValueError` or `OSError` becomes
`source_preparation_failed` (exit 10), and anything else reaches the command's top level
as `internal_error` (exit 30). `sos_parsed` reports as `parse-sos` does: a
`SosParseError` is `sos_parse_error` (exit 10). No case claims `internal_error`: a
refusal that reaches it is a defect.

## Readers

  | Reader             | Reads                                                                                                  |
  | ------------------ | ------------------------------------------------------------------------------------------------------ |
  | `sos_workbook`     | `clean_sos_source(parse_register_file(file))` (below)                                                  |
  | `sos_parsed`       | `parse_register_file(file)`, the register `parse-sos` prints; with `args.directory`, `parse_directory` |
  | `lisa_workbook`    | `read_lisa_source(file)`: `records`, `worksheet_context`                                               |
  | `code_list`        | `read_code_list(file, name=args.name or the file stem)`                                                |
  | `scb_column_types` | `read_scb_column_types(file)`: `declarations`, `tables`                                                |
  | `scb_join_keys`    | `read_scb_join_keys(file)`: `declarations`, `tables`                                                   |
  | `scb_records`      | `iter_scb_observations(snapshot)`: a list of `{record, issue}`                                         |
  | `scb_auxiliary`    | `iter_scb_auxiliary_records(snapshot, args.file)`: a list of records                                   |
  | `scb_events`       | `read_scb_events(snapshot)`: `declarations`, `tables`                                                  |
  | `scb_values`       | `clean_scb_values(snapshot)`: `provenance`, `descriptors`, `values`, `associations`, `validity`        |

`sos_workbook` returns `records`, `tables`, `descriptors`, `values`, `associations`,
`validity` and `declarations`. `file` is `args.file`, else the one file the case
delivers. `args.revision_from` names another delivered file whose bytes the revision
declares, for a claim about a file that no longer matches its selected revision.

Expected values are read from the test a case replaces, or from the delivery or the
spec. Never copy them from a run of the code under test. A new reader goes in
`test_prepare_cases.py` and in this README in the same change.
