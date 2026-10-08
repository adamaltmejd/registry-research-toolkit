# Curation-TOML cases

Each directory here is one boundary claim about loading committed curation files. The
claim is stated as data:

- the files a curator commits,
- the public loader that reads them,
- the expected result: the loaded value, or the located configuration error the loader
  refuses with.

`test_curation_toml_cases.py` runs every case and implements this format. A case fails
when its loader stops matching its `expected.json`. Every loader reads the case's
`files/` directory in place, so the corpus runs in well under a second.

## Layout

```text
cases/curation_toml/
  <surface>-<behavior>/
    files/**          the files exactly as committed (TOML, CSV or JSON)
    expected.json     the loader and the oracle
```

A case whose files are absent (the "no file loads empty" claims) has no `files/`
directory.

## Naming

Name a case `<surface>-<behavior>`: the curation surface it exercises, then the behavior
in plain words. A case whose loader refuses ends in `-refused`; a case that loads ends
in `-load` or `-loads`. Keep a refusal beside the allowed twin it guards when the twin
is the harder claim.

  | Surface prefix          | Covers                                                                       |
  | ----------------------- | ---------------------------------------------------------------------------- |
  | `register-`             | the register file itself: `[register]`, paths, duplicates, unknown tables    |
  | `errata-`               | `[[errata.*]]` register tables                                               |
  | `scb-errata-`           | SCB errata as the build resolves them (`[[errata.column]]` and its siblings) |
  | `enrichment-`           | `[[enrichment.description]]` and `[[enrichment.alias]]`                      |
  | `identity-`             | `[[identity.*]]` register tables                                             |
  | `representation-`       | `[[representation.*]]` register tables                                       |
  | `coding-`               | `[[coding.*]]` register tables                                               |
  | `acknowledge-`          | `[[acknowledge]]`                                                            |
  | `group-`                | register `[[group]]` concept groups                                          |
  | `classification-`       | `classifications/<short_name>.toml` books and families                       |
  | `classification-group-` | `classification_groups.toml` umbrellas                                       |
  | `worklist-group-`       | the concept-group candidate worklist (`concept_groups.auto.toml`)            |
  | `relations-`            | `relations.toml` edges                                                       |
  | `tags-`                 | `tags.toml`                                                                  |
  | `lineage-`              | `lineage.toml`                                                               |
  | `curation-tree-`        | rules that span files of one curation tree                                   |
  | `slugs-`                | register-owned and provider slug files, panel keys and reserved slugs        |
  | `matrix-`               | CIS answer-matrix evidence JSON                                              |
  | `codes-`                | classification code-list CSVs                                                |
  | `related-documents-`    | `related_documents.toml`                                                     |
  | `curated-source-`       | authored thin-provider source TOML (`Forsakringskassan/`, `scb_canonical/`)  |

## `expected.json`

```json
{
  "loader": "register_files",
  "replaces": "test_delivery_enrichment.py::test_unknown_key_is_rejected_with_file_and_entry",
  "fails_if": "the register loader accepts a misspelled key in an [[enrichment.description]] entry",
  "error": {
    "code": "register_unknown_key",
    "locator": "curation/registers/scb/agi.toml [[enrichment.description.descripton]] entry 1",
    "message_contains": ["Extra inputs are not permitted"]
  }
}
```

  | Key        | Meaning                                                                                                            |
  | ---------- | ------------------------------------------------------------------------------------------------------------------ |
  | `loader`   | Required. The loader, by its name in the table below.                                                              |
  | `args`     | The loader's arguments, for the loaders that take any.                                                             |
  | `replaces` | The Python test (`file::function[param]`), or a list of them, whose assertions the expected values were read from. |
  | `fails_if` | Required. The concrete product change that makes this case fail. The runner refuses a case without one.            |
  | `note`     | Optional prose: why this case exists, or why it is the hardest form of its rule.                                   |
  | `loads`    | The loader returns. `true` claims only that; an object or list is a projection of the result (below).              |
  | `match`    | How `loads` is compared: `exact` (the default) or `includes` (below).                                              |
  | `error`    | The loader refuses. Exactly one of `loads` and `error` is present.                                                 |

### `fails_if`

Name the product change, not the assertion: "`load_tags` lets two `[[tag]]` entries
share a slug", not "the error code differs". A reviewer reads it as the failure proof
for the case, so it must be a change someone could plausibly make.

### `error`

  | Key                    | Meaning                                                                                                                                                        |
  | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `code`                 | Required. The located error code.                                                                                                                              |
  | `exit_code`            | The exit code. Defaults to 10 (`EXIT_CONFIG`): every curation refusal is a configuration error.                                                                |
  | `locator`              | Required. Text that must appear in the message and names where the refusal points: `curation/<file> [[<table>]] entry <n>` for register files, else the entry. |
  | `message_contains`     | Further texts that must appear in the message.                                                                                                                 |
  | `remediation_contains` | Texts that must appear in the remediation.                                                                                                                     |

Every curation loader refuses with a located `RegMetaError`. Beyond the case's own
claims, the runner checks two properties of every refusal: `str()` of the error is its
message, and the message names checkout paths repo-relative, never absolute.

### `loads` projection

The loaded value is turned into JSON data: a model by its field names as written in TOML
(the alias where one exists), a dataclass by its fields, tuples and lists as lists, sets
as sorted lists, and a tuple mapping key joined with `/` (`("scb", "rtb")` is
`"scb/rtb"`).

`match` decides how the expected value is compared, mirroring the build cases' `match`
(`cases/build/README.md`):

- `exact` (the default): the result equals the expected value. An object names every
  key, so an extra key fails and `{}` claims an empty mapping.
- `includes`: an object compares only the keys it names. Use it for a result model too
  large to state whole. Inside an `includes` projection, `{"$exact": value}` compares
  that value exactly: for example `"descriptors": {"$exact": {}}` claims an empty
  mapping. `{"$any": true}` is a value that must be present but is not claimed, such as
  a list element the replaced test never looked at, kept so the list still pins length
  and position. `{"$contains": [...]}` claims a list by membership: each listed
  projection matches some element, in any order. `{"$once": [...]}` claims that each
  matches exactly one element. `{"$lacks": [...]}` claims that no element matches any
  listed projection. Beside them, `"$length": n` claims the list's length and
  `"$last": projection` its last element; one object may carry several.
  `{"$absent": true}` as a key's value claims that the object has no such key. The CLI
  cases (`cases/cli/README.md`) use these for reports whose lists are ordered by minted
  ids.

The runner refuses a bare `{}` inside an `includes` projection: it would check only that
some object is there, so a case whose `fails_if` names that element's content could not
fail. Pin the element's values, or write `{"$any": true}` when the case claims nothing
about it. `$any`, `$contains`, `$once`, `$lacks`, `$length`, `$last` and `$absent` are
refused under `exact` and inside `$exact`, where they would weaken the claim (an exact
object claims an absent key by leaving it out), and so is an empty `$contains`, `$once`
or `$lacks` list or a `$length` that is not a non-negative integer.

In both modes a list compares element by element, in order, and must have the same
length; a scalar compares by value and JSON type, so `true` never matches `1`. There is
no `set` mode: a loader returns ordered lists, and a Python set already projects as a
sorted list.

Do not project content-derived identifiers (record or revision hashes); they restate the
code under test.

## Loaders

Each loader reads `files/` as the curation root, or the named file inside it.

  | Loader                    | Reads                                                                                                                                                                  |
  | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `curation_tree`           | `load_curation_tree(files)`: the whole tree. Needs a `classifications/` file.                                                                                          |
  | `register_files`          | `load_register_files(files)`: `registers/**/*.toml`                                                                                                                    |
  | `classifications`         | `load_classifications(files)` and its families: `{"classifications": [...], "families": {...}}`                                                                        |
  | `scb_errata`              | `resolve_scb_errata` over the loaded registers and declared classification short names, as the build; each column also projects its `key` (register id, folded column) |
  | `concept_groups`          | `load_concept_groups(files)`: register `[[group]]` entries                                                                                                             |
  | `classification_groups`   | `load_classification_groups(files)`: `classification_groups.toml`, as the build reads it                                                                               |
  | `worklist_concept_groups` | `load_worklist_concept_groups(files/concept_groups.auto.toml)`                                                                                                         |
  | `relations`               | `load_relations(files/relations.toml)`                                                                                                                                 |
  | `tags`                    | `load_tags(files/tags.toml)`                                                                                                                                           |
  | `lineage`                 | `load_lineage_config(files/lineage.toml)`                                                                                                                              |
  | `slug_dir`                | `load_slug_dir(files)`: register-owned slugs, or the provider slug files when there is no `registers/`                                                                 |
  | `provider_slugs`          | `load_provider_toml` of the one `*.toml` file                                                                                                                          |
  | `freeze_states`           | `load_freeze_states(files)`: the zone states in `freeze.toml`, or `slug_state.toml` beside a `registers/` tree                                                         |
  | `matrix_evidence`         | `load_matrix(files/matrix.json)` with `args.source_mode` and `args.expected_selector`                                                                                  |
  | `valid_codes`             | `load_valid_codes(files/codes.csv)`                                                                                                                                    |
  | `related_documents`       | `load_related_documents(files/related_documents.toml)`                                                                                                                 |
  | `curated_source`          | `read_curated_source(files/<args.file>)` as `args.provider`, its revision from `args.revision_from`                                                                    |

`curated_source` declares the file's source revision from its own bytes, or from the
bytes of `args.revision_from` to claim a file that no longer matches its reviewed
revision.

Expected values are read from the test a case replaces, or from the source files or the
spec. Never copy them from a run of the code under test. A new loader goes in
`test_curation_toml_cases.py` and in this README in the same change.
