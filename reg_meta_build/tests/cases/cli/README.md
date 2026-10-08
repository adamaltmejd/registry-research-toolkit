# CLI cases

Each directory here is one boundary claim about a `reg-meta-build` command run on a
built catalog, about a maintainer program that compares databases of its own (`dbdiff`),
or about a command that reads an input bundle of its own (`inspect-source-records`). The
claim is stated as data:

- the argument list a maintainer types,
- the files in the working directory before the run,
- the expected result: the exit code, the JSON the command prints, its stderr, and the
  files it writes.

`test_cli_cases.py` runs every case and implements this format. A case fails when the
command stops matching its `expected.json` or `stdout.json`.

## Layout

```text
cases/cli/
  _artifact/
    source.json                   the source spec the artifact is built from
    curation/                     the curation tree that builds it
    docs/                         the doc library built beside it (`reg_meta_docs.db`)
  _classified/                    another artifact (`artifact`): source.json and curation/
  <command>/<case>/
    request.json                  the argument list and the replaced test
    *.sql                         optional: the case's own databases (`databases`)
    files/**                      laid into the working directory before the run
    artifact_curation/**          optional: laid over the artifact's curation (below)
    expected.json                 the exit code, stderr and file claims
    stdout.json                   optional: a projection of the printed JSON
```

`<command>` is the subcommand the case runs (`seed-slugs`, `precheck-slugs`,
`concept-group-candidates`), run through `reg_meta_build.cli.run`; the runner checks
that the argument list names it. A command directory named in the runner's `_PROGRAMS`
table is a program of its own and runs through that entry point instead, with the
argument list after its program name:

  | `<command>` | Entry point                  | Program                           |
  | ----------- | ---------------------------- | --------------------------------- |
  | `dbdiff`    | `reg_meta_build.dbdiff.main` | `python -m reg_meta_build.dbdiff` |

A command directory named in `_CHECKOUT_COMMANDS` (`inspect-source-records`) runs
`python -m reg_meta_build` with its argument list in a subprocess, from a committed copy
of the imported `reg_meta` and `reg_meta_build` packages
(`_source_inspection_fixtures.InterpreterCheckout`). The command pins the commit of the
code that interprets its sources and refuses a working tree with uncommitted changes, so
it cannot run in-process from a developer's checkout. The copy is made once per xdist
worker, under that worker's temporary root. The case reads the subprocess's stdout,
stderr and exit code as it reads an in-process run's. A run costs about a second of CPU,
almost all of it the interpreter importing the CLI, so a case states its claims with as
few runs as it can: one report per column filter, and a full report only where the
replaced test claimed the full one too.

## The artifact

Every case reads one catalog, built once from `_artifact/` (or a variant of it, below);
a command that reads databases or an input bundle of its own ignores it. The source spec
has the format of `cases/build/README.md` → "Source spec", and the build runs every
register in diagnostic mode (a publishable build stamps the builder commit and so
refuses a working tree with uncommitted changes). The runner refuses an artifact whose
build reports an error. The build sits beside the build cases' prepared inputs, keyed by
the content hash of `_artifact/` and the runner, and is published by an atomic rename,
so xdist workers share it. Cases read it in place and never write it; the runner fails a
case that changes the artifact's directory.

The artifact delivers:

  | Register (native id)                                    | Slug      | Variants                                                           | Variables                                                          |
  | ------------------------------------------------------- | --------- | ------------------------------------------------------------------ | ------------------------------------------------------------------ |
  | TESTREG (1)                                             | `sample`  | `1.10` `people`, entity key `[value, kon]`                         | `1.101` `value` (VALUE in 2019, VALUE_NY in 2020)                  |
  |                                                         |           |                                                                    | `1.102` `kon` (Kön in 2019, Kon in 2020)                           |
  | Konjunkturstatistik, löner för statlig sektor (KLS) (2) | `other`   | `2.20` `people` (named like the register less its `(KLS)`)         | `2.201` `value`                                                    |
  | Nybörjare i Komvux (3)                                  | `komvux`  | `3.30` `nyborjare` (named like the register), entity key `komvux`  | `3.301` `komvux`                                                   |
  | PART (4)                                                | `part`    | `4.40` `people`, entity key `lopnr`                                | `4.5.first`, `4.5.second`: a partition of variable 5               |
  |                                                         |           |                                                                    | `4.7.lopnrny` `lopnr`, `4.7.konx` `kon`: a partition of variable 7 |
  |                                                         |           |                                                                    | `4.41` `first1`, `4.42` `first2` (names Ålder, Kön)                |
  | Företag (6)                                             | `foretag` | `6.60` `foretag` (named like the register), `6.61` `arbetsstallen` | `6.601` `f1`, `6.602` `f2`                                         |
  |                                                         |           |                                                                    | the worklist rows below, all in `6.60`                             |

Register 6's worklist rows carry the worklist commands' hard cases:

- Succession: the column FORVERS under var 31395 (2019) and var 47670 (2020); ANNINK
  under 41660 and 37046, joined by the curated edge in `curation/relations.toml`; KOD
  under 700 (2019, then KODX in 2020) and 701 (2020); ORT under 800 (2018) and 801
  (2020).
- Concept-group families, one var_id per member: `morsak1-3`, `flop1-3`,
  `tillsyn-1-skolbarn-1-3`, `artal-person-1-3` and `artal-person4-6`, `foo-1-3` and
  `foo4-6`, `q1-3`, `sun-niva2000/2010`, `inkomst1-2000/2010`, `kod3/7/11`,
  `dodsorsak1-3` (with `dodsorsak-text`, the curated group `dodsorsak-forsta` claims
  `dodsorsak1`), `ink1-2`, `solo1`, `agi1lonfink`/`agi2lonfink` and `diag1-2`. Each
  case's `note` says which claim a row carries.

The build groups each same-definition partition (`first`, `kon`) into an `edge` concept
group. The runner builds the doc library from `docs/<register slug>/*.md` into the
artifact's `--db` directory. It documents columns of `sample` and `komvux` and one
directory, `nowhere`, that names no register.

A change to `_artifact/` changes what every case reads. Add a register or variable only
for a claim no existing row can carry, and check the cases that enumerate the catalog
(pins files, snapshots, file lists).

A claim about one catalog row that the shared artifact cannot carry without changing
what the other cases read gets its own artifact instead. When the claim needs only
curation, the case names a directory in `artifact_curation`, and the runner builds an
artifact from `_artifact/` with that directory laid over its curation tree, replacing
files of the same path. It is keyed and published like the shared one, so cases with the
same overlay share it, and the case's working directory gets that artifact's curation
tree.

When the claim needs source rows the shared artifact does not deliver (code lists,
classification books, more registers), the case names another artifact directory in
`artifact`: a directory beside `_artifact/`, its name starting with `_`, with its own
`source.json` and `curation/` (and optional `docs/`). It is built, keyed and published
like `_artifact/`, and every case that names it reads one build. An `artifact_curation`
overlay lays over that directory's curation instead.

`_classified/` is the artifact of the `same-as-candidates` and `classification-residue`
cases. Its `source.json` description lists what it delivers: three registers (`ulf`,
`par`, `hub`), a code list on every column, the classification books in `files`, and
their bindings in `curation/classifications/`. Each case's `note` says which rows carry
its claims.

## The working directory

Each case runs in its own empty directory, `{work}`:

1. The artifact's curation tree is copied to `{work}/curation`, or to each directory
   named in `curation_dirs`.
2. The case's `files/` tree is copied over `{work}`, replacing files of the same path. A
   case changes one register file by shipping its own copy under
   `files/curation/registers/...`.

The copy is cheap, so every case gets one, whether or not its command writes.

## Per-case databases

A command that compares databases (`dbdiff`) reads databases of its own, not the shared
artifact. Their per-case source is readable SQL in the case directory: `databases` maps
a path under `{work}` to a `.sql` file beside `request.json`, and the runner builds each
database with `sqlite3.executescript` after laying in `files/` and before the first run.
Two paths may name the same file, for two databases with the same content. Write a BLOB
as an `X'..'` literal and a control character as `char(n)`, so the file stays readable
as text.

## Input bundles

A command that reads an accepted catalog-input bundle (`inspect-source-records`) reads
one of its own, written from readable source before the first run: `input_bundle` is a
source spec in the format of `cases/build/README.md` → "Source spec" (only its `scb`
key; the bundle captures nothing else) and an optional `lisa` key that selects the
synthetic LISA workbook (`_lisa_fixtures.write_lisa_workbook`, revision `2024-2025`):
`true` as written, or `{"cells": {"Företag!A9": "X", "Företag!C9": 2020}}` with each
`Sheet!A1` cell set first (`null` clears one). Without `lisa` the bundle selects no
workbook. The runner commits the bundle in a repository outside `{work}`, since the
command refuses to write into its input repository, and fills three placeholders that
pin it: `{bundle}`, `{bundle_commit}` and `{bundle_manifest}`.

An `inspect-source-records` report names its records by minted hashes. Before comparing,
the runner replaces each record id in a printed report, and in a report file claimed by
`includes` (below), with an alias read from the report's own `source_records`: an SCB
record is `scb:<native member id>` (the source spec's `cvid`), a workbook record
`<sheet>!row:<n>` (`Företag!row:9`). A claim then reads `"scb_record_ids": ["scb:12"]`.
Two records with one alias fail the case. A report lists records and outcomes in the
order of their hashes, so a case claims a list of them by membership (`$contains`,
`$lacks`, below) and its length through the report's own counts (`summary`,
`expected_source_target_count`, an outcome's sorted `member_ids`).

## `request.json`

  | Key                 | Meaning                                                                                                                                               |
  | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `replaces`          | Required. The Python test (`file::function[param]`), or a list of them, whose assertions the expected values were read from.                          |
  | `fails_if`          | Required. The product change that would make this case fail. The runner refuses a request without one.                                                |
  | `note`              | Optional prose: why this case exists, and where an expected value comes from.                                                                         |
  | `argv`              | The argument list after the program name.                                                                                                             |
  | `env`               | Optional environment variables set for the run, such as `{"REG_META_QUIET": "1"}`. The runner unsets `REG_META_QUIET` otherwise.                      |
  | `runs`              | Instead of `argv` and `env`: a list of `{argv, env}` run in order in one working directory. The runner refuses a top-level `argv` or `env` beside it. |
  | `curation_dirs`     | Optional. Where the artifact's curation tree is copied; defaults to `["curation"]`.                                                                   |
  | `artifact`          | Optional. The artifact directory beside `_artifact/` (for example `_classified`) that the case reads instead (above).                                 |
  | `artifact_curation` | Optional. A directory in the case laid over the artifact's curation tree before the build, for a case that reads its own artifact.                    |
  | `databases`         | Optional. `{path under {work}: SQL file in the case directory}`: the databases built before the run (above).                                          |
  | `input_bundle`      | Optional. The input bundle written and committed before the run (above).                                                                              |

`fails_if` has the meaning it has in the build cases (`cases/build/README.md`): name a
change to the product, not the behavior restated. It sits in `request.json` there and
here, so a reviewer reads it beside the arguments.

Strings in `argv` take two placeholders: `{db}` is the artifact's `--db` directory and
`{work}` the working directory, resolved, so it matches a path a command prints. A case
with an `input_bundle` also takes `{bundle}`, `{bundle_commit}` and `{bundle_manifest}`.

## `expected.json`

  | Key               | Meaning                                                                                   |
  | ----------------- | ----------------------------------------------------------------------------------------- |
  | `exit_code`       | Required. The exit code of every run.                                                     |
  | `stdout_contains` | Texts that must appear in the printed output of the last run.                             |
  | `stderr`          | Claims on the last run's stderr: `empty: true`, `contains` and `excludes` lists of texts. |
  | `files`           | Claims on files under `{work}`, by path (below).                                          |
  | `same_bytes`      | Pairs of paths under `{work}` whose bytes must be equal after every run.                  |
  | `reloads_with`    | A loader that must read a written tree back, and what it returns (below).                 |

A `files` path may be a glob. Each claim is an object:

- `absent: true`: nothing matches the path.
- `toml`: the file parses as TOML to exactly this value.
- `json`: the file parses as JSON to exactly this value.
- `unchanged: true`: the file has the bytes it had before the run.
- `contains`, `excludes`: texts that must (not) appear in the file.
- `includes`: the file parses as strict JSON, with record ids aliased as above, and
  matches this projection the way `stdout.json` does (below). A run that writes its
  report with the global `--output` is claimed this way, so one case can claim several
  runs.
- `jsonl_gz`: the file is gzip-compressed JSON Lines; the list of its values matches
  this projection. Record ids are not aliased. It is the file's only claim.

A claim other than `absent` needs exactly one file to match.

`reloads_with` names a `loader`, the `path` under `{work}` it reads, and the `result` it
must return exactly:

  | Loader                   | Reads                                                                                                   | Returns                                                                                                                    |
  | ------------------------ | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
  | `slug_dir`               | a slug directory, through `load_slug_dir`                                                               | `snapshot_payload(...)`: `{kind: {"<provider>/<native id>": slug}}`                                                        |
  | `relations`              | a relations file, through `load_relations`                                                              | `{"same_as": [{a, b, note}], "replaced_by": [{from, to, from_column, to_column, variant, effective_year}]}`, in file order |
  | `classification_binding` | a worklist's `[binding]` table, through `ClassificationBinding` (a classification file's binding model) | `{"variable": [{variable, note}]}`, in file order                                                                          |
  | `concept_group_worklist` | a candidate catalog, through `load_worklist_concept_groups`                                             | `{"<provider>/<register>/<key>": {label, axes, members: [{variable, delivery_column, coords}]}}`, axes and coords as lists |

## `stdout.json`

A `reg-meta-build` subcommand prints the JSON payload of its envelope, or
`{"error": {...}}` when it refuses; `dbdiff --json` prints its report. The output is
parsed as strict JSON: `NaN`, `Infinity` and `-Infinity`, which Python's parser accepts,
fail the case. `stdout.json` is a projection of what the last run printed, compared the
way an `includes` projection is in `cases/curation_toml/README.md` → "`loads`
projection": an object compares only the keys it names, a list compares element by
element and must have the same length, a scalar compares by value and JSON type,
`{"$exact": value}` compares a value whole, and `{"$any": true}` is a value that must be
present but is not claimed. `{"$contains": [...]}` claims a list by membership, each
listed projection matching some element in any order; `{"$lacks": [...]}` claims that no
element matches any listed projection; and `{"$absent": true}` claims that an object has
no such key (a report leaves out a field that is None). A bare `{}` is refused. Strings
take the `{work}` placeholder. A printed `inspect-source-records` report has its record
ids aliased first (above).

Do not project values the catalog mints, such as a built register id: they are
surrogates, not claims.

Expected values are read from the test a case replaces, or from the artifact's or the
input bundle's source and curation. Never copy them from a run of the code under test. A
new key, loader or placeholder goes in `test_cli_cases.py` and in this README in the
same change.
