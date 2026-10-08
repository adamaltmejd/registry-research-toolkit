# CLI cases

Each directory here is one boundary claim about a `reg-meta-build` command run on a
built catalog. The claim is stated as data:

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
  <command>/<case>/
    request.json                  the argument list and the replaced test
    files/**                      laid into the working directory before the run
    artifact_curation/**          optional: laid over the artifact's curation (below)
    expected.json                 the exit code, stderr and file claims
    stdout.json                   optional: a projection of the printed JSON
```

`<command>` is the subcommand the case runs (`seed-slugs`, `precheck-slugs`). The runner
checks that the argument list names it.

## The artifact

Every case reads one catalog, built once from `_artifact/` (or a variant of it, below).
The source spec has the format of `cases/build/README.md` → "Source spec", and the build
runs every register in diagnostic mode (a publishable build stamps the builder commit
and so refuses a working tree with uncommitted changes). The runner refuses an artifact
whose build reports an error. The build sits beside the build cases' prepared inputs,
keyed by the content hash of `_artifact/` and the runner, and is published by an atomic
rename, so xdist workers share it. Cases read it in place and never write it; the runner
fails a case that changes the artifact's directory.

The artifact delivers:

  | Register (native id)                                    | Slug      | Variants                                                           | Variables                                            |
  | ------------------------------------------------------- | --------- | ------------------------------------------------------------------ | ---------------------------------------------------- |
  | TESTREG (1)                                             | `sample`  | `1.10` `people`                                                    | `1.101` `value` (VALUE in 2019, VALUE_NY in 2020)    |
  |                                                         |           |                                                                    | `1.102` `kon` (Kön in 2019, Kon in 2020)             |
  | Konjunkturstatistik, löner för statlig sektor (KLS) (2) | `other`   | `2.20` `people` (named like the register less its `(KLS)`)         | `2.201` `value`                                      |
  | Nybörjare i Komvux (3)                                  | `komvux`  | `3.30` `nyborjare` (named like the register)                       | `3.301` `komvux`                                     |
  | PART (4)                                                | `part`    | `4.40` `people`                                                    | `4.5.first`, `4.5.second`: a partition of variable 5 |
  | Företag (6)                                             | `foretag` | `6.60` `foretag` (named like the register), `6.61` `arbetsstallen` | `6.601` `f1`, `6.602` `f2`                           |

A change to `_artifact/` changes what every case reads. Add a register or variable only
for a claim no existing row can carry, and check the cases that enumerate the catalog
(pins files, snapshots, file lists).

A claim about one catalog row that the shared artifact cannot carry without changing
what the other cases read gets its own artifact instead: the case names a directory in
`artifact_curation`, and the runner builds an artifact from `_artifact/` with that
directory laid over its curation tree, replacing files of the same path. It is keyed and
published like the shared one, so cases with the same overlay share it, and the case's
working directory gets that artifact's curation tree.

## The working directory

Each case runs in its own empty directory, `{work}`:

1. The artifact's curation tree is copied to `{work}/curation`, or to each directory
   named in `curation_dirs`.
2. The case's `files/` tree is copied over `{work}`, replacing files of the same path. A
   case changes one register file by shipping its own copy under
   `files/curation/registers/...`.

The copy is cheap, so every case gets one, whether or not its command writes.

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
  | `artifact_curation` | Optional. A directory in the case laid over the artifact's curation tree before the build, for a case that reads its own artifact.                    |

`fails_if` has the meaning it has in the build cases (`cases/build/README.md`): name a
change to the product, not the behavior restated. It sits in `request.json` there and
here, so a reviewer reads it beside the arguments.

Strings in `argv` take two placeholders: `{db}` is the artifact's `--db` directory and
`{work}` the working directory.

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

A claim other than `absent` needs exactly one file to match.

`reloads_with` names a loader, the directory under `{work}` it reads, and the expected
result:

  | Loader     | Returns                                                                             |
  | ---------- | ----------------------------------------------------------------------------------- |
  | `slug_dir` | `snapshot_payload(load_slug_dir(path))`: `{kind: {"<provider>/<native id>": slug}}` |

## `stdout.json`

The command prints the JSON payload of its envelope, or `{"error": {...}}` when it
refuses. `stdout.json` is a projection of what the last run printed, compared the way an
`includes` projection is in `cases/curation_toml/README.md` → "`loads` projection": an
object compares only the keys it names, a list compares element by element and must have
the same length, a scalar compares by value and JSON type, `{"$exact": value}` compares
a value whole, and `{"$any": true}` is a value that must be present but is not claimed.
A bare `{}` is refused. Strings take the `{work}` placeholder.

Do not project values the catalog mints, such as a built register id: they are
surrogates, not claims.

Expected values are read from the test a case replaces, or from the artifact's source
and curation. Never copy them from a run of the code under test. A new key, loader or
placeholder goes in `test_cli_cases.py` and in this README in the same change.
