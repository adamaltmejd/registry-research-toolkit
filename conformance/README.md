# Conformance suite

The case corpus has one home here. CLI JSON, public library return models, order
manifests, HTTP responses, application boot and project validation are the boundaries.
The library cases retain documented public contracts that have no equivalent adapter
projection. Public naming alone is insufficient: cases compare domain outputs or located
errors, not object internals or query implementation. Product adapters are not added
solely for testing. No private product imports or internal patches are allowed.

```sh
uv run python -m pytest conformance -q
uv run python -m pytest conformance --run-release --artifact-dir=/path/to/catalog -q
```

The first invocation runs the artifact checks on the synthetic catalog and steward
artifacts. Fixture-bound cases always build their own named readable source; they never
substitute the selected real artifact into synthetic value goldens. The second
invocation adds checks on exactly the selected artifact. Both flags are required for
tier 3; the reader rejects missing, schema-incompatible and non-publishable artifacts
before execution. Existing package `release` consumers keep their marker contract.

Use the equals form for `--artifact-dir`: pytest discovers roots before loading custom
options, and a space-separated external directory can select that directory's checkout
configuration. The documented equals form selects the current checkout reliably.

## Case contract

Each case directory has `request.json` and `expected.json`. `observe` fields and JSON
pointer projections define the compared public result; errors also pin the exit/status
and located findings. Orders additionally compare CLI and materializer serialization and
repeat raw bytes, including provenance. Every validate and order step in the validate
surface also runs through `reg-meta validate` or `reg-meta order`: a 200 compares bytes
and exit code with the HTTP response, and a 400 (a malformed document, sent verbatim
from a step's `content` string, encoded with its optional `encoding`, or built by
`nested_arrays: N` as an object nesting N arrays deep) compares the CLI's exit 10
envelope message with the HTTP `detail`. No volatile fields are removed from those
comparisons. Keys and lists retain their order. Path placeholders in the selection
oracle expand to the test filesystem before comparison.

  | Surface directory                                             | Boundary and request interpretation                                                |
  | ------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
  | cli_scope                                                     | CLI argv, optional second page, observe projection                                 |
  | order                                                         | Public materializer plus CLI bytes, project and observe projection                 |
  | coverage                                                      | Public coverage return models, provider/register                                   |
  | logical                                                       | Public query/catalog operation, args/kwargs and observe projection                 |
  | reader                                                        | Public listing/cursor/concept group return models; also source fixtures            |
  | selection                                                     | CLI artifact selection; implicit annual-series source                              |
  | update                                                        | Downloaded-artifact identity via CLI/update library; implicit annual-series source |
  | boot                                                          | App startup; implicit reader source, kind and manifest mutation                    |
  | http_catalog, http_context, http_scope, http_search, validate | HTTP request sequence and status/pointer oracle; implicit compiled source          |
  | validate (also)                                               | CLI validate/order bytes or refusal against each HTTP project response             |
  | fixtures                                                      | HTTP readable sources, not independently executed cases                            |

## Fixture cache

Every synthetic artifact is built once through the real pipeline into a per-user cache:
`$REG_FIXTURE_CACHE`, else `$XDG_CACHE_HOME` (or `~/.cache`) under
`registry-research-toolkit/fixture-artifacts`. Worktrees and pytest-xdist workers share
it. The key hashes the fixture source, the shared `cases/reader/fixture` defaults, the
kind, identity overrides, the `reg_meta_build`, `reg_meta` and `reg_schema` sources, the
builder, `uv.lock` and the Python and SQLite versions, so any edit is a new entry.
Entries are read-only; `build_reader_artifact` hands a mutating case its own copy. A
miss builds into a staging directory and renames it into place. Generations idle for an
hour are pruned when a new one starts; deleting the directory between runs is safe.
`uv run python conformance/fixture_cache.py --fixture reader catalog` builds one entry
and prints its path for consumers outside pytest.

Reader fixtures named `reader` or `reader/<name>` live under `cases/reader`; other named
sources live under `reg_meta_build/tests/cases/holdings`. HTTP fixture names resolve
under `cases/fixtures` unless they name a reader source. The shared builder remains
`reg_meta/tests/reader_artifacts.py` because package tests and dev servers use it. The
dev script retains its path and consumes these same sources.

Two inherited standalone oracles retain their consumers:
`selection/update-expected.json` compares named/path/environment update trials;
`selection/doc-cursor-expected.json` is consumed by the existing package document CLI
case. Its request builds selection/docs, runs docs search for Example with limit 1,
follows the returned cursor, reads the named catalog document, and lists local docs. The
source-backed document case stays in its package; its fixture and oracle have one home
here. Requests without explicit fixture keys retain the defaults above so all relocated
bytes remain unchanged.

## Artifact checks

SQLite integrity and FKs, reader admission, existing manifest table/column scope
accounting, deterministic search/order, sampled browse/search/validate agreement,
CLI/HTTP/materializer order bytes, CLI/HTTP validation bytes, and located
unheld/unresolved refusal run on both synthetic kinds by default. A real run uses one
admitted schema-9 artifact. Real identifiers stay in memory and temporary test request
files; failure messages omit them. Catalog artifacts support global-fallback orders;
steward artifacts only order compiled holdings regardless of reference browsing.

Complete HTTP register-child membership is compared with an independently derived
whole-variable admission set. CLI browse, search and order agreement uses a generation-
seeded, stratified sample of up to 50 distinct bindings. The public point resolver
selects applicable native spellings within independently derived physical mappings. CLI
`get schema` exposes applicable delivery columns, so it cannot establish the whole
variable-node census; name search also omits unnamed variables and has a bounded cursor.
A sampled binding must appear in its CLI and HTTP name-search traversals unless that
traversal consumed the whole 1,000-result depth ceiling and every consumed row matches
the query exactly. Exact identity matches always lead the order, so only a name shared
by more bindings than the ceiling holds can push one past it. Such a name is unreachable
by contract, so the register-refined CLI search must find the binding instead. That
proves reader reachability; HTTP search has no register refinement, and the catalog
browse checks prove HTTP reachability. The receipt counts these refinements.

`integration.yml` downloads the global catalog and SWECOV steward assets from the
selected release, verifies each digest and admits its embedded manifest before running
tier 3. The artifact jobs and native registry-install job run independently; a failed
artifact gate does not suppress the other artifact run or native install checks.
Incompatible published assets remain failures until a matching release is available.

Schema-9 manifests do not contain mapping, period or unmapped-reason totals. Comparisons
of those relation counts to manifest fields cannot run until that contract exists. Exact
accepted-input table/cell accounting, authored mappings and policy digests run only with
explicit `--holdings-input` alongside both tier-3 flags:

```sh
uv run python -m pytest conformance --run-release --artifact-dir=/path/to/steward/catalog --holdings-input=/path/to/accepted-candidate -q
```

Admission requires a steward artifact, a clean accepted-input Git tree, matching commit
and manifest pins, and every consumed member's size and digest. A wrong path fails;
without the option the census check is deselected. Readable synthetic sources exercise
the same comparison by default. Private identifiers never become repository fixtures or
failure messages. Exhaustive canonical-representative resolution, pinned historical
orders, reference dbdiff, performance, cold boot and rendered acceptance remain
maintainer checks. This suite alone makes no release or deployment acceptance claim.

The inherited `catalog_method`/`method` request fields name Python APIs. A later
portability pass can replace those with language-neutral domain operation names and
explicit input/output contracts. That is a separate corpus content review; this move
preserves every existing request and expected byte.
