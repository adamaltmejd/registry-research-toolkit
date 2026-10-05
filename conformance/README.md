# Conformance suite

The case corpus has one home here. CLI JSON, public library return models, order
manifests, HTTP responses, application boot and project validation are the boundaries.
The library cases retain public contracts that have no equivalent adapter projection. No
private product imports or internal patches are allowed.

```sh
uv run python -m pytest conformance -q
uv run python -m pytest conformance --run-release --artifact-dir=/path/to/catalog -q
```

The first invocation builds catalog and steward artifacts once per session for the
artifact checks. Fixture-bound cases always build their own named readable source; they
never substitute the selected real artifact into synthetic value goldens. The second
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
repeat bytes. See `normalization.py` for the only permitted volatile-field removal list.
Keys and lists retain their order during byte comparisons. Path placeholders in the
selection oracle expand to the test filesystem before comparison.

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
  | fixtures                                                      | HTTP readable sources, not independently executed cases                            |

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
CLI/HTTP/materializer order bytes, and located unheld/unresolved refusal run on both
synthetic kinds by default. A real run uses one admitted schema-9 artifact. Real
identifiers stay in memory and temporary test request files; failure messages omit them.
Catalog artifacts support global-fallback orders; steward artifacts only order compiled
holdings regardless of reference browsing.

Plan 02 manifests do not contain mapping, period or unmapped-reason totals. Comparisons
of those relation counts to manifest fields cannot run until that contract exists.
Accepted-private-input census and performance acceptance remain maintainer checks in
plan 05. This suite makes no release or private-input acceptance claim.
