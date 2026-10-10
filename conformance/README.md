# Conformance suite

The case corpus has one home here. It runs against the Rust server (`reg-meta serve`):
HTTP responses, order manifests, server startup and project validation are the
boundaries. Cases compare domain outputs or located errors, not object internals or
query implementation. Product adapters are not added solely for testing. No private
product imports or internal patches are allowed.

```sh
uv run python -m pytest conformance -q
cargo build --workspace
uv run python -m pytest conformance --run-release --artifact-dir=/path/to/catalog --server-cmd='target/debug/reg-meta serve --db {db} --catalog {catalog} --stewards reg_webapp/stewards --port {port} --write-limit 100000' -q
```

Release admission searches the Rust server, so `--run-release` without `--server-cmd`
fails; without `--run-release`, the search-carrying artifact tests skip. Its sampled
projects all come from one address, so the template raises the server's write limit
(`--write-limit`); the burst case in `test_mcp.py` drops the flag and pins the default.

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
and located findings. Order cases run only against `--server-cmd`: the manifest
download's bytes compare with the case's committed `order.json` (the frozen CLI's,
provenance included), `order`'s `data` with that document, and a blocked case's
`order_blocked` findings on both routes. An `api` step's body is `body` (JSON), a
`content` string sent verbatim (encoded with its optional `encoding`) or
`nested_arrays: N`, an object nesting N arrays deep. No volatile fields are removed from
the comparisons. Keys and lists retain their order. An HTTP response oracle may also pin
raw bytes: `media_type` (the content type without parameters), `headers` (exact values
by lower-case name) and `bytes` (a file in the case directory compared byte for byte).
Where ties have no contract order, a step's `members` (`steps`, `pointer`, `range`,
`equals`) joins `pointer` over those steps (this one or earlier), slices it to `range`
and requires no repeats and exactly the `equals` set.

  | Surface directory | Boundary and request interpretation                                                |
  | ----------------- | ---------------------------------------------------------------------------------- |
  | order             | HTTP order and download (`--server-cmd`), `order.json` bytes, observe projection   |
  | api               | HTTP request sequence and status/pointer oracle, startup refusals (`--server-cmd`) |
  | artifact_sample   | Sampled order entries/clips and search contracts on a readable source              |
  | reader            | Readable sources, not independently executed cases                                 |
  | fixtures          | HTTP readable sources, not independently executed cases                            |

## Out-of-process runner

`--server-cmd` runs the HTTP surfaces above over a real socket; without it they skip. It
is a command template: `{db}` (the artifact directory), `{catalog}` (`global` or the
artifact's steward) and `{port}` are substituted in each word. The runner starts one
server per cached artifact from the repository root, with `REG_META_DB`,
`REG_WEBAPP_STEWARD` and `REG_WEBAPP_STEWARDS_DIR` in its environment, waits until
`GET /openapi.json` answers (120 s at most), reuses it for every case on that artifact
and stops it at session end. A server that exits before answering is retried on a fresh
port, and a start that fails is not retried for later cases on that artifact. Its output
goes to `servers*/server-N.log` under the pytest base temp. A case's `search_pins` names
a pins file in its directory that the artifact build stores (fixtures have no pins
otherwise); an expected `build_error` is that build's located refusal, and such a case
sends no requests.

The Rust server (`reg-meta serve`), on the whole suite, the `api` corpus and the MCP
equivalence suite included (the Rust HTTP run of `RUST_RUNTIME_SPEC.md` section 10, part
of G0 and CI), and the release-admission command on the synthetic steward artifact:

```sh
uv run --no-project scripts/gate.py rust release
```

`scripts/gate.py` holds both commands. `release` skips the synthetic catalog artifact:
on a selected catalog artifact `validate_built_db` applies the real-corpus floors, which
it cannot meet.

`test_mcp.py` (section 9's MCP equivalence) sends raw JSON-RPC to `/mcp` on the
`--server-cmd` server: each `search` step of the `api` cases it names, as a tool call,
returns the HTTP body as `structuredContent` (an error with `isError`), and those steps
cover every error `search` lists in `operations.toml`. `tools/list`, over HTTP and
stdio, equals the golden `cases/mcp/tools-list.json`, and its names are the `tool` of
each served operation. The server builds each tool's schemas from its OpenAPI entry, so
a schema change shows as a reviewed diff of that file. The body cap and the rate limit
on `/mcp` answer with their error documents, and the drained bucket also refuses the
address's project POSTs while its reads pass; the burst runs on a server of its own.
`--mcp-cmd` is a command template (`{db}`, `{catalog}`) for one stdio session:
initialize, list the tools and call `search`. Without `--server-cmd` the module skips;
without `--mcp-cmd` the stdio session does.

## Fixture cache

Every synthetic artifact is built once through the real pipeline into a cache:
`$REG_FIXTURE_CACHE`, else `registry-research-toolkit-fixtures` in the system temp
directory (writable inside agent sandboxes, cleared on reboot). Worktrees and
pytest-xdist workers share it. The key hashes the fixture source, the shared
`cases/reader/fixture` defaults, the kind, identity overrides, a case's search pins, the
`reg_meta_build` sources, the builder, the installed distributions and the Python and
SQLite versions, so any edit is a new entry. Entries live under
`generations/<build-inputs digest>/`, are read-only, and `build_reader_artifact` hands a
mutating case its own copy. A miss builds into a staging directory and renames it into
place. Creating a generation prunes generations idle for 6 hours, and only those, so a
returned path stays valid for 6 hours after its last lookup. Deleting the directory
between runs is safe. The `reg_meta_build` build cases keep their accepted prepared
inputs in generations of the same cache, under the same rule
(`reg_meta_build/tests/cases/build/README.md`). CI sets no `REG_FIXTURE_CACHE`, so a
fresh runner starts both caches cold in its temp directory.
`uv run python conformance/fixture_cache.py reader catalog` builds one entry and prints
its path for consumers outside pytest.

Reader fixtures named `reader` or `reader/<name>` live under `cases/reader`; other named
sources live under `reg_meta_build/tests/cases/holdings`. HTTP fixture names resolve
under `cases/fixtures` unless they name a reader source. The shared builder is
`conformance/reader_artifacts.py`; package tests and the dev server's steward catalog
(`dev.sh --fixture-db` with `REG_WEBAPP_STEWARD`) use it too.

A reader source whose `request.json` names a `filler` binding and a `copies` count is
built with that binding replicated (`replicate_filler`), so a case can rank past a full
candidate prefix without committing a thousand-row catalog.

## API cases

`cases/api` holds the cases for the new API (`api/operations.toml`, `api/errors.toml`),
written red ahead of the Rust server. They use the HTTP case shape above, with these
additions:

- Every step names its `operation`, and its method and path are one of that operation's
  routes. `test_api_spec.py` lints the corpus: cases parse, name existing operations,
  use catalogued error codes and existing fixtures.
- Pointers address the new bodies: `{"data": ..., "meta": ...}` on success and
  `{"error": ..., "meta": ...}` on failure, for example `/data/items/*/fqid`,
  `/data/next_cursor` or `/error/fields/parameter`.
- `artifacts` maps a name to `{"identity": {...}}`, a second artifact built from the
  same fixture and kind with those `identity.json` overrides, applied over the fixed
  import date every case uses (a new generation); a step with `artifact: <name>` is sent
  to it. The stale-cursor case uses this.
- A startup case sets `serve: {"catalog": <name>}` and optional `manifest` overrides,
  which are written to `import_manifest` of a private copy after the build (or
  `serve.db: "absent"`, which serves an empty directory instead), and expects
  `{"startup_error": {"code": ...}}`: the template, run with `{catalog}` set to that
  name, prints the error document as the last line of stderr and exits with the code's
  `exit` status in `api/errors.toml`, without listening.
- A step with `etag_from: N` sends step N's `ETag` as `If-None-Match`.
- `docs` names a readable docs source under `cases/fixtures` (markdown under
  `markdown/<register>/`, `related_documents.toml`, binaries under
  `related/<register>/`); its `reg_meta_docs.db` is built beside the catalog. A case
  without it has no docs database. A startup case's `doc_meta` overrides are written to
  the docs database's `doc_meta`.
- A download step names the operation whose metadata it downloads (`[[download]]`).
  `test_mcp.py` replays each operation's listed cases as tool calls, the path's
  parameters (by the operation's route) as arguments; download steps and a parameter
  sent in both the path and the query have no tool-call spelling and are skipped.

## Artifact checks

SQLite integrity, reader admission, deterministic search/order, sampled
browse/search/validate agreement, order data against its download's bytes, and located
unheld/unresolved refusal run on both synthetic kinds by default, all on the
`--server-cmd` server. A real run uses one admitted schema-9 artifact and also runs
`validate_built_db`, the build's structural authority (foreign keys, manifest identity,
holdings table/column accounting); synthetic artifacts already pass it when they are
built. Real identifiers stay in memory and temporary test request files; failure
messages omit them. Catalog artifacts support global-fallback orders; steward artifacts
only order compiled holdings regardless of reference browsing.

Complete HTTP register-child membership is compared with an independently derived
whole-variable admission set. Browse, search, validation and order agreement uses a
generation-seeded, stratified sample of up to 50 distinct bindings. The public point
resolver (`states` at the binding's period and variant, reference scope) selects
applicable native spellings within independently derived physical mappings. Name search
omits unnamed variables and has a bounded cursor. A sampled binding must appear in its
name-search traversal unless that traversal consumed the whole 1,000-result depth
ceiling and every consumed row matches the query exactly. Exact identity matches always
lead the order, so only a name shared by more bindings than the ceiling holds can push
one past it. Such a name is unreachable by contract, so search narrowed to the binding's
register must find it instead. A name longer than the 200-character `q` cap must be
refused on `q`; the catalog browse checks prove its reachability. The receipt counts the
refinements and refusals.

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
uv run python -m pytest conformance --run-release --artifact-dir=/path/to/steward/catalog --holdings-input=/path/to/accepted-candidate --server-cmd='target/debug/reg-meta serve --db {db} --catalog {catalog} --stewards reg_webapp/stewards --port {port} --write-limit 100000' -q
```

Admission requires a steward artifact, a clean accepted-input Git tree, matching commit
and manifest pins, and every consumed member's size and digest. A wrong path fails;
without the option the census check is deselected. Readable synthetic sources exercise
the same comparison by default. Private identifiers never become repository fixtures or
failure messages. Exhaustive canonical-representative resolution, pinned historical
orders, reference dbdiff, performance, cold boot and rendered acceptance remain
maintainer checks. This suite alone makes no release or deployment acceptance claim.
