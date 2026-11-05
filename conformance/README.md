# Conformance suite

The case corpus has one home here. It runs against the Rust server (`reg-meta serve`):
HTTP responses, order manifests, server startup and project validation are the
boundaries. Cases compare domain outputs or located errors, not object internals or
query implementation. Product adapters are not added solely for testing. No private
product imports or internal patches are allowed.

```sh
cargo build --workspace
cargo test -p reg-conformance --test conformance   # the surfaces below
cargo test -p reg-conformance --test release       # the release checks (below)
uv run --no-project scripts/gate.py rust           # both, after the workspace build
```

The harness is `crates/reg-conformance`. The builder surfaces run
`target/debug/reg-build` on readable case sources and assert exit codes, located
refusals and report files; the synthetic steward is locked once per run and copied for
each case that edits it. It runs `$REG_META_BIN` (default `reg-meta` beside the test in
its cargo target, so a per-worktree `CARGO_TARGET_DIR` works) as
`serve --db D --port 0 --write-limit 100000`, plus `$REG_META_SERVE_ARGS` split on
whitespace. The sampled projects and MCP calls all come from one address, so the raised
write limit keeps the suite inside its rate bucket; the burst case
(`mcp::burst_is_rate_limited`) drops it and pins the default.
`REG_CONFORMANCE_CASE=<substring>` narrows a surface's cases; libtest's own filter
selects surfaces. A failing surface lists every failing case as
`<case>: <first mismatch>`. The `conformance` and `release` test targets are
`test = false`: `cargo test --workspace` would not rebuild the binaries they run, so
they run only by name after `cargo build --workspace`. `scripts/gate.py` holds the
commands.

## Case contract

Each case directory has `request.json` and, except for an order case that orders,
`expected.json`. JSON pointer projections define the compared public result; errors also
pin the status and located findings. In an order case `order`'s `data` compares with the
case's committed `order.json` (authoring it is a content decision, see the `order`
module of `crates/reg-conformance`), less its two volatile fields:
`catalog.generation_id`, checked against the artifact's `meta`, and
`project.project_hash`, checked as 64 hex digits. A blocked case's `order_blocked`
findings compare with its `expected.json`, and `validate` must agree with `order` on
both. An `api` step's body is `body` (JSON), a `content` string sent verbatim (encoded
with its optional `encoding`) or `nested_arrays: N`, an object nesting N arrays deep. No
other volatile fields are removed from the comparisons. Keys and lists retain their
order. An HTTP response oracle may also pin raw bytes: `media_type` (the content type
without parameters), `headers` (exact values by lower-case name) and `bytes` (a file in
the case directory compared byte for byte). Where ties have no contract order, a step's
`members` (`steps`, `pointer`, `equals`) joins `pointer` over those steps (this one or
earlier) and requires no repeats and exactly the `equals` set.

  | Surface directory | Boundary and request interpretation                               |
  | ----------------- | ----------------------------------------------------------------- |
  | order             | HTTP order and validate, `order.json`, blocked findings           |
  | api               | HTTP request sequence and status/pointer oracle, startup refusals |
  | mcp               | The `tools/list` golden                                           |
  | folds, grammar    | Corpora for the fold and the identifier grammars                  |
  | build-report      | `reg-build build`: one complete refusal report before publication |
  | lock              | `reg-build lock`: located refusals, statement identity            |
  | release-ids       | Published ids across catalog releases                             |
  | holdings          | Holdings verification, pinned source admission, the paired server |
  | materialize       | The MONA order file over HTTP                                     |
  | fixtures          | Readable sources, not independently executed cases                |

## Out-of-process runner

The HTTP surfaces run over a real socket. The harness starts one server per artifact
(and served catalog) from the repository root, with `REG_META_DB` in its environment, on
`--port 0`, so the kernel picks a free port. It reads the bound port from the
`listening on ADDR:PORT` line `reg-meta serve` prints on stderr, and waits until
`GET /openapi.json` answers on it (120 s at most). Two listeners cannot share a port, so
parallel runs never reach each other's servers. It reuses the server for every case on
that artifact. A server that exits first fails with its log tail, and a start that fails
is not retried for later cases on that artifact. Its output goes to
`conformance-<pid>/server-N.log` under `<target>/tmp`, and each server runs under a
watchdog `sh` that kills it when the test process exits, however it exits.

The `mcp` module (MCP equivalence) sends raw JSON-RPC to `/mcp` on the case servers:
each step of the `api` cases it names, as a tool call, returns the HTTP body as
`structuredContent` (an error with `isError`) that validates against the tool's
`outputSchema`, and those steps cover every error the operation lists in
`operations.toml`. `tools/list`, over HTTP and stdio, equals the golden
`cases/mcp/tools-list.json`, and its names are the `tool` of each served operation. The
server builds each tool's schemas from its OpenAPI entry, so a schema change shows as a
reviewed diff of that file; the stdio session (`reg-meta mcp`: initialize, list the
tools and call `search`) writes it under
`REG_META_BLESS=1 cargo test -p reg-conformance --test conformance stdio_session`
(`scripts/gate.py regen`). The body cap and the rate limit on `/mcp` answer with their
error documents, and the drained bucket also refuses the address's project POSTs while
its reads pass; the burst runs on a server of its own.

## Fixtures and the fixture cache

A fixture is a frame-JSONL input tree under `cases/fixtures/<name>/`: `inputs/` (the
frame JSONL, and whatever else its readers read) and `curation/` (`inputs.toml` and the
curation beside it), the shape of `crates/reg-build/fixture`. The harness builds it with

```sh
target/debug/reg-build build --inputs <fixture>/inputs --curation <fixture>/curation \
  --out <dir>/reg_meta.db --report <dir>/report.json
```

and never runs cargo itself: `scripts/gate.py rust` builds the workspace first. `build`
verifies what it writes (`reg-build verify`), so a synthetic artifact is checked when it
is built. `fixtures/base` has one provider (`demo`), one register with one table, two
yearly releases, three variables (one with a value set whose third item has the blank
code) and a label that differs between the years. A fixture without
`curation/inputs.toml` is refused by this harness.

Every artifact is built once into a cache: `$REG_FIXTURE_CACHE`, else
`registry-research-toolkit-fixtures` in the system temp directory (writable inside agent
sandboxes, cleared on reboot). Worktrees and parallel test runs share it. The key hashes
the fixture tree's file paths and bytes and the `reg-build` binary's bytes, so any edit
or rebuild is a new entry. Entries live under `generations/<binary digest>/`, are
read-only, and a mutating case copies its own. A miss builds into a staging directory
and renames it into place. Creating a generation prunes generations idle for 6 hours,
and only those, so a returned path stays valid for 6 hours after its last lookup.
Deleting the directory between runs is safe. CI sets no `REG_FIXTURE_CACHE`, so a fresh
runner starts cold in its temp directory. `target/debug/reg-fixture base` builds one
entry and prints its path for consumers outside the suite (`dev.sh`);
`reg-fixture --paired <out>` builds the synthetic holdings pair.

## API cases

`cases/api` holds the cases for the API (`api/operations.toml`, `api/errors.toml`),
served by the Rust server. They use the HTTP case shape above, with these additions:

- Every step names its `operation`, and its method and path are one of that operation's
  routes. The `api_spec` module lints the corpus: cases parse, name existing operations
  and use catalogued error codes.
- Pointers address the new bodies: `{"data": ..., "meta": ...}` on success and
  `{"error": ..., "meta": ...}` on failure, for example `/data/items/*/fqid`,
  `/data/next_cursor` or `/error/fields/parameter`.
- `fixture` names a directory under `cases/fixtures` (default `base`).
- `artifacts` maps a name to a second artifact for the case (it may name its own
  `fixture`); a step with `artifact: <name>` is sent to it.
- A startup case sets optional `manifest` overrides, which are written to `meta` of a
  private copy after the build (a `null` deletes the key), or `serve.db: "absent"`,
  which serves an empty directory instead, and expects
  `{"startup_error": {"code": ..., "fields": ...}}` (`fields` optional): the template
  prints the error document as the last line of stderr and exits with the code's `exit`
  status in `api/errors.toml`, without listening.
- An oracle's `keys` maps a pointer to the sorted key set of the object there, for an
  object whose values no oracle can spell (`meta`'s generation).
- A step with `etag_from: N` sends step N's `ETag` as `If-None-Match`, and one with
  `if_none_match` sends that literal header.
- A download step names the operation whose metadata it downloads (`[[download]]`). The
  `mcp` module replays each operation's listed cases as tool calls, the path's
  parameters (by the operation's route) as arguments; download steps and a parameter
  sent in both the path and the query have no tool-call spelling and are skipped.

## Release checks

The `release` target (`crates/reg-conformance/tests/release`) holds the checks that need
a real catalog, and only behaviour neither the fixture corpus nor the release's
`reg-build verify` and `diff` show. `REG_ARTIFACT_DIR` names the artifact directory;
`REG_CATALOG=NAME` serves `reg_meta_NAME.db` beside `reg_meta.db` as `--catalog NAME`
(`global` or unset: the reference catalog alone). Without `REG_ARTIFACT_DIR` it runs on
the `base` fixture's artifact, as `gate.py rust` does, so it stays runnable between
releases. Release step 8f runs it once per catalog with a release-built server
(`REG_META_BIN`); integration.yml does the same on the published assets.

- `order`: sampled whole-table projects agree with an independent SQL census of the
  artifact on `validate` and `order`, each entry carries the spelling the census holds
  at its start, repeated orders are byte-identical, and the first three also order
  through the MCP `order` tool with the same document. On a pair the census samples the
  reference tables the holdings place something in, binds the variables placed there and
  reads the holdings file too. It prints how many tables it sampled and how many it
  found blocked, and fails when every sampled table is blocked.
- `search`: paging stops at depth 1000 without repeating a hit. The fixture never
  reaches the depth, so the check skips there and fails on a real catalog that does not.
- `held` (a pair only): the browse root, every register and table it lists, a search per
  letter and every listed variable's segments name only ids the holdings place something
  in, and each segments row's (variable, table) pair is placed. The filter lives in each
  reader operation, so an operation that forgets it would leak.

## Verification gates

One gate, named G0 so it is not confused with the test tiers of `ARCHITECTURE.md`
("Tiers", which also has the budgets): `scripts/gate.py g0` (lint,
`cargo test --workspace`, and the touched packages) and `scripts/gate.py rust` (this
suite against the Rust server), all on synthetic artifacts. Each release is checked by
`reg-build diff` against the previous one.
