# Conformance suite

The case corpus has one home here. It runs against the Rust server (`reg-meta serve`):
HTTP responses, order manifests, server startup and project validation are the
boundaries. Cases compare domain outputs or located errors, not object internals or
query implementation. Product adapters are not added solely for testing. No private
product imports or internal patches are allowed.

```sh
cargo build --workspace
cargo test -p reg-conformance --test conformance   # the Rust harness (surfaces below)
uv run --no-project scripts/gate.py rust           # it, then the pytest surfaces
```

The Rust harness (`crates/reg-conformance`) is replacing the pytest one surface by
surface; `api`, `api_spec`, `mcp`, `order`, `materialize` and the builder CLI (`lock`,
`ids`, `build_report`, `holdings*`) have moved, and only the tier 3 checks remain in
pytest. The builder surfaces run `target/debug/reg-build` on readable case sources and
assert exit codes, located refusals and report files; the synthetic steward is locked
once per run and copied for each case that edits it. It runs
`$REG_META_BIN` (default `reg-meta` beside the test in its cargo target, so a
per-worktree `CARGO_TARGET_DIR` works) as `serve --db D --port 0 --write-limit 100000`,
plus `$REG_META_SERVE_ARGS` split on whitespace. The sampled projects and MCP calls all
come from one address, so the raised write limit keeps the suite inside its rate bucket;
the burst case (`mcp::burst_is_rate_limited`) drops it and pins the default.
`REG_CONFORMANCE_CASE=<substring>` narrows a surface's cases; libtest's own filter
selects surfaces. A failing surface lists every failing case as
`<case>: <first mismatch>`. The `conformance` test target is `test = false`:
`cargo test --workspace` would not rebuild the binaries it runs, so it runs only by name
after `cargo build --workspace`.

Every pytest run needs `--server-cmd`: the suite's boundary is the Rust server, and a
run without it is refused at startup. The template raises the server's write limit
(`--write-limit`) for the same reason. `scripts/gate.py` holds the commands.

The `rust` step runs the whole suite on the synthetic `base` artifact, then only the
checks on exactly the selected artifact (`-m release --run-release --artifact-dir=…`),
again on the `base` artifact. Both flags are required for tier 3. Use the equals form
for `--artifact-dir`: pytest discovers roots before loading custom options, and a
space-separated external directory can select that directory's checkout configuration.

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

`--server-cmd` runs the HTTP surfaces above over a real socket. It is a command
template: `{db}` (the artifact directory) and `{port}` are substituted in each word;
`{port}` is 0, so the kernel picks a free port. The runner starts one server per cached
artifact from the repository root, with `REG_META_DB` in its environment, reads the
bound port from the `listening on ADDR:PORT` line `reg-meta serve` prints on stderr, and
waits until `GET /openapi.json` answers on it (120 s at most). Two listeners cannot
share a port, so parallel workers never reach each other's servers. It reuses the server
for every case on that artifact and stops it at session end. A server that exits first
fails with its log tail, and a start that fails is not retried for later cases on that
artifact. Its output goes to `servers*/server-N.log` under the pytest base temp. The
Rust harness does the same, logging to `conformance-<pid>/server-N.log` under
`<target>/tmp`, and starts each server under a watchdog `sh` that kills it when the test
process exits, however it exits.

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
sandboxes, cleared on reboot). Worktrees and pytest-xdist workers share it. The key
hashes the fixture tree's file paths and bytes and the `reg-build` binary's bytes, so
any edit or rebuild is a new entry. Entries live under `generations/<binary digest>/`,
are read-only, and a mutating case copies its own. A miss builds into a staging
directory and renames it into place. Creating a generation prunes generations idle for 6
hours, and only those, so a returned path stays valid for 6 hours after its last lookup.
Deleting the directory between runs is safe. CI sets no `REG_FIXTURE_CACHE`, so a fresh
runner starts cold in its temp directory. `target/debug/reg-fixture base` builds one
entry and prints its path for consumers outside the suite (`dev.sh`, the gate);
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

## Artifact checks

`test_artifact.py` runs `reg-build verify` on the selected artifact (`--artifact-dir`),
the one structural authority over a catalog file, and fails on a non-zero exit printing
only the first line of its stderr. Synthetic artifacts are verified when they are built.
The `rust` step's release admission runs it on the `base` fixture's artifact; a release
runs it on the artifact it ships. The release search checks sample each indexed kind and
traverse paging to the depth cap when the artifact has enough hits. The release order
check compares sampled whole-table projects with an independent artifact census and
checks HTTP/MCP agreement. Both compare repeated responses as raw bytes.

## Verification gates

One gate, named G0 so it is not confused with the test tiers of `ARCHITECTURE.md`
("Tiers", which also has the budgets): `scripts/gate.py g0` (lint,
`cargo test --workspace`, and the touched packages) and `scripts/gate.py rust` (this
suite against the Rust server), all on synthetic artifacts. Each release is checked by
`reg-build diff` against the previous one.
