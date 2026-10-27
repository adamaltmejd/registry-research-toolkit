# reg_webapp — design

The Rust server (`reg-meta serve`) answers every `/api` route: catalog reads (context,
search, docs and catalog nodes) and project operations (validate / order). There is no
Python web server. This file records the package-local design rationale. Cross-cutting
topology (package tree, dependency graph, perf budgets, version policy, testing-strategy
overview) lives in the root `ARCHITECTURE.md`; remaining/unbuilt work lives in GitHub
issues. The API contract is `conformance/api/operations.toml` with its snapshot
`crates/reg-meta/openapi.json`.

The SPA is deleted from this checkout and archived on `archive/reg-webapp-spa`.
Milestone E rewrites it on the v5 API from that archive and
`claude/rebuild-s-shell-search`.

**Catalog artifact.** `ARCHITECTURE.md` and `crates/DESIGN.md` own the compiled
contract. The webapp admits one immutable catalog DB and delegates reads to the shared
SQL reader. Holdings, scope and steward branding return in milestone E.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private state**
(project files belong to clients, never to the server). "Auth" here is really cost
protection: read GETs support ETag revalidation, and POSTs carry an origin-side
body-size cap and per-client rate limit (Cost protection, below). Real auth is a v2+
concern, layered on only if a steward ever needs private data.

## Layout

```text
reg_webapp/
  Dockerfile              # catalog bake and Rust server image
  docker-entrypoint.sh    # server startup and readiness probes
  fly.toml                # global origin deployment
  edge/                   # global hosted MCP edge worker
  stewards/               # only swecov/: old builder inventory retained until E
  DESIGN.md
```

`stewards/swecov/` remains input to the old builder until milestone E. The current
server and container load no steward configuration or logos.

## Boot seam (artifact admission)

The Rust server admits its catalog at startup and refuses to serve otherwise
(`Catalog::open` in `crates/reg-catalog`): metadata must be well formed and its schema
version must exactly equal `reg_core::CATALOG_SCHEMA_VERSION`. The `api` admission
corpus pins the refusals. The webapp ships no DDL or schema-version authority;
`reg-build verify` owns published artifact invariants.

## Catalog operations (the Rust server)

`reg-build` writes the immutable catalog; `reg-meta serve` reads it through
`reg-catalog`. The Python builder remains for its existing release and steward paths
until milestone F. It does not write the catalog served by the current global server.

`conformance/api/operations.toml` defines the complete operation surface, parameters,
response shapes and error codes. `crates/reg-meta/openapi.json` is its server snapshot.
Catalog operations expose nodes, segments, findings, coverage, schemas, relationships
and content-addressed value sets. Search ranks FTS hits across registers, tables,
variables, classifications, codes and documents. Pins and synonyms arrive with curation
in milestone D. Holdings and physical orders arrive in milestone E.

Documentation lives in the same admitted catalog. The docs operations search document
sections, return a document by content hash, list linked documents and serve embedded
PDF bytes. There is no separate docs database or schema gate.

## Context and catalog sizes (the Rust server)

`GET /api/context` returns catalog metadata and headline sizes as `{data, meta}`. The
committed OpenAPI snapshot describes its response; a Rust snapshot test keeps it
identical to the server's rendered document.

## ETag / Cache-Control

The Rust server sets the reads' ETags and `Cache-Control` (tiers in
`crates/reg-catalog`, validators in `crates/reg-meta/src/validator.rs`); a POST answer
(`validate`, `order`) carries no validator.

**Early revalidation (#1453).** App code and the compiled catalog DB are immutable for a
process lifetime; changing either replaces the process. At startup the server derives
one token from the SHA-256 of its own binary and the catalog database file. File bytes,
not identity fields: `generation_id` does not cover `catalog_version`, which reaches
responses. The image hashes the binary and database when it is built (`Dockerfile`,
passed in as `REG_META_CONTENT_DIGEST` by the entrypoint): hashing the catalog at boot
on the throttled rootfs would outlast the entrypoint's 60 s readiness deadline. Without
the variable (local runs, the conformance suite) the server hashes them at boot. A GET
read's strong ETag is that token plus the request's path and query. A tower layer in
front of every GET operation and download (a route layer, so another method keeps its 405)
answers a matching `If-None-Match` with a 304 before the route runs, so a revalidation
does no DB work and no body serialization. `If-None-Match: *` still runs the route,
since it matches only a representation that exists. Only a 200 carries the ETag and
tier.

The server owns each read operation's cache tier. Context revalidates always; catalog
and search reads use a short browser lifetime and a longer shared-cache lifetime;
document reads use a day. The global edge forwards only MCP, so these HTTP read cache
headers apply to direct API clients. Origin cache headers do not establish that a public
edge served a response from cache.

## Production performance budgets

Search origin p95 must stay under 500 ms. Catalog reads target p95 under 200 ms and
project validation/order under 1 s. Measurements at the origin remain separate from edge
evidence.

## Compiled artifact identity

Boot opens the single `reg_meta.db` artifact and admits its metadata against
`reg_core::CATALOG_SCHEMA_VERSION` by exact equality. Published catalog invariants
belong to `reg-build verify`. The runtime loads no steward branding or delivery
inventory; steward artifacts and their deployment return in milestone E.

The Rust server's `context` reports schema version, catalog version, server version and
catalog sizes. Every JSON response's `meta` carries contract version, catalog version
and generation. Context's optional `period_span` is the hull of the catalog's register
periods; it is null when the catalog has no registers.

## OpenAPI snapshot (the drift gate)

`crates/reg-meta/openapi.json` is committed and is the canonical server contract.
`cargo test -p reg-meta --test openapi_snapshot` asserts that it equals the server's
rendered document. `scripts/gate.py regen` refreshes the snapshot.

## Deployment (`global` on Fly.io, Cloudflare edge in front)

The container runs on Fly.io behind Cloudflare. The immutable SQLite artifact and server
binary are baked into a plain Docker image; `fly.toml` and the deploy job supply the
platform-specific configuration.

The global catalog has no public website (maintainer, 2026-10-20, #1709): it is not what
SWECOV offers, so it is not published under swecov.se. Agents use its hosted MCP (no
install, no 1.2 GB download), so `reg-webapp-global` serves `/mcp` alone at
`reg-meta.vqv.uk`. vqv.uk is the maintainer's side-project zone, chosen to keep the
personal altmejd.se zone out of it. `data.swecov.se` retains its last old-schema
catalog, SPA and MCP. Its running app is untouched; it has no deploy path until
milestone E.

- **The image runs the Rust server alone**: `reg-meta serve --db /opt/reg_meta` with one
  baked DB, no Python or steward branding at runtime. The Dockerfile has three stages:
  the `regmeta-db` bake (Debian slim with `curl` and `zstd`: download the catalog asset,
  verify it against the SHA-256 digest GitHub records for it, unpack), a pinned Rust
  stage building `reg-meta`, and a Debian slim runtime with `curl` for the smoke gate.
  The image serves the API and `/mcp` only; the existing SWECOV edge worker serves the
  last old-schema SPA until milestone E restores its deploy jobs. The global image
  serves `/api` too (the binary has no per-route switch, and fly.toml's health check
  reads `/api/context`); its edge worker forwards none of it.
- **Hosted MCP** (`crates/DESIGN.md`, "Agent and web interface"): `/mcp` on
  `reg-meta.vqv.uk` (the global catalog) and on `data.swecov.se`, whose SWECOV artifact
  is public. Both workers forward `/mcp` (`ROUTE_MCP`); the active `fly.toml` passes
  `REG_META_PUBLIC_HOST`, which rmcp's allowed hosts admit beside the loopback names.
  `/mcp` and the project POSTs share one rate limit per client address (60 a minute;
  Cost protection, below). Behind Fly the peer is Fly's proxy, so the worker proves a
  request came through the edge with a shared secret (`EDGE_TOKEN` on the worker,
  `REG_META_EDGE_TOKEN` on the Fly app, sent as `x-edge-token`); only such a request is
  keyed on its `CF-Connecting-IP`. Any other request, a direct-origin hit included, is
  keyed on its peer, so a forged `CF-Connecting-IP` buys nothing. A worker without the
  secret refuses every forward with a `no-store` 503, rather than put every edge client
  in the proxy's one bucket. After each edge deploy, `container-build.yml` sends
  `initialize`, `tools/list` and one `search` to the global public `/mcp`. SWECOV has no
  edge deploy until E.
- **Apps**: `reg-webapp-global` serves `reg-meta.vqv.uk`; `reg-webapp-swecov` serves
  `data.swecov.se`. Each is a single always-on `shared-cpu-1x`/2GB machine in `arn`
  (Stockholm, where the users are). Always-on is deliberate: Fly's ephemeral-rootfs I/O
  is throttled (\~8 MiB/s), so a cold boot re-reads the SQLite database slowly — keep
  the OS page cache warm rather than scale to zero (\~$6/mo per app). Config:
  `reg_webapp/fly.toml`; `--ha=false` keeps the machine count at one. `fly.swecov.toml`
  and the three SWECOV image, deploy and edge-deploy jobs are deleted until E; no CI or
  manual workflow path deploys that app.
- **Read-only SQLite on the ephemeral rootfs is the right model** — the DB is baked into
  the image and replaced with it. No volume, no LiteFS, nothing persists.
- **Deploys**: one workflow (`container-build.yml`) owns the global origin and edge
  worker, scoped by a `changes` paths-filter job. Image-affecting main pushes
  (Dockerfile COPY surfaces + bake inputs) build, push to `registry.fly.io`
  (SHA-tagged), and `flyctl deploy --image` the global origin. The bake build-args are
  the RESOLVED newest `reg_meta/v*` tag (never `latest` — a literal `latest` would make
  the bake layer's buildx cache key insensitive to data-only releases and could even
  resurrect a stale cached layer after a pinned dispatch) and the SHA-256 digest GitHub
  records for `reg_meta.db.zst`, which the `schema-guard` job resolves with the tag; the
  bake has no defaults and refuses an unverified download. The global Fly app uses
  `FLY_API_TOKEN`. The bake downloads one catalog DB into `/opt/reg_meta`; the server
  admits it at boot, and the schema guard below blocks a mismatched release. Nothing
  deploys without green CI: a `wait-ci` job polls this commit's ci.yml run and the
  origin/edge deploy jobs require its success — an image that builds but fails
  lint/ty/pytest never ships. Each deploy job carries a HEAD-of-main guard (GHA
  concurrency serializes by build-completion order, not commit order — without the guard
  an older commit's slow build could overwrite a newer deploy; it also makes non-main
  dispatches deploy-inert). Because a guarded commit skips, `changes` diffs a push
  against the last deployed commit, not the push's `before` (#1481): otherwise a
  served-path push followed by a non-served one before its deploy ran would deploy
  neither. The last deployed commit is the newest successful main run of the workflow
  that no later push overlapped (so it finished as HEAD); a failed or overlapped run
  never counts, which errs toward a redundant redeploy, and no such run in the last 200
  counts as everything changed. Dispatching HEAD's run from the skipped job was
  rejected: it needs `actions: write`, which the workflow does not hold. Two gates guard
  a bad image: the entrypoint smoke gate (it probes `context`, `search`, docs search and
  `/mcp` with `curl`, and the container exits non-zero before ever serving when artifact
  admission or a probe fails) and fly.toml's `/api/context` HTTP check (flyctl reports
  failure if it never passes). The docs probe searches `lisa` and requires a `doc` field
  in a hit. Rollback: `flyctl releases --image` lists history;
  `flyctl deploy --image <old>` restores in seconds.
- **Schema guard**: a standalone `schema-guard` job compares `CATALOG_SCHEMA_VERSION`
  (`crates/reg-core/src/lib.rs`) with the same constant at the newest `reg_meta/v*` tag
  (`git show <tag>:crates/reg-core/src/lib.rs`). Two outcomes: equal passes; different,
  or absent at the tag (every release before the v4 switch), fails red with both values
  and "publish the switch release first". There is no pending outcome and no
  pass-through, so between the merge of a schema change and its release every main push
  is red here and nothing deploys (fail closed; the server would refuse to boot on the
  mismatched asset anyway). The guard is its **own** job so every deploy path consults
  it: `build-image`, `deploy` and `edge-deploy` all gate on
  `needs.schema-guard.result == 'success'`, including an edge-only push that has no bake
  to fail. Publishing the release auto-dispatches `container-build.yml`
  (`publish_reg_meta.yml`'s `deploy-image` job), which then passes and deploys. A
  `workflow_dispatch` `reg_meta_tag` pin follows the same rule. SWECOV has no deploy job
  until milestone E. A PR that bumps the constant is expected to be red at
  `schema-guard` until it merges and its release publishes; the check is not required on
  main. The maintainer runs the release after the merge; a few hours of disruption at
  the switch are accepted.
- **Build/registry economics (#290)**: the reg_meta DB bake lives in its own Dockerfile
  stage (`regmeta-db`) that copies nothing from the build context, so its cache key is
  the base image and its build args (tag, digest) — app-code edits reuse the cached DB
  layer instead of re-downloading the catalog, and an asset re-uploaded under an
  existing tag changes its digest and rebakes. PR builds neither `load` the image into
  the runner's docker (nothing runs it; all gates execute during the build) nor write
  GHA buildx cache (PR-scoped cache is unreadable from main and would only evict useful
  entries from the repo's 10 GB pool); PRs still read main's cache. Every pushed tag is
  an immutable rollback handle: `workflow_dispatch` rebuilds on an existing HEAD get a
  `-<run_id>` suffix instead of overwriting `:sha`. A post-deploy prune step keeps the
  newest 10 tags and deletes older manifests via the registry v2 DELETE (supported by
  Fly — verified live 2026-06-11; buildx pushes OCI indexes, so age is read from the
  image config's `.created`, and a digest shared with any kept tag is never deleted).
- **Cloudflare zones**: `reg-meta.vqv.uk` (zone vqv.uk, global catalog MCP) and
  `data.swecov.se` (zone swecov.se, SWECOV flavor), orange-cloud A/AAAA → each
  hostname's matching Fly app shared IPv4 + dedicated IPv6, plus `_fly-ownership` TXT
  records (prove ownership behind the proxy) and grey-cloud `_acme-challenge` CNAMEs
  (DNS-01 cert issuance — the reliable path behind a proxy; never proxy a hostname
  pointing at `*.fly.dev`: Fly's edge has no cert for the custom SNI → 525). SSL mode
  Full (strict). No dedicated IPv4 — the free shared IPv4 works behind the proxy.
- **Edge worker** (`reg_webapp/edge/`, Workers free plan): `wrangler.jsonc`
  (`reg-webapp-global-edge`, `reg-meta.vqv.uk/*`) has no assets binding. It forwards
  `/mcp` to the origin with the edge token and answers anything else with a `no-store` 404.
  Only the global worker deploys until E.

The global `edge-deploy` job checks that the Cloudflare token can see zone vqv.uk,
deploys the MCP-only worker after the origin deploy, then runs the public MCP smoke.
SWECOV's deploy jobs return in milestone E; its running deployment remains untouched.

## Why CI uses a fixture DB, not a real asset

Conformance builds synthetic catalogs from readable source contracts with `reg-build`
and runs the Rust server against them. HTTP requests and expected status/body
projections live in the owning JSON corpora. Admission, catalog, project and cache cases
assert their public boundaries. Real-artifact checks run separately; synthetic success
is not a real-corpus or deployment claim.

## Project operations (the Rust server)

Clients POST a project document to `/api/project/validate` or `/api/project/order`, the
two operations of the `order` MCP tool. The body is parsed strictly: one UTF-8 JSON
object, no byte-order mark or duplicate keys, within the JSON depth limit. Malformed
bodies are refused before the operation runs. Unknown project fields are reported by
structural validation.

Validation returns `200` with `{ok, issues}`, including for an invalid project. Order
returns a logical order or a located `422`: `project_invalid` for structural errors,
`order_blocked` for blocking findings. A blocked order returns every finding, never a
partial order. The logical order check and semantic findings live in `reg-catalog`;
`crates/DESIGN.md` documents their rules. Physical files and the manifest download
return with holdings in milestone E.

## Cost protection (`crates/reg-meta/src/limit.rs`)

Every project POST and `/mcp` sit behind the same two origin-side guards; GET reads are
not limited. Cloudflare fronts production with its own edge budgets; these catch direct
origin hits that bypass the edge.

- **Rate limit** — one in-memory token bucket per client address, shared by `/mcp` and
  the POSTs: 60 tokens, refilled one a second, then `rate_limited` (429,
  `Retry-After: 1`). `serve --write-limit N` changes the 60, for verification runs (the
  conformance suite and release admission) that replay many projects from one address; a
  deployment keeps the default. The client is the edge's `CF-Connecting-IP` when the
  request carries the edge token (`REG_META_EDGE_TOKEN`), otherwise the peer, and an
  IPv6 address counts as its /64. **Address-only** by design: a session token would
  bucket per browser (helpful behind NAT) but adds a fingerprinting surface for
  anonymous public data. Buckets are per process (lost on restart, not shared across
  replicas), which suffices as the origin backstop behind the edge. A deployment whose
  worker sends no edge token keys every client on Fly's proxy, so all of them share one
  bucket.
- **Body cap** — `payload_too_large` (413) over 1 MiB, counted as the body streams in
  (axum's `DefaultBodyLimit`), never trusting `Content-Length`. 1 MiB is far above any
  plausible `project_data.json`.

## Input-validation gates (security boundary)

Hostile input on the catalog and docs reads (a malformed ref, a traversal-shaped path, a
bad period, table or query) is refused by the Rust server with a located error; the
grammars and refusals are pinned in `conformance/cases/api`. A project body is parsed
strictly before validation or order checking. Unknown query parameters are refused as
`invalid_parameter`.
