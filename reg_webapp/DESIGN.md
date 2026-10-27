# reg_webapp — design

The Rust server (`reg-meta serve`) answers every `/api` route: catalog reads (context,
search, docs and catalog nodes) and project operations (validate / order). There is no
Python web server. This file records the package-local design rationale. Cross-cutting
topology (package tree, dependency graph, perf budgets, version policy, testing-strategy
overview) lives in the root `ARCHITECTURE.md`; remaining/unbuilt work lives in GitHub
issues. The API contract is `conformance/api/operations.toml` with its snapshot
`crates/reg-meta/openapi.json`.

The SPA is archived on `archive/reg-webapp-spa` and returns in milestone E, rewritten on
the v5 API from that archive and `claude/rebuild-s-shell-search`.

**Catalog artifact.** `ARCHITECTURE.md` and `crates/DESIGN.md` own the compiled
contract. The webapp admits one immutable catalog DB and delegates reads to the shared
SQL reader. Holdings, scope and steward branding return in milestone E.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private state**
(project files belong to clients, never to the server). "Auth" here is really cost
protection, on two axes: read GETs are edge-cacheable + ETag-revalidated (cheap), and
the actual-work POSTs carry an origin-side body-size cap + per-client rate limit (Cost
protection, below). Real auth is a v2+ concern, layered on only if a steward ever needs
private data.

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

## Catalog reads (the Rust server)

`reg-meta serve` answers `show` (`GET /api/catalog` for the root,
`GET /api/catalog/{ref}` for other nodes) and its facets `states`, `warnings`, `values`,
`graph` and `lineage`. Every response is `{data, meta}`; an error is
`{error: {code, message, fields}, meta}`. Ref and period grammars, cursors and located
errors live in `crates/reg-catalog`. The contract is `conformance/api/operations.toml`
with its snapshot `crates/reg-meta/openapi.json`.

## Global catalog search (the Rust server)

`GET /api/search` is answered by the Rust server (`reg-meta serve`, the `search`
operation in `conformance/api/operations.toml`): one ranked list per call, as
`{data: {items, next_cursor}, meta}`. With `type` the list is one arm's hits
(`register`, `variable`, `classification`, `classification_code`, `register_value`), its
curated pins first; without it, every arm's hits ranked together, grouped variable
members hidden. Ranking, curated pins (`reg_meta_build/curation/search_pins.toml`, read
from the catalog's `search_pin` table), concept-group folding, input limits and cursors
live in `crates/reg-catalog`. Maintainers measure relevance with the repository's
`scripts/run_search_eval.py` (`scripts/search_eval.toml`, #393 item 10) against a
running `reg-meta serve`.

## Docs library (the Rust server's `docs` operations)

`/api/docs/*` (#354/#742) is served by the Rust server. The operations
(`conformance/api/operations.toml`) read documentation from the single `reg_meta.db`
catalog, each answering `{data, meta}`:

- `docs_search` (`GET /api/docs/search?q=&register=&limit=&cursor=`): with `q`, the
  entries matching every word, best first, with an FTS `snippet`; without `q`, every
  entry by filename. `register` is a register ref (FQID or bare name); the page carries
  `total` and `register_ingested` (whether that register has docs). It absorbs the old
  `/api/docs/for-variable` hook (`q` = the variable's name). The index holds
  `fold_search` text (folded by the docs build), and snippets come from the stored body,
  so they keep case and diacritics.
- `docs_get` (`GET /api/docs/doc/{identifier}`): metadata, source pointer, a BOUNDED
  `excerpt` and the full markdown `body` for agents.
- `docs_related` (`GET /api/docs/related/{register ref}`) and its download
  (`GET /api/docs/file/{register ref}/{filename}`): the curated #739/#742 rehost
  surface, PDF bytes served verbatim with `Content-Type: application/pdf`, an inline
  filename disposition, and JSON metadata carrying `source_url`, `license`, `fetched`,
  `sha256` and `byte_size`.

`source` is the SCB source-document identifier; `source_url` is the resolved SCB PDF
link (populated at doc-DB build from the curated `doc_sources.toml` map, #372), null
when the source is uncurated; `source_title` is the publication title. Coverage is
LISA-only today.

- **Embedded docs**: documentation is part of the admitted catalog; there is no optional
  docs database or separate schema gate.
- **Caching**: the Rust server's cache tier for the docs operations is a day
  (`public, max-age=86400, must-revalidate`): doc-library content is rebuild-stable.

## Coverage aggregates (#351)

The catalog listing nodes carry a `coverage` object so a browse row shows its
study-window span without resolving every state:

- **Register children** (a register's `show`): per-variable `coverage` — `coverage_from`
  (min `valid_from`), `coverage_to` (max finite `valid_to`; null when `open_ended`),
  `open_ended`, `state_count` (>1 in a window = a break worth surfacing).
- **Provider children** (a provider's `show`): per-register `coverage` —
  `variable_count` (slugged variables) + the span over all their states.

In holdings scope, the reader computes coverage over mapped source variants and
canonical representations before aggregation. A partial-column hold cannot inherit the
whole variable's coverage. Unnamed coverage stays separate from named columns.

Members on a register-scoped concept group's own page carry per-column coverage too, but
a missing per-column key is a known curated member with no `variable_state` row, not
absent enrichment. Those members carry the zero-state coverage shape
(`state_count == 0`, null bounds, not open-ended) instead of `coverage = None`, so
downstream period lenses can distinguish "never delivered" from "unknown".

**Where coverage is computed.** Reference-scope variable, column and register coverage
are grouped reads over `variable_state` in the Rust reader
(`crates/reg-catalog/src/ops/show/browse.rs`), behind the ETag/edge cache. The covering
index `idx_variable_state_coverage` on
`variable_state(variable_id, valid_from, valid_to)` (#371) lets the grouped MIN/MAX span
scan be satisfied index-only. Holdings coverage folds the compiled deliveries below.

- **Open-ended sentinel**: `coverage_to` is None + `open_ended` True when the latest
  window is the `9999-12-31` DDL sentinel ("ongoing"); a stateless variable is
  `state_count == 0` with both bounds None (distinct from open-ended). The sentinel is
  `9999-12-31`.
- **Cadence DEFERRED**: #351 also lists a per-register "cadence", but reg_meta has no
  cadence attribute and no clean derivation (a modal period-grain is fuzzy for
  mixed-grain registers), and no UI consumes it yet. The load-bearing study-window
  signal is span + counts; cadence is a follow-up (a defined source or a build-time
  field) — not shipped here.

### Per-variable deliveries (Y-82)

A register-child also carries `deliveries`: the `(variant, delivery column)` pairs that
deliver the variable, each with its own `VariableCoverage` window and — since Y-104 —
its disjoint `windows`. It answers the two questions the coverage span can't: *which
variant delivers this?* and *what column name does a researcher know it by?* (LISA's
`forvink-ers` is `ForvErs` on paper).

The deliveries are compiled at build, not computed per request: `reg-meta-build derive`
(`reg_meta_build/src/reg_meta_build/derive/browse.py`) writes one `browse_delivery` row
per delivery and scope, with its windows in `delivery_window`, and the validator checks
both against a recomputation. The reader lists the rows of the requested scope.

- **Windows, not a span (Y-104)**: 7,295 of the corpus's 77,843 delivery groups (9.4%)
  do not tile contiguously, so a MIN/MAX span would assert an unbroken delivery for one
  group in eleven. Each delivery carries its disjoint windows; `coverage` is the span
  over them, which is what a browse row prints.
- `column` is None for a state SCB named no delivery column for. The variant still
  delivers the variable, so the row is kept. Variants with a NULL slug are excluded (an
  unslugged variant isn't addressable).
- **Alias-backed columns (Y-93)**: `variable_state` names only the state's own column,
  so a monthly family (`LonFinkJan` … `LonFinkDec`) or a #945 co-delivered spelling
  would be absent from the listing. The derive adds every column of the alias history
  with the coverage it is delivered over: a windowed column over its windows, which
  replace the base state's claim where the two name the same column; a column with no
  window over the variant's states. This is the "delivered under" view, not the
  resolver's: a browse row names columns rather than promising a resolution, so it keeps
  an alias window no state contains. Columns are identified case-insensitively
  (`py_lower`), so an alias that only re-spells a listed column is that one delivery.
- **Steward semantics**: the shared reader limits deliveries to mapped variants and
  canonical representations in holdings scope. Discovery unions held mappings across
  variants; each returned delivery retains its own variant and windows. Reference scope
  returns the catalog's delivery evidence. `deliveries` defaults to `[]` and is
  populated only in the register listing payload.

## Context and catalog sizes (the Rust server)

`GET /api/context` returns catalog metadata and headline sizes as `{data, meta}`. The
committed OpenAPI snapshot describes its response; a Rust snapshot test keeps it
identical to the server's rendered document.

## ETag / Cache-Control

The Rust server sets the reads' ETags and `Cache-Control` (tiers in
`crates/reg-catalog`, validators in `crates/reg-meta/src/validator.rs`); a POST answer
(`validate`, `order`, the manifest download) carries no validator.

**Early revalidation (#1453).** App code and the compiled catalog DB are immutable for a
process lifetime; changing either replaces the process. At startup the server derives
one token from the SHA-256 of its own binary and the catalog database file. File bytes,
not identity fields: `generation_id` does not cover `catalog_version`, which reaches
responses. The image hashes the binary and database when it is built (`Dockerfile`,
passed in as `REG_META_CONTENT_DIGEST` by the entrypoint): hashing the catalog at boot
on the throttled rootfs would outlast the entrypoint's 60 s readiness deadline. Without
the variable (local runs, the conformance suite) the server hashes them at boot. Loose
delivery inventory is not an HTTP input. A GET read's strong ETag is that token plus the
request's path and query, without `__edge_v`, so read scope (a parameter or the
catalog's default) is part of it. A tower layer in front of every GET operation and
download (a route layer, so another method keeps its 405) answers a matching
`If-None-Match` with a 304 before the route runs, so a revalidation does no DB work and
no body serialization. `If-None-Match: *` still runs the route, since it matches only a
representation that exists. Only a 200 carries the ETag and tier.

Browser and shared-cache freshness are separate concerns. Keep a short browser window
where prompt redeploy visibility matters, but let the Cloudflare cache retain immutable,
deploy-generation-keyed catalog/search responses for substantially longer without
synchronous origin revalidation at every browser expiry. A warm search must be served
without route execution. This complements rather than masks the bounded cold-query work:
arbitrary first-time queries still have to meet the origin budget. #1135's bounded SQL
path meets it, so no second in-process response cache is warranted.

- **The Rust server's tiers**: `/api/context` revalidates always (`no-cache`) so clients
  see the current deploy version and date. Catalog and search reads use `max-age=60`
  because both embed the #322 concept-group folds, which can change at the same browser
  URL on redeploy, and `s-maxage=86400` for the edge, whose key carries the deploy
  generation (#318), so a warm search is not revalidated at the origin every minute.
  `must-revalidate` stays, so no cache serves them stale (it and `s-maxage` both rule
  out `stale-while-revalidate`). `public` keeps the CF edge cacheable (the #220 probe
  survives). `/api/docs/*` keeps `max-age=86400` — doc-library content is rebuild-stable
  and a sub-day-stale list is acceptable there; the ETag still guarantees correctness on
  revalidation. The edge worker (`reg_webapp/edge/`) defers to this origin's
  `Cache-Control` contract (it only stamps the `__edge_v` cache-generation param,
  orthogonal to caching policy), so the per-route policy needs no edge change.
- The **edge** side (Cloudflare edge caching / DDoS shielding / edge rate-limits) is a
  deploy/maintainer concern and not backend code.

## Production performance budgets

Search cache-miss origin p95 must stay under 500 ms; a warm edge hit must not execute
the origin. Catalog reads target p95 under 200 ms and project validation/order under 1 s.
Measurements at the origin remain separate from edge-cache evidence.

## Compiled artifact identity and read scope

Boot opens the single `reg_meta.db` artifact and admits its metadata against
`reg_core::CATALOG_SCHEMA_VERSION` by exact equality. Published catalog invariants
belong to `reg-build verify`. The runtime loads no steward branding or delivery
inventory; steward artifacts and their deployment return in milestone E.

The Rust server's reads (the catalog pages' `show` and facets, `context`, `search`)
accept `?scope=holdings|reference`. Catalog artifacts default to reference and reject
holdings; steward artifacts default to holdings and allow reference. Scope is applied
inside the shared reader before hydration, grouping, ranking, counts, or pagination. The
adapter consumes scoped pages directly. Finite curated pins are admitted through
`Catalog.exists` before ranking and pagination. Cursors bind both scope and the full
artifact generation. Project endpoints reject any `scope` query parameter with a located
422; browse scope cannot override the selected artifact's orderability.

Provider and register discovery requires a mapped binding. Variable discovery unions
mappings across source variants; states and deliveries retain their actual mapped
variant and canonical representation. Concept-group membership and inherited tags are
scoped by that same representation rule. Register and variable warnings are admitted
with their subjects. A live unheld browse subject returns 404.

Classifications, codes, value-set contents, same-as links, succession relationships,
graph edges, and lineage remain reference evidence. The browse subject is scoped; a
reference neighbor does not grant holdings or orderability. Hydrated graph states use
the scoped reader; unheld neighboring variables remain thin with no selectable states.

The Rust server's `context` reports schema version, catalog version and the `reg_meta`
version; its `meta` carries the full generation and the effective scope. Its optional
`period_span` is computed from compiled physical `holding_period` MIN/MAX bounds of
known tables with an explicit mapping, then capped at the catalog import year. It is a
coarse UI slider bound, never a coverage or validity check. Catalog artifacts and
holdings without dated periods return null.

The Docker image contains one catalog DB and the server binary. It has no steward
branding input. SWECOV's delivery pipeline returns in milestone E.

## OpenAPI snapshot (the drift gate)

`crates/reg-meta/openapi.json` is committed and is the canonical server contract.
`cargo test -p reg-meta --test openapi_snapshot` asserts that it equals the server's
rendered document. `scripts/gate.py regen` refreshes the snapshot.

## Deployment (`global` on Fly.io, Cloudflare edge in front)

The origin-platform decision (2026-06-11): the container runs on **Fly.io**, with a
Cloudflare zone in front. The deciding factor was the edge-cache contract: the origin
ETag/`Cache-Control` machinery (above) and the #220 FQID round-trip gate assume a
classic URL-addressed origin behind Cloudflare's zone cache. Cloudflare's own Containers
product routes all traffic through a Worker via a Durable Object binding — zone Cache
Rules never see those responses — so the shipped ETag design would need re-implementing
in Worker code against a per-colo-only cache. Fly is also \~5x cheaper for this shape
and officially documents the Cloudflare-in-front topology
(`fly.io/docs/networking/understanding-cloudflare`). Lock-in is nil: the artifact is the
plain Docker image; only `fly.toml` and the CI deploy job are Fly-specific.

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
  `/mcp` with `curl`, each carrying `__edge_v`, and the container exits non-zero before
  ever serving when artifact admission or a probe fails) and fly.toml's `/api/context`
  HTTP check (flyctl reports failure if it never passes). The docs probe searches `lisa`
  and requires a `doc` field in a hit. Rollback: `flyctl releases --image` lists
  history; `flyctl deploy --image <old>` restores in seconds.
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
  Only the global worker deploys until E. Cloudflare downgrades a strong origin ETag to
  weak (`W/`) when compression applies; weak comparison is correct for GET revalidation.
- **Edge cache generations (#318)**: the worker stamps a per-deploy `DEPLOY_VERSION`
  (wrangler var; CI passes the commit SHA, `-<run_id>`-suffixed on dispatch so same-SHA
  data-only rebuilds still count) onto every origin-bound URL as an `__edge_v` query
  param. The zone cache key is the full URL, so each deploy orphans all prior `/api/*`
  cache entries — fresh payloads immediately after deploy, while the per-route TTL still
  bounds origin traffic *within* a generation (24h, `s-maxage` for catalog + search and
  `max-age` for doc-library). This is the free-plan substitute for `cf.cacheKey`
  (Enterprise-only) and needs no purge credentials. Origin-side the param is inert:
  `reg-meta serve` drops it before validating the query (it rejects every other unknown
  parameter) and before deriving the ETag. Consequence: `edge-deploy` runs on
  **image-affecting** pushes too, not just edge paths — an origin deploy that changes
  API payloads must still ship a new cache generation. The global `edge-deploy` job
  deploys the MCP-only worker on matching pushes and checks first that the token can see
  zone vqv.uk (`CLOUDFLARE_API_TOKEN` repo secret). It needs the global origin deploy
  and runs the public `/mcp` smoke afterward. SWECOV's deploy jobs return in milestone
  E; its running deployment remains untouched.
- **Zone rules (dashboard, free plan, zone swecov.se)**: a Cache Rule making `/api/*` on
  the hostname cache-eligible (Cloudflare never caches extensionless API paths by
  default, even with `Cache-Control: public` — without the rule every read is
  `cf-cache-status: DYNAMIC`), and the free plan's one WAF rate-limiting rule (path-only
  match — free-tier rate limiting can't match hostname; 100 req/10s/IP → block,
  burst-verified to 429).
- **#220 gate: PASSED (2026-06-11)** — 20 slash-bearing FQID paths (3-segment bindings,
  `/states` suffixes, `/variants`) round-trip the edge cache byte-identical to origin,
  MISS→HIT per URL, ETag→body mapping consistent, and conditional GETs answer 304 from
  the edge (`CF-Cache-Status: HIT`, no origin traffic). The path-based FQID surface
  stands; no query-string fallback needed before publishing the OpenAPI.
- **V1 performance-probe extension (pending, #1455)** — add a representative
  `/api/search` MISS→HIT + `Age`/`CF-Cache-Status` assertion and prove its warm
  conditional request performs no origin route work. The original #220 path gate remains
  unchanged.

## Why CI uses a fixture DB, not a real asset

Backend tests build artifacts in temporary directories and point the app at them with
`REG_META_DB`; they do not fetch released DB assets. Compiled stewardship cases load
readable logical JSON, inventory TOML, policies, and census CSV through the real builder
and compiler. HTTP requests and expected status/body projections live in the owning JSON
corpora. Boot cases vary manifest admission and deployment identity; catalog,
validation, scope, and cache cases assert their public boundaries. No runtime index
fixtures or release-marked inventory reconciliation suite remains.

Older catalog-only fixtures continue to serve unrelated route tests. Real pinned
artifacts are checked separately for order parity, latency, and rendered evidence;
synthetic success is not a real-corpus or deployment claim.

## Project operations (the Rust server)

Clients POST the whole serialized draft, a raw object, to two operations of the `order`
MCP tool (`crates/reg-catalog/src/ops/slice_3e.rs`). The body is parsed strictly
(`ops/body.rs`: one UTF-8 JSON object, no byte-order mark, no duplicate key at any
depth, `serde_json`'s depth limit); anything else is `malformed_request` (400) before
the operation runs. Unknown keys survive the parse, so `validate` reports each as
`unexpected_field`. Neither operation takes a `scope`: a project is read in the
artifact's own identity, and a `scope` parameter is `invalid_parameter`.

- **`POST /api/project/validate`** answers `{data: {ok, issues}, meta}`. A project that
  FAILS validation is a successful answer — **200 with `ok: false`** and every issue in
  emission order (supported version → structural → semantic). A non-2xx is a refused
  request (`malformed_request`, `payload_too_large`, `rate_limited`).
- **`POST /api/project/order`** answers `{data: manifest, meta}`;
  **`POST /api/project/order/manifest`** serves the same manifest as the exact manifest
  bytes, an attachment named `order_<slug>_<YYYY-MM-DD>.json`: the project name folded
  to ASCII lowercase (å→a, é→e), other runs `-`, at most 60 characters, then the
  server's UTC date. The server owns the `Content-Disposition` filename. Anything that
  is not an order is a 422 on both routes, never a partial manifest: `project_invalid`
  for a document `validate` rejects structurally (its issues in `fields.issues`),
  `order_blocked` when any finding leaves part of the request undeliverable (every
  finding in `fields.findings`, each with its stable `code` and its `source` /
  `variable` / `period` coordinates). Fail-closed is a contract, so the findings ship as
  data, pinned by `conformance/cases/api/order-errors`.

The order pipeline and its findings are `ops/order.rs` (see `crates/DESIGN.md` → Order
manifest): steward holdings on a steward artifact, canonical columns on the global one.

## Semantic validation

The semantic layer, its rules and its issue codes are `ops/validate.rs`
(`crates/DESIGN.md` → Project semantic validation). G1 compares its results between the
pinned release's server and the checkout's, over derived copies of the release artifacts
(`conformance/differential/`).

## Cost protection (`crates/reg-meta/src/limit.rs`)

Every POST (the project operations and the manifest download) and `/mcp` sit behind the
same two origin-side guards; GET reads are not limited (they have the cheaper edge-cache

+ ETag axis). Cloudflare fronts production with its own edge budgets; these catch direct
  origin hits that bypass the edge.

- **Rate limit** — one in-memory token bucket per client address, shared by `/mcp` and
  the POSTs: 60 tokens, refilled one a second, then `rate_limited` (429,
  `Retry-After: 1`). `serve --write-limit N` changes the 60, for verification runs (the
  conformance suite, release admission, G1) that replay many projects from one address;
  a deployment keeps the default. The client is the edge's `CF-Connecting-IP` when the
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

## API surface

The Rust server (`reg-meta serve`; contract `conformance/api/operations.toml`, snapshot
`crates/reg-meta/openapi.json`) answers every `/api` route. This table is the
orientation map. Read GETs are edge-cacheable, write POSTs are not.

  | Method | Path                                 | Server | Purpose                                                                                                                   |
  | ------ | ------------------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------- |
  | GET    | `/api/context`                       | Rust   | `context`: branding, build info, period span, catalog sizes.                                                              |
  | GET    | `/api/search`                        | Rust   | `search`: one ranked list per call (`?type=` keeps one arm), `{items, next_cursor}`.                                      |
  | GET    | `/api/catalog`, `/api/catalog/{ref}` | Rust   | `show`: the root, or any node by ref (`kind`-tagged).                                                                     |
  | GET    | `/api/states/{ref}`                  | Rust   | A variable's states, cursor-paged; `period`, `variant`, `value_set_version` narrow.                                       |
  | GET    | `/api/warnings/{ref}`                | Rust   | A register's or variable's data warnings; `period`, `variant`, `representation`, `unassigned_only` filter.                |
  | GET    | `/api/values/{ref}`                  | Rust   | A classification's codes, or a variable state's value set (`state=`), cursor-paged; `q` filters, `total`.                 |
  | GET    | `/api/graph/{ref}`                   | Rust   | The relationship graph of a variable, classification or group, succession included.                                       |
  | GET    | `/api/lineage/{ref}`                 | Rust   | A variable's lineage edges, lineage warnings and source registers.                                                        |
  | GET    | `/api/docs/search`                   | Rust   | `docs_search`: docs matching `q` (or every doc), optional `?register=`, `{items, next_cursor, total, register_ingested}`. |
  | GET    | `/api/docs/doc/{identifier}`         | Rust   | `docs_get`: one doc by variable/filename — metadata, source pointer, excerpt, body.                                       |
  | GET    | `/api/docs/related/{ref}`            | Rust   | `docs_related`: metadata of the register's rehosted source PDFs.                                                          |
  | GET    | `/api/docs/file/{ref}/{filename}`    | Rust   | One rehosted source PDF's bytes.                                                                                          |
  | POST   | `/api/project/validate`              | Rust   | `validate`: 200 + `ok` + issues, an invalid project included.                                                             |
  | POST   | `/api/project/order`                 | Rust   | `order`: the manifest; 422 `project_invalid` or `order_blocked` (with every finding) when not an order.                   |
  | POST   | `/api/project/order/manifest`        | Rust   | The same manifest as the exact bytes, an attachment named `order_<slug>_<date>.json` as a download.                       |

## Input-validation gates (security boundary)

Hostile input on the catalog and docs reads (a malformed ref, a traversal-shaped path, a
bad period, variant or query) is refused by the Rust server with a located error; the
grammars and refusals are pinned in `conformance/cases/api`. A project body is parsed
strictly (`ops/body.rs`, Project operations above), and a `scope` parameter on a project
operation is refused as `invalid_parameter`.
