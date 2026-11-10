# reg_webapp — design

The Rust server (`reg-meta serve`) answers every `/api` route: catalog reads (context,
search, docs and catalog nodes) and project operations (validate / order). There is no
Python web server. This file records the package-local design rationale. Cross-cutting
topology (package tree, dependency graph, perf budgets, version policy, testing-strategy
overview) lives in the root `ARCHITECTURE.md`; remaining/unbuilt work lives in GitHub
issues. The API contract is `conformance/api/operations.toml` with its snapshot
`crates/reg-meta/openapi.json`.

The SPA shell, search, provider, register, variable, classification and loaded-project
pages use the current API. The earlier SPA is archived on
`archive/reg-webapp-spa`.

## Loaded projects

The browser keeps one working `project_data.json` in localStorage, opened from a file
(there is no paste field). WASM checks every opened file and edit before it becomes a
typed draft. Rejected input stays unchanged for download or discard; no migration is
attempted, and its structural issues are listed in full under the verdict. Opening a
replacement or discarding the working copy asks first. Download remains available if
browser persistence fails.

The banner and project page share one catalog check (`useProjectCheck`): `order`,
through the shared abortable resource hook, 350 ms after the last accepted edit, with no
manual trigger. `order` answers everything the page shows, so the SPA does not call
`validate`: both run the same check (on a steward server including the holdings stage of
`materialize_order`, so a column the steward does not hold blocks here too), an
`order_blocked` refusal carries every finding that `validate` would list, and a success
carries the non-blocking findings with the entries, row keys and clips the delivery
summary draws. Those clips are the logical order's: years the steward's tables lack
(`coverage_gap`) are listed as a warning but not subtracted from the summary's years.
The SPA sends only drafts WASM accepted; a `project_invalid` refusal, should the server
disagree, is read the same way. One verdict function names the result for both, in the
words of `frontend/DESIGN.md` → Words. Issues sit under their table or column, with
links from the verdict that focus them. The banner retains the project name, table and
column counts, verdict and download save state across routes. A restored local draft is
recovery, so it remains unsaved until downloaded. The study window is an inline
window-band picker bounded by the catalog's half-open period span. A new project carries
over the browsing window or starts at 2015 through the catalog's last year. Moving the
study window never rewrites source periods.

Variable pages project their segments into a single coverage LaneGraph. A column is the
exact table and spelling pair; selection and its availability-bounded add window are
local to the page. Adding creates or widens one source per table and appends one binding
per variable, using the segments' default type and agreed classification. WASM checks
the candidate before the existing project store writes its sources. Source cards author
binding type, classification pin, removal and source period through checked candidates
and the same sources write path. Each table supplies a coverage row over its binding
lanes on one shared data axis. Stored periods are never narrowed on load. Variable pages
read one hop of succession relations into read-only neighbour lanes on the same
timeline, with arrows from predecessor to successor and names linking to their variable
pages. Adding starts from the variable page; register rows link there, and both those
rows and already-bound columns show in-project state without a checkbox.

The project page shows the logical order under the tables as "What will be delivered",
grouped by project table and catalog table: columns and years, the years a column cannot
deliver (clips) and the row identifier. There is no order page and no order-file
download (maintainer ruling 2026-11-04): the steward's `order_instructions` say how to
order. Register-name reads are shared with catalog browsing.

**Catalog artifact.** `ARCHITECTURE.md` and `crates/DESIGN.md` own the compiled
contract. The webapp admits one immutable catalog DB and delegates reads to the shared
SQL reader. A steward deployment also admits that steward's holdings DB, which carries
its scope and its `steward_config` branding (`/api/context`).

## Catalog pages

Provider, register and classification pages read `catalog/{ref}`. A table has no page:
its address reads the table, then renders its register's page with that table's chip on
(#2316). A provider page draws each register from its `periods` (merged
spans of its dated tables). The generated OpenAPI types own the response shapes. An
implicit table shares its register's FQID, so its canonical link opens the register.

Catalog periods are half-open ISO date intervals. The frontend adapts the exclusive end
to reg-core's inclusive interval renderer, without copying its period grammar. The
register's `unplaced` list contains release names, not variable identities.

Catalog pages show no build findings: the catalog does not carry them (maintainer
2026-11-06, #2335; #2318 decides what comes back). `Finding.tsx` is the
project-validation renderer. The obsolete variants browser has no route or data source.

Variable value sets stay inline, deduplicated by hash across segments, with table and
period coverage. Disclosures fetch codes on first expansion through `values`; hashes
never become routes. Flat sets use debounced server search and cursor paging.
Hierarchical sets start at level one, load children by parent code, and keep expanded
branches while search shows flat matches with levels. Only active search shows a
matching result count; sentinel items are labelled in plain words.

The list of classifications (`/catalog/class`) reads `show` at ref `class`: each
family's name, description and versions (name, part and years). On a steward site the
server lists only the families a held placement's rows carry. The browse column lists
them from the same read, below the providers.

A classification family page (`/catalog/class/<family>`) reads `show`: its publisher,
description and versions in start order, each with its part, the versions it succeeds
and its value set (the `variables` it also serves are not shown). A version route
(`/catalog/class/<family>/<version>`) reads the version, then its family, and renders
the family page with that version picked: one page, as a grouped variable's address
renders its group. The picked version's codes are its whole list through `values` with
`classification`; its changes since each version it succeeds are `values` with
`classification` and `compare`, paged, the counts from the first page. Every
classification name on screen is the name the server serves (`show`, or
`classification_name` on a segment row); the SPA never makes a name up from a slug, and
a null value-set classification is never inferred from its names. The project editor and
its findings name versions only from the segment rows already read, never with a `show`
per version (one binding can have dozens of candidates); where no loaded row carries a
version (a pin not delivered on the project's tables), it shows the version's own id,
the ref's last part, as an id. A cross-register version list names each variable's
register from the root read the browse column makes.

## Why no auth — cost protection instead

The data is public-ish registry metadata; there is **no server-side user-private state**
(project files belong to clients, never to the server). "Auth" here is really cost
protection: read GETs support ETag revalidation, and POSTs carry an origin-side
body-size cap and per-client rate limit (Cost protection, below). Real auth is a v2+
concern, layered on only if a steward ever needs private data.

## Layout

```text
reg_webapp/
  Dockerfile              # catalog bake and Rust server image (global)
  steward.Dockerfile      # a steward's image from released binaries
  docker-entrypoint.sh    # server startup and readiness probes
  fly.toml                # global origin deployment
  edge/                   # edge worker: global hosted MCP, steward SPA
  frontend/               # the SPA
  dev/                    # local dev servers and browser driver
  DESIGN.md
```

Steward configuration and logos are not files here: `reg-build holdings` writes them
into the holdings DB as `steward_config`, and the server serves them at `/api/context`.

## Boot seam (artifact admission)

The Rust server admits its catalog at startup and refuses to serve otherwise
(`Catalog::open` in `crates/reg-catalog`): metadata must be well formed and its schema
version must exactly equal `reg_core::CATALOG_SCHEMA_VERSION`. A steward selection also
admits its holdings partner, whose manifest pins the base catalog's SHA-256. The `api`
admission corpus pins the refusals. The webapp ships no DDL or schema-version authority;
`reg-build verify` owns published artifact invariants.

## Catalog operations (the Rust server)

`reg-build` writes the immutable catalog; `reg-meta serve` reads it through
`reg-catalog`.

`conformance/api/operations.toml` defines the complete operation surface, parameters,
response shapes and error codes. `crates/reg-meta/openapi.json` is its server snapshot.
Catalog operations expose nodes, segments, coverage, schemas, relationships and
content-addressed value sets. Search ranks FTS hits across registers, tables, variables,
classifications, codes and documents. A steward deployment adds holdings and physical
orders (the MONA order file).

Documentation lives in the same admitted catalog. The docs operations search document
sections, return a document by content hash, list linked documents and serve embedded
PDF bytes. There is no separate docs database or schema gate.

## Context (the Rust server)

`GET /api/context` returns what the SPA shell shows as `{data, meta}`: the period span,
the server version and, in steward mode, the steward's presentation and logo
(`steward_config`). Catalog identity is in `meta`. The committed OpenAPI snapshot
describes its response; a Rust snapshot test keeps it identical to the server's rendered
document.

## ETag / Cache-Control

The Rust server sets the reads' ETags and `Cache-Control` (tiers in
`crates/reg-catalog`, validators in `crates/reg-meta/src/validator.rs`); a POST answer
(`validate`, `order`) carries no validator.

**Early revalidation.** App code and the compiled catalog DB are immutable for a process
lifetime; changing either replaces the process. At startup the server derives one token
from the SHA-256 of its own binary and the selected database files. File bytes, not
identity fields: `generation_id` does not cover `catalog_version`, which reaches
responses. The image hashes the binary and database when it is built (`Dockerfile`,
passed in as `REG_META_CONTENT_DIGEST` by the entrypoint): hashing the catalog at boot
on the throttled rootfs would outlast the entrypoint's 60 s readiness deadline. Without
the variable (local runs, the conformance suite) the server hashes its binary and base
catalog at boot; only the image digest covers the holdings partner too. A GET read's
strong ETag is that token plus the request's path and query, excluding `__edge_v`. The
edge reserves this parameter for its cache generation; the server removes it before
operation parameter validation and ETag derivation. Every other unknown parameter is
still rejected. An edge redeploy therefore keeps browser validators valid for unchanged
responses. A tower layer in front of every GET operation and download (a route layer, so
another method keeps its 405) answers a matching `If-None-Match` with a 304 before the
route runs, so a revalidation does no DB work and no body serialization.
`If-None-Match: *` still runs the route, since it matches only a representation that
exists. Only a 200 carries the ETag and tier.

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

Boot admits `reg_meta.db` against `reg_core::CATALOG_SCHEMA_VERSION` by exact equality.
`--catalog swecov` also admits `reg_meta_swecov.db`: its owner, holdings and catalog
schemas, and pinned catalog SHA-256 must match. Published catalog invariants belong to
`reg-build verify`. The paired holdings artifact supplies steward branding and delivery
metadata; no runtime steward repository is loaded.

Every JSON response's `meta` carries contract version, catalog version and generation
(and the steward's identity in steward mode). Context's `period_span` bounds the
study-years slider: in steward mode the hull of the years held, otherwise of the
registers' periods, leaving out timeless periods (an ongoing register leaves the end
open); it is null when every period served is timeless.

## OpenAPI snapshot (the drift gate)

`crates/reg-meta/openapi.json` is committed and is the canonical server contract.
`cargo test -p reg-meta --test openapi_snapshot` asserts that it equals the server's
rendered document. `scripts/gate.sh regen` refreshes the snapshot.

## Deployment (global on Fly.io, a Cloudflare edge in front)

The container runs on Fly.io behind Cloudflare. The immutable SQLite artifact and server
binary are baked into a plain Docker image; `fly.toml` and the deploy job supply the
platform-specific configuration. This section is the toolkit's own deployment, the
global catalog. Steward deployments are their repositories' (Steward repositories,
below).

The global catalog has no public website (maintainer ruling): it is not what a steward
site offers. Agents use its hosted MCP (no install, no 1.2 GB download), so
`reg-webapp-global` serves `/mcp` alone at `reg-meta.vqv.uk`. vqv.uk is the maintainer's
side-project zone, chosen to keep the personal altmejd.se zone out of it.

- **The image runs the Rust server alone**: `reg-meta serve --db /opt/reg_meta` with the
  base catalog. There is no Python at runtime. The Dockerfile has three stages: the
  `regmeta-db` bake (Debian slim with `curl` and `zstd`: download the catalog asset,
  verify it against the SHA-256 digest GitHub records for it, unpack), a pinned Rust
  stage building `reg-meta`, and a Debian slim runtime with `curl` for the smoke gate.
  The image serves the API and `/mcp` only; a steward's edge worker serves the SPA. The
  global image serves `/api` too (the binary has no per-route switch); its edge worker
  forwards none of it, and the origin lock refuses it to anyone else.
- **Hosted MCP** (`crates/DESIGN.md`, "Agent and web interface"): `/mcp` on
  `reg-meta.vqv.uk` (the global catalog) and on each public steward host
  (`data.swecov.se`). Every worker forwards `/mcp`; the active Fly config passes
  `REG_META_PUBLIC_HOST`, which rmcp's allowed hosts admit beside the loopback names.
  `/mcp` and the project POSTs share one rate limit per client address (60 a minute;
  Cost protection, below). Behind Fly the peer is Fly's proxy, so the worker proves a
  request came through the edge with a shared secret (`EDGE_TOKEN` on the worker,
  `REG_META_EDGE_TOKEN` on the Fly app, sent as `x-edge-token`); only such a request is
  keyed on its `CF-Connecting-IP`. With the secret set, the origin refuses every other
  request (Origin lock, below), so a forged `CF-Connecting-IP` buys nothing. A worker
  without the secret refuses every forward with a `no-store` 503, rather than put every
  edge client in the proxy's one bucket. After each edge deploy, `container-build.yml`
  (and `steward-deploy.yml` for a steward) sends `initialize`, `tools/list` and one
  `search` to the public `/mcp`. Those smoke requests send
  `User-Agent: reg-meta-ci-smoke`, which usage statistics count as client `ci` and the
  reports leave out.
- **Usage recording** (#1323; the origin's label is "Usage statistics" below): each
  worker writes one Workers Analytics Engine data point (dataset `reg_meta_usage`,
  binding `USAGE`, kept 3 months) per forwarded `/api` and `/mcp` request, from the
  origin's `x-reg-usage` label, which `edge/src/usage.ts` strips before answering. The
  blobs are steward (`STEWARD` var), surface, kind, op, fqids, q, err, cache status,
  client family, country and status. The doubles are edge ms (to the origin's response
  headers), app ms, bytes (`-1` when unknown) and hits. The index is `steward:op`.
  - **Surface** is `mcp` for `/mcp`, `spa` for an `/api` request with
    `Sec-Fetch-Site: same-origin`, and `rest` otherwise.
  - **App ms** comes from `Server-Timing`, but only when the response came from the
    origin. On a HIT it would be the time of the request that filled the cache. Hits are
    `-1` when the request is not a search.
  - **Client family** comes from a fixed User-Agent substring table. The raw User-Agent,
    the IP address and any cookie are never written.
  - **An MCP `order`** adds one `pick` data point per bound variable.
  - **`POST /api/event`** (SPA deployment only) is the SPA's pick beacon, one request
    per add, `{"fqids": [...]}` with at most 100 ids, answered 204 `no-store` at the
    edge and never forwarded. It is taken only same-origin (`Sec-Fetch-Site`) with a
    declared length of at most 24 KiB. It never reaches the origin's rate limit; the
    site's Cloudflare rate-limiting rule on `/api/` counts it with the reads, one
    request per add.
  - **Invocation logs are off** (`observability.logs.invocation_logs: false`). They
    record each request's URL, search text included, outside the scrub.
- **App**: `reg-webapp-global` serves `reg-meta.vqv.uk` from a single always-on
  `shared-cpu-1x`/2GB machine in `arn` (Stockholm, where the users are). Always-on is
  deliberate: Fly's ephemeral-rootfs I/O is throttled (\~8 MiB/s), so a cold boot
  re-reads the SQLite database slowly — keep the OS page cache warm rather than scale to
  zero (\~$6/mo per app). Config: `reg_webapp/fly.toml`; `--ha=false` keeps the machine
  count at one.
- **Read-only SQLite on the ephemeral rootfs is the right model** — the DB is baked into
  the image and replaced with it. No volume, no LiteFS, nothing persists.
- **Deploys**: one workflow (`container-build.yml`) owns the origin and the edge worker,
  scoped by a `changes` paths-filter job. Image-affecting main pushes (Dockerfile COPY
  surfaces + bake inputs) build, push to `registry.fly.io` (SHA-tagged), and
  `flyctl deploy --image` the origin. The bake build-args are the RESOLVED newest
  `reg_meta/v*` tag (never `latest` — a literal `latest` would make the bake layer's
  buildx cache key insensitive to data-only releases and could even resurrect a stale
  cached layer after a pinned dispatch) and the SHA-256 digest GitHub records for
  `reg_meta.db.zst`, which the `schema-guard` job resolves with the tag; the bake has no
  defaults and refuses an unverified download. The Fly app uses `FLY_API_TOKEN`.
  `content.sha256` covers the binary and every baked DB in sorted order. Nothing deploys
  without green CI: a `wait-ci` job polls this commit's ci.yml run and the origin/edge
  deploy jobs require its success — an image that builds but fails the CI gate never
  ships. Each deploy job carries a HEAD-of-main guard (GHA concurrency serializes by
  build-completion order, not commit order — without the guard an older commit's slow
  build could overwrite a newer deploy; it also makes non-main dispatches deploy-inert).
  Because a guarded commit skips, `changes` diffs a push against the last deployed
  commit, not the push's `before`: otherwise a served-path push followed by a non-served
  one before its deploy ran would deploy neither. The last deployed commit is the newest
  successful main run of the workflow that no later push overlapped (so it finished as
  HEAD); a failed or overlapped run never counts, which errs toward a redundant
  redeploy, and no such run in the last 200 counts as everything changed. Two gates
  guard a bad image: the entrypoint smoke gate (it probes `context`, `search`, docs
  search and `/mcp` with `curl`, and the container exits non-zero before ever serving
  when artifact admission or a probe fails) and fly.toml's `/healthz` HTTP check (flyctl
  reports failure if it never passes). The gate's probes send the edge token, which the
  origin lock requires on loopback too. The docs probe searches `lisa` and requires a
  `doc` field in a hit.
- **Schema guard**: a standalone `schema-guard` job compares `CATALOG_SCHEMA_VERSION`
  (`crates/reg-core/src/lib.rs`) with the same constant at the newest `reg_meta/v*` tag
  (`git show <tag>:crates/reg-core/src/lib.rs`). Two outcomes: equal passes; different,
  or absent at the tag, fails red with both values and "publish the release first".
  There is no pending outcome and no pass-through, so between the merge of a schema
  change and its release every main push is red here and nothing deploys (fail closed;
  the server would refuse to boot on the mismatched asset anyway). The guard is its
  **own** job so every deploy path consults it: `build-image`, `deploy` and
  `edge-deploy` all gate on `needs.schema-guard.result == 'success'`, including an
  edge-only push that has no bake to fail. Publishing the release auto-dispatches
  `container-build.yml` (`publish_reg_meta.yml`'s `deploy-image` job), which then passes
  and deploys. A `workflow_dispatch` `reg_meta_tag` pin follows the same rule. A PR that
  bumps the constant is expected to be red at `schema-guard` until it merges and its
  release publishes; the check is not required on main. The maintainer runs the release
  after the merge.
- **Build/registry economics**: the reg_meta DB bake lives in its own Dockerfile stage
  (`regmeta-db`) that copies nothing from the build context; its build args (tag,
  digest) name the asset, so one re-uploaded under an existing tag changes its digest
  and rebakes. CI builds use no layer cache: the GHA buildx cache cost about 2 min per
  export, never hit the cargo layer (`COPY crates/` busts it on every change) and saved
  only the apt layers and the DB download (about 10 s from cache). PR builds do not
  `load` the image into the runner's docker (nothing runs it; all gates execute during
  the build). Every pushed tag is an immutable rollback handle: `workflow_dispatch`
  rebuilds on an existing HEAD get a `-<run_id>` suffix instead of overwriting `:sha`. A
  prune job after each deploy keeps the newest 10 tags and deletes older manifests via
  the registry v2 DELETE (supported by Fly — verified live 2026-06-11; buildx pushes OCI
  indexes, so age is read from the image config's `.created`, and a digest shared with
  any kept tag is never deleted).
- **Cloudflare zone**: `reg-meta.vqv.uk` (zone vqv.uk), orange-cloud A/AAAA → the Fly
  app's shared IPv4 + dedicated IPv6, plus a `_fly-ownership` TXT record (proves
  ownership behind the proxy) and a grey-cloud `_acme-challenge` CNAME (DNS-01 cert
  issuance — the reliable path behind a proxy; never proxy a hostname pointing at
  `*.fly.dev`: Fly's edge has no cert for the custom SNI → 525). SSL mode Full (strict).
  No dedicated IPv4 — the free shared IPv4 works behind the proxy. The Cloudflare token
  must see zone vqv.uk; the edge job checks that before deploying.
- **Edge worker** (`reg_webapp/edge/`): `wrangler.jsonc` deploys
  `reg-webapp-global-edge` on `reg-meta.vqv.uk/*` without assets. It forwards `/mcp`
  alone with the edge token, returns a `no-store` 404 elsewhere and never stamps a cache
  generation. The edge job probes hosted MCP only.

After the smoke gate passes, the entrypoint reads the served DBs into the page cache in
the background (`nice cat`), so the site serves while the cache warms; the read ends
with the container. The global boot allows 60 seconds (`REG_META_READY_DEADLINE`). The
entrypoint caps glibc at two malloc arenas for the server, so the process stays near 100
MB instead of climbing with each burst of requests (#2335).

### Rollback

The image prune job's `KEEP=10` window retains only the newest ten tags: note the
running image tag before a risky deploy so the rollback image is not pruned. Steward
deployments, their rollback and their secrets are documented in each steward repository
(swecov-steward `docs/deploy.md`).

## Steward repositories (reusable workflows)

A steward repository builds, publishes and deploys itself (maintainer 2026-11-04, SWECOV
repository move R1, R8, R14). It pins one `reg_meta` release and calls three reusable
workflows from the toolkit at that tag, passing the same tag as `reg_meta_tag`. None of
them builds Rust for the steward: they download the release's `reg-build`, `reg-meta`,
base catalog and SPA bundle, each checked against its digest. Each workflow's header
lists its inputs and an example call.

- `steward-build.yml`: `reg-build holdings` on the caller's clean checkout (the pin is
  that checkout: `origin`, `HEAD` and `--config`), which also runs the pair verifier and
  its content check, then `reg-build diff --holdings` against a previous holdings asset
  (a release in the caller repository or a URL). With no previous asset, the diff is
  skipped and the summary says so. A second job runs the release's pair conformance with
  the released `reg-meta`. The diff goes to the job summary. The pair leaves as a
  workflow artifact.
- `steward-deploy.yml`: bakes `reg_webapp/steward.Dockerfile` (the runtime stage of the
  toolkit image, with the binary and both DBs from the build context, so the image needs
  no token), deploys it to the steward's Fly app, deploys the toolkit's edge worker with
  the caller's wrangler config and the release's SPA, then probes the edge (`.wasm`,
  cache, `/api/context`, hosted MCP). It deploys a holdings release by tag, so rollback
  is a redeploy of an older tag. `access: cloudflare-access` serves a host behind
  Cloudflare Access: the probes send a service token, and the cache probe is skipped.
  `dry_run` builds and runs the image and stops there.
- `steward-bump.yml`: when a newer `reg_meta/v*` release exists, re-locks the curation
  against its base, refreshes the config's `[pins]` digests (`reg-build pin`, which adds
  and drops no pin), moves `toolkit.toml` and every mention of the old tag in the
  caller's workflows (the `uses:` refs and `reg_meta_tag` inputs), and opens a PR with a
  token the caller supplies (`GITHUB_TOKEN` cannot change workflow files).

Steward repositories are private, so their CI spends a metered Actions quota; the
toolkit's own CI does not. A steward therefore builds where it releases, not on every PR
(maintainer 2026-11-06). PR CI checks only that the pins agree. The merge gate is an
independent review that builds, verifies and diffs the holdings locally against the
newest release with the pinned `reg-build`; each steward's `AGENTS.md` has the commands.
A release is a `workflow_dispatch` of the steward's release workflow, which builds, runs
conformance, publishes and deploys. The repository variable `STEWARD_RELEASE=auto`
restores a release on every push to `main`, for when the PR pace allows it; unset, a
push releases nothing. A bump PR is reviewed and released the same way.

The inputs of these workflows are a contract with every steward repository. A change to
them breaks a steward only at its next bump, as a failed re-lock or in the bump PR's
review; the release notes name it.

A steward's Fly app sets `REG_META_EDGE_TOKEN` and its Fly config checks `/healthz`, as
the global one does: with the token set, a check on any other path gets 403 and fails
the deploy (Origin lock, below). Where a site sits behind Cloudflare Access, the lock is
what keeps its `*.fly.dev` origin from answering around Access.

A steward image boots as the global one does, through the same entrypoint, with three
differences:

- **Pair admission** checks the holdings pin against the base catalog's SHA-256. The
  image computes that digest when it is built and the entrypoint passes it in as
  `REG_META_CATALOG_SHA256`. Hashing the 1.33 GB catalog at boot instead took about 80 s
  on Fly's rootfs (\~17 MB/s), and the steward site was down that long on every deploy
  or restart. A wrong digest refuses admission like a wrong file. Without the variable
  (local runs, CI, the conformance suite) the server hashes the file at boot.
- **Boot allowance**: paired boot allows 300 seconds (`REG_META_READY_DEADLINE` and the
  steward Fly check's `grace_period`); the gate's first search reads cold pages and
  reuses that deadline. The check's five-second request timeout applies after binding;
  it is not the startup allowance. The smoke gate also requires a non-null
  `steward_config` object in context.
- **The SPA worker**: the steward's wrangler config adds the assets binding, serving
  `frontend/dist/` with `single-page-application` fallback. Its `run_worker_first` paths
  and `ORIGIN_PATHS` (plus `MCP_PATH`) in `edge/src/index.ts` must stay in lockstep:
  `/api`, `/api/*`, `/openapi.json`, `/mcp` reach the origin, including browser
  navigations. `/assets/*` also runs the worker first: an HTML fallback becomes a
  `no-store` 404. Existing hashed assets receive the immutable one-year policy from
  `frontend/public/_headers`, copied to `dist/`. The SPA HTML keeps Workers' default
  revalidation policy. Only this assets-enabled worker stamps its API/OpenAPI forwards
  with `__edge_v=DEPLOY_VERSION` (`/mcp` is never stamped); `steward-deploy.yml`
  supplies a fresh version per deploy, so earlier `/api/*` zone-cache entries, with
  their 24-hour shared TTL, become unreachable. The server ignores the parameter for
  operation validation and ETags. The steward's zone needs an `/api/*` Cache Rule: these
  extensionless paths need explicit cache eligibility, using the origin cache headers
  and bypassing responses without them. Cloudflare can weaken ETags during compression;
  GET revalidation permits weak comparison.

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
`crates/DESIGN.md` documents their rules. Physical files and the manifest come from the
steward's holdings (`ops/order/materialize.rs`).

## Origin lock (`crates/reg-meta/src/limit.rs`)

Cloudflare guards only the hostnames it fronts. The Fly origin also answers on
`<app>.fly.dev` and its public IPs, where neither the edge's rate limits nor Cloudflare
Access apply. So when `REG_META_EDGE_TOKEN` is set, `serve` refuses every request whose
`x-edge-token` header does not carry it with `edge_token_required` (403, the error
document), on every route and on unknown paths, before the rate limit. The comparison is
of SHA-256 digests without an early exit. Unset, nothing is refused (local runs, CI, the
conformance suite).

- **No separate switch.** A set token that is not enforced is the gap itself: the worker
  already sends the token on every forward, and a deployment that sets it on the origin
  is behind an edge. Every deployment (global, SWECOV, IFAU) sets it.
- **`GET /healthz` is exempt**: 200 with an empty body, once the server is listening
  (that is, after artifact admission). Fly's HTTP check calls the origin directly, and a
  header in a Fly config would commit the secret. It serves nothing, so the exemption
  publishes nothing.
- **Loopback is not exempt.** The entrypoint gate sends the token from its environment
  instead. One rule with one exemption is easier to reason about than a peer-address
  rule, and Fly's proxy is never loopback anyway.
- **Rotation** sets both sides (`wrangler secret put EDGE_TOKEN` and
  `flyctl secrets set REG_META_EDGE_TOKEN=…`) to one new value. Between the two, the
  edge gets 403; set the worker right after the Fly secret (which restarts the machine).
- **Not auth.** It proves a request came through the worker, nothing about the person.
  Who may use a private steward site is Cloudflare Access's decision, at the edge.

## Cost protection (`crates/reg-meta/src/limit.rs`)

Every project POST and `/mcp` sit behind the same two origin-side guards; GET reads are
not limited. Cloudflare fronts production with its own edge budgets; these catch direct
origin hits that bypass the edge.

- **Rate limit** — one in-memory token bucket per client address, shared by `/mcp` and
  the POSTs: 60 tokens, refilled one a second, then `rate_limited` (429,
  `Retry-After: 1`). `serve --write-limit N` changes the 60, for verification runs (the
  conformance suite and release admission) that replay many projects from one address; a
  deployment keeps the default. The client is the edge's `CF-Connecting-IP` when the
  edge token (`REG_META_EDGE_TOKEN`) is set (the origin lock has admitted only requests
  that carry it), otherwise the peer, and an IPv6 address counts as its /64.
  **Address-only** by design: a session token would bucket per browser (helpful behind
  NAT) but adds a fingerprinting surface for anonymous public data. Buckets are per
  process (lost on restart, not shared across replicas), which suffices as the origin
  backstop behind the edge. A deployment without the edge token keys every client on
  Fly's proxy, so all of them share one bucket.
- **Body cap** — `payload_too_large` (413) over 1 MiB, counted as the body streams in
  (axum's `DefaultBodyLimit`), never trusting `Content-Length`. 1 MiB is far above any
  plausible `project_data.json`.

## Usage statistics (#1323)

Aggregate counts only, for the maintainer. Nothing links one request to another: no IP
address, no user id, no cookie, no hash of either. The edge worker records each request
in Workers Analytics Engine (3-month retention, fixed by Cloudflare); the server keeps
nothing and logs nothing. Counting at the edge also counts cache hits, which never reach
the server.

The server labels what a request asked for (`crates/reg-meta/src/usage.rs`), so the
worker needs no copy of the route table and parses no bodies. Every operation's
response, over HTTP and over `/mcp`, carries `x-reg-usage`, a compact JSON object with
only the fields the request has:

- `op`: the operation name; for an `/mcp` request that runs none, its JSON-RPC method
  (`initialize`, `tools/list`), or `other` for a method outside the MCP methods the
  server knows, since the client chooses that text.
- `fqids`: the ref parameters, and for an MCP `order` each bound variable (at most 100):
  an agent's pick. The SPA calls `order` after every edit (Loaded projects), so its
  `order` names none; the SPA reports a pick with its own beacon instead.
- `q`, `n`: a search's text and the hits on the answered page. `n: 0` is a search that
  found nothing.
- `err`: a refusal's code, including an MCP tool call or a POST body refused before the
  operation ran (`op` is then the tool or operation name). A refusal from the write
  limits (rate limit, body cap) comes before any operation is known and carries no
  label; the edge records its status.

Search text and ids pass a scrub first: a word holding an `@` and any run of six or more
digits (`-` or `+` allowed between them, any Unicode digit) become `<redacted>`. That
covers person and organisation numbers, dates, phone numbers written without spaces and
email addresses, and also removes a six-digit code someone searched for (accepted,
maintainer 2026-11-04). Digits split by spaces pass: joining them would also join years
listed in a search. The text is kept at most 200 characters and never shown to users.
The header is ASCII (other characters as JSON `\u` escapes) and at most 4 KiB. Ids are
dropped first, then the text is cut. The cached copy of a response keeps its label, so a
cache hit is labelled too. A 304 revalidation answered before the route runs carries no
label. The edge counts it as a revalidation without attributing it, which also stops one
browser's repeat views from counting as fresh opens.

Every response also carries `Server-Timing: app;dur=<ms>`, the server's own time. The
edge records it beside its own time to the origin, which lets real traffic be checked
against the origin budgets (Production performance budgets).

## Input-validation gates (security boundary)

Hostile input on the catalog and docs reads (a malformed ref, a traversal-shaped path, a
bad period, table or query) is refused by the Rust server with a located error; the
grammars and refusals are pinned in `conformance/cases/api`. A project body is parsed
strictly before validation or order checking. Unknown query parameters are refused as
`invalid_parameter`.
