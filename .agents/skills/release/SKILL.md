---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and release reg_meta (the `reg-meta` crate and its DB
  assets) or reg_meta_build, create package tags/releases, upload reg_meta DB assets, or
  monitor the release workflows. Usage: /release [package] <patch|minor|major>
---

# Release pipeline

Create and publish a release for one or more of the packages. Nothing is published to
PyPI: a `reg_meta` release is a crate version bump, a tag, the DB assets and the
`reg-meta` binaries; a `reg_meta_build` release is a version bump and a tag.

**Never start a release unless the user explicitly asks for one.** This skill may be
invoked via `/release` or merely referenced in conversation — either way, do not proceed
without clear intent to release. Stop and ask if the package or bump level is ambiguous;
**major bumps require explicit confirmation** after showing the current and planned
versions.

## Direct execution and dependency handoff

Run this workflow in a task-owned worktree. Coordinate changes to main so only one
integration advances it at a time, including the sequential release pushes. Manual PRs
require green CI and the maintainer's review before merge.

Start from the verified source revision. When a dependency upgrade was also requested,
[upgrade-deps](../upgrade-deps/SKILL.md) owns that upgrade and its compatibility fixes
first; consume its integrated head and verification evidence. Do not start a new
dependency refresh as an incidental publication step. Reuse unchanged source/asset
verification where applicable; the publication and shipped-asset checks below still
apply to each release.

Repair blocking packaging or release-workflow problems directly within release
preparation. Keep non-bump fixes in separate commits with focused verification and
independent review; manual PRs retain the maintainer-review and CI requirement.
Unrelated product changes remain outside the release. Once the repaired source is
integrated, continue this pipeline from the affected check. If a draft or tag already
exists, follow Error recovery below first so publication uses the repaired revision.

## Packages

  | Package        | Version files                                                                    | Release workflow                                                                  |
  | -------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
  | reg_meta       | `crates/reg-meta/Cargo.toml` (and `Cargo.lock`)                                  | `publish_reg_meta.yml` on `release: published` (CI, artifact conformance, deploy) |
  | reg_meta_build | `reg_meta_build/pyproject.toml`, `reg_meta_build/src/reg_meta_build/__init__.py` | none (tag and GitHub release only)                                                |

`reg_meta` is the Rust runtime: the `reg-meta` binary (`serve` and `mcp`) and the
catalog DB assets it reads. Its `reg_meta/v*` release carries the three DB assets (step
8\) and, once package 4.12 of `RUST_RUNTIME_SPEC.md` lands, the `reg-meta` binaries for
macOS arm64 and Linux x86_64 with SHA-256 checksums (step 10).

`reg_meta_build` is the build pipeline that produces the DB assets. It is
maintainer-only and runs from a checkout (it depends on the workspace-only
`reg-core-py`). Its release is a version bump, the `reg_meta_build/v*` tag and a GitHub
release, with no workflow and no assets: the DB assets attach to the parallel
`reg_meta/v*` release.

## Validation

Before doing anything, validate and resolve the inputs.

1. **Resolve the bump level**: one of the arguments must be `patch`, `minor`, or
   `major`. If none is provided, stop and ask.

2. **Resolve the package(s)**: if a package name is provided, use it. Otherwise infer
   from unreleased commits since each package's last `<package>/vX.Y.Z` tag. The
   `reg_meta` sources are `crates/`, `reg_webapp/` and `conformance/`; the
   `reg_meta_build` sources are `reg_meta_build/`:

   ```sh
   git fetch --tags origin
   tag="$(git tag --list '<package>/v*' --sort=-v:refname | head -n 1)"
   if [ -n "$tag" ]; then git log --oneline "$tag"..HEAD -- <paths>; else git log --oneline -- <paths>; fi
   ```

   - If only one package has changes, use it.
   - If multiple have changes, release them sequentially — run the full pipeline below
     for each, one at a time, with separate commits, tags, and releases.
   - **Also compare `reg_meta_build/` changes since the last `reg_meta/v*` tag**, even
     when no `crates/` code changed: builder content that affects the built DBs (curated
     TOMLs, provider `sources/`, `db.py` content) requires a matching `reg_meta` release
     so the DB asset is refreshed. When schema-affecting changes touch
     `reg_meta_build/`, the `reg_meta` release that publishes the rebuilt asset leads.
   - If nothing has changed, tell the user there is nothing to release.

3. If any required input is still ambiguous, stop and ask.

4. **Major version bumps require explicit confirmation** — show current and planned
   versions before proceeding.

## Steps

Run the following steps for each resolved package.

### 1. Determine new version

- Read the current version from the package's version file (table above).
- Apply the semver bump: patch increments Z; minor increments Y and resets Z; major
  increments X and resets Y.Z.

### 2. Generate release notes

- Run `git log --oneline <package>/v<current>..HEAD -- <paths>` for commits since the
  last release tag (all commits touching the paths if no prior tag exists).
- Write a brief grouped bullet list (skip merge commits); link associated PRs/issues
  inline (e.g. `Fix widget crash (#42)`).
- Credit external contributors: get the last tag's date with
  `git log -1 --format=%cs <tag>`, then
  `gh pr list --search "is:merged merged:>=<date>" --json number,author,title`. For a
  bullet from a non-owner author, append `(HT @username)`.
- **Show the draft notes to the user before proceeding.**

### 3. Bump version

- reg_meta: the `version` line in `crates/reg-meta/Cargo.toml`, then
  `cargo update -p reg-meta --offline` to refresh `Cargo.lock`. The server reports this
  version (`/openapi.json`, the ETag) and it names the release.
- reg_meta_build: the `version = "X.Y.Z"` line in `reg_meta_build/pyproject.toml` and
  the `__version__ = "X.Y.Z"` line in `reg_meta_build/src/reg_meta_build/__init__.py`,
  then `uv lock`.

**reg_meta only — schema version check.** The catalog and docs schemas each have a
builder constant and a reader gate, and the two move together:

  | Schema  | Builder constant                                                      | Rust reader gate                                 |
  | ------- | --------------------------------------------------------------------- | ------------------------------------------------ |
  | catalog | `SCHEMA_VERSION` in `reg_meta_build/src/reg_meta_build/db.py`         | `SCHEMA` in `crates/reg-catalog/src/lib.rs`      |
  | docs    | `DOC_SCHEMA_VERSION` in `reg_meta_build/src/reg_meta_build/doc_db.py` | `DOC_SCHEMA` in `crates/reg-catalog/src/docs.rs` |

Run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/db.py reg_meta_build/src/reg_meta_build/doc_db.py crates/reg-catalog/src/`
and check for DDL changes (`CREATE TABLE`, `CREATE VIRTUAL TABLE`, column lists,
`DOC_DDL`, new `doc_meta` keys) and for reader reads of new tables or columns. A schema
change lands with its bump in the PR that makes it; if one was missed, stop and land the
bump through a reviewed PR before this release:

- **Major bump** (breaking): renamed/removed tables or columns, changed column
  semantics.
- **Minor bump** (new columns the reader reads): added columns/tables that queries
  reference. The server refuses to boot on a DB whose minor is behind its gate, so this
  forces a DB rebuild before the release is usable.

A schema bump forces fresh DB assets in step 8 and usually a `reg_meta_build` release
too (so `reg-meta-build build-db` from a checkout produces the new schema). Release
`reg_meta_build` first in that case.

### 4. Verify, test, lint

```sh
uv run --no-project scripts/gate.py all
uv run ruff check
uv run ruff format --check
uvx --from ty==0.0.79 ty check
```

`gate.py all` is the release's test gate: G0 (conformance, the Python suites,
`cargo test --workspace`), the Rust HTTP conformance run, release admission on the
synthetic steward artifact, the Playwright flows and the frontend. There is no pre-push
test hook, so run it on the version-bumped tree before the commit is pushed to main.

Run it on a **committed** bump: create step 5's bump commit locally first, then run the
gate, and push only once it is green. On an uncommitted bump the builder's
clean-tracked-tree guard fails its strict-build cases.

If Rust compiles fail with `Operation not permitted` writing `deps/*.d` files, a shared
`sccache` wrapper (`~/.cargo/config.toml`) is running under another session's sandbox.
Bypass it for this run with `RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=` rather than
editing the config.

If anything fails, stop and fix. Do not release broken code.

### 5. Commit and push

Complete the manual integration handoff above before the first main push. Keep the same
coordination through subsequent package pushes.

Before committing, verify that all non-bump changes are already committed in their own
commits. The bump commit must contain **only** version-bump files — for reg_meta
`crates/reg-meta/Cargo.toml` and `Cargo.lock`; for reg_meta_build its `pyproject.toml`,
`__init__.py` and `uv.lock`:

```text
Bump <package> version to X.Y.Z
```

Then push to main and **verify the bump landed on `origin/main`** before tagging (the
tag in step 6 is created from `origin/main`, not from a possibly-stale local HEAD):

```sh
git push origin HEAD:main
git fetch origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "bump is not origin/main; resolve before tagging" >&2; exit 1
fi
```

**No hook tests this push.** The step 4 gate is the release's test gate; do not push the
bump commit until it is green. If you rebased the bump onto a moved `origin/main` after
step 4, re-run step 4 before pushing. CI re-runs the suite on main after the push.

### 6. Create draft GitHub release

`publish_reg_meta.yml` fires on `release: published`, so a reg_meta release must be
created as a **draft** until all its assets are uploaded. The workflow's artifact
conformance and image deploy read this release's assets; a published release with a
missing asset fails both. The draft step is the ONLY thing standing between a missing
asset and a failed deploy.

Pass the verified `origin/main` commit as `--target` so the tag is created from it. The
tag is created by this command — do not create it separately.

```sh
target="$(git rev-parse origin/main)"
gh release create <package>/vX.Y.Z --draft --target "$target" --title "<package> vX.Y.Z" --notes-file <notes-file>
```

The `--draft` flag means no workflow fires yet. If the tag already exists, a prior
attempt went wrong — see Error recovery.

### 7. (reg_meta_build) Publish and stop

A reg_meta_build release has no assets and no workflow: publish the draft
(`gh release edit reg_meta_build/vX.Y.Z --draft=false`) and report it done.

### 8. Build and upload release assets (reg_meta only)

reg_meta ships **three** DB assets, and **every release must carry all three before it
is published** (self-contained releases). The container deploy pipeline
(`.github/workflows/container-build.yml`) resolves the newest `reg_meta/v*` release and
bakes its assets into the origin images with curl, a SHA-256 check against the digest
GitHub records, and zstd (`reg_webapp/Dockerfile`): `reg_meta.db.zst` and
`reg_meta_docs.db.zst` for the public catalog — a release published without them breaks
every main-push image build until assets appear (#343, the asset-less
`reg_meta/v0.11.0`) — and `reg_meta_swecov.db.zst` (8c), the compiled SWECOV steward DB
the `build-swecov-image` job bakes for `data.swecov.se`; a release missing it fails
every SWECOV deploy at asset resolution (broke v0.36.0–v0.38.0, #1091). The conditions
in 8a/8b/8c decide whether each asset needs a **fresh build**; one that doesn't is
**copied forward** from the prior release (8d). Never skip an asset outright.

The main catalog build requires a complete accepted prepared root and its exact prepared
acceptance commit and top-level `manifest.json` SHA-256. Raw source capture and
preparation are separate maintainer operations; follow the [build-db
skill](../build-db/SKILL.md) if those inputs or pins are unavailable. Do not combine
loose inputs from different checkouts or overlay files onto an accepted candidate.
Curation comes from this release's tracked builder checkout. Documentation inputs follow
the separate doc-asset workflow in 8b; private holdings follow 8c.

#### 8a. Main DB asset (`reg_meta.db.zst`)

Build and upload fresh if **any** condition is true:

- The catalog schema version was bumped since the prior release.
- The release is a **major** version bump.
- The builder or its curated inputs changed since the prior release's asset —
  `git log <prev reg_meta tag>..HEAD -- reg_meta_build/ ':(exclude)reg_meta_build/docs/'`
  is non-empty. Build-side changes (curated TOMLs, `sources/`, `db.py` content, new
  indexes, errata) alter DB **content** without necessarily bumping the schema, so
  copying the old asset forward would ship a **stale** DB. The `docs/` exclude matters:
  `build-db` does not consume `reg_meta_build/docs/` (that drives the doc-DB asset in
  8b), so a docs-only release still copies the main DB forward. When this fires only
  because of a cosmetic change (e.g. a formatter pass that cannot move DB bytes), a
  fresh build is still the safe choice — it doubles as the real-data validation gate and
  captures any upstream SCB input drift you cannot prove absent.

Otherwise copy the prior release's asset forward (8d) and skip the rest of 8a.

The shipped DB is the full **global catalog**, built from the maintainer's accepted
complete prepared-source root. Follow the [build-db skill](../build-db/SKILL.md) to
verify the exact source and curation pins. Do not use a `--registers` subset or a
diagnostic database as a release asset. Steward-private providers remain the separate
`extend-db` overlay.

Build to a fresh scratch directory with strict validation. The accepted root is
read-only; there are no loose CSV or mutable slug-directory build overrides. Every
unresolved error must block publication. Check the report's `status` and
`publication_ready` before compression. A diagnostic build retains errors and is never
releasable.

```sh
set -euo pipefail
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_db.XXXXXX")"
prepared="/absolute/path/to/accepted-prepared"
prepared_commit="EXACT_PREPARED_ACCEPTANCE_SHA"
prepared_manifest_sha256="EXACT_TOP_LEVEL_PREPARED_SHA256"
uv run reg-meta-build --db "$db_dir/catalog" build-db \
  --prepared "$prepared" \
  --input-commit "$prepared_commit" \
  --input-manifest-sha256 "$prepared_manifest_sha256" \
  --report-dir "$db_dir/report" --timing
uv run python -c 'import json,sys; s=json.load(open(sys.argv[1])); assert s["status"] == "complete" and s["publication_ready"] is True' "$db_dir/report/summary.json"
db="$db_dir/catalog/reg_meta.db"
zstd -3 -T0 "$db" -o reg_meta.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta.db.zst
rm reg_meta.db.zst
```

The common writer validates structural and full-corpus invariants before atomically
placing a self-contained SQLite file. Keep the build report and accepted prepared input
pins with the release evidence. Confirm all expected global providers and their content
are represented before shipping. Source discrepancies require a separately reviewed
input or curation update; do not bypass validation or modify an accepted prepared root
in place.

#### 8b. Doc DB asset (`reg_meta_docs.db.zst`)

Build and upload fresh if **any** of these is true:

- The docs schema version was bumped since the prior release.
- `git diff <tag>..HEAD -- reg_meta_build/docs/` is non-empty (docs content changed).
- `git diff <tag>..HEAD -- reg_meta_build/related_documents.toml` is non-empty
  (related-document provenance changed; the binaries are gitignored but the doc asset
  consumes this tracked map).
- Any gitignored related-document PDF under `reg_meta_build/input_data/SCB/docs/` was
  added, replaced, or refetched since the prior doc asset. Because git cannot detect
  this, compare the maintainer seed / build-computed `related_document.sha256` against
  the prior asset when in doubt; a binary change means build fresh.
- The release is a **major** version bump.

Otherwise copy the prior release's asset forward (8d) and skip the rest of 8b.

If `reg_meta_build/docs/` changed because a newly-published SCB PDF was ingested, the
PDF→markdown recipe (marker flags, `GEMINI_API_KEY`, \~$1-2 cost, multiprocessing-crash
workaround, `--MarkdownRenderer_keep_pageheader_in_output` footgun) lives in the
docstring at the top of `scripts/parse_lisa_docs.py`. Run that step first, then continue
here.

Run the same checkpoint as 8a before compressing. Today this is a no-op guard —
`build-docs` already produces a `DELETE`-mode file (only the main catalog build sets
WAL) — but it keeps the shipped-asset invariant ("self-contained single file, no
sidecars") independent of the builder's journal-mode choices:

```sh
set -euo pipefail
docs_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_docs.XXXXXX")"
uv run reg-meta-build --db "$docs_dir" build-docs
db="$docs_dir/reg_meta_docs.db"
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db"
zstd -3 -T0 "$db" -o reg_meta_docs.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_docs.db.zst
rm -rf "$docs_dir" reg_meta_docs.db.zst
```

#### 8c. SWECOV compiled steward asset (`reg_meta_swecov.db.zst`)

The SWECOV steward app (`data.swecov.se`) bakes one **compiled steward artifact** — the
global catalog, accepted steward provider overlays, and compiled physical holdings — as
the DB its `reg-meta serve --catalog swecov` reads. `container-build.yml`'s
`build-swecov-image` job bakes this asset from the newest `reg_meta/v*` release;
**absent, the SWECOV image build fails** and `deploy-swecov` / `edge-deploy-swecov`
skip. This producer step must run on **every** reg_meta release (#1091 — omitting it
broke v0.36.0–v0.38.0's SWECOV deploys silently, since the global apps deploy fine
without it).

Build and upload fresh if **any** condition is true:

- 8a rebuilt the main DB asset — the flavor is `extend-db`-baked **on top of** the main
  DB content, so a fresh main DB requires a fresh flavored DB.
- The SWECOV flavor inputs changed since the prior asset:
  `git diff <prev reg_meta tag>..HEAD -- reg_meta_build/fqid_slugs/swecov/` is
  non-empty. The generated `input_data/swecov/providers/` TOMLs are accepted
  maintainer-local inputs, so compare their exact acceptance pins rather than relying on
  the public diff. A changed holdings commit, manifest, provider overlay, inventory,
  census, or policy requires a fresh steward build.
- The release is a **major** version bump.
- The immediately-previous release does **not** carry `reg_meta_swecov.db.zst` (e.g.
  recovering from the v0.36.0–v0.38.0 gap). Copy-forward would then reach back to an
  older release whose flavored DB was baked on a **different** main catalog than this
  release ships — a stale mismatch. Rebuild instead.

Otherwise copy the prior release's asset forward (8d) — but **only from the
immediately-previous release**, never a further-back one, so the flavored DB always
pairs with the same global content 8a copied forward. Also verify the prior artifact's
base generation and accepted holdings pins still match this release's inputs and current
admission contract. Otherwise rebuild. Then skip the rest of 8c.

Build the flavored DB from **this release's** main asset by downloading and
decompressing it (`extend-db` opens the base with sqlite, never the `.zst`). Where that
asset lives when 8c runs depends on 8a's decision, because the main-DB copy-forward is
deferred to 8d (which runs **after** 8c): if 8a **rebuilt** the main DB it is already on
this release's draft (`reg_meta/vX.Y.Z`); if 8a is **copying it forward**, pull it from
the copy-forward source `reg_meta/v<prev>` instead — it is not on the draft yet. Either
way the base is a fetched file, not 8a's temp dir. Select a clean accepted private
holdings candidate and its full input commit and manifest SHA-256. That candidate owns
provider overlays, policies, generated inventory, and raw census; the tracked generator
and its default policies do not select or accept a candidate. Compile before publishing
this release; no post-release runtime inventory regeneration is required. Checkpoint
WAL→DELETE (self-contained single file, same invariant as 8a):

```sh
set -euo pipefail
# main_src = where this release's reg_meta.db.zst lives when 8c runs:
#   reg_meta/vX.Y.Z   if 8a rebuilt it (already uploaded to the draft), or
#   reg_meta/v<prev>  if 8a is copying it forward (8d uploads to the draft after 8c).
main_src="reg_meta/vX.Y.Z"
# Set these from the reviewed acceptance evidence before running this block.
: "${accepted_holdings:?clean accepted private candidate required}"
: "${holdings_commit:?full accepted input commit required}"
: "${holdings_manifest_sha256:?full accepted input manifest SHA-256 required}"
base_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_base.XXXXXX")"
gh release download "$main_src" --pattern reg_meta.db.zst --dir "$base_dir"
zstd -d "$base_dir/reg_meta.db.zst" -o "$base_dir/reg_meta.db"
flav_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_swecov.XXXXXX")"
uv run reg-meta-build --db "$flav_dir" extend-db \
    --base-db "$base_dir/reg_meta.db" \
    --steward swecov \
    --holdings-input "$accepted_holdings" \
    --input-commit "$holdings_commit" \
    --input-manifest-sha256 "$holdings_manifest_sha256"
db="$flav_dir/reg_meta.db"
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db"
zstd -3 -T0 "$db" -o reg_meta_swecov.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_swecov.db.zst
rm -rf "$base_dir" "$flav_dir" reg_meta_swecov.db.zst
```

Strict `extend-db` validates structural invariants, accepted mappings, and census
accounting before atomic publication. It requires the clean accepted input candidate and
both exact pins; committed builder sources and steward slug pins are also validated.
There are no runtime inventory, skip-validation, or skip-holdings gates. Inspect the
result's manifest and build report before uploading: it must be a complete, publishable
`steward` artifact naming `swecov`, with the expected base generation, accepted-input
pins, builder commit, and full generation ID. Verify the final file digest separately;
output bytes do not enter the semantic generation identity. Physical holdings may extend
beyond semantic windows; the shared order materializer checks applicability for each
request.

A red publication gate requires a reviewed correction at its owning input or curation
surface, followed by fresh acceptance where input bytes changed and a rebuild. Global
source corrections go through the global build before steward extension. Do not widen
windows or change an accepted candidate in place merely to make the gate pass. The
server admits only complete, publishable artifacts at boot, and `serve --catalog swecov`
additionally requires the artifact's steward to match. Those admission checks replace
the deleted runtime inventory reconciliation and drift banner, rather than deferring a
failed compiler check until deployment.

**Maintainer-local inputs**: the accepted private holdings candidate and its provider
overlays, inventory, policies, and census are local-only. If a **fresh** SWECOV build is
required (per the conditions above) but these inputs are absent — a non-maintainer or CI
environment — **stop and do not publish**. Publishing (`--draft=false`, step 9)
dispatches `container-build.yml`, whose `build-swecov-image` job hard-fails on the
missing (or stale) asset — recreating exactly the broken-release state this step exists
to prevent. Ask the maintainer to build and upload `reg_meta_swecov.db.zst` before
publishing. (The 8d copy-forward path needs no maintainer-local inputs, so it is always
available when a fresh build was **not** required.)

#### 8d. Copy-forward for assets not rebuilt

For each asset whose 8a/8b/8c conditions did **not** require a fresh build, copy the
prior release's asset forward so the new release stays self-contained. `<prev>` is the
newest existing `reg_meta/v*` release that carries the asset — normally the immediately
previous release; check with `gh release view reg_meta/v<prev> --json assets`. This is
safe precisely because the rebuild conditions did not fire. Run `gh` from the repo root
(cd-ing out of the checkout breaks its repo detection) and stage through a temp dir with
`--dir`:

```sh
set -euo pipefail
asset="<asset-name>"   # reg_meta.db.zst, reg_meta_docs.db.zst, or reg_meta_swecov.db.zst
cf_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_cf.XXXXXX")"
gh release download reg_meta/v<prev> --pattern "$asset" --clobber --dir "$cf_dir"
gh release upload reg_meta/vX.Y.Z "$cf_dir/$asset"
rm -rf "$cf_dir"
```

#### 8e. Verify before publishing

Verify **all three** assets are present on the draft release, each with one SHA-256
digest — do not publish without them (#343, #1091):

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[] | [.name, .digest] | @tsv'
```

Then run the tier-3 artifact conformance on each new catalog (the global one and the
SWECOV steward), downloaded from the draft and decompressed into its own directory, with
the command in `CLAUDE.md` → "Lint and test"
(`--run-release --artifact-dir=... --server-cmd=...`). It admits the artifact and fails
on an incompatible or non-publishable one. A red run stops the publication: fix the
asset (step 8) and re-run.

### 9. Publish the draft release

This is what fires `publish_reg_meta.yml`.

```sh
gh release edit reg_meta/vX.Y.Z --draft=false
```

### 10. Monitor the release workflow

`publish_reg_meta.yml` runs CI on the tag, then artifact conformance against this
release's global and SWECOV assets (`integration.yml`, served by the Rust server), and
dispatches `container-build.yml` on main, which re-resolves the newest `reg_meta/v*`
release (this one), bakes its assets and deploys. **Scope the run lookup to this
release** — fetch tags first, then filter by `--event release` and the tag's commit. An
unfiltered `--limit 1` can match a stale completed run, because GitHub may not have
queued the new release event yet:

```sh
git fetch --tags origin
target="$(git rev-list -n 1 reg_meta/vX.Y.Z)"
run_id=""
while [ -z "$run_id" ]; do
  run_id="$(gh run list --workflow=publish_reg_meta.yml --event release --commit "$target" --json databaseId --jq '.[0].databaseId // ""')"
  [ -n "$run_id" ] || sleep 10
done
gh run watch "$run_id" --exit-status
```

Share the run URL with the user, then watch the dispatched `container-build.yml` run on
main through its deploys.

If the `integration` job is red, read the step that failed:

- **Asset resolution or digest** — an asset is missing, duplicated, or its recorded and
  actual SHA-256 disagree. Upload an asset that is missing (step 8); a duplicated or
  wrong one means a new patch release (Error recovery).
- **Artifact conformance** — an incompatible, incomplete, nonpublishable, or mismatched
  artifact, or a conformance case the asset fails. Repair the build or selected input
  and cut a new patch release; never resurrect loose runtime inventory as a workaround.

Re-validate with `gh workflow run integration.yml --ref main` (then watch that
dispatched run). Do **not** re-release a working version over a stale check.

**Binaries (once package 4.12 lands).** A matrix workflow on `reg_meta/v*` builds
`reg-meta` for macOS arm64 and Linux x86_64 and uploads them to the release with SHA-256
checksums. Watch that run and confirm both binaries and their checksum files are on the
release. Until 4.12 lands, a release carries no binaries; local `reg-meta mcp` users
build from a checkout.

### 11. Verify the deployed artifacts

After the deploy, confirm each origin serves this release's artifact. `GET /api/context`
on `catalog.swecov.se` and `data.swecov.se` carries the full generation and the default
read scope in its `meta`; it must match the build evidence recorded in step 8. Catalog
artifacts default to `reference` and steward artifacts to `holdings`, and on the catalog
deployment `GET /api/catalog?scope=holdings` must refuse with `scope_unavailable`.
Spot-check a scoped catalog or search read and a known order. Record what these checks
actually exercised; a digest or compiler report alone does not prove deployed HTTP
behavior.

Do not regenerate or commit an inventory after publication. Generated inventory is an
accepted local builder input consumed before compilation, not a deploy file. Changed
inputs require a fresh accepted candidate and a rebuilt steward asset under the normal
release workflow. If the private candidate is unavailable when a fresh build is
required, stop publication at step 8c; a runtime drift flag cannot make an old asset
safe.

## Error recovery

- If the commit was pushed but `gh release create` fails: the commit is on main — just
  retry the release creation.
- If the draft exists and something fails before publication: delete the draft and tag,
  fix the issue, and start over from the verified bump (step 5).
- If source changes after draft/tag creation and before publication: recreate the
  release and tag from the verified repaired `origin/main` revision (steps 5–6), refresh
  the notes, and replace assets invalidated by the repair. Re-run the applicable checks
  at that same revision. A passing check on a newer worktree does not validate an older
  release target.
- If a tag already exists for the target version: a previous attempt went wrong.
  Investigate before proceeding.
- If `build-db` or `build-docs` fails: fix the issue before publishing. The draft
  release exists but `--draft=false` must not run until all three assets are valid.
- If `gh release upload` fails on a draft: retry the upload. The draft and tag are fine.
- **Once a release is published, do not delete, re-cut or re-upload its assets.**
  Deploys and the G1 baseline pin the tag and its asset digests. Fix downstream failures
  in place (a deploy failure, a stale artifact check), or cut a new patch version if the
  released code or assets are wrong.
- Never force-push or amend commits already on main.
