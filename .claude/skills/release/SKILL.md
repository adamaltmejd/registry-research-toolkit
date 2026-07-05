---
name: release
description: >-
  Create and publish a release. Usage: /release [package] <patch|minor|major>;
  chief-of-staff may invoke /release minor or /release patch after an authorized
  build/release boundary
disable-model-invocation: false
argument-hint: "[package] <patch|minor|major>"
---

# Release pipeline

Create and publish a release for one or both PyPI packages.

**Never start a release unless the user explicitly asks for one.** This skill may be
invoked via `/release` or merely referenced in conversation — either way, do not proceed
without clear intent to release. Exception: if `chief-of-staff` invokes exactly
`/release minor` or `/release patch` after merging a PR that creates a required
build/release boundary, treat that handoff as explicit user authorization for that minor
or patch release. Stop and ask if the package or bump level is ambiguous; **major bumps
require explicit confirmation** after showing the current and planned versions.

## Packages

  | Package        | pyproject.toml                  | `__init__.py`                                   | Publish workflow                                                                              |
  | -------------- | ------------------------------- | ----------------------------------------------- | --------------------------------------------------------------------------------------------- |
  | reg_meta       | `reg_meta/pyproject.toml`       | `reg_meta/src/reg_meta/__init__.py`             | `publish_reg_meta.yml` (unattended — `pypi` environment review gate removed 2026-06-10)       |
  | reg_meta_build | `reg_meta_build/pyproject.toml` | `reg_meta_build/src/reg_meta_build/__init__.py` | `publish_reg_meta_build.yml` (unattended — `pypi` environment review gate removed 2026-06-10) |

reg_meta_build is the build pipeline that produces `reg_meta`'s SQLite assets. It has
its own PyPI release on the `reg_meta_build/v*` tag but ships no DB release assets
(those attach to the parallel `reg_meta/v*` release). It **depends on `reg-meta`** (a
`reg-meta>=` floor in its pyproject), so reg_meta is upstream. That floor is normally
already satisfied by the published reg_meta, so either publish order resolves — but if a
release raises the floor to the new reg_meta, publish reg_meta first and verify it is on
PyPI before publishing the builder.

`reg_schema` is a library with `reg_schema/pyproject.toml` only — no checked
`__version__` and no publish workflow exist yet. It is **not** a current /release
target. Before any first PyPI release of it, stop and add or confirm the publish path
rather than silently shipping a package with no workflow.

## Validation

Before doing anything, validate and resolve the inputs.

1. **Resolve the bump level**: one of the arguments must be `patch`, `minor`, or
   `major`. If none is provided, stop and ask.

2. **Resolve the package(s)**: if a package name is provided, use it. Otherwise infer
   from unreleased commits since each package's last `<package>/vX.Y.Z` tag:

   ```sh
   git fetch --tags origin
   tag="$(git tag --list '<package>/v*' --sort=-v:refname | head -n 1)"
   if [ -n "$tag" ]; then git log --oneline "$tag"..HEAD -- '<package>/'; else git log --oneline -- '<package>/'; fi
   ```

   - If only one package has changes, use it.
   - If multiple have changes, release them sequentially — run the full pipeline below
     for each, one at a time, with separate commits, tags, and releases.
   - **Also compare `reg_meta_build/` changes since the last `reg_meta/v*` tag**, even
     when no `reg_meta/` code changed: builder content that affects the built DBs
     (curated TOMLs, provider `sources/`, `db.py` content) requires a matching
     `reg_meta` release so the prebuilt DB asset is refreshed. When schema-affecting
     changes touch `reg_meta_build/`, the `reg_meta` release that publishes the rebuilt
     asset leads.
   - If nothing has changed, tell the user there is nothing to release.

3. If any required input is still ambiguous, stop and ask.

4. **Major version bumps require explicit confirmation** — show current and planned
   versions before proceeding.

## Steps

Run the following steps for each resolved package.

### 1. Determine new version

- Read the current version from `<package>/pyproject.toml`.
- Apply the semver bump: patch increments Z; minor increments Y and resets Z; major
  increments X and resets Y.Z.

### 2. Generate release notes

- Run `git log --oneline <package>/v<current>..HEAD -- <package>/` for commits since the
  last release tag (all commits touching `<package>/` if no prior tag exists).
- Write a brief grouped bullet list (skip merge commits); link associated PRs/issues
  inline (e.g. `Fix widget crash (#42)`).
- Credit external contributors: get the last tag's date with
  `git log -1 --format=%cs <tag>`, then
  `gh pr list --search "is:merged merged:>=<date>" --json number,author,title`. For a
  bullet from a non-owner author, append `(HT @username)`.
- **Show the draft notes to the user before proceeding.**

### 3. Bump version

Update the version string in both files:

- `<package>/pyproject.toml` — the `version = "X.Y.Z"` line
- `<package>/src/<package>/__init__.py` — the `__version__ = "X.Y.Z"` line

**reg_meta only — main-DB schema version check:** run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/db.py reg_meta/src/reg_meta/db.py`
and check for changes to `CREATE TABLE`, `CREATE VIRTUAL TABLE`, or column lists (DDL
lives in `reg_meta_build/src/reg_meta_build/db.py` post-split). If the schema changed
but `SCHEMA_VERSION` in `reg_meta/src/reg_meta/db.py` was not already bumped, bump it
now:

- **Major bump** (breaking): renamed/removed tables or columns, changed column
  semantics.
- **Minor bump** (new columns the code reads): added columns/tables that queries
  reference. `open_db` rejects DBs whose minor is < the code's minor, so this forces a
  DB rebuild before the package release is usable.

A `SCHEMA_VERSION` bump may require a coordinated `reg_meta_build` release if the
matching DDL also needs to ship in the builder wheel (so an end-user
`reg-meta-build build-db` produces the new schema). Release `reg_meta_build` first in
that case.

**reg_meta only — doc-DB schema version check:** run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/doc_db.py reg_meta/src/reg_meta/doc_db.py`
for changes to `DOC_DDL` or reads of new `doc_meta` keys (DDL lives in
`reg_meta_build/src/reg_meta_build/doc_db.py` post-split). If the doc schema changed but
`DOC_SCHEMA_VERSION` in `reg_meta/src/reg_meta/doc_db.py` was not bumped, bump it now
(same major/minor rules). A bump forces a fresh doc-DB asset in step 8.

### 4. Update lockfile

```sh
uv lock
```

### 5. Verify, test, lint

```sh
bash scripts/check_versions.sh
uv run python -m pytest <package>/ -x -q
uv run ruff check
uv run ruff format --check
uvx --from ty==0.0.54 ty check
```

This pytest is a fast per-package pre-flight; the **full** suite runs at push time (step
6). If anything fails, stop and fix. Do not release broken code.

### 6. Commit and push

Before committing, verify that all non-bump changes are already committed in their own
commits. The bump commit must contain **only** version-bump files — `pyproject.toml`,
`__init__.py`, `uv.lock`, and (if a schema version was bumped) `db.py` or `doc_db.py`:

```text
Bump <package> version to X.Y.Z
```

Then push to main and **verify the bump landed on `origin/main`** before tagging (the
tag in step 7 is created from `origin/main`, not from a possibly-stale local HEAD):

```sh
git push origin HEAD:main
git fetch origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "bump is not origin/main; resolve before tagging" >&2; exit 1
fi
```

**The push runs the full pre-push gate** (#710): the entire pytest suite — including the
`reg_meta` Docker integration test as a *hard* gate (`--run-integration`, so its
`docker` fixture **fails** rather than skips) — runs at push time, not commit. The bump
commit touches `pyproject.toml` and `__init__.py`, which the gate's
`files: \.(py|toml|json)$` filter matches, so the suite **will** run on this push.
**Docker must be running** or the push is blocked — start it and push again, never
bypass with `--no-verify`. (The release-marked `test_update_and_query`, which downloads
the published asset, is carved off pre-push and runs only post-publish — see step 10.)

### 7. Create draft GitHub release

The publish workflow fires on `release: published`, so the release must be created as a
**draft** until any required assets (reg_meta only) are uploaded. With no
environment-approval pause (the review gate was removed 2026-06-10), a
`release: published` event with missing assets races the workflow's smoke step against
the upload — the smoke step may walk back to a prior release, pick up an incompatible
asset, and fail the publish. The draft step is therefore the ONLY thing standing between
a missing asset and a failed publish.

Pass the verified `origin/main` commit as `--target` so the tag is created from it. The
tag is created by this command — do not create it separately.

```sh
target="$(git rev-parse origin/main)"
gh release create <package>/vX.Y.Z --draft --target "$target" --title "<package> vX.Y.Z" --notes-file <notes-file>
```

The `--draft` flag means no workflow fires yet. If the tag already exists, a prior
attempt went wrong — see Error recovery.

### 8. Build and upload release assets (reg_meta only)

reg_meta ships **three** release assets, and **every release must carry all three before
it is published** (self-contained releases). Two are consumed by `reg-meta update`: the
container deploy pipeline (`.github/workflows/container-build.yml`) resolves the newest
`reg_meta/v*` release into a concrete `reg-meta update --tag`, which fetches
`reg_meta.db.zst` + `reg_meta_docs.db.zst` from that single tag — a release published
without them breaks every main-push image build until assets appear (#343, the
asset-less `reg_meta/v0.11.0`). The **third**, `reg_meta_swecov.db.zst` (8c), is the
flavored SWECOV DB the same workflow's `build-swecov-image` job bakes as
`data.swecov.se`'s `REG_META_DB`; a release missing it fails every SWECOV deploy at
asset resolution (broke v0.36.0–v0.38.0, #1091). The conditions in 8a/8b/8c decide
whether each asset needs a **fresh build**; one that doesn't is **copied forward** from
the prior release (8d). Never skip an asset outright.

(`reg-meta update` in `latest` mode still walks backwards through releases to find the
most recent one carrying each asset — robustness for historical asset-less releases —
but new releases must not rely on it. The CI smoke step runs `reg-meta update` and fails
if it can't resolve a compatible pair of assets.)

The raw SCB CSV exports and curated classification CSVs live under
`reg_meta_build/input_data/` (gitignored). If missing, ask the user. If running from a
worktree whose untracked seed lives in another checkout, build an overlay input root:
start with that seed-bearing checkout's untracked inputs, then copy this release
checkout's tracked `reg_meta_build/input_data/**` files on top and mirror tracked
deletions/renames. Do not point `--input-dir` directly at another checkout if tracked
inputs changed in this release.

#### 8a. Main DB asset (`reg_meta.db.zst`)

Build and upload fresh if **any** condition is true:

- `SCHEMA_VERSION` was bumped (already in the commits or by step 3).
- The release is a **major** version bump.
- The builder or its curated inputs changed since the prior release's asset —
  `git log <prev reg_meta tag>..HEAD -- reg_meta_build/ ':(exclude)reg_meta_build/docs/'`
  is non-empty. Build-side changes (curated TOMLs, `sources/`, `db.py` content, new
  indexes, grafts) alter DB **content** without necessarily bumping `SCHEMA_VERSION`, so
  copying the old asset forward would ship a **stale** DB. The `docs/` exclude matters:
  `build-db` does not consume `reg_meta_build/docs/` (that drives the doc-DB asset in
  8b), so a docs-only release still copies the main DB forward. When this fires only
  because of a cosmetic change (e.g. a formatter pass that cannot move DB bytes), a
  fresh build is still the safe choice — it doubles as the real-data validation gate and
  captures any upstream SCB input drift you cannot prove absent.

Otherwise copy the prior release's asset forward (8d) and skip the rest of 8a.

The shipped DB is the full **global catalog** — every global provider, built with
build-db's **default** `--providers` set (currently
`scb,sos,fohm,fk,lakemedelsverket,pliktverket,riksarkivet,umu`, one per
`fqid_slugs/*.toml`). **Do NOT pin `--providers scb,sos`** — that drops the global thin
providers (FK, FOHM, Umeå, Riksarkivet, Pliktverket, Läkemedelsverket) and ships an
incomplete catalog (what shipped before v0.16.0). Omit the flag so newly onboarded
global providers are picked up automatically. `input_data/` **must** contain every
global provider's seed dir (`SCB/`, `Socialstyrelsen/`, `Folkhalsomyndigheten/`,
`Forsakringskassan/`, `Lakemedelsverket/`, `Pliktverket/`, `Riksarkivet/`, `UMU/`); a
missing dir hard-fails the checkout-staleness preflight (#550/#556, exit 10,
`EXIT_CONFIG`), and a curated `[variable]` pin for a non-built provider hard-fails
`slug_variable_override_stale`. Flavor/steward providers (`fqid_slugs/swecov/*`, e.g.
AMS/IAF/Skatteverket) are an extend-db overlay, **not** part of this build. (To rebuild
the legacy SCB-only asset, use `--providers scb`.)

Build to a temp DB dir and against a **copy** of the slug TOMLs so the repo tree stays
pristine: `build-db` writes gitignored `*.auto.toml` into `--slug-dir`, which would
otherwise trip `test_slug_snapshot` on the next commit. Checkpoint the WAL into the base
file and switch the journal mode to `DELETE` before compressing, so the shipped asset is
a self-contained single file (no `-wal`/`-shm` sidecars). `open_db` opens read-only with
`immutable=1` (#283), so a WAL asset would still open on a read-only dir — but a
`DELETE`-mode asset is robust for anyone opening it directly. Run the checkpoint via
`uv run python -c` (no sqlite3 CLI is assumed on the release host).

```sh
set -euo pipefail
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_db.XXXXXX")"
slug_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_slugs.XXXXXX")"
cp -R reg_meta_build/fqid_slugs/. "$slug_dir/"
input_dir="reg_meta_build/input_data"
# If the untracked seed lives in another checkout, set input_dir to an overlay
# root that includes this checkout's tracked input_data changes.
uv run reg-meta-build --db "$db_dir" build-db --input-dir "$input_dir" --slug-dir "$slug_dir"
db="$db_dir/reg_meta.db"
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db"
zstd -3 -T0 "$db" -o reg_meta.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta.db.zst
rm -rf "$db_dir" "$slug_dir" reg_meta.db.zst
```

`build-db` validates by default: it runs the value-set dedup + year-projection
invariants (plus the SOS corpus-volume gate, since this is a real build) inline and
exits 10 on failure (same checks as `scripts/validate_valueset_dedup.py`). Pass
`--no-validate` to skip — only ever for a throwaway build where the checks are noise.

If the build fails with `vardemangder_drift` (exit 10), SCB has shipped a new
`kod==version` row whose kod is in neither `_VARDEMANGDER_SENTINELS` nor
`_VARDEMANGDER_REAL_SHAPED` (both in `reg_meta_build/src/reg_meta_build/db.py`). Inspect
the listed values, add each to the appropriate allowlist (sentinel placeholder vs. real
single-code value set), then rerun. See `reg_meta_build/DESIGN.md` § "Vardemängder
sentinel filtering".

After the build, confirm it is the full catalog before shipping — `provider` should list
all eight global providers, not just `scb`/`sos`.

#### 8b. Doc DB asset (`reg_meta_docs.db.zst`)

Build and upload fresh if **any** of these is true:

- `DOC_SCHEMA_VERSION` was bumped.
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

#### 8c. SWECOV flavored DB asset (`reg_meta_swecov.db.zst`)

The SWECOV steward app (`data.swecov.se`) bakes a **flavored** DB — the global catalog
plus SWECOV's flavor providers — as its `REG_META_DB`. `container-build.yml`'s
`build-swecov-image` job resolves this asset (by url + sha256) from the newest published
`reg_meta/v*` release; **absent, the SWECOV deploy fails** at "Resolve SWECOV DB release
artifact" and `deploy-swecov` / `edge-deploy-swecov` skip. The consumer side is PR
#1014; this producer step must run on **every** reg_meta release (#1091 — omitting it
broke v0.36.0–v0.38.0's SWECOV deploys silently, since the global apps deploy fine
without it).

Build and upload fresh if **any** condition is true:

- 8a rebuilt the main DB asset — the flavor is `extend-db`-baked **on top of** the main
  DB content, so a fresh main DB requires a fresh flavored DB.
- The SWECOV flavor inputs changed since the prior asset:
  `git diff <prev reg_meta tag>..HEAD -- reg_meta_build/fqid_slugs/swecov/` is
  non-empty. The `flavor_inventory.json` and the generator are
  maintainer-local/untracked, so git can't see their drift — when in doubt, rebuild.
- The release is a **major** version bump.

Otherwise copy the prior release's asset forward (8d) and skip the rest of 8c.

Build the flavored DB from **this release's** main asset — download the
`reg_meta.db.zst` you just uploaded to the draft in 8a and decompress it (`extend-db`
opens the base with sqlite, never the `.zst`). Downloading from the draft is
self-contained: the asset is on the draft whether 8a rebuilt it or copied it forward, so
this does not depend on 8a's temp dir surviving. It is the same flavored DB step 11
regenerates the steward catalog against — 8c builds it from the draft **before
publish**, step 11 from the published release after. Checkpoint WAL→DELETE
(self-contained single file, same invariant as 8a):

```sh
set -euo pipefail
# Base = the reg_meta.db.zst already on the draft from 8a (fresh build or copy-forward).
base_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_base.XXXXXX")"
gh release download reg_meta/vX.Y.Z --pattern reg_meta.db.zst --dir "$base_dir"
zstd -d "$base_dir/reg_meta.db.zst" -o "$base_dir/reg_meta.db"
flav_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_swecov.XXXXXX")"
uv run reg-meta-build --db "$flav_dir" extend-db \
    --base-db "$base_dir/reg_meta.db" \
    --inventory reg_meta_build/input_data/swecov/derived/flavor_inventory.json \
    --slug-dir reg_meta_build/fqid_slugs/swecov
db="$flav_dir/reg_meta.db"
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db"
zstd -3 -T0 "$db" -o reg_meta_swecov.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_swecov.db.zst
rm -rf "$base_dir" "$flav_dir" reg_meta_swecov.db.zst
```

`extend-db` validates by default (it skips only the code-less↔code-bearing guard, which
the base already passed). After the build, confirm it carries **more** than the eight
global providers — the SWECOV flavor providers raise the `provider` count.

**Maintainer-local inputs**: `reg_meta_build/input_data/swecov/` (holding
`flavor_inventory.json`) is untracked/maintainer-local. If a **fresh** SWECOV build is
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

Verify **all three** assets are present on the draft release — do not publish without
them (#343 for the two `reg-meta update` assets; #1091 for `reg_meta_swecov.db.zst`):

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[].name'
```

### 9. Publish the draft release

This is what fires the publish workflow.

```sh
gh release edit <package>/vX.Y.Z --draft=false
```

### 10. Monitor deployment

If the package has a publish workflow (see table above), find the triggered run, watch
it, and verify PyPI. **Scope the run lookup to this release** — fetch tags first, then
filter by `--event release` and the tag's commit. An unfiltered `--limit 1` can match a
stale completed run, because GitHub may not have queued the new release event yet:

```sh
git fetch --tags origin
target="$(git rev-list -n 1 <package>/vX.Y.Z)"
run_id=""
while [ -z "$run_id" ]; do
  run_id="$(gh run list --workflow=<workflow> --event release --commit "$target" --json databaseId --jq '.[0].databaseId // ""')"
  [ -n "$run_id" ] || sleep 10
done
gh run watch "$run_id" --exit-status
```

The run proceeds unattended (the `pypi` environment review gate was removed 2026-06-10).
Share the run URL with the user for visibility. Then verify the new version is on PyPI
(its `info.version` lags the workflow finishing by a beat):

```sh
for _ in $(seq 1 30); do
  v="$(curl -s https://pypi.org/pypi/<package>/json | python3 -c 'import sys,json; print(json.load(sys.stdin)["info"]["version"])')"
  [ "$v" = "X.Y.Z" ] && break; sleep 10
done
```

**reg_meta post-publish gate:** `publish_reg_meta.yml` calls `integration.yml`
(`workflow_call`) after publishing, running the **release-marked** Docker test
(`test_update_and_query`) against the just-published asset. If the `publish` job is
green but the `integration` job is red, **the publish succeeded** — PyPI and the assets
are fine; the failure is in the release test (e.g. it still calls a CLI flag a refactor
renamed). This test is carved off pre-push and never runs on push/PR, so a CLI-surface
change can strand it silently until this gate. Fix the test on main and re-validate with
`gh workflow run integration.yml --ref main` (then watch that dispatched run). Do
**not** re-release a working package over a stale test.

If the package has no publish workflow, report the release is done after the tag is
created.

### 11. Refresh steward catalogs (reg_meta releases)

reg_meta only, and **after** the release is published and deployment is monitored.
Steward catalogs are built against the reg_meta DB, so a new reg_meta release can drift
any committed `reg_webapp/stewards/<id>/steward.project_data.json` whose
`reg_meta_version` predates the new tag (slug churn, new content, overlap fixes). The
webapp boots through the drift, so this is coverage hygiene — regenerate the stale
catalogs so they track the just-published release.

**Why after publish (not before):** a steward catalog is a `reg_webapp` **deploy**
artifact, not part of the tagged PyPI / DB-asset release. It is **image-affecting** —
`.github/workflows/container-build.yml` watches `reg_webapp/stewards/**`, and the
container bakes the DB asset of the **newest *published*** `reg_meta/v*` release (it
resolves `gh release list … reg_meta/v*`, asset-blind by newest tag). So the catalog
must be generated against the **published** release's shipped asset. Pushing the
regenerated catalog *before* the new release is published would deploy the new catalog
against the **old** baked DB — inconsistent prod (catalog references content the baked
DB lacks) until a later dispatch rebuilds against the new asset. Publishing first, then
refreshing, keeps the deployed catalog and baked DB in lockstep.

Land the regen as its **own commit pushed to `origin/main`** — separate from the
version-bump commit, which was already tagged in step 7. `origin/main` advancing past
the tag is expected and harmless; the catalog ships on the next webapp deploy, and the
staleness alert (`scripts/check_issue_hygiene.py`, keyed on the latest *published*
release) clears once the commit is on `main`.

Loop over **every** `reg_webapp/stewards/*/steward.project_data.json` (do not
special-case any one steward); for each whose `reg_meta_version` is older than the new
`reg_meta/vX.Y.Z`:

- **download the just-published asset** (not a local rebuild) so the catalog matches
  exactly what the container bakes — this is critical for copy-forward releases (8d),
  where a local rebuild could admit FQIDs absent from the shipped DB. Decompress it to
  an uncompressed `reg_meta.db` (`extend-db` opens the base with sqlite, never the
  `.zst`):

  ```sh
  set -euo pipefail
  base_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_base.XXXXXX")"
  gh release download reg_meta/vX.Y.Z --pattern reg_meta.db.zst --dir "$base_dir"
  zstd -d "$base_dir/reg_meta.db.zst" -o "$base_dir/reg_meta.db"
  ```

- `reg-meta-build extend-db --base-db "$base_dir/reg_meta.db" …` to overlay the steward
  providers, then regenerate the catalog per `reg_webapp/stewards/<id>/README.md` so its
  `reg_meta_version` records the published `reg_meta/vX.Y.Z`. **Pass the release tag to
  the generator explicitly** — swecov's `build_catalog.py steward` takes a required
  `--reg-meta-version reg_meta/vX.Y.Z` that stamps that field; older copies defaulted it
  to a fixed tag and **silently downgraded** the stamp (caught in 0.25.0). And when you
  run from a worktree, pass `--out <this-checkout>/reg_webapp/stewards/<id>` — the
  generator's default output dir is its own repo root (the main checkout), not the
  worktree, so the regen would otherwise land outside your release branch;

- review the coverage/binding diff (catalog size, representation pins, co-delivery
  prune) before accepting it;

- commit the regenerated catalog(s) as their own commit, `git push origin HEAD:main`,
  and verify it landed on `origin/main`.

The catalog generator and its confidential inputs are untracked/maintainer-local, so in
a non-maintainer or CI environment they are absent — **skip with a note** when they are.
The staleness alert then tracks the residual drift until a maintainer regenerates.

## Error recovery

- If the commit was pushed but `gh release create` fails: the commit is on main — just
  retry the release creation.
- If the release was created but CI fails **before** PyPI publication: delete the
  release and tag, fix the issue, and start over from the verified bump (step 6).
- If a tag already exists for the target version: a previous attempt went wrong.
  Investigate before proceeding.
- If `build-db` or `build-docs` fails: fix the issue before publishing. The draft
  release exists but `--draft=false` should not run until assets are valid — the CI
  smoke step blocks the publish if the walker can't resolve them.
- If `gh release upload` fails on a draft: retry the upload. The draft and tag are fine.
- If the publish workflow fails because assets weren't on the release at trigger time
  (race between `release: published` and asset upload): re-run the failed job with
  `gh run rerun <run-id> --failed` once assets are uploaded. This is what the step-7
  draft prevents — only relevant when recovering from a prior non-draft release.
- **If PyPI publication already succeeded, the version is immutable** — do not delete
  the release/tag or restart the same version. Fix downstream failures in place (a
  deploy-image failure, a stale post-publish integration test), or cut a new patch
  version if the released package or assets are actually wrong. Before deleting any
  release/tag, verify PyPI does not already list `X.Y.Z`.
- Never force-push or amend commits already on main.
