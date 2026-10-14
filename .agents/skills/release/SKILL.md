---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and release reg_meta (the `reg-meta` crate and its DB
  assets) or reg_meta_build, create package tags/releases, upload reg_meta DB assets, or
  monitor the release workflows. Usage: /release [package] <patch|minor|major>
---

# Release pipeline

**Never start a release unless the user explicitly asks for one.** Nothing goes to PyPI.

Run in a task-owned worktree, from the verified source revision, with one writer
advancing main at a time. A requested dependency upgrade goes through
[upgrade-deps](../upgrade-deps/SKILL.md) first; consume its integrated head. Blocking
packaging or workflow fixes land as separate reviewed commits before the bump; if a
draft or tag already exists, follow Error recovery first.

## Packages

  | Package        | Version files                                                                    | Release workflow                                                                            |
  | -------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
  | reg_meta       | `crates/reg-meta/Cargo.toml` (and `Cargo.lock`)                                  | `publish_reg_meta.yml` on `release: published` (CI, artifact conformance, deploy, binaries) |
  | reg_meta_build | `reg_meta_build/pyproject.toml`, `reg_meta_build/src/reg_meta_build/__init__.py` | none (tag and GitHub release only)                                                          |

A `reg_meta/v*` release carries the three DB assets (step 8) and the `reg-meta` binaries
(step 10). A `reg_meta_build/v*` release is a tag with no assets.

## Validation

1. **Bump level**: one of `patch`, `minor`, `major`. If none is given, stop and ask.

2. **Package(s)**: use the one named, or infer from unreleased commits since each
   package's last `<package>/vX.Y.Z` tag. `reg_meta` sources are `crates/`,
   `reg_webapp/` and `conformance/`; `reg_meta_build` sources are `reg_meta_build/`:

   ```sh
   git fetch --tags origin
   tag="$(git tag --list '<package>/v*' --sort=-v:refname | head -n 1)"
   if [ -n "$tag" ]; then git log --oneline "$tag"..HEAD -- <paths>; else git log --oneline -- <paths>; fi
   ```

   - Several packages changed: release them sequentially, each with its own commit, tag
     and release.
   - **Also compare `reg_meta_build/` since the last `reg_meta/v*` tag**: builder
     content that changes the built DBs (curated TOMLs, `sources/`, `db.py`) needs a
     `reg_meta` release to refresh the asset.
   - Nothing changed: say there is nothing to release.

3. Anything still ambiguous: stop and ask. **A major bump needs explicit confirmation**
   after showing current and planned versions.

## Steps

### 1–2. Version and release notes

Apply the semver bump to the version file's current version. Notes: a brief grouped
bullet list of `git log --oneline <package>/v<current>..HEAD -- <paths>`, no merges, PRs
linked inline; append `(HT @username)` to a non-owner's bullet
(`gh pr list --search "is:merged merged:>=<last tag date>" --json number,author,title`).
**Show the draft notes to the user before proceeding.**

### 3. Bump version

- reg_meta: the `version` line in `crates/reg-meta/Cargo.toml`, then
  `cargo update -p reg-meta --offline` to refresh `Cargo.lock`.
- reg_meta_build: `version` in `reg_meta_build/pyproject.toml` and `__version__` in
  `reg_meta_build/src/reg_meta_build/__init__.py`, then `uv lock`.

**reg_meta only — schema version check.** Each schema has a builder constant and a
reader gate that move together:

  | Schema  | Builder constant                                                      | Rust reader gate                                 |
  | ------- | --------------------------------------------------------------------- | ------------------------------------------------ |
  | catalog | `SCHEMA_VERSION` in `reg_meta_build/src/reg_meta_build/db.py`         | `SCHEMA` in `crates/reg-catalog/src/lib.rs`      |
  | docs    | `DOC_SCHEMA_VERSION` in `reg_meta_build/src/reg_meta_build/doc_db.py` | `DOC_SCHEMA` in `crates/reg-catalog/src/docs.rs` |

Run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/db.py reg_meta_build/src/reg_meta_build/doc_db.py crates/reg-catalog/src/`
and check for DDL changes and reader reads of new tables or columns. A schema change
lands with its bump (major: renamed/removed tables or columns, changed semantics; minor:
new columns the reader reads) in the PR that makes it; if one was missed, stop and land
it through a reviewed PR first. A schema bump forces fresh DB assets in step 8 and
usually a `reg_meta_build` release, which goes first.

### 4. Gate the committed bump

Create step 5's bump commit locally, then run:

```sh
uv run --no-project scripts/gate.py all
```

On the **committed** tree (the builder's clean-tree guard fails strict builds
otherwise); push only when green, and re-run after a rebase. `Operation not permitted`
on `deps/*.d` means a shared `sccache` wrapper: run with
`RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=`.

### 5. Commit and push

The bump commit, `Bump <package> version to X.Y.Z`, contains **only** the version files
(and `Cargo.lock` / `uv.lock`). Push and **verify the bump is `origin/main`** before
tagging:

```sh
git push origin HEAD:main
git fetch origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "bump is not origin/main; resolve before tagging" >&2; exit 1
fi
```

### 6. Create draft GitHub release

`publish_reg_meta.yml` fires on `release: published`, so a reg_meta release stays a
**draft** until all its assets are uploaded. This creates the tag; do not tag
separately:

```sh
target="$(git rev-parse origin/main)"
gh release create <package>/vX.Y.Z --draft --target "$target" --title "<package> vX.Y.Z" --notes-file <notes-file>
```

### 7. (reg_meta_build) Publish and stop

`gh release edit reg_meta_build/vX.Y.Z --draft=false`, and report it done.

### 8. Build and upload release assets (reg_meta only)

**Every release carries all three DB assets before it is published.**
`container-build.yml` bakes the newest `reg_meta/v*` release's `reg_meta.db.zst` and
`reg_meta_docs.db.zst` into the public catalog image and `reg_meta_swecov.db.zst` into
the SWECOV image (`data.swecov.se`); a missing asset breaks that image build. Each asset
is either **built fresh** (8a/8b/8c conditions) or **copied forward** (8d). Never skip
one.

The main build needs a complete accepted prepared root with its exact acceptance commit
and top-level `manifest.json` SHA-256; follow the [build-db skill](../build-db/SKILL.md)
if those are unavailable. Never mix inputs from different checkouts or edit an accepted
root in place.

#### 8a. Main DB asset (`reg_meta.db.zst`)

Build fresh if **any** is true:

- The catalog schema version was bumped since the prior release.
- The release is a **major** bump.
- `git log <prev reg_meta tag>..HEAD -- reg_meta_build/ ':(exclude)reg_meta_build/docs/'`
  is non-empty (builder content changes DB bytes without a schema bump). Even a cosmetic
  hit builds fresh.

Otherwise copy forward (8d). Ship the full global catalog only: no `--registers` subset,
no diagnostic build. A build whose report is not `complete` and `publication_ready` must
not ship:

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

Keep the report and input pins as release evidence; confirm every expected global
provider is present.

#### 8b. Doc DB asset (`reg_meta_docs.db.zst`)

Build fresh if **any** is true:

- The docs schema version was bumped since the prior release.
- `git diff <tag>..HEAD -- reg_meta_build/docs/ reg_meta_build/related_documents.toml`
  is non-empty.
- A gitignored related-document PDF under `reg_meta_build/input_data/SCB/docs/` was
  added, replaced or refetched (git cannot see it; compare `related_document.sha256`
  against the prior asset when in doubt).
- The release is a **major** bump.

Otherwise copy forward (8d). If a new SCB PDF must be converted first, the recipe is the
docstring of `scripts/parse_lisa_docs.py`.

```sh
set -euo pipefail
docs_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_docs.XXXXXX")"
uv run reg-meta-build --db "$docs_dir" build-docs
zstd -3 -T0 "$docs_dir/reg_meta_docs.db" -o reg_meta_docs.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_docs.db.zst
rm -rf "$docs_dir" reg_meta_docs.db.zst
```

#### 8c. SWECOV compiled steward asset (`reg_meta_swecov.db.zst`)

The compiled steward artifact `reg-meta serve --catalog swecov` reads. Runs on **every**
reg_meta release.

Build fresh if **any** is true:

- 8a rebuilt the main DB (the steward artifact is built on top of it).
- `git diff <prev reg_meta tag>..HEAD -- reg_meta_build/fqid_slugs/swecov/` is
  non-empty, or the accepted holdings pins (commit, manifest, overlays, inventory,
  census, policy) changed.
- The release is a **major** bump.
- The immediately-previous release lacks `reg_meta_swecov.db.zst`.

Otherwise copy forward (8d) **only from the immediately-previous release**, after
checking its base generation and holdings pins still match; else rebuild.

The base is **this release's** main asset, fetched: from the draft `reg_meta/vX.Y.Z` if
8a rebuilt it, else from `reg_meta/v<prev>` (8d runs after 8c). The private holdings
candidate and its pins come from the reviewed acceptance evidence: `accepted_holdings`
is the `swecov/` folder of a worktree of `adamaltmejd/registry-catalog-inputs` at
`holdings_commit` (build-db skill, "Select inputs"). After publishing, tag that commit
`swecov/accepted-reg_meta-X.Y.Z` in the inputs repository and push the tag.

```sh
set -euo pipefail
main_src="reg_meta/vX.Y.Z"   # or reg_meta/v<prev> when 8a copies forward
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
# extend-db writes WAL; ship a self-contained single file.
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db"
zstd -3 -T0 "$db" -o reg_meta_swecov.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_swecov.db.zst
rm -rf "$base_dir" "$flav_dir" reg_meta_swecov.db.zst
```

Before uploading, check the manifest and report: a complete, publishable `steward`
artifact naming `swecov`, with the expected base generation, pins and builder commit.
The holdings candidate is maintainer-local: if a fresh build is required and it is
absent, **stop and do not publish**; ask the maintainer to upload the asset.

#### 8d. Copy-forward for assets not rebuilt

`<prev>` is the newest `reg_meta/v*` release carrying the asset (check with
`gh release view reg_meta/v<prev> --json assets`). Run from the repo root:

```sh
set -euo pipefail
asset="<asset-name>"   # reg_meta.db.zst, reg_meta_docs.db.zst, or reg_meta_swecov.db.zst
cf_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_cf.XXXXXX")"
gh release download reg_meta/v<prev> --pattern "$asset" --clobber --dir "$cf_dir"
gh release upload reg_meta/vX.Y.Z "$cf_dir/$asset"
rm -rf "$cf_dir"
```

#### 8e. Verify before publishing

All three assets on the draft, each with one SHA-256 digest:

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[] | [.name, .digest] | @tsv'
```

Then run the release-tier artifact conformance on each new catalog (global and SWECOV),
downloaded from the draft and decompressed into its own directory, with the conformance
command the `scripts/gate.py` `release` step runs, pointed at that directory
(`--run-release --artifact-dir=... --server-cmd=...`; `cargo build --workspace` first).
A red run stops publication: fix the asset and re-run.

### 9. Publish the draft release

This fires `publish_reg_meta.yml`:

```sh
gh release edit reg_meta/vX.Y.Z --draft=false
```

### 10. Monitor the release workflow

`publish_reg_meta.yml` runs CI on the tag, artifact conformance on this release's assets
(`integration.yml`), and dispatches `container-build.yml`, which bakes and deploys them.
Scope the run lookup to this release; an unfiltered `--limit 1` can match a stale run:

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

Share the run URL, then watch the dispatched `container-build.yml` through its deploys.
A red `integration` job: upload a missing asset (step 8); a wrong or duplicated asset,
or failing artifact conformance, means a fixed build and a new patch release.
Re-validate with `gh workflow run integration.yml --ref main`. Do **not** re-release a
working version over a stale check.

**Binaries.** The `binaries` job uploads `reg-meta-aarch64-apple-darwin`,
`reg-meta-x86_64-unknown-linux-gnu` and a `.sha256` for each. Confirm all four:

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[].name' | grep '^reg-meta-'
```

If one failed, fix the cause on main and upload by hand from the release commit:
`cargo build --release --locked -p reg-meta`, copy to `reg-meta-<target>`,
`shasum -a 256 reg-meta-<target> > reg-meta-<target>.sha256`, then
`gh release upload reg_meta/vX.Y.Z reg-meta-<target> reg-meta-<target>.sha256`.
`gh workflow run publish_reg_meta.yml --ref <branch>` tests the job without a release.

### 11. Verify the deployed artifacts

`GET /api/context` on `catalog.swecov.se` and `data.swecov.se` must report the
generation recorded in step 8, with default scope `reference` (catalog) and `holdings`
(steward); on the catalog, `GET /api/catalog?scope=holdings` must refuse with
`scope_unavailable`. Spot-check a scoped catalog or search read and a known order, and
record what the checks exercised. Never regenerate or commit an inventory after
publication.

### 12. Re-pin G1 to the new tag

Open a PR moving the pin in `conformance/differential/config.toml` to `reg_meta/vX.Y.Z`
(tag and asset digests from `gh release view reg_meta/vX.Y.Z --json assets`), then run
`uv run --no-project scripts/gate.py g1` on the committed tree. Report any difference
beyond the standing exceptions instead of adding one (`conformance/README.md`, "The
pin").

## Error recovery

- Commit pushed but `gh release create` failed, or an upload to a draft failed: retry.
- Draft exists and something fails before publication: delete the draft and tag, fix,
  and restart from the verified bump (step 5).
- Source changed after draft/tag creation: recreate release and tag from the repaired
  `origin/main` (steps 5–6), refresh the notes, replace invalidated assets and re-run
  the checks at that revision.
- Tag already exists for the target version: investigate before proceeding.
- `build-db` or `build-docs` fails: fix it; never publish without all three assets.
- **Once published, never delete, re-cut or re-upload a release's assets.** Deploys and
  the G1 pin reference the tag and digests. Fix downstream failures in place, or cut a
  new patch version.
- Never force-push or amend commits already on main.
