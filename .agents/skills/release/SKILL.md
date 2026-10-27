---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and release reg_meta (the `reg-meta` crate and its DB
  asset) or reg_meta_build, create package tags/releases, upload the reg_meta DB asset,
  or monitor the release workflows. Usage: /release [package] (patch|minor|major)
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

A `reg_meta/v*` release carries one DB asset, `reg_meta.db.zst` (step 8) and the
`reg-meta` binaries (step 10). A `reg_meta_build/v*` release is a tag with no assets.

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
   - **Also compare the builder and inputs since the last `reg_meta/v*` tag**: catalog
     content or schema changes under `crates/reg-build/`, `crates/reg-resolve/`,
     `crates/reg-readers/`, `crates/reg-frame/`, `curation/` (including the inputs pin)
     need a `reg_meta` release to refresh the asset.
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

**reg_meta only — schema version check.** Compare the DDL and the single version
authority with the previous release:

```sh
git diff <prev-tag>..HEAD -- crates/reg-build/src/schema.sql crates/reg-core/src/lib.rs
```

If `schema.sql` changed, `reg_core::CATALOG_SCHEMA_VERSION` must have changed in the
same release, and vice versa. The reader admits on exact equality, without a major/minor
compatibility rule. If a paired change is missing, stop and land it through a reviewed
PR first. A bump's PR and subsequent main pushes are expected to be red at
`schema-guard` until the matching release publishes; that check is not required on main.
Merge first, then release when the maintainer runs it. A gap is allowed; the guard
blocks deployment throughout it.

### 4. Gate the committed bump

Create step 5's bump commit locally, then run:

```sh
uv run --no-project scripts/gate.py all
```

Run on the **committed** tree so checks and release evidence name one revision; push
only when green, and re-run after integrating source changes. `Operation not permitted`
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

Every release builds one fresh asset, `reg_meta.db.zst`, with documents inside the
catalog. There is no separate docs asset and no copying an asset from an older release.

**C-13 supersedes the old standing requirement until E:** no `reg_meta_swecov.db.zst` is
produced; SWECOV's hosted app stays on its last old-schema image, untouched, with no
build, integration or deploy path (`fly.swecov.toml` and its image, deploy and edge jobs
are deleted). Local binaries released after the switch cannot read SWECOV's old DB;
SWECOV users retain the last old binary until E. Include this in the release notes.

#### 8a. Build the catalog

Use an inputs worktree at the exact repository and commit in `curation/inputs.toml`.
Check its HEAD and clean status before building; never edit the pinned inputs in place
or mix files from different revisions. For every input with a `release` field, fetch its
pinned asset beforehand to `<assets>/<release>/<path>`:

```sh
gh release download <input-release> --repo <inputs-repo> --pattern <input-path> --dir <assets>/<input-release>
```

The reader verifies the manifest's SHA-256. Keep the source revision, inputs commit,
manifest, build report and subsequent check outputs in the release evidence directory.
Use the full manifest: no register subset or diagnostic artifact.

```sh
set -euo pipefail
inputs=/absolute/path/to/pinned-inputs-worktree
assets=/absolute/path/to/pinned-release-assets
evidence=/absolute/path/to/release-evidence
version=X.Y.Z
mkdir -p "$evidence/catalog"
cargo build --workspace --locked
target/debug/reg-build build --inputs "$inputs" --assets "$assets" \
  --curation curation --out "$evidence/catalog/reg_meta.db" \
  --catalog-version "$version" --report "$evidence/build-report.json"
```

`--out` is a database filename, not a directory. **The switch release and every release
until D's minting omit `--release`** (C-14): unminted variables publish under proposed
ids, which may change in D. D restores the refusal; after minting use `--release`.
Record the build report's total unminted count in the release notes:

```sh
uv run python -c 'import json,sys; r=json.load(open(sys.argv[1])); print(sum(len(v["unminted"]) for v in r["registers"].values()))' "$evidence/build-report.json"
```

Read the report's warnings and confirm all expected providers and inputs are present.

#### 8b. Verify the artifact

For every release after the switch, download the immediately previous published
release's `reg_meta.db.zst`, verify its compressed SHA-256 against GitHub's asset
digest, and decompress it into a separate evidence directory. Never substitute an older
release or a synthetic artifact. Run the single validation authority:

```sh
target/debug/reg-build verify "$evidence/catalog/reg_meta.db" --previous <previous-release-dir>/reg_meta.db
```

The switch release has no v4 predecessor: run `verify` without `--previous` and state
that exception in its notes. From the next release on, `--previous` is required. A
verification failure stops publication. The current verifier requires the previous DB to
have the same schema version; a future schema bump that makes it incomparable needs a
reviewed procedure change before release, not an unrecorded skip.

#### 8c. Review the catalog diff

Except for the switch release, compare with that same previous DB and retain the full
JSON output in the evidence directory:

```sh
target/debug/reg-build diff <previous-release-dir>/reg_meta.db "$evidence/catalog/reg_meta.db"
target/debug/reg-build diff <previous-release-dir>/reg_meta.db "$evidence/catalog/reg_meta.db" --json > "$evidence/catalog-diff.json"
```

Exit 0 means no changes, 1 means differences to review, and 2 means an error that stops
the release. Handle exit 1 explicitly if running under `set -e`. The builder agent reads
the diff, confirms only intended changes, and puts its summary in the release notes. For
the switch release, skip this comparison because there is no v4 predecessor; record
`x-compare-orders` and `x-real-conformance` as its evidence instead.

#### 8d. Run conformance against the new artifact

Use the release tier's command from `scripts/gate.py`, pointed at the new catalog:

```sh
uv run python -m pytest conformance -q -n auto -m release --run-release \
  --artifact-dir="$evidence/catalog" \
  --server-cmd='target/debug/reg-meta serve --db {db} --port {port} --write-limit 100000'
```

`gate.py release` itself selects a synthetic artifact and accepts no artifact override;
that run does not validate the release DB. A red artifact run stops publication.

#### 8e. Compress, checksum and upload to the draft

After verification, diff review and conformance pass:

```sh
zstd -19 "$evidence/catalog/reg_meta.db" -o "$evidence/reg_meta.db.zst"
shasum -a 256 "$evidence/reg_meta.db.zst" > "$evidence/reg_meta.db.zst.sha256"
gh release upload reg_meta/vX.Y.Z "$evidence/reg_meta.db.zst"
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[] | [.name, .digest] | @tsv'
```

Require exactly one catalog asset on the draft with a SHA-256 digest matching the local
compressed file. Keep the checksum with the evidence; the binaries and their checksums
are uploaded by step 10. Remove disposable downloads after the release, keeping the
evidence.

### 9. Publish the draft release

This fires `publish_reg_meta.yml`:

```sh
gh release edit reg_meta/vX.Y.Z --draft=false
```

### 10. Monitor the release workflow

`publish_reg_meta.yml` runs CI on the tag, global-only artifact conformance
(`integration.yml`), and dispatches `container-build.yml`, which bakes and deploys the
catalog. Integration and deployment dispatch both depend on CI, not on each other. The
deploy guard passes only when the released `CATALOG_SCHEMA_VERSION` equals main's; a
missing constant or a mismatch blocks deployment. Scope the run lookup to this release;
an unfiltered `--limit 1` can match a stale run:

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

### 11. Verify the deployed artifact

The global catalog has no public `/api`. Over `https://reg-meta.vqv.uk/mcp`, call `show`
with no `ref`: it must answer and its `meta.catalog_version` must equal the version
passed in step 8a. Compare `meta.generation` with the build report's `generation_id`.
Record what the checks exercised. SWECOV is untouched until E; do not deploy it.

## The switch release

The switch release follows the merge of `rebuild-c` into main when the maintainer runs
it; no immediate release is required. Main is expected to be red at `schema-guard` until
it publishes, so deployment is blocked during the gap. A few hours of disruption at the
switch are accepted. The maintainer must first merge the inputs repository's
`rebuild-c-docs` branch into its main; confirm that the pin in `curation/inputs.toml`
contains those docs. The minor-or-major bump is decided on the maintainer's sheet, not
by this skill. Notes name the unminted count and proposed-id caveat, the missing v4
predecessor and substitute evidence, and the SWECOV old-binary requirement.

## Error recovery

- Commit pushed but `gh release create` failed, or an upload to a draft failed: retry.
- Draft exists and something fails before publication: delete the draft and tag, fix,
  and restart from the verified bump (step 5).
- Source changed after draft/tag creation: recreate release and tag from the repaired
  `origin/main` (steps 5--6), refresh the notes, replace invalidated assets and re-run
  the checks at that revision.
- Tag already exists for the target version: investigate before proceeding.
- A `reg-build` build, verification or conformance failure stops the release: fix it,
  rebuild with `reg-build`, and repeat steps 8b--8e; never copy an older asset forward.
- **Once published, never delete, re-cut or re-upload a release's assets.** Deploys and
  release consumers reference the tag and digests. Fix downstream failures in place, or
  cut a new patch version.
- Never force-push or amend commits already on main.
