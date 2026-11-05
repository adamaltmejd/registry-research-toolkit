---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and release reg_meta (the `reg-meta` crate and its paired
  DB assets), create the tag and release, upload the reg_meta DB assets, or monitor the
  release workflows. Usage: /release [reg_meta] (patch|minor|major)
---

# Release pipeline

**Never start a release unless the user explicitly asks for one.**

Run in a task-owned worktree, from the verified source revision, with one writer
advancing main at a time. A requested dependency upgrade goes through
[upgrade-deps](../upgrade-deps/SKILL.md) first; consume its integrated head. Blocking
packaging or workflow fixes land as separate reviewed commits before the bump; if a
draft or tag already exists, follow Error recovery first.

## Packages

  | Package  | Version files                                   | Release workflow                                                                            |
  | -------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------- |
  | reg_meta | `crates/reg-meta/Cargo.toml` (and `Cargo.lock`) | `publish_reg_meta.yml` on `release: published` (CI, artifact conformance, deploy, binaries) |

A `reg_meta/v*` release carries two DB assets, `reg_meta.db.zst` and
`reg_meta_swecov.db.zst` (step 8), and the `reg-meta` and `reg-export-gate` binaries
(step 10; the gate's is named by its own crate version).

## Validation

1. **Bump level**: one of `patch`, `minor`, `major`. If none is given, stop and ask.

2. **Package**: `reg_meta`; check for unreleased commits since its last
   `<package>/vX.Y.Z` tag. `reg_meta` sources are `crates/`, `reg_webapp/` and
   `conformance/`:

   ```sh
   git fetch --tags origin
   tag="$(git tag --list '<package>/v*' --sort=-v:refname | head -n 1)"
   if [ -n "$tag" ]; then git log --oneline "$tag"..HEAD -- <paths>; else git log --oneline -- <paths>; fi
   ```

   - **Also compare the builder and inputs since the last `reg_meta/v*` tag**: catalog
     content or schema changes under `crates/reg-build/`, `crates/reg-resolve/`,
     `crates/reg-readers/`, `crates/reg-frame/` and `curation/` (including the inputs
     pin) need a `reg_meta` release to refresh the assets. Steward repository commits do
     not: a steward builds from its own checkout.
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

The `version` line in `crates/reg-meta/Cargo.toml`, then
`cargo update -p reg-meta --offline` to refresh `Cargo.lock`.

The export gate too: bump `version` in `crates/reg-export-gate/Cargo.toml` (then
`cargo update -p reg-export-gate --offline`) when its sources or its `Cargo.lock`
dependency closure changed since the previous `reg_meta/v*`
(`cargo tree -p reg-export-gate` against that tag's lock). Its asset is named by that
version, receipts record it and `export` relies on byte identity per version, so two
different binaries must never ship under one version. Unchanged: leave it, and the
release ships the same version again.

**Schema version check.** Compare the DDL and the single version authority with the
previous release:

```sh
git diff <prev-tag>..HEAD -- crates/reg-build/src/schema.sql crates/reg-build/src/holdings/schema.sql crates/reg-core/src/lib.rs
```

If `schema.sql` changed, `reg_core::CATALOG_SCHEMA_VERSION` must have changed in the
same release, and vice versa. **The holdings DDL pairs the same way:**
`holdings/schema.sql` and `reg_core::HOLDINGS_SCHEMA_VERSION` change together. The
reader admits on exact equality, without a major/minor compatibility rule. If a paired
change is missing, stop and land it through a reviewed PR first. A bump's PR and
subsequent main pushes are expected to be red at `schema-guard` (which compares both
`CATALOG_SCHEMA_VERSION` and `HOLDINGS_SCHEMA_VERSION`) until the matching release
publishes; that check is not required on main. Merge first, then release when the
maintainer runs it. A gap is allowed; the guard blocks deployment throughout it.

### 4. Gate the committed bump in CI

The bump commit, `Bump <package> version to X.Y.Z`, contains **only** the version files
(and `Cargo.lock`). Commit it on `origin/main` and push it to a PR branch; CI is the
gate, so there is no local `gate.sh all`. `ci.yml` runs every step `gate.sh all` runs
(lint, the scripts suite, `rust` with the synthetic release admission, and, because the
`webapp` filter matches `Cargo.toml`, the frontend and flows jobs):

```sh
git push origin HEAD:refs/heads/release/<package>-vX.Y.Z
gh pr create --base main --head release/<package>-vX.Y.Z \
  --title "Bump <package> version to X.Y.Z" --body "Release bump; CI gates it."
```

Do not wait: start the reg_meta base build (8a) at this commit while CI runs. CI and the
build then name the same revision.

### 5. Fast-forward main

Once `ci.yml` is green on the bump (only `ci.yml`: a schema bump's PR is red at
`container-build.yml`'s `schema-guard` until the release publishes, step 3),
fast-forward `main` to the bump (GitHub then shows the PR as merged) and **verify the
bump is `origin/main`** before tagging:

```sh
run=$(gh run list --workflow ci.yml --commit "$(git rev-parse HEAD)" --limit 1 \
  --json databaseId -q '.[0].databaseId')
gh run watch "$run" --exit-status || { echo "ci.yml is not green on the bump" >&2; exit 1; }
git push origin HEAD:main
git fetch origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  echo "bump is not origin/main; resolve before tagging" >&2; exit 1
fi
```

`git push` refuses a non-fast-forward. If `main` moved meanwhile, rebase the bump onto
it, push the branch again and wait for CI on the new commit. The base build made at the
old bump commit is then reused or rebuilt by the rule in 8a.

### 6. Create draft GitHub release

`publish_reg_meta.yml` fires on `release: published`, so a reg_meta release stays a
**draft** until all its assets are uploaded. This creates the tag; do not tag
separately:

```sh
target="$(git rev-parse origin/main)"
gh release create <package>/vX.Y.Z --draft --target "$target" --title "<package> vX.Y.Z" --notes-file <notes-file>
```

### 8. Build and upload release assets

There is no step 7. The numbers stay because `cargo xtask release-base-reuse` and each
evidence directory's `passed` marks name steps 8a–8c.

Every release builds a **pair**: `reg_meta.db.zst` (the reference catalog, documents
inside) and `reg_meta_swecov.db.zst` (the SWECOV holdings: the catalog schema's
supplemental part plus the `holding_*` tables). There is no separate docs asset and no
copying an asset from an older release. Both are built during this release, normally at
the release commit; the one exception is reusing this release's own earlier evidence
build when the builder's inputs have not changed (8a, "Reusing an earlier build").

- Holdings pin the base's `catalog_sha256`, so **every reg_meta release builds holdings
  against the base it ships**, even when nothing in the steward repository changed. Any
  rebuild of the base invalidates the holdings: repeat from 8d.
- A holdings or pair failure stops the release. A global-only release (no
  `reg_meta_swecov.db.zst`) needs the maintainer's explicit choice, recorded in the
  notes. Its consequence: `swecov-asset-guard` fails, SWECOV is not rebuilt and stays on
  the previous pair, and the commit still counts as deployed (step 10).
- The scope check (8h) is an automated gate: the holdings asset is public and carries
  only what the catalog serves.

Variables used below (absolute paths, as in 8a):

```sh
steward=/absolute/path/to/swecov-steward-checkout   # a new path: 8d clones into it
prev=/absolute/path/to/previous-release-dir          # reg_meta.db, decompressed (8b)
```

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
Use the full manifest: no register subset or diagnostic artifact. Build optimized
(`target/release`): the real build is long and the verifiers and conformance run the
same binaries. `Operation not permitted` on `deps/*.d` means a shared `sccache` wrapper:
run with `RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=`.

```sh
set -euo pipefail
inputs=/absolute/path/to/pinned-inputs-worktree
assets=/absolute/path/to/pinned-release-assets
evidence=/absolute/path/to/release-evidence
version=X.Y.Z
mkdir "$evidence"  # a new directory per build: refuses an existing one
mkdir "$evidence/catalog"
test -z "$(git status --porcelain)"
git rev-parse HEAD > "$evidence/source-revision"
rustc -vV > "$evidence/rustc.txt"
cargo build --release --workspace --locked
args=(build --inputs "$inputs" --assets "$assets" \
  --curation curation --out "$evidence/catalog/reg_meta.db" \
  --catalog-version "$version" --report "$evidence/build-report.json" --release)
printf '%s\n' "${args[@]}" > "$evidence/build-args.txt"
target/release/reg-build "${args[@]}"
shasum -a 256 "$evidence/catalog/reg_meta.db" > "$evidence/base.sha256"
base_sha=$(cut -d' ' -f1 "$evidence/base.sha256")
```

`source-revision`, `rustc.txt`, `build-args.txt` and `base.sha256` are the records a
later reuse is checked against (below). Every build gets a new evidence directory, so an
interrupted build leaves no `base.sha256` and an older one's records and pass marks
never describe a newer base. `--out` is a database filename, not a directory.
`--release` refuses unplaced or split ledger ids and variables without a ledger id. Read
the report's warnings and confirm all expected providers and inputs are present.
`base_sha` is the digest every later step re-checks: the holdings pin it, and the base
uploaded in 8i must still equal it.

**Reusing an earlier build** (maintainer ruling 2026-11-04). When the release commit is
not the commit an evidence build of this release ran at (`main` moved under the bump, a
re-pin, a reader-only fix), do not rebuild by default. From a clean checkout at the
release commit, run:

```sh
cargo xtask release-base-reuse "$earlier_evidence"
```

It diffs the two commits over the builder's dependency closure, read from
`cargo metadata` (the workspace crates `reg-build` depends on), every workspace member's
`Cargo.toml` (a `--workspace` build unifies features), plus `curation/` (including the
inputs pin), `Cargo.toml`, `Cargo.lock`, `.cargo/` and any `rust-toolchain*`. It also
checks the recorded toolchain, `--release`, `--catalog-version` and that the base still
hashes to `base.sha256`. A missing or inconsistent record fails closed (exit 2). Follow
what it prints:

- `base: rebuild` (exit 1 or 2): build again at the release commit, from 8a.
- `base: reuse` with exit 0 (a later error can follow a printed `reuse`; any non-zero
  exit means rebuild): keep `$earlier_evidence` as `$evidence` and set
  `base_sha=$(cut -d' ' -f1 "$evidence/base.sha256")`.
- A step carries over only when `$evidence/passed` lists it; otherwise the command says
  `run it`. 8b and 8c append `<step> <base sha>` to `passed` once they pass, so a build
  whose checks never finished carries nothing over, and a mark made against another base
  does not count.
- Always run 8d-8h and the 8i `base_sha` check at the release commit: the holdings build
  from the steward checkout, which no toolkit file pins, so an earlier pair never
  carries over.

The release notes record both commits, the built one and the release one, and the
command's output, including when the built commit is a squash-merged PR head that is not
on `main`. Merging reader-only fixes before the bump avoids the question altogether.

#### 8b. Verify the base

Download the immediately previous published release's `reg_meta.db.zst`, verify its
compressed SHA-256 against GitHub's asset digest, and decompress it into a separate
directory (`$prev`). Never substitute an older release or a synthetic artifact. Run the
single validation authority:

```sh
target/release/reg-build verify "$evidence/catalog/reg_meta.db" --previous "$prev/reg_meta.db" > "$evidence/catalog-verify.txt"
```

Retain stdout in `catalog-verify.txt` beside `catalog-diff.json`, including the pre-v1
id-change report when the previous release is pre-v1. `--previous` is required. A
verification failure stops publication. The verifier compares across a catalog schema
bump: the previous DB's schema version must be present and no newer than the current
one, and when it is older stdout carries a
`cross-schema comparison: previous schema N, current schema M` line that the evidence
keeps. If the id queries cannot run on the older schema (a dropped table or column),
verify refuses with `previous release: schema N is not comparable: <sqlite error>`; that
refusal needs a reviewed procedure change before release, not an unrecorded skip.

#### 8c. Review the catalog diff

Compare with that same previous DB and retain the full JSON output in the evidence
directory:

```sh
target/release/reg-build diff "$prev/reg_meta.db" "$evidence/catalog/reg_meta.db"
target/release/reg-build diff "$prev/reg_meta.db" "$evidence/catalog/reg_meta.db" --json > "$evidence/catalog-diff.json"
```

Exit 0 means no changes, 1 means differences to review, and 2 means an error that stops
the release. Handle exit 1 explicitly if running under `set -e`. Across a catalog schema
bump the diff accepts a previous DB on an older schema (never a newer one): the text
starts with `cross-schema comparison: previous schema N, current schema M` (JSON
`cross_schema`), and a `schema changes` section (JSON `schema_changes`) names each table
and column on one side only, without diffing their rows. Review that section against the
bump's PR. A query the older schema cannot answer exits 2 with
`schema N is not comparable: <sqlite error>`, which needs the same reviewed procedure
change as 8b. The builder agent reads the diff, confirms only intended changes, and puts
its summary in the release notes. After 8b passes, and again after the diff review,
record each step: `echo "8b $base_sha" >> "$evidence/passed"`,
`echo "8c $base_sha" >> "$evidence/passed"`.

#### 8d. Compile the holdings

The pin is the steward checkout itself: a fresh clone of swecov-steward at `main`. The
compiler records its `origin` URL, `HEAD` commit and `--config` path in the pair, and
refuses a dirty tree, untracked files included, with `holdings_pin_invalid`. Never edit
the steward checkout. The steward's re-lock against this release's base lands on
swecov-steward `main` in a reviewed PR before the bump, not during the release.

```sh
git clone git@github.com:adamaltmejd/swecov-steward.git "$steward"
git -C "$steward" rev-parse HEAD > "$evidence/steward-revision"
target/release/reg-build holdings --base "$evidence/catalog/reg_meta.db" --steward swecov \
  --steward-repo "$steward" --config steward.toml \
  --out "$evidence/catalog/reg_meta_swecov.db" --report "$evidence/holdings-report.json" \
  2> "$evidence/holdings.log"
```

`holdings` re-verifies the base, builds in `.reg_meta_swecov.db.holdings-scratch` and
`.reg_meta_swecov.db.holdings-staged` beside `--out`, runs the pair verifier on the
staged file and renames it into place only on success; it refuses when either scratch
file already exists. Its stderr ends with one summary line
(`holdings: tables=… columns=… mapped=… unmatched=… storage_extras=…`); the release
notes record that line, the steward pin and the comparison counts (8g). The report's
`steward` object (repo, commit, config and every input pin) is the build's input
provenance; the published DB carries only the steward pin and one input-manifest digest.
The report and log are private evidence: do not paste their contents anywhere public.

#### 8e. Verify the pair

```sh
target/release/reg-build verify "$evidence/catalog/reg_meta_swecov.db" \
  --catalog "$evidence/catalog/reg_meta.db" --steward-repo "$steward" > "$evidence/pair-verify.txt"
sqlite3 -readonly "$evidence/catalog/reg_meta_swecov.db" "SELECT value FROM meta WHERE key='catalog_sha256'"
```

The pair verifier takes `--catalog` and `--steward-repo` and no `--previous`. The `meta`
value must equal `$base_sha`. A failure stops publication.

#### 8f. Run conformance against the pair

Run the release checks (`crates/reg-conformance`, `--test release`) on the new catalog
directory with the release server, first for the global catalog and then for the pair.
`REG_CATALOG=swecov` makes the server admit `reg_meta_swecov.db` beside `reg_meta.db`
(`--catalog swecov`) and adds the held-only sweep:

```sh
export REG_META_BIN=target/release/reg-meta REG_ARTIFACT_DIR="$evidence/catalog"
cargo test -p reg-conformance --test release --locked -- --nocapture
REG_CATALOG=swecov cargo test -p reg-conformance --test release --locked -- --nocapture
```

`gate.sh rust` runs the same target on a synthetic artifact; that run does not validate
the release DB. A red artifact run stops publication; a failure only in the pair run
stops it too. Do not change tests to get a pass. If pair admission times out at server
start, raise `SERVER_READY_SECONDS` (120) in
`crates/reg-conformance/tests/common/server.rs` through a reviewed PR, not in place.

Reading the pair run: the order census reads `reg_meta.db` and the holdings file
directly, and the server serves both. On the pair it samples the reference tables the
holdings place something in and binds only the variables placed there. It reads the
holdings file's own segments beside the catalog's (steward attestation). It counts a row
key only when the holdings place it in the table, since a steward takes its row keys
from the holdings (`ops/order/rows.rs`). Each run prints
`release order: N tables sampled, M blocked`; record both lines in the notes. A sample
where most tables are blocked covers little: report it to the maintainer, do not wave it
through. Any census/server disagreement fails the run and stops the release for the
maintainer's call. Never adjust the census, or its row-key approximation, to get a pass.

#### 8g. Compare the holdings census with the previous paired release

The baseline (`$old`) is always the previous paired release's published
`reg_meta_swecov.db`, what users had: download its `reg_meta_swecov.db.zst` to a
separate directory, verify its compressed SHA-256 against GitHub's asset digest and
decompress it. Never recompile an old steward pin. `reg-build diff --holdings` accepts a
baseline on an older holdings schema, never a newer one; it refuses a missing or
non-numeric schema, or a `PRAGMA user_version` that differs from the `schema_version`
meta key, and names both sides. Across a schema bump the text starts with
`cross-schema comparison: previous schema N, current schema M` (JSON `cross_schema`) and
a `schema changes` section (JSON `schema_changes`) counts the version change and names
each table and column on one side only, without diffing their rows; a query the older
schema cannot answer exits 2 with `holdings schema N is not comparable: <sqlite error>`.
It reports census placement per column (placed, held, unplaced, and the changed
placements by old to new `table_pid`, classed newly placed, folded or renamed, variable
or interval changed, or unmapped), reader-visible text changes by kind (keys, never
bodies), and finding and holdings-notice deltas by code. The JSON goes to the evidence
directory only.

```sh
target/release/reg-build diff --holdings "$old/reg_meta_swecov.db" \
  "$evidence/catalog/reg_meta_swecov.db" > "$evidence/holdings-compare.txt" || [ $? -eq 1 ]
target/release/reg-build diff --holdings "$old/reg_meta_swecov.db" \
  "$evidence/catalog/reg_meta_swecov.db" --json > "$evidence/holdings-compare.json" || [ $? -eq 1 ]
```

Exit 1 only means the two differ (`|| [ $? -eq 1 ]` keeps `set -e` from stopping there);
exit 2 is an error and stops the release. The maintainer reads that summary for
go/no-go; its counts go in the release notes.

#### 8h. Scope check

The SWECOV catalog is public (maintainer 2026-11-03), so there is no disclosure review.
The release must carry only what the catalog serves, never background data: steward
repository notes, raw census or storage exports, inventory working files.
`cargo xtask scope-check` fails when the holdings DB's schema differs from the schema
the builder declares (every table, FTS shadow tables included, and every column must
match, none extra or missing), when a meta key is not on the check's allowlist, when
`occurrence_locator` has rows, when a text or blob value exceeds its size bound, or when
a `text` body names a steward working file (`*.json`, `*.jsonl`, `*.toml`) or a census
row (`census:row=`); the last needs a steward-repo fix and re-pin, not a check change.
Run it from this release checkout (it compiles in the builder's DDL at that commit) and
record its one-line result in the release notes:

```sh
set -o pipefail
cargo xtask scope-check "$evidence/catalog/reg_meta_swecov.db" | tee "$evidence/scope-check.txt"
```

A failure stops the release. Fix the build or, when the item belongs in the paired
catalog, add its meta key or size bound to `crates/xtask/src/scope_check.rs` in a
reviewed PR; then rebuild from 8d. Then re-check `$base_sha` against the local base file
(it must be unchanged since 8a) and proceed.

#### 8i. Compress, checksum and upload both assets to the draft

After verification, diff review, conformance and the scope check:

```sh
[ "$(shasum -a 256 "$evidence/catalog/reg_meta.db" | cut -d' ' -f1)" = "$base_sha" ]
zstd -19 "$evidence/catalog/reg_meta.db" -o "$evidence/reg_meta.db.zst"
zstd -19 "$evidence/catalog/reg_meta_swecov.db" -o "$evidence/reg_meta_swecov.db.zst"
shasum -a 256 "$evidence/reg_meta.db.zst" "$evidence/reg_meta_swecov.db.zst" > "$evidence/assets.sha256"
gh release upload reg_meta/vX.Y.Z "$evidence/reg_meta.db.zst" "$evidence/reg_meta_swecov.db.zst"
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[] | [.name, .digest] | @tsv'
```

Admission: the draft must hold exactly those two DB assets, each with a SHA-256 digest
equal to the local compressed file (`assets.sha256`). Only then publish (step 9). Keep
the checksums with the evidence; the binaries and their checksums are uploaded by step 10.
Remove disposable downloads after the release, keeping the evidence.

**Publish ordering.** Publishing a release fires `publish_reg_meta.yml`, which
dispatches `container-build.yml`. That workflow resolves the release's assets when it
runs: a holdings upload that lands after publication makes `swecov-asset-guard` skip
SWECOV while the commit counts as deployed. Both assets are therefore on the draft
before step 9, and **a published release's assets are never re-uploaded**; a missing or
wrong asset after publication is a new patch release carrying both.

### 9. Publish the draft release

Only after 8i's admission shows both DB assets. This fires `publish_reg_meta.yml`:

```sh
gh release edit reg_meta/vX.Y.Z --draft=false
```

### 10. Monitor the release workflow

`publish_reg_meta.yml` checks that `ci.yml` is green on the tagged commit (the bump's CI
from step 4, not a re-run), then runs artifact conformance (`integration.yml`: the
global asset, and the paired SWECOV asset when the release has one), and dispatches
`container-build.yml`, which bakes and deploys the catalog (and the pair). Integration
and deployment dispatch both depend on that check, not on each other. The deploy guard
passes only when the released `CATALOG_SCHEMA_VERSION` and `HOLDINGS_SCHEMA_VERSION`
both equal main's; a missing constant or a mismatch blocks deployment. Scope the run
lookup to this release; an unfiltered `--limit 1` can match a stale run:

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
A red `integration` job, a missing or wrong or duplicated asset, or failing artifact
conformance means a fixed build and a new patch release carrying both assets: a
published release's assets are never re-uploaded (step 8i, publish ordering).
Re-validate a transient failure with `gh workflow run integration.yml --ref main`. Do
**not** re-release a working version over a stale check.

Every release redeploys SWECOV with global.

**Binaries.** The `binaries` job uploads, per target (`aarch64-apple-darwin` and
`x86_64-unknown-linux-gnu`), `reg-meta-<target>` and
`reg-export-gate-<gate version>-<target>`, each with a `.sha256`. Confirm all eight:

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[].name' |
  grep -E '^reg-(meta|export-gate)-'
```

If one failed, fix the cause on main and upload by hand from the release commit:
`cargo build --release --locked -p reg-meta -p reg-export-gate`, copy to
`reg-meta-<target>` and `reg-export-gate-<gate version>-<target>` (the version from
`cargo pkgid -p reg-export-gate`), `shasum -a 256 <asset> > <asset>.sha256` for each,
then `gh release upload reg_meta/vX.Y.Z <asset> <asset>.sha256`.
`gh workflow run publish_reg_meta.yml --ref <branch>` tests the job without a release.

### 11. Verify the deployed artifact

The global catalog has no public `/api`. Over `https://reg-meta.vqv.uk/mcp`, call `show`
with no `ref`: it must answer and its `meta.catalog_version` must equal the version
passed in step 8a. Compare `meta.generation` with the build report's `generation_id`.
Then SWECOV, once its deploy jobs are green: the public
`GET https://data.swecov.se/api/context` must answer with `meta.catalog_version` equal
to the version, `data.steward_config.presentation` present, and
`meta.steward.catalog_generation` and `meta.steward.holdings_generation` equal to the
`generation_id` in `meta` of the local `reg_meta.db` and `reg_meta_swecov.db`. Record
what the checks exercised, the steward pin, the `holdings:` summary line from
`holdings.log` and the census-comparison counts (8g) in the release notes.

## Error recovery

- Commit pushed but `gh release create` failed, or an upload to a draft failed: retry.
- Draft exists and something fails before publication: delete the draft and tag, fix,
  and restart from the verified bump (step 5).
- Source changed after draft/tag creation: recreate release and tag from the repaired
  `origin/main` (steps 5--6), refresh the notes, replace invalidated assets and re-run
  the checks at that revision.
- Tag already exists for the target version: investigate before proceeding.
- A `reg-build` build, verification or conformance failure stops the release: fix it,
  rebuild with `reg-build` (or reuse the earlier base when
  `cargo xtask release-base-reuse` says the fix cannot move it), and repeat steps 8b--8i
  (a base rebuild changes `catalog_sha256`, so holdings are rebuilt too); never copy an
  asset from an older release forward.
- **Once published, never delete, re-cut or re-upload a release's assets.** Deploys and
  release consumers reference the tag and digests. A pair that was published incomplete
  or wrong is recovered by a new patch release with both assets. Fix downstream failures
  in place, or cut a new patch version.
- Never force-push or amend commits already on main.
