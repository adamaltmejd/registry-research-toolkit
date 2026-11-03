---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and release reg_meta (the `reg-meta` crate and its paired
  DB assets) or reg_meta_build, create package tags/releases, upload the reg_meta DB
  assets, or monitor the release workflows. Usage: /release [package]
  (patch|minor|major)
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

A `reg_meta/v*` release carries two DB assets, `reg_meta.db.zst` and
`reg_meta_swecov.db.zst` (step 8), and the `reg-meta` binaries (step 10). A
`reg_meta_build/v*` release is a tag with no assets.

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
     and the root `stewards.toml` (the steward pin) need a `reg_meta` release to refresh
     the assets.
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
git diff <prev-tag>..HEAD -- crates/reg-build/src/schema.sql crates/reg-build/src/holdings/schema.sql crates/reg-core/src/lib.rs
```

If `schema.sql` changed, `reg_core::CATALOG_SCHEMA_VERSION` must have changed in the
same release, and vice versa. **The holdings DDL pairs the same way:**
`holdings/schema.sql` and `reg_core::HOLDINGS_SCHEMA_VERSION` change together. The
reader admits on exact equality, without a major/minor compatibility rule. If a paired
change is missing, stop and land it through a reviewed PR first. A bump's PR and
subsequent main pushes are expected to be red at `schema-guard` (which compares only
`CATALOG_SCHEMA_VERSION`) until the matching release publishes; that check is not
required on main. Merge first, then release when the maintainer runs it. A gap is
allowed; the guard blocks deployment throughout it.

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

Every release builds a **pair**, fresh, in one run: `reg_meta.db.zst` (the reference
catalog, documents inside) and `reg_meta_swecov.db.zst` (the SWECOV holdings: the
catalog schema's supplemental part plus the `holding_*` tables). There is no separate
docs asset and no copying an asset from an older release.

- Holdings pin the base's `catalog_sha256`, so **every reg_meta release rebuilds
  holdings in the same run**, even when nothing in the steward repository changed. Any
  rebuild of the base invalidates the holdings: repeat from 8d.
- A holdings or pair failure stops the release. A global-only release (no
  `reg_meta_swecov.db.zst`) needs the maintainer's explicit choice, recorded in the
  notes. Its consequence: with `SWECOV_DEPLOY=on`, `swecov-asset-guard` fails, SWECOV is
  not rebuilt and stays on the previous pair, and the commit still counts as deployed
  (step 10).
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
same binaries.

```sh
set -euo pipefail
inputs=/absolute/path/to/pinned-inputs-worktree
assets=/absolute/path/to/pinned-release-assets
evidence=/absolute/path/to/release-evidence
version=X.Y.Z
mkdir -p "$evidence/catalog"
cargo build --release --workspace --locked
target/release/reg-build build --inputs "$inputs" --assets "$assets" \
  --curation curation --out "$evidence/catalog/reg_meta.db" \
  --catalog-version "$version" --report "$evidence/build-report.json" --release
base_sha=$(shasum -a 256 "$evidence/catalog/reg_meta.db" | cut -d' ' -f1)
```

`--out` is a database filename, not a directory. `--release` refuses unplaced or split
ledger ids and variables without a ledger id. Read the report's warnings and confirm all
expected providers and inputs are present. `base_sha` is the digest every later step
re-checks: the holdings pin it, and the base uploaded in 8i must still equal it.

#### 8b. Verify the base

For every release after the switch, download the immediately previous published
release's `reg_meta.db.zst`, verify its compressed SHA-256 against GitHub's asset
digest, and decompress it into a separate directory (`$prev`). Never substitute an older
release or a synthetic artifact. Run the single validation authority:

```sh
target/release/reg-build verify "$evidence/catalog/reg_meta.db" --previous "$prev/reg_meta.db" > "$evidence/catalog-verify.txt"
```

Retain stdout in `catalog-verify.txt` beside `catalog-diff.json`, including the pre-v1
id-change report when the previous release is pre-v1. `--previous` is required. A
verification failure stops publication. The current verifier requires the previous DB to
have the same schema version; a future schema bump that makes it incomparable needs a
reviewed procedure change before release, not an unrecorded skip.

#### 8c. Review the catalog diff

Compare with that same previous DB and retain the full JSON output in the evidence
directory:

```sh
target/release/reg-build diff "$prev/reg_meta.db" "$evidence/catalog/reg_meta.db"
target/release/reg-build diff "$prev/reg_meta.db" "$evidence/catalog/reg_meta.db" --json > "$evidence/catalog-diff.json"
```

Exit 0 means no changes, 1 means differences to review, and 2 means an error that stops
the release. Handle exit 1 explicitly if running under `set -e`. The builder agent reads
the diff, confirms only intended changes, and puts its summary in the release notes.

#### 8d. Compile the holdings

The pin is `stewards.toml`: the steward repository, an exact commit and the config path.
The compiler refuses unless the checkout is exactly the pin: `origin` must equal the
registry's URL byte for byte (SSH, so an HTTPS clone fails with `holdings_pin_invalid`),
`HEAD` must be the commit, and the tree must be clean including untracked files. Never
edit the steward checkout. Re-lock the pin in a reviewed PR before the bump, not during
the release.

```sh
pin=$(uv run --no-project python -c 'import tomllib;print(tomllib.load(open("stewards.toml","rb"))["swecov"]["commit"])')
git clone git@github.com:adamaltmejd/swecov-steward.git "$steward"
git -C "$steward" checkout --detach "$pin"
target/release/reg-build holdings --base "$evidence/catalog/reg_meta.db" --steward swecov \
  --steward-repo "$steward" --registry stewards.toml \
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

Use the release tier's command from `scripts/gate.py`, pointed at the new catalog
directory, first for the global catalog and then for the pair. The second run adds
`--catalog swecov` to the server command, which makes the server admit
`reg_meta_swecov.db` beside `reg_meta.db`:

```sh
uv run python -m pytest conformance -q -n auto -m release --run-release \
  --artifact-dir="$evidence/catalog" \
  --server-cmd='target/release/reg-meta serve --db {db} --port {port} --write-limit 100000'
uv run python -m pytest conformance -q -n auto -m release --run-release \
  --artifact-dir="$evidence/catalog" \
  --server-cmd='target/release/reg-meta serve --db {db} --port {port} --write-limit 100000 --catalog swecov'
```

`gate.py release` itself selects a synthetic artifact and accepts no artifact override;
that run does not validate the release DB. A red artifact run stops publication; a
failure only in the pair run stops it too. Do not change tests to get a pass. If pair
admission times out at server start, raise `SERVER_READY_SECONDS` (120) in
`conformance/http_cases.py` through a reviewed PR, not in place.

Reading the pair run: the tests read `reg_meta.db` directly and serve both files. Under
a steward, `ops/order` takes row keys from the holdings (`holding_mapping` joined to
each table's `key_requirements`, `ops/order/rows.rs`), not from the catalog's `row_key`
segments. The window and serving logic has no steward branch. A table the steward holds
nothing for therefore orders with the same entries but no row keys and no row-key window
findings. `test_release_order.py` marks a table blocked when a row-key segment is
ambiguous, so a `validate`/`order` disagreement on exactly such a table in the pair run
is this divergence, not an order defect. It still stops the release: the maintainer
decides, and the finding is recorded in the notes.

#### 8g. Compare the holdings census with the previous paired release

From 0.72.0 on both sides are built by `reg-build holdings`. Compare against the previous
paired release's `reg_meta_swecov.db`: download its `reg_meta_swecov.db.zst` to a separate
directory (`$old`), verify its compressed SHA-256 against GitHub's asset digest and
decompress it. The `rebuilt` mode refuses a holdings schema mismatch (`PRAGMA user_version`
or the `schema_version` meta key) and names it. It reports census placement per column
(placed, held, unmatched and the changed placements by old to new `table_pid`),
reader-visible text changes by kind (bodies, not ids) and finding deltas by code. Details
go to `.local/` or the evidence directory only; stdout carries aggregate counts.

```sh
uv run --no-project scripts/rebuild_compare.py rebuilt --old-db "$old/reg_meta_swecov.db" \
  --new-db "$evidence/catalog/reg_meta_swecov.db" \
  --out "$evidence/holdings-compare" > "$evidence/holdings-compare.txt"
```

The maintainer reads that summary for go/no-go; its counts go in the release notes.

#### 8h. Scope check

The SWECOV catalog is public (maintainer 2026-11-03), so there is no disclosure review.
The release must carry only what the catalog serves, never background data: steward
repository notes, raw census or storage exports, inventory working files.
`holdings_scope_check.py` fails when the holdings DB's schema differs from the schema
the builder declares (every table, FTS shadow tables included, and every column must
match, none extra or missing), when a meta key is not on the script's allowlist, when
`occurrence_locator` has rows, when a text or blob value exceeds its size bound, or when
a `text` body names a steward working file (`*.json`, `*.jsonl`, `*.toml`) or a census
row (`census:row=`); the last needs a steward-repo fix and re-pin, not a script change.
Run it against this release checkout and record its one-line result in the release
notes:

```sh
uv run --no-project scripts/holdings_scope_check.py "$evidence/catalog/reg_meta_swecov.db" \
  "$PWD" | tee "$evidence/scope-check.txt"
```

A failure stops the release. Fix the build or, when the item belongs in the paired
catalog, add its meta key or size bound to the script in a reviewed PR; then rebuild
from 8d. Then re-check `$base_sha` against the local base file (it must be unchanged
since 8a) and proceed.

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

`publish_reg_meta.yml` runs CI on the tag, artifact conformance (`integration.yml`: the
global asset, and the paired SWECOV asset when the release has one), and dispatches
`container-build.yml`, which bakes and deploys the catalog (and the pair, when
`SWECOV_DEPLOY` is `on`). Integration and deployment dispatch both depend on CI, not on
each other. The deploy guard passes only when the released `CATALOG_SCHEMA_VERSION`
equals main's; a missing constant or a mismatch blocks deployment. Scope the run lookup
to this release; an unfiltered `--limit 1` can match a stale run:

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

**First SWECOV activation.** Only once, after the global deploy of the first paired
release is green: `gh variable set SWECOV_DEPLOY --body on`, then
`gh workflow run container-build.yml` (never wait for the next push). Every later
release redeploys SWECOV with global while the variable stays `on`.

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
Then SWECOV, once `SWECOV_DEPLOY` is `on` and its deploy jobs are green: the public
`GET https://data.swecov.se/api/context` must answer with `data.catalog_version` equal
to the version, `data.steward_config.presentation` present, and
`data.steward.catalog_generation` and `data.steward.holdings_generation` equal to the
`generation_id` in `meta` of the local `reg_meta.db` and `reg_meta_swecov.db`. Record
what the checks exercised, the steward pin, the `holdings:` summary line from
`holdings.log` and the census-comparison counts (8g) in the release notes.

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
  rebuild with `reg-build`, and repeat steps 8b--8i (a base rebuild changes
  `catalog_sha256`, so holdings are rebuilt too); never copy an older asset forward.
- **Once published, never delete, re-cut or re-upload a release's assets.** Deploys and
  release consumers reference the tag and digests. A pair that was published incomplete
  or wrong is recovered by a new patch release with both assets. Fix downstream failures
  in place, or cut a new patch version.
- Never force-push or amend commits already on main.
