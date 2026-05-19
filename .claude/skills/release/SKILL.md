---
name: release
description: "Create and publish a release. Usage: /release [package] <patch|minor|major>"
disable-model-invocation: false
argument-hint: "[package] <patch|minor|major>"
---

# Release pipeline

Create a release with arguments: `$ARGUMENTS`

**Never start a release unless the user explicitly asks for one.** This skill may be invoked by the user via `/release` or referenced in conversation — either way, do not proceed without clear intent to release.

## Packages

| Package | pyproject.toml | `__init__.py` | Publish workflow |
|---|---|---|---|
| reg_meta | `reg_meta/pyproject.toml` | `reg_meta/src/reg_meta/__init__.py` | `publish_reg_meta.yml` (needs environment approval) |
| reg_meta_build | `reg_meta_build/pyproject.toml` | `reg_meta_build/src/reg_meta_build/__init__.py` | `publish_reg_meta_build.yml` (needs environment approval) |
| mock_data_wizard | `mock_data_wizard/pyproject.toml` | `mock_data_wizard/src/mock_data_wizard/__init__.py` | `publish_mock_data_wizard.yml` (needs environment approval) |

reg_meta_build is the build pipeline that produces `reg_meta`'s SQLite
assets; it has its own PyPI release on the `reg_meta_build/v*` tag but
ships no DB release assets (those attach to the parallel `reg_meta/v*`
release).

## Validation

Before doing anything, validate and resolve the inputs. To avoid unnecessarily asking for user confirmation, avoid `$(...)` or backticks inside Bash commands — prefer running each command separately and using the returned value in the next call.

1. **Resolve the bump level**: one of `$0` or `$1` must be `patch`, `minor`, or `major`. If neither is provided, stop and ask the user.
2. **Resolve the package(s)**: if a package name (`reg_meta`, `reg_meta_build`, or `mock_data_wizard`) is provided, use it. Otherwise, infer from context:
   - Find the last release tag for each package (tags follow the pattern `<package>/vX.Y.Z`).
   - Run `git log --oneline <tag>..HEAD -- <package>/` for each to see which packages have unreleased commits.
   - If only one package has changes, use that one.
   - If multiple have changes, release them sequentially — run the full pipeline below for each package, one at a time, with separate commits, tags, and releases. When schema-affecting changes touch `reg_meta_build/`, the `reg_meta` release that publishes the rebuilt asset is the one that must lead.
   - If none has changes, tell the user there is nothing to release.
3. If any required input is still ambiguous, stop and ask the user.
4. **Major version bumps require explicit confirmation.** Show the current and planned version and ask the user to confirm before proceeding.

## Steps

Run the following steps for each resolved package.

### 1. Determine new version

- Read the current version from `<package>/pyproject.toml`.
- Apply the semver bump: patch increments Z, minor increments Y and resets Z, major increments X and resets Y.Z.

### 2. Generate release notes

- Run `git log --oneline <package>/v<current>..HEAD -- <package>/` to get commits since the last release tag for this package.
- If no previous tag exists, use all commits touching `<package>/`.
- Write a brief bullet list summarizing the changes (group related commits, skip merge commits). For each item, link any associated PRs or issues inline (e.g. `Fix widget crash (#42)`).
- Credit contributors: first get the date of the last release tag with `git log -1 --format=%cs <tag>`, then run `gh pr list --search "is:merged merged:>=<date>" --json number,author,title` to find PRs merged since then. For each bullet that came from an external contributor (not the repo owner), append `(HT @username)`.
- Show the draft notes to the user before proceeding.

### 3. Bump version

Update the version string in both files:

- `<package>/pyproject.toml`: the `version = "X.Y.Z"` line
- `<package>/src/<package>/__init__.py`: the `__version__ = "X.Y.Z"` line

**reg_meta only — main-DB schema version check:** Run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/db.py
reg_meta/src/reg_meta/db.py` and check for changes to `CREATE TABLE`,
`CREATE VIRTUAL TABLE`, or column lists (DDL lives in
`reg_meta_build/src/reg_meta_build/db.py` post-split). If the schema
changed but `SCHEMA_VERSION` in `reg_meta/src/reg_meta/db.py` was not
already bumped, bump it now:

- **Major bump** (breaking): renamed/removed tables or columns, changed column semantics
- **Minor bump** (new columns the code reads): added columns/tables that queries reference. `open_db` rejects DBs whose minor is < the code's minor, so this forces a DB rebuild before the package release is usable.

A `SCHEMA_VERSION` bump may require a coordinated `reg_meta_build`
release if the matching DDL changes also need to ship in the builder
wheel (so an end-user rebuild from `reg-meta-build build-db` produces
the new schema). Release `reg_meta_build` first in that case.

**reg_meta only — doc-DB schema version check:** Run
`git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/doc_db.py
reg_meta/src/reg_meta/doc_db.py` and check for changes to `DOC_DDL` or
reads of new `doc_meta` keys (DDL lives in
`reg_meta_build/src/reg_meta_build/doc_db.py` post-split). If the doc
schema changed but `DOC_SCHEMA_VERSION` in
`reg_meta/src/reg_meta/doc_db.py` was not bumped, bump it now. Same
major/minor rules as `SCHEMA_VERSION`. A bump forces a fresh doc-DB
asset upload in step 8.

### 4. Update lockfile

```bash
uv lock
```

### 5. Verify, test, lint

```bash
bash scripts/check_versions.sh
uv run python -m pytest <package>/ -x -q
uv run ruff check
uv run ruff format --check
```

If anything fails, stop and fix. Do not release broken code.

### 6. Commit and push

Before committing, verify that all non-bump changes are already committed in
their own commits with clear messages. The bump commit should contain **only**
version bump files — `pyproject.toml`, `__init__.py`, `uv.lock`, and (if the
relevant schema version was bumped) `db.py` or `doc_db.py`:

```text
Bump <package> version to X.Y.Z
```

Then push to main.

### 7. Create draft GitHub release

The publish workflow fires on `release: published`, so the release must be
created as a **draft** until any required assets (reg_meta only) are uploaded.
A `release: published` event with missing assets races the workflow's smoke
step against the upload — if the maintainer approves the environment gate
before assets land, the smoke step walks back to a prior release and may pick
up an incompatible asset, failing the publish.

```bash
gh release create <package>/vX.Y.Z --draft --title "<package> vX.Y.Z" --notes "$(cat <<'EOF'
<release notes>
EOF
)"
```

The tag is created by this command from the current HEAD — do not create it
separately. The `--draft` flag means no workflow fires yet. If the tag
already exists, something went wrong; see error recovery below.

### 8. Build and upload release assets (reg_meta only, conditional)

reg_meta ships two release assets. Each is optional per release — `maintain
update` walks backwards through releases to find the most recent one
carrying each asset, so a doc-less package release still serves the prior
doc asset. Missing required assets must be uploaded **before** publishing
the release: the CI smoke step runs `maintain update` and fails if the
walker can't resolve a compatible pair of assets.

The raw SCB CSV exports and curated classification CSVs live under
`reg_meta_build/input_data/` (gitignored), with `SCB/`,
`Socialstyrelsen/`, and `classifications/` subdirectories. If missing,
ask the user.

#### 8a. Main DB asset (`reg_meta.db.zst`)

Upload if **either** condition is true:

- `SCHEMA_VERSION` was bumped (either already in the commits or by step 3)
- The release is a **major** version bump

Otherwise skip.

```bash
uv run reg-meta-build build-db --input-dir reg_meta_build/input_data/ --validate
zstd -3 -T0 ~/.local/share/reg_meta/reg_meta.db -o reg_meta.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta.db.zst
rm reg_meta.db.zst
```

`--validate` runs the value-set dedup + year-projection invariants
inline and exits 10 on failure (same checks as
`scripts/validate_valueset_dedup.py`). Skip the flag only for the rare
`--skip-slugs` bootstrap build, which intentionally produces a partial DB.

If the build fails with `vardemangder_drift` (exit 10), SCB has shipped a
new `kod==version` row whose kod is in neither `_VARDEMANGDER_SENTINELS`
nor `_VARDEMANGDER_REAL_SHAPED` (both in
`reg_meta_build/src/reg_meta_build/db.py`). Inspect the listed values, add
each to the appropriate allowlist (sentinel placeholder vs. real
single-code value set), then rerun. See `reg_meta_build/DESIGN.md` §
"Vardemängder sentinel filtering".

#### 8b. Doc DB asset (`reg_meta_docs.db.zst`)

Upload if **any** of these is true:

- `DOC_SCHEMA_VERSION` was bumped
- `git diff <tag>..HEAD -- reg_meta_build/docs/` is non-empty (docs content changed)
- The release is a **major** version bump

Otherwise skip — users keep getting the prior release's doc asset via the
walker.

If `reg_meta_build/docs/` changed because a newly-published SCB PDF was
ingested, the PDF→markdown recipe (marker flags, `GEMINI_API_KEY`, ~$1-2
cost, multiprocessing-crash workaround,
`--MarkdownRenderer_keep_pageheader_in_output` footgun) lives in the
docstring at the top of `scripts/parse_lisa_docs.py`. Run that step
first, then continue here.

```bash
uv run reg-meta-build build-docs
zstd -3 -T0 ~/.local/share/reg_meta/reg_meta_docs.db -o reg_meta_docs.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_docs.db.zst
rm reg_meta_docs.db.zst
```

Verify the expected assets are present on the draft release before publishing:

```bash
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[].name'
```

### 9. Publish the draft release

This is what fires the publish workflow.

```bash
gh release edit <package>/vX.Y.Z --draft=false
```

### 10. Monitor deployment

- If the package has a publish workflow (see table above):
  - Find the triggered run: `gh run list --workflow=<workflow> --limit 1 --json databaseId,url`
  - Tell the user: **"Publish workflow started — approve the deployment at `<run URL>`"**
  - Watch the run to completion: `gh run watch <run-id> --exit-status`
  - Verify the new version is on PyPI: `curl -s https://pypi.org/pypi/<package>/json | python3 -c "import sys,json; print(json.load(sys.stdin)['info']['version'])"`
- If the package has no publish workflow, report the release is done after the tag is created.

## Error recovery

- If the commit was pushed but `gh release create` fails: the commit is on main — just retry the release creation.
- If the release was created but CI fails: delete the release and tag, fix the issue, and start over from step 6.
- If a tag already exists for the target version: something went wrong in a previous attempt. Investigate before proceeding.
- If `build-db` or `build-docs` fails: fix the issue before publishing. The draft release exists but `--draft=false` should not run until assets are valid — the CI smoke step will block the publish if the walker can't resolve them.
- If `gh release upload` fails on a draft: retry the upload. The draft and tag are fine.
- If the publish workflow fails because assets weren't on the release at trigger time (race between `release: published` and asset upload): re-run the failed publish job with `gh run rerun <run-id> --failed` once assets are uploaded. This is what `--draft` in step 7 prevents — only relevant when recovering from a prior non-draft release.
- Never force-push or amend commits that are already on main.
