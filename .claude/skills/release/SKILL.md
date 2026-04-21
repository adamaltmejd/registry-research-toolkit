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
| regmeta | `regmeta/pyproject.toml` | `regmeta/src/regmeta/__init__.py` | `publish_regmeta.yml` (needs environment approval) |
| mock_data_wizard | `mock_data_wizard/pyproject.toml` | `mock_data_wizard/src/mock_data_wizard/__init__.py` | None |

## Validation

Before doing anything, validate and resolve the inputs. To avoid unnecessarily asking for user confirmation, avoid `$(...)` or backticks inside Bash commands — prefer running each command separately and using the returned value in the next call.

1. **Resolve the bump level**: one of `$0` or `$1` must be `patch`, `minor`, or `major`. If neither is provided, stop and ask the user.
2. **Resolve the package(s)**: if a package name (`regmeta` or `mock_data_wizard`) is provided, use it. Otherwise, infer from context:
   - Find the last release tag for each package (tags follow the pattern `<package>/vX.Y.Z`).
   - Run `git log --oneline <tag>..HEAD -- <package>/` for each to see which packages have unreleased commits.
   - If only one package has changes, use that one.
   - If both have changes, release both sequentially — run the full pipeline below for each package, one at a time, with separate commits, tags, and releases.
   - If neither has changes, tell the user there is nothing to release.
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

**regmeta only — main-DB schema version check:** Run
`git diff <tag>..HEAD -- regmeta/src/regmeta/db.py` and check for changes to
`CREATE TABLE`, `CREATE VIRTUAL TABLE`, or column lists. If the schema changed
but `SCHEMA_VERSION` in that file was not already bumped, bump it now:

- **Major bump** (breaking): renamed/removed tables or columns, changed column semantics
- **Minor bump** (new columns the code reads): added columns/tables that queries reference. `open_db` rejects DBs whose minor is < the code's minor, so this forces a DB rebuild before the package release is usable.

**regmeta only — doc-DB schema version check:** Run
`git diff <tag>..HEAD -- regmeta/src/regmeta/doc_db.py` and check for changes
to `DOC_DDL` or reads of new `doc_meta` keys. If the doc schema changed but
`DOC_SCHEMA_VERSION` in that file was not bumped, bump it now. Same
major/minor rules as `SCHEMA_VERSION`. A bump forces a fresh doc-DB asset
upload in step 8.

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

### 7. Create GitHub release

```bash
gh release create <package>/vX.Y.Z --title "<package> vX.Y.Z" --notes "$(cat <<'EOF'
<release notes>
EOF
)"
```

The tag is created by this command from the current HEAD — do not create it separately. If the tag already exists, something went wrong; see error recovery below.

### 8. Build and upload release assets (regmeta only, conditional)

regmeta ships two release assets. Each is optional per release — `maintain
update` walks backwards through releases to find the most recent one
carrying each asset, so a doc-less package release still serves the prior
doc asset. Missing assets must be uploaded **before** approving the publish
workflow: the CI smoke step runs `maintain update` and fails if the walker
can't resolve a compatible pair of assets.

The SCB CSV exports live in `SCB-data/` (gitignored). If missing, ask the user.

#### 8a. Main DB asset (`regmeta.db.zst`)

Upload if **either** condition is true:

- `SCHEMA_VERSION` was bumped (either already in the commits or by step 3)
- The release is a **major** version bump

Otherwise skip.

```bash
uv run regmeta maintain build-db --csv-dir SCB-data/
zstd -3 -T0 ~/.local/share/regmeta/regmeta.db -o regmeta.db.zst
gh release upload regmeta/vX.Y.Z regmeta.db.zst
rm regmeta.db.zst
```

#### 8b. Doc DB asset (`regmeta_docs.db.zst`)

Upload if **any** of these is true:

- `DOC_SCHEMA_VERSION` was bumped
- `git diff <tag>..HEAD -- regmeta/docs/` is non-empty (docs content changed)
- The release is a **major** version bump

Otherwise skip — users keep getting the prior release's doc asset via the
walker.

```bash
uv run regmeta maintain build-docs
zstd -3 -T0 ~/.local/share/regmeta/regmeta_docs.db -o regmeta_docs.db.zst
gh release upload regmeta/vX.Y.Z regmeta_docs.db.zst
rm regmeta_docs.db.zst
```

Verify both assets are present on the release before approving the workflow:

```bash
gh release view regmeta/vX.Y.Z --json assets --jq '.assets[].name'
```

### 9. Monitor deployment

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
- If `build-db` or `build-docs` fails: fix the issue before approving the publish workflow. The release exists but the package must not go live on PyPI without compatible assets — the CI smoke step will block the publish if the walker can't resolve them.
- If `gh release upload` fails: retry the upload. The release and tag are fine.
- Never force-push or amend commits that are already on main.
