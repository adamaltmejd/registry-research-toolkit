---
name: release
description: >-
  Registry Research Toolkit release workflow. Use when the user explicitly asks to run
  the release workflow, bump and publish reg_meta, reg_meta_build, reg_schema,
  reg_monabundle, or mock_data_wizard, create package tags/releases, upload reg_meta DB
  assets, or monitor publish workflows.
---

# Registry Release

## Start Condition

Never start a release unless the user explicitly asks for one. Stop and ask if package
or bump level remains ambiguous. Major bumps require explicit confirmation after showing
current and planned versions.

## Packages

- `reg_meta`: `reg_meta/pyproject.toml`, `reg_meta/src/reg_meta/__init__.py`, workflow
  `publish_reg_meta.yml`.
- `reg_meta_build`: `reg_meta_build/pyproject.toml`,
  `reg_meta_build/src/reg_meta_build/__init__.py`, workflow
  `publish_reg_meta_build.yml`.
- `reg_schema`: `reg_schema/pyproject.toml` only; no checked `__version__` and no
  publish workflow exist yet. Before the first PyPI release, stop and add or confirm the
  publish path instead of silently omitting the package.
- `reg_monabundle`: `reg_monabundle/pyproject.toml`,
  `reg_monabundle/src/reg_monabundle/__init__.py`; no publish workflow exists yet.
  Before the first PyPI release, stop and add or confirm the publish path instead of
  silently omitting the package.
- `mock_data_wizard`: `mock_data_wizard/pyproject.toml`,
  `mock_data_wizard/src/mock_data_wizard/__init__.py`, workflow
  `publish_mock_data_wizard.yml`.

Publish workflows are unattended as of 2026-06-10; do not rely on a PyPI environment
approval pause.

## Resolve Inputs

1. Resolve bump: `patch`, `minor`, or `major`.

2. Resolve package. If omitted, compare unreleased commits since each package's last
   `<package>/vX.Y.Z` tag:

   ```sh
   package="<package>"
   git fetch --tags origin
   tag="$(git tag --list "$package/v*" --sort=-v:refname | head -n 1)"
   if [ -n "$tag" ]; then
     git log --oneline "$tag"..HEAD -- "$package/"
   else
     git log --oneline -- "$package/"
   fi
   ```

   Also compare `reg_meta_build/` changes since the last `reg_meta/v*` tag. Builder
   content changes that affect the built DBs, including curated TOMLs, provider source,
   and docs inputs, require a matching `reg_meta` release so the prebuilt DB assets are
   refreshed even when no `reg_meta/` code changed. Use the same tag-fetch/no-tag
   fallback for this comparison; if there is no prior `reg_meta/v*` tag, inspect all
   `reg_meta_build/` history.

3. If multiple packages changed, release sequentially. For schema-affecting DDL or doc
   DB changes shared by `reg_meta` and `reg_meta_build`, keep `reg_meta` ahead of the
   builder and bump `reg_meta_build`'s minimum `reg-meta` dependency to the new
   `reg_meta` version before publishing the builder. Do not ship a builder that can
   resolve against stale schema constants.

## Per-Package Steps

1. Determine new semver from `<package>/pyproject.toml`.

2. Draft release notes from commits since the last package tag. Include relevant
   PR/issue numbers and contributor credit where applicable. Show notes before
   proceeding.

3. Bump the package's listed version file(s):

   - `<package>/pyproject.toml`
   - `<package>/src/<package>/__init__.py`

   `reg_schema` is currently pyproject-only; do not invent an unchecked
   `reg_schema.__version__` just for a release.

4. For `reg_meta`, check schema changes:

   ```sh
   git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/db.py reg_meta/src/reg_meta/db.py
   git diff <tag>..HEAD -- reg_meta_build/src/reg_meta_build/doc_db.py reg_meta/src/reg_meta/doc_db.py
   ```

   If DDL or doc DB reads changed, ensure `SCHEMA_VERSION` or `DOC_SCHEMA_VERSION` was
   bumped appropriately.

5. Update lockfile and verify:

   ```sh
   uv lock
   bash scripts/check_versions.sh
   uv run python -m pytest <package>/ -x -q
   uv run ruff check
   uv run ruff format --check
   ```

6. Commit only version/lock/schema-bump files:

   ```text
   Bump <package> version to X.Y.Z
   ```

7. Push and verify the bump commit is on `origin/main`, then create a draft GitHub
   release. The tag is created by this command; do not create it separately. Pass the
   verified main commit as `--target` so the tag cannot be created from an older default
   branch head.

   ```sh
   git push origin HEAD:main
   git fetch origin main
   if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
     echo "release bump is not origin/main; merge or push it before creating the tag" >&2
     exit 1
   fi
   target="$(git rev-parse origin/main)"
   gh release create <package>/vX.Y.Z --draft --target "$target" --title "<package> vX.Y.Z" --notes-file <notes-file>
   ```

## reg_meta Assets

Every `reg_meta` release must carry both `reg_meta.db.zst` and `reg_meta_docs.db.zst`
before publishing.

Build a fresh main DB if `SCHEMA_VERSION` changed, the release is major, or consumed
`reg_meta_build/` content changed since the prior `reg_meta` tag excluding
`reg_meta_build/docs/`. Otherwise copy forward the newest prior asset.

Fresh main DB:

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta.XXXXXX")"
db_path="$db_dir/reg_meta.db"
uv run reg-meta-build --db "$db_dir" build-db --input-dir reg_meta_build/input_data/
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$db_path"
zstd -3 -T0 "$db_path" -o reg_meta.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta.db.zst
rm -r "$db_dir" reg_meta.db.zst
```

Build a fresh doc DB if `DOC_SCHEMA_VERSION` changed, the release is major, or any
doc-DB consumed input/build logic changed: `reg_meta_build/docs/`,
`reg_meta_build/doc_sources.toml`, `reg_meta_build/src/reg_meta_build/doc_db.py`, or
`reg_meta/src/reg_meta/doc_db.py`. Otherwise copy forward the newest prior docs asset.

Fresh doc DB:

```sh
docs_db_dir="$(mktemp -d "${TMPDIR:-/tmp}/reg_meta_docs.XXXXXX")"
docs_db_path="$docs_db_dir/reg_meta_docs.db"
uv run reg-meta-build --db "$docs_db_dir" build-docs
uv run python -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE'); c.commit(); c.close()" "$docs_db_path"
zstd -3 -T0 "$docs_db_path" -o reg_meta_docs.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta_docs.db.zst
rm -r "$docs_db_dir" reg_meta_docs.db.zst
```

Copy forward:

```sh
gh release download reg_meta/v<prev> --pattern <asset-name>
gh release upload reg_meta/vX.Y.Z <asset-name>
rm <asset-name>
```

Verify both assets before publishing:

```sh
gh release view reg_meta/vX.Y.Z --json assets --jq '.assets[].name'
```

## Publish And Monitor

Publish:

```sh
gh release edit <package>/vX.Y.Z --draft=false
```

Monitor:

```sh
run_id=""
while [ -z "$run_id" ]; do
  run_id="$(gh run list --workflow=<workflow> --event release --commit "$target" --json databaseId,headSha,event,createdAt --jq '.[0].databaseId // ""')"
  [ -n "$run_id" ] || sleep 10
done
gh run watch "$run_id" --exit-status
```

Do not watch `gh run list --workflow=<workflow> --limit 1` without filtering or
verifying the run belongs to this release; GitHub may not have queued the new release
event yet, so the latest run can be stale.

Verify PyPI:

```sh
curl -s https://pypi.org/pypi/<package>/json | python3 -c "import sys,json; print(json.load(sys.stdin)['info']['version'])"
```

## Recovery

If release creation fails after the bump commit was pushed, retry creation.

If validation or CI fails before PyPI publication, delete the draft release/tag, fix the
issue, and restart from the verified bump step.

If PyPI publication succeeded, do not delete the GitHub release/tag or restart the same
version: PyPI versions are immutable. Fix downstream failures in place when possible
(for example a deploy-image failure), or publish a new patch version if the released
package or assets are wrong. Before deleting any release/tag, verify PyPI does not
already list `X.Y.Z`.
