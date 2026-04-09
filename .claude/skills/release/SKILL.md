---
name: release
description: "Create and publish a release. Usage: /release <package> <patch|minor|major> [--notes 'extra notes']"
disable-model-invocation: true
argument-hint: "<package> <patch|minor|major> [--notes 'extra notes']"
---

# Release pipeline

Create a release for **$0** with a **$1** version bump.

All raw arguments: `$ARGUMENTS`

## Packages

| Package | pyproject.toml | `__init__.py` | Publishes to PyPI |
|---|---|---|---|
| regmeta | `regmeta/pyproject.toml` | `regmeta/src/regmeta/__init__.py` | Yes (via `publish_regmeta.yml`, needs environment approval) |
| mock_data_wizard | `mock_data_wizard/pyproject.toml` | `mock_data_wizard/src/mock_data_wizard/__init__.py` | No |

## Validation

Before doing anything, validate the inputs:

1. `$0` must be one of: `regmeta`, `mock_data_wizard`
2. `$1` must be one of: `patch`, `minor`, `major`
3. If either is missing or invalid, stop and ask the user.
4. **Major version bumps require explicit confirmation.** If `$1` is `major`, stop and warn the user that this is a major release (breaking API changes). Show the current version and the planned new version, and ask them to confirm before proceeding. Do not continue unless the user explicitly approves.

## Steps

### 1. Determine new version

- Read the current version from `<package>/pyproject.toml`
- Apply the semver bump (`$1`): patch increments Z, minor increments Y and resets Z, major increments X and resets Y.Z
- Announce the planned bump (e.g. "0.5.3 -> 0.6.0") before proceeding.

### 2. Generate release notes

- Run `git log --oneline <package>/v<current>..HEAD` to get commits since the last release tag for this package.
- If no previous tag exists, use all commits.
- Write a brief bullet list summarizing the changes (group related commits, skip merge commits). For each item, link any associated PRs or issues inline (e.g. `Fix widget crash (#42)`).
- Credit contributors: use `gh pr list --search "is:merged" --json number,author,title` and `gh issue list --state closed --json number,author,title` to find PRs and issues closed since the last release. For each bullet that came from an external contributor (not the repo owner), append `(HT @username)`.
- If `--notes` was provided in `$ARGUMENTS`, append that text under a separate section.
- Show the draft notes to the user before proceeding.

### 3. Bump version

Update the version string in both files — **nowhere else**:

- `<package>/pyproject.toml`: the `version = "X.Y.Z"` line
- `<package>/src/<package>/__init__.py`: the `__version__ = "X.Y.Z"` line

### 4. Update lockfile

```bash
uv lock
```

### 5. Verify consistency

```bash
bash scripts/check_versions.sh
```

If this fails, stop and fix. Do NOT skip this step.

### 6. Run tests

```bash
uv run python -m pytest <package>/ -x -q
```

If tests fail, stop. Do not release broken code.

### 7. Lint

```bash
uv run ruff check
uv run ruff format --check
```

### 8. Commit and push

Commit the version bump files (`pyproject.toml`, `__init__.py`, `uv.lock`):

```text
Bump <package> version to X.Y.Z
```

Then push to main.

### 9. Create GitHub release

```bash
gh release create <package>/vX.Y.Z --title "<package> vX.Y.Z" --notes "<release notes>"
```

Use `--verify-tag` if the tag already exists. The tag must point at the commit that contains the version bump — this is the commit you just pushed.

### 10. Monitor deployment

- If the package publishes to PyPI (see table above):
  - Find the triggered publish run: `gh run list --workflow=publish_regmeta.yml --limit 1 --json databaseId,url`
  - Tell the user: **"Publish workflow started — approve the deployment at `<run URL>`"**
  - Watch the run to completion: `gh run watch <run-id> --exit-status`
  - Verify the new version is on PyPI: `curl -s https://pypi.org/pypi/<package>/json | python3 -c "import sys,json; print(json.load(sys.stdin)['info']['version'])"`
- If the package does NOT publish to PyPI, report the release is done after the tag is created.

## Error recovery

- If the commit was pushed but `gh release create` fails: the commit is on main — just retry the release creation.
- If the release was created but CI fails: delete the release and tag, fix the issue, and start over from step 8.
- Never force-push or amend commits that are already on main.
