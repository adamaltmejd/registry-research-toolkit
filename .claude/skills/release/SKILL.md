---
name: release
description: "Create and publish a release. Usage: /release [package] <patch|minor|major> [--notes 'extra notes']"
disable-model-invocation: false
argument-hint: "[package] <patch|minor|major> [--notes 'extra notes']"
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

Before doing anything, validate and resolve the inputs:

1. **Resolve the bump level**: one of `$0` or `$1` must be `patch`, `minor`, or `major`. If neither is provided, stop and ask the user.
2. **Resolve the package(s)**: if a package name (`regmeta` or `mock_data_wizard`) is provided, use it. Otherwise, infer from context:
   - Find the last release tag for each package (tags follow the pattern `<package>/vX.Y.Z`).
   - Run `git log --oneline <tag>..HEAD -- <package>/` for each to see which packages have unreleased commits.
   - If only one package has changes, use that one.
   - If both have changes, release both sequentially — run the full pipeline below for each package, one at a time, with separate commits, tags, and releases.
   - If neither has changes, tell the user there is nothing to release.
3. If any required input is still ambiguous, stop and ask the user.
4. **Major version bumps require explicit confirmation.** If the bump level is `major`, stop and warn the user that this is a major release (breaking API changes). Show the current version and the planned new version, and ask them to confirm before proceeding. Do not continue unless the user explicitly approves.

## Implementation notes

- **No command substitution in Bash calls.** Never use `$(...)` or backticks inside a single Bash command. Instead, run each command separately and use the returned value in the next call. For example, first run `git tag --list '<package>/v*' --sort=-version:refname` to get the latest tag, then use that literal tag value in `git log --oneline <tag>..HEAD -- <package>/`.

## Steps

Run the following steps for each resolved package.

### 1. Determine new version

- Read the current version from `<package>/pyproject.toml`.
- Apply the semver bump: patch increments Z, minor increments Y and resets Z, major increments X and resets Y.Z.
- Announce the planned bump (e.g. "0.5.3 -> 0.6.0") before proceeding.

### 2. Generate release notes

- Run `git log --oneline <package>/v<current>..HEAD -- <package>/` to get commits since the last release tag for this package.
- If no previous tag exists, use all commits touching `<package>/`.
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

The tag is created by this command from the current HEAD — do not create it separately. If the tag already exists, something went wrong; see error recovery below.

### 10. Monitor deployment

- If the package has a publish workflow (see table above):
  - Find the triggered run: `gh run list --workflow=<workflow> --limit 1 --json databaseId,url`
  - Tell the user: **"Publish workflow started — approve the deployment at `<run URL>`"**
  - Watch the run to completion: `gh run watch <run-id> --exit-status`
  - Verify the new version is on PyPI: `curl -s https://pypi.org/pypi/<package>/json | python3 -c "import sys,json; print(json.load(sys.stdin)['info']['version'])"`
- If the package has no publish workflow, report the release is done after the tag is created.

## Error recovery

- If the commit was pushed but `gh release create` fails: the commit is on main — just retry the release creation.
- If the release was created but CI fails: delete the release and tag, fix the issue, and start over from step 8.
- If a tag already exists for the target version: something went wrong in a previous attempt. Investigate before proceeding.
- Never force-push or amend commits that are already on main.
