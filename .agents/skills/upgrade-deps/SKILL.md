---
name: upgrade-deps
description: >-
  Upgrade Registry Research Toolkit dependencies and pinned toolchains directly in an
  isolated worktree, including necessary compatibility fixes and verification. Use for
  dependency refreshes or package/runtime upgrades; publishing is handled separately by
  the release skill.
---

# Upgrade dependencies

Own the requested upgrade through a verified candidate without a Yard ticket or lane.
Dependency maintenance is a manual-workflow exception in
[AGENTS.md](../../../AGENTS.md). It includes the compatibility fixes required by the
selected versions; unrelated product features remain outside this task. Do not publish
packages or deploy merely because an upgrade is complete.

## Establish the candidate

Inspect dirty work, the current branch, origin/main and `yard status --json`. Preserve
unrelated changes and create a task-owned `codex/` branch in an isolated worktree from
an agreed base. Work can proceed while Yard runs elsewhere; final integration follows
[the manual handoff](../../../.yard/OPERATOR.md#manual-integration). Do not take over
another operator's lanes or edit `.yard/local`.

Resolve the requested scope before updating. A named dependency stays a named upgrade
with its necessary dependency closure. For a complete refresh, audit these surfaces
together, reading actual files rather than a saved version list:

- Root/member `pyproject.toml`, `uv.lock`, Python floor and `.python-version`.
- Frontend `package.json`, `bun.lock`, `bunfig.toml`, including peer constraints and npm
  aliases such as the native TypeScript checker.
- Runtime/tool/image pins in `.github/workflows/`, `.pre-commit-config.yaml`,
  `.yard/Dockerfile`, `.yard/config.toml`, `reg_webapp/Dockerfile` and integration test
  Dockerfiles. Include related version references in agent guidance/skills.

Verify current stable versions and compatibility from package registries and upstream
release documentation. Distinguish latest stable, latest compatible and a requested LTS
line. Record held-back packages and the specific constraint; do not force every
transitive package to its newest version through overrides. Docker tag/digest pairs must
identify the same exact multi-architecture image.

Read the repository's release-age policy. Its explicit uv `exclude-newer = false` and
Bun `minimumReleaseAge = 0` prevent inherited host delays; deleting them is not
equivalent. Project-less `uvx` can still inherit host configuration: use a task-scoped
`UV_EXCLUDE_NEWER=false` when applying this policy there. Prefer task-local tooling to
modifying shared host installations, and verify the tool actually invoked by a hook or
image rather than relying on the shell's PATH.

## Update and repair in one worktree

Use the repository's package managers: `uv lock --upgrade` for resolution;
`uv add`/`uv add --dev` only when a new dependency is necessary; Bun for the frontend.
An existing declared constraint may be edited to admit the chosen version, then
regenerated through its package manager. For a scoped upgrade, use targeted resolution
and avoid unrelated lockfile churn.

Update all homes of a selected runtime together. Frontend tool launchers may resolve
`node` differently in CI, the Yard image and production. Check the actual runtime and
shared libraries in each affected environment. Test a changed image early with ordinary
Docker builds and plain progress output, before expensive review. Keep task-owned
images/containers identifiable; no shared-cache pruning.

Fix resulting API, typing, build and test incompatibilities directly. Extend focused
regression tests for behavior changes and preserve the repository's
contract/disclosure/determinism guards. A failed build or test returns to repair in this
worktree, without creating a Yard lane for the same upgrade.

## Verify and review

Run focused checks during repair. On the resulting candidate, execute the relevant
existing checks once; read their current commands and checker pins from AGENTS.md, CI,
hooks and `.yard/config.toml` instead of duplicating version pins in this skill:

- Python: frozen installation, lint/format/types, version consistency and the relevant
  full test suite. Keep the hard Docker packaging checks; source coherence uses
  `--install-mode workspace`. Registry availability is a distinct release check and
  cannot be inferred from a local sibling-wheel install.
- Frontend: lint, Svelte/TypeScript checking, tests, production build and generated
  API-type drift check when its dependency/runtime stack changes.
- Images/toolchains: build affected stages and exercise the existing affected gate
  commands directly in the candidate image, including their documented offline
  environment. A host test pass alone does not verify Linux image tools.
- Browser/runtime changes: use the existing flow driver and inspect its images under the
  design-review skill. Extend coverage for changed rendered behavior; a source review
  does not establish browser behavior.

Normal commit hooks, pre-push checks and CI remain in force. Reuse successful evidence
when the tested source, dependency lock, environment and inputs are unchanged; rerun
checks invalidated by a repair or integration change. Do not add repeated full audits
merely to repeat a passing result.

Once checks pass, use one independent review of the candidate diff, selected version
constraints and verification evidence. Repair actionable findings and recheck the
affected behavior. Ask for a focused follow-up only if the repair leaves a material
concern. No mandatory multi-agent simplification round on every edit. If independent
review is unavailable, record that remaining check for the maintainer rather than
claiming it happened.

Real-corpus verification uses the existing [build-db skill](../build-db/SKILL.md) when
required by the change or requested; synthetic tests alone establish no real-corpus
result. Preserve seed inputs and keep row-level data on MONA.

## Integrate or hand off to release

Commit only this upgrade and its required fixes. Prepare a PR with the tested head,
version changes, compatibility constraints and verification limits. A manual PR requires
green CI and the maintainer's review before merge; a skill does not supply that review
or grant publication authority. Coordinate the merge using the shared manual handoff,
then verify the integrated source.

Keep detailed, temporary evidence in ignored `archive/reports/`; the PR carries the
concise durable result. Report the branch/PR, checks and unresolved holds. If release
was also requested, hand the integrated revision and relevant asset rebuild implications
to [release](../release/SKILL.md). That skill determines package versions, builds or
reuses assets, publishes in dependency order and checks deployment. Do not start another
dependency refresh during that handoff.
