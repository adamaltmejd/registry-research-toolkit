---
name: init-mona-project
description: Scaffold or resume a local R environment for an existing SCB MONA research project. Use when the user wants MONA project setup, mock-data handoff, or scaffold enrichment around `stats.json`.
---

# Initialize MONA Research Project

Arguments: `$ARGUMENTS` (optional project slug).

This skill scaffolds the local workbench for an already-approved MONA project.
It prepares structure, documentation, mock-data workflows, and guardrails. It
does not write analysis code, invent variable semantics, or make research
design choices for the user.

## Safety and scope

- MONA contains personal data. Under no circumstances may row-level data leave
  MONA. Only aggregate statistics may be exported.
- Treat mock data as provisional. Schema findings are useful; distributional
  findings must be verified on MONA before they drive research code.
- Keep scaffolding minimal. Do not pre-populate speculative functions,
  packages, estimators, or compatibility shims.
- Run the local CLIs directly once installed. Do not use `uv run
  mock-data-wizard` or `uv run regmeta`.

## Your role

You are the scaffolder, not the analyst.

- Do:
  - Interview carefully and record answers faithfully.
  - Copy bundled templates verbatim.
  - Produce concrete documentation that helps the next cold-start session.
  - Leave the project ready for a follow-up session to implement data
    processing and analysis.
- Do not:
  - Write project-specific wrangling or estimator code.
  - Guess value codings or schema semantics not confirmed on MONA.
  - Add packages "just in case".

## Memory files

- In Claude Code, write `CLAUDE.md`.
- In other agent runtimes, write `AGENTS.md`.
- Write exactly one runtime-specific memory file during a scaffold run. In the
  rest of this skill, `{MEMORY}.md` means that single file.
- Never create both files.
- Never create a symlink between them.
- This skill does not auto-manage cross-runtime duplication or conversion.

## Project directory resolution

Resolve `{projdir}` in this order:

1. If `$ARGUMENTS` is set and a directory with that name already exists, use
   it.
2. Else if the current working directory already contains `.Rproj`,
   `stats.json`, or `mock_data/`, treat the current working directory as the
   project root.
3. Else interview the user and decide whether to scaffold into the current
   working directory or into a new `{slug}/` subdirectory.

When the current working directory contains only invisible agent metadata such
as `.claude/` or `.codex/`, scaffold into it directly without asking.

## Phase detection

| Condition | Action |
|-----------|--------|
| No project signals found | Run Phase 1 interview and bootstrap |
| Project dir exists, no `stats.json` | Stop after MONA handoff and wait for `stats.json` |
| Project dir exists, has `stats.json`, no `mock_data/manifest.json` | Generate mock data, then continue to Phase 2 |
| Project dir exists, has `mock_data/manifest.json`, no `{MEMORY}.md` | Run Phase 2 and write `{MEMORY}.md` |
| Project dir exists, has `mock_data/manifest.json`, and `{MEMORY}.md` | Already initialized; tell the user |

## Workflow

Read [workflow.md](references/workflow.md) before acting. It contains the
exact interview flow, commands, MONA handoff text, enrichment tasks, testing,
and git-init rules.

High-level sequence:

1. Phase 1:
   - collect slug, target directory, SCB project number, and research plan
   - create the minimal scaffold
   - verify `mock-data-wizard` and `regmeta`
   - generate `extract_stats.R`
   - stop after MONA handoff instructions
2. Mock data generation:
   - when `stats.json` is present but `mock_data/manifest.json` is absent,
     generate mock data and verify the manifest was created
3. Phase 2:
   - document data sources by register
   - probe the mock data and write concrete findings
   - write the remaining project files
   - run `Rscript tests/testthat.R`
   - initialize git only after checking install/config state

## Generated files

Read [generated-files.md](references/generated-files.md) before writing any
files. It contains:

- the exact file list to generate
- the required templates for `.Rproj`, `.gitignore`, `_targets.yaml`, and
  `src/pipeline.R`
- the exact stubs for `src/data_processing.R` and `src/analysis.R`
- the required structure for `{MEMORY}.md` and `ROADMAP.md`
- the bundled template copy map for `templates/`

## Hard rules

- Copy everything under `templates/` verbatim; do not edit those files during
  scaffolding.
- `src/data_processing.R` and `src/analysis.R` stay as stubs only.
- `tar_option_set(packages = ...)` must keep the template's minimal package
  list at scaffold time.
- Group `notes/data_*.md` by register, not by CSV file.
- `notes/mock_data_assessment.md` must separate measured results from
  interpretation, and it must be grounded in scripted checks plus any
  project-specific follow-up probes.
- Never fabricate warnings, schema mismatches, code-set issues, or join
  findings. If something was not measured, say `Not assessed`.
- Run `Rscript tests/testthat.R` before you declare the scaffold complete.
- If git is absent or unconfigured, ask the user before installing or
  configuring it. Never silently set global git config.
- Keep the final chat summary short and tell the user to start a fresh agent
  session for implementation work.

## Troubleshooting

- If `mock-data-wizard` or `regmeta` commands fail unexpectedly, inspect
  their `--help` output and follow the install steps in
  [workflow.md](references/workflow.md).
- If generated R files fail the ASCII guard, replace non-ASCII characters with
  `\\uXXXX` escapes or ASCII equivalents and re-run the tests.
- If the project is partially initialized, prefer repairing it in place over
  creating parallel scaffolds.
