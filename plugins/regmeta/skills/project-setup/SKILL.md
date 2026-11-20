---
name: project-setup
description: Set up a Swedish register research project folder so an agent can work in
  it, in a new folder or around an existing project. Writes the research spec, the
  run script and pipeline layout, the pinned export gate and the agent instructions,
  then hands over to the data order and mock data. Use when the user starts a register
  project, or wants an existing one organized for mock runs, batches and gated exports.
---

# Project setup

Setup prepares the folder. It does no research: it proposes no research questions,
writes no analysis and runs no pipeline. The `research` skill does that work afterwards,
and the first mock run happens there.

The row-level data is on a server no agent runs on (MONA, or a private server). The
agent works in the project folder, on code, catalog metadata and mock data. Read only
the project folder and the sources the user names. Never ask for or use the user's
server access.

## 1. Look, then ask

Look at the folder first: is it a Git repository and is it clean, and which of
`AGENTS.md` or `CLAUDE.md`, `RESEARCH.md`, `project_data.json`, `run.R`, `src/` and
other code exist. Then ask only for what the folder does not answer, in one round:

1. Is this a new project or an existing one? The folder the agent was started in is the
   project folder.
2. What is the research about, in a few sentences? Skip it if `RESEARCH.md` says.
3. Does it run on MONA? If so, the project code (`P` and four digits). If not, where.
4. Is the data delivered? If so, the absolute folder the files are in, and their format.
5. Which catalog: the global one, or a data steward's host?

Do not ask about populations, estimands, outcomes or outputs. Those belong to a
question, later.

## 2. Git

- **An empty folder**: `git init`, and one commit when setup is done.
- **A clean Git repository**: work in it, one commit per step below.
- **Uncommitted changes**: stop. Ask the user to commit or set them aside first.
- **Files but no Git**: stop and ask the user how to proceed.

Add no remote and push nothing unless the user asks.

## 3. What setup writes

```text
AGENTS.md  RESEARCH.md  project_data.json  run.R
src/       flat, text only: what runs on the server
tests/     local tests on small fixtures
output/    a run's export, review copies, manifest and diagnostics (ignored)
results/   approved real exports, by date
.regmeta/  mock rules (committed); mock data, keys, staging, cache (ignored)
```

1. **`RESEARCH.md`**, from [template/RESEARCH.md](template/RESEARCH.md): the user's
   answers, half a page. Leave what is not known as "not decided".
2. **The catalog.** For a data steward's host, follow the `catalog-search` skill's
   `references/data-stewards.md`. The global catalog needs nothing.
3. **The pipeline**, copied unchanged from [template/](template/): `run.R` and
   `src/pipeline.R`, `src/inputs.R`, `src/disclosure.R`, `src/staging.R`. Set `DATA_DIR`
   in `run.R` to the delivered files' absolute folder, with forward slashes, or leave it
   empty until the data is delivered. Nothing else in `run.R` is edited.
4. **The pinned files**, in `src/`: `cell_key.R` copied from
   [scripts/cell_key.R](scripts/cell_key.R), and `export_gate.R`,
   `aggregates.schema.json` and `aggregates.policy.json` from the toolkit at the
   `reg_meta` tag of the catalog release in use (the `research` skill's
   `references/export-gate.md` has the address). Check each file's SHA-256 against
   `PINS` in `run.R`. If one differs, stop and report it: never edit a pin to match.
5. **`.gitignore`**, from [template/gitignore](template/gitignore), and the empty
   `tests/` and `.regmeta/` folders. Add to an existing `.gitignore`; do not replace it.
6. **`AGENTS.md`**, from [template/AGENTS.md](template/AGENTS.md), with the project's
   values filled in. It holds pointers, not explanations: keep it that short. For Claude
   Code also write a `CLAUDE.md` with the one line `@AGENTS.md`. Add to an existing
   instructions file; do not replace it. For a server other than MONA, rewrite the batch
   section from the `research` skill's `references/operator-review.md`.

The template is R, and reads Parquet files. For a project in another language, write
everything but the pipeline, and tell the user that its run script and gate call are
theirs to write (`references/export-gate.md` in the `research` skill, "A Python pipeline").

## 4. An existing project

Do the same steps around the code that is there, in dialogue with the user:

1. List the code files and what each does. Propose where each goes: what runs on the
   server into `src/` (flat, no subfolders), tests into `tests/`, the rest left alone.
2. Agree on the plan before moving anything. Move one group at a time, fix the paths the
   move breaks, and commit each step.
3. Do not rewrite an analysis. Wiring existing code into `src/pipeline.R` and the
   disclosure stage is work for the `research` skill, one analysis at a time.

Never delete a file the user has not agreed to delete.

## 5. Hand over

Setup ends with the data, in this order:

1. The `project-data` skill writes `project_data.json`. If the data is delivered, it
   starts from a data summary the user makes on the server; if not, from the first
   order.
2. The `mock-data` skill writes mock files shaped like the delivery.

Then report: what was written, what is still missing (an empty `DATA_DIR`, an undecided
part of `RESEARCH.md`), and that the `research` skill takes the first question. Nothing
has run yet, and nothing in the folder has been checked against real data.
