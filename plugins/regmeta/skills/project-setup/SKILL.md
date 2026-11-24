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
3. Does it run on MONA? If so, the project number (`P` and four digits) and the folder
   the project's code goes in on MONA. MONA normally gives a project `P<nnnn>_Data`
   (read-only; delivered files, often in a subfolder) and `P<nnnn>_Gem` (shared space for
   the code). Propose `P<nnnn>_Gem\<project>` for the code, and let the user correct it:
   some projects use another layout. Never assume the path. If not on MONA, where.
4. Is the data delivered? If so, the absolute folder the files are in (on MONA by default
   `P<nnnn>_Data\<subfolder>`, to confirm with the user), and their format.
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
rproject.toml  rv/  .Rprofile  air.toml  jarl.toml    local R tooling
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
   in `run.R` to the delivered files' absolute folder from the answer to question 4, with
   forward slashes, or leave it empty until the data is delivered. Later only `PACKAGES`
   changes, when the project adds a package.
4. **The pinned files**, in `src/`: `cell_key.R` copied from
   [scripts/cell_key.R](scripts/cell_key.R), and `export_gate.R`,
   `aggregates.schema.json` and `aggregates.policy.json` from the toolkit at the
   `reg_meta` tag of the catalog release in use (the `research` skill's
   `references/export-gate.md` has the address). Check each file's SHA-256 against
   `PINS` in `run.R`. If one differs, stop and report it: never edit a pin to match.
5. **Local R tooling.** None of it goes to the server.
   - Packages with [rv](https://github.com/A2-ai/rv): in the project folder,
     `rv init --add targets --add arrow --add jsonlite --add openssl`, then `rv sync`.
     The names are `PACKAGES` in `run.R`; a mock run stops if the two lists differ.
     Commit `rproject.toml`, the lock file, `rv/scripts/` and the `.Rprofile` rv writes.
   - Add the line `Sys.setenv(REGMETA_MOCK = "1")` to that `.Rprofile`. A local run is
     then always a mock run: `Rscript run.R`.
   - Formatting with [air](https://posit-dev.github.io/air/) and linting with
     [jarl](https://jarl.etiennebacher.com/): copy [template/air.toml](template/air.toml)
     and [template/jarl.toml](template/jarl.toml). Both leave the pinned files and `rv/`
     alone.

   If `rv`, `air` or `jarl` is not installed, say which and give the user the install
   command to run; do not install tools yourself. Without rv, skip `.Rprofile`: a mock
   run is then `REGMETA_MOCK=1 Rscript run.R`. A project that already has a package
   manager (renv, its own `.Rprofile`) keeps it; add the mock line only with the user's
   agreement.
6. **`.gitignore`**, from [template/gitignore](template/gitignore), and the empty
   `tests/` and `.regmeta/` folders. Add to an existing `.gitignore`; do not replace it.
7. **`AGENTS.md`**, from [template/AGENTS.md](template/AGENTS.md), with the project's
   values filled in, including the MONA project number and the code folder from
   question 3. It holds pointers, not explanations: keep it that short. If the
   project already has an instructions file (`AGENTS.md` or `CLAUDE.md`), add to it; do
   not replace it or write a second one. For a server other than MONA, rewrite the batch
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
