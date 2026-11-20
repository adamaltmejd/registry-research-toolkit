---
name: register-research-scaffold
description: Set up a small batch-based Swedish register-data research project from the
  skill's lean R reference project, copied unchanged and run on mock data until the
  export gate passes. The project has separate local and restricted execution, `reg-mock`
  inputs, a disclosure stage and the export gate. Adds a research question to the
  project only when the user asks for one. Supports MONA (a steward's extract in, the
  toolkit export gate out) and other processing servers; does not authorize microdata
  access, transfer, or server execution.
---

# Register research scaffold

The skill has two stages.

- **Stage 1, the scaffold**, is what the skill does when invoked. It copies the R
  reference project in [reference/](reference/) unchanged into the project folder, sets
  up the catalog and the pins, runs the pipeline on mock data until the export gate
  passes, then stops and reports.
- **Stage 2, a research question**, runs only when the user asks for a research question
  to be added. It adapts the project to that one question.

Rules for both stages:

- Read only the project folder and the sources the user names. Do not open neighbouring
  folders, other repositories or the user's other projects unless the user points to
  them. The plugin's own files and the catalog tools are always available.
- Do not start stage 2 on your own, and do not propose research questions. During
  stage 1, ask nothing about research questions, populations or outputs.
- Preserve a user's existing project; create a separate project when requested. A
  different stack (Python with DuckDB, say) keeps the reference's stages and boundaries
  in its own tools.
- Do not build a custom workflow framework, research-record database, RAG service or
  independent mock generator. Do not configure hosted Git or transfer data through it
  without an explicit request.

Read [contracts.md](references/contracts.md) before implementation. Read
[execution.md](references/execution.md) when wiring batches, packaging, and transfers.
Read [operator-review.md](references/operator-review.md) when preparing human-operated
transfer and model-access approval. Read [export-gate.md](references/export-gate.md)
when wiring the disclosure stage (`scripts/cell_key.R`, which needs the `openssl` R
package) or the final release stage: the toolkit's export gate, its pinned version and
how a MONA project calls it. Read [mona.md](references/mona.md) for a project on MONA.

## On MONA

No agent runs on MONA, and row-level data never leaves it: only aggregates the export
gate passed and a human reviewed. The agent works locally on code, catalog metadata and
mock data. A project whose data a data steward delivers runs in this order
([mona.md](references/mona.md) has who does what):

1. Add the steward's catalog to the project as its `steward-catalog` server, in
   `.mcp.json` (Claude Code) or `.codex/config.toml` (Codex), and record in the project
   instructions that it is the catalog to use (`register-metadata-search` skill,
   `references/data-stewards.md`).
2. Draft and validate `project_data.json` with the `register-project` skill.
3. Generate mock inputs from it with the `register-mock-data` skill (`reg-mock`).
4. Build and run the pipeline locally on the mocks, ending with `export_gate.R`.
5. The steward materializes the order and runs `extract.R` on MONA; the researcher runs
   the same pipeline on the extract, reviews the gate's export and sends it out.

Stage 1 does step 1, and steps 3 and 4 on the reference's own order. Stage 2 does steps
2 to 4 for the user's question. Steps 2 to 4 need no MONA access, so a project can be
complete and tested before its extract exists.

## The execution boundary

Do not inspect microdata, restricted caches or credentials merely to infer a schema.

Server connections are researcher-operated. Do not request or use the researcher's SSH
access, inspect private connection configuration, or download results for them. Prepare
placeholders, operator commands and local mock simulations; the researcher supplies
server details privately. Restricted compute has no remote model access. No AI
invocation or model API dependency belongs in the batch runner. Keep agent-readable
local inputs to code, documentation, clearly fake data, and aggregate releases
explicitly approved by a human for model access. An automatic disclosure pass alone
never authorizes loading real outputs into model context, including through locally
running tools connected to a hosted model. Never configure automatic GitHub
synchronization. Git records history locally; add approved institutional remotes only
when requested.

## Stage 1: the scaffold

Ask the user only what setup needs: where the project lives, and whether its data comes
from a data steward (and that steward's host) or from the global catalog. Ask nothing
about research questions, populations or outputs.

1. Copy [reference/](reference/) unchanged into the project folder, with `PROJECT.md`
   as the project's `CLAUDE.md` or `AGENTS.md`. Change no code, order, mock file or
   ticket. The project keeps the reference's example until stage 2: LISA Individer
   2021–2022 from the global catalog, and the ticket `q001`, persons and mean disposable
   income by sex and year. In a folder that already holds files, overwrite none; ask
   where a name collides.
2. Set up the catalog. For a steward's project, add the steward's catalog as the
   `steward-catalog` server in the project's `.mcp.json` (Claude Code) or
   `.codex/config.toml` (Codex), and record it in the project instructions, following
   the `register-metadata-search` skill's `references/data-stewards.md`. A project on
   the global catalog needs no server: the plugin's `catalog` is already there.
3. Set the pins. Fill `export_gate/`: `export_gate.R` and the default schema and policy
   from the `reg_meta` tag the project's `reg_meta_version` names
   ([export-gate.md](references/export-gate.md)), and `cell_key.R` copied unchanged from
   [scripts/](scripts/cell_key.R). Check each file's version and SHA-256 against the pin
   table in the project instructions; a file that differs is the wrong file, so do not
   edit the table to match it. Use the `reg-mock` release the same tag names.
4. Generate the mock inputs from the committed `mock/mock_spec.json` with
   `reg-mock generate` (the command is in the project instructions). The reference
   ships its saved answers and compiled spec, so this needs no catalog call. The mock
   and key folders stay outside the project, where `config.R` puts them.
5. Commit. A new project folder gets `git init` first: a mock run reads its commit. In
   a folder that is already a Git repository, ask before committing. Commit everything
   stage 1 wrote: the copied files, the catalog configuration and the four files in
   `export_gate/`.
6. Run `Rscript make_keys.R mock` once and `Rscript run.R mock` until the gate passes.
   A failure here is a setup fault (a missing R package, the wrong `reg-mock` release, a
   path): fix the setup, not the reference's code, the schema or the policy.
7. Stop and report: the project path, the files that exist, the pins, the commit and the
   mock run's result. Name the two folders created beside the project folder, the mock
   inputs and the mock key file (`MOCK_DIR` and `KEY_FILE` in `config.R`). Say that the
   project holds the reference's example order and question, not the user's. With a
   steward's catalog, tell the user to restart the session in the project and approve
   the `steward-catalog` server before any catalog work; the tools are not loaded until
   then. Say that a research question is added when the user asks for one. Do not
   propose one, and do not continue to stage 2.

## Stage 2: a research question, on request

This stage runs only when the user asks for a research question to be added and names
the question. The agent does not start it on its own and does not propose questions. It
handles one question per request, with one Markdown ticket per question in `questions/`,
and stops when that question runs on mock data.

In a steward's project, check first that the `steward-catalog` tools are loaded. If they
are not, ask the user to restart the session in the project and stop.

1. Establish what the question needs, from the user and from the sources the user
   names: entity and grain, keys, source definitions, measurement timing, population,
   missingness and the outputs. Source code the user points to is evidence of an
   intended contract, not confirmation of real data. Distinguish approved choices from
   proposed choices. Ask about ambiguity that changes the estimand or population;
   otherwise state a reversible assumption. Keep unresolved scientific definitions,
   source coverage and disclosure policy visibly pending; do not label real-data
   integration verified.
2. Write the ticket, `questions/<id>-<short-name>.md`
   ([contracts.md](references/contracts.md), "Questions"). The project's first question
   replaces the example: delete `q001` and its code as the steps below replace them. A
   later question adds to the project and leaves earlier questions in place.
3. Put the question's data in the order: `project_data.json` with the
   `register-project` skill, on the project's catalog, then `mock/` with its saved
   answers, rules and compiled spec (`register-mock-data` skill). In a steward's project
   the steward's release sets `reg_meta_version`; when it differs from the pinned one,
   replace `export_gate/` and the `reg-mock` binary from that release's tag and update
   the pin table in the same commit.
4. Adapt the code to the question: the delivered files and column names in `config.R`
   (from the order answer; a steward's extract keys persons by `LopNr`), the source
   adapter (`R/inputs.R`), the analysis (`R/analysis.R`), the cells and statistics in
   `R/disclosure.R` (its suppress, perturb, round order stays) and the calls in `run.R`.
5. Update the project instructions (`CLAUDE.md` or `AGENTS.md`) to match: the title,
   the paragraph about the example, the commands, the names under "Before MONA" and the
   pin table.
6. Generate the mock inputs again, commit, and run `Rscript run.R mock` until the gate
   passes. A prototype can run on mocks while parts of the question remain open.
7. Stop and report: the ticket, what changed, the commit, the mock run's result and what
   is still pending. The ticket's answer stays pending: a mock run is not an answer.

## What every project keeps

Keep the parts that carry a rule, whatever the question:

- `run.R mock|real` first removes that mode's earlier export, using nothing sourced,
  then checks the mode's input folder and never falls back; mock and real runs write
  to separate `out/<mode>/` folders. A file's year comes from the order answer
  (`LISA_FILES` in `config.R`), never from its name.
- A mock input must be listed in `reg-mock`'s `mock_manifest.json` and carry its
  `reg_mock` Parquet metadata; a real input must not carry it.
- An analysis returns person-level rows, never counts or statistics. The one disclosure
  stage (`R/disclosure.R`) is the only code that makes a publishable number: suppress,
  perturb with `ck_perturb()`, round ([export-gate.md](references/export-gate.md), "The
  disclosure policy").
- The staging writer (`R/staging.R`) writes every file the schema declares, exactly its
  columns and types, into a fresh staging folder; the gate call is the run's last step.
- The key file and the run record (with its `key_epoch`) stay outside the export; the
  key file outside the project.

Build later, when the need arrives:

- Before the first export is imported: the import check against `receipt.json`
  ([export-gate.md](references/export-gate.md), "Importing an export"). No export is
  read until that check exists and passes.
- After a first real export: a local HTML view of tickets and results, paper and slides.
  Mock outputs are visibly labelled mock.
- When a run's time matters: a dependency-aware build tool (`targets` in R), per
  [execution.md](references/execution.md), "Incremental builds".
- On a server other than MONA: the operator's `sync.sh`
  ([operator-review.md](references/operator-review.md)).

Do not build a ticket validator, a hash manifest of the mock inputs (`reg-mock` output
is fixed by the committed spec and seed and labels itself), or a test that the gate
refuses a bad file (the gate's own case corpus covers it).

Remote artifacts are exactly the aggregate files the chosen export-gate schema declares:
one Parquet file per schema file, a suppressed cell null (never zero), plus the gate's
`receipt.json`. Render tables and figures locally from those files; deterministic
human-only previews may be generated before approval, but models must not see their
contents until approval.

Routine execution includes only active analyses. A closed ticket stays in Git and its
code at its execution revision; promote what current research still needs into
maintained code rather than archiving live dependencies.

The mock generator is `reg-mock` (the `register-mock-data` skill), not part of this
skill or a new subsystem to build. For a trial without it, the user may provide an
independent wholly invented mock sample; keep it separate from unit fixtures and
identify its limitations. Never derive unit fixtures from mock data or restricted
records.

The reference pins catalog `reg_meta/v0.91.0` and the gate version in
[export-gate.md](references/export-gate.md); a gate update changes both together.

## Verification proportional to risk

Keep these jobs separate:

- Unit tests run locally only, on minimal hand-authored fixtures. Add a test only for a
  named failure not already protected. Prefer extending a focused test. Do not test
  every helper, mirror implementation details, or add large snapshots/combinatorial
  matrices.
- The actual pipeline must run locally on mock inputs before every remote handoff. The
  same data assertions run on mock and real inputs. Fix mock contracts rather than
  adding mock-only bypasses. Assertions protect design requirements; diagnostics answer
  open questions.
- When a transfer artifact exists, execute that exact artifact against mock data in an
  isolated directory. This can also satisfy the full mock-pipeline gate; avoid two
  identical full runs.
- Every remote batch must invoke the toolkit's export gate as its final computational
  stage, before any result becomes downloadable: on MONA, the pinned `export_gate.R`
  with the project's chosen schema and policy (see
  [export-gate.md](references/export-gate.md)). This is mandatory on MONA and other
  servers; see the release contract. The same call runs last in every local mock run.
  Keep real release disabled until the project's outputs are shaped to the chosen
  schema and policy. Local success does not establish real coverage, findings, performance or
  disclosure clearance.

Use relevant tests/targets while iterating; run the complete small suite and full mock
gate on the final execution revision. Record commands, results and runtime. At batch
completion consolidate obsolete/redundant tests, retaining meaningful regressions and
independent scientific/disclosure checks. Do not impose coverage percentages or
arbitrary test-count quotas.

## Finish with evidence

Commit executable inputs: in a new project folder without asking, in an existing
repository after the user's go-ahead. Keep
source/configuration/lockfiles free of individual data and stage specific paths. Run
from the commit, not a dirty working directory. Record answers in later commits
referencing that execution revision; a mock pass is not an empirical answer.

Demonstrate one local mock run from the commit that ends in a gate pass, with its run
record. Use Git plus ticket status and a recorded command; do not build an archival
subsystem.

Report usable project path, skill version, execution commit, checks, and operator
prerequisites. Leave restricted connections, execution, downloads and approval of real
results to the researcher. Scaffolding for a real project permits source-led
preparation; it does not authorize exposing microdata to the agent.
