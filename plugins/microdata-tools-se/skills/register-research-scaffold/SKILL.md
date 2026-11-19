---
name: register-research-scaffold
description: Scaffold a small batch-based Swedish register-data research project from a
  research brief and documented source contracts, starting from the skill's lean R
  reference project: question tickets, separate local and restricted execution,
  `reg-mock` inputs, a disclosure stage and the export gate. Supports MONA (a steward's
  extract in, the toolkit export gate out) and other processing servers; does not
  authorize microdata access, transfer, or server execution.
---

# Register research scaffold

Create the smallest working project that answers the user's first research questions,
starting from the R reference project in [reference/](reference/) (below). Preserve a
user's existing project; create a separate project when requested. A different stack
(Python with DuckDB, say) keeps the reference's stages and boundaries in its own tools.
Do not build a custom workflow framework, research-record database, RAG service or
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

Steps 2 to 4 need no MONA access, so a project can be complete and tested before its
extract exists.

## Establish the research and execution boundary

Read supplied source code/documentation and applicable instructions. Establish
entity/grain, keys, source definitions, measurement timing, population, missingness and
output questions. Record exact reference-repository commits and relevant paths. Do not
inspect microdata, restricted caches or credentials merely to infer a schema. Source
code is evidence of an intended contract, not confirmation of real data.

Distinguish approved choices from proposed choices. Ask about ambiguity that changes the
estimand or population; otherwise state a reversible assumption. A prototype can run on
mocks while questions remain open. Keep unresolved scientific definitions, source
coverage and disclosure policy visibly pending; do not label real-data integration
verified.

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

## Scaffold only what the first batch needs

The first scaffold is [reference/](reference/) adapted to the user's first question:
about 150 lines of the project's own code. The reference orders LISA Individer
2021–2022 from the global catalog and publishes persons and mean disposable income by
sex and year. Copy it to the project folder, `PROJECT.md` as the project's `CLAUDE.md`
or `AGENTS.md`, then:

1. Replace `project_data.json` with the user's order (`register-project` skill), and
   `mock/` with its saved answers, rules and compiled spec (`register-mock-data` skill).
   Set the paths in `config.R`; the mock and key folders stay outside the project.
2. Fill `export_gate/`: `export_gate.R` and the chosen schema and policy from the
   `reg_meta` tag [export-gate.md](references/export-gate.md) names, checked against its
   SHA-256, and `cell_key.R` copied unchanged from [scripts/](scripts/cell_key.R).
   Record each file's version and SHA-256 in the instructions' pin table. Commit all four.
3. Rewrite for the question: the delivered files and column names in `config.R`
   (from the order answer; a steward's extract keys persons by `LopNr`), the source
   adapter (`R/inputs.R`), the analysis (`R/analysis.R`) and the cells and statistics
   in `R/disclosure.R` (its suppress, perturb, round order stays). One Markdown ticket
   per question in `questions/`.
4. Run `Rscript make_keys.R` once and `Rscript run.R` on the mocks until the gate passes.

Keep the parts that carry a rule, whatever the question:

- `run.R` picks the mode once from which input folder exists, prints it and never
  falls back; mock and real runs write to separate `out/<mode>/` folders.
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
skill or a new subsystem to build. For a trial without it, the brief may provide an
independent wholly invented mock sample; keep it separate from unit fixtures and
identify its limitations. Never derive unit fixtures from mock data or restricted
records.

The reference pins catalog `reg_meta/v0.90.0` and the gate version in
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

Commit executable inputs when the user has authorized a committed-code trial. Keep
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
