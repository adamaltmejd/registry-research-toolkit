---
name: register-research-scaffold
description: Scaffold a small batch-based Swedish register-data research project from a research brief and documented source contracts. Creates Git-tracked investigation tickets, separate local and restricted execution, mock validation, evidence provenance, and local reporting. Supports MONA and other processing servers; does not authorize microdata access, transfer, or server execution.
---

# Register research scaffold

Create the smallest working project that answers the user's first research questions and demonstrates the workflow below. Preserve an existing reference project; create a separate project when requested. Use the chosen language/build tools, not the reference project's stack by default. Do not build a custom workflow framework, research-record database, RAG service or independent mock generator for an MVP. Use an established dependency-aware build tool when incremental execution is needed; keep its choice independent of the data-processing engine. Do not configure hosted Git or transfer data through it without an explicit request.

Read [contracts.md](references/contracts.md) before implementation. Read [execution.md](references/execution.md) when wiring batches, packaging, and transfers.

## Establish the research and execution boundary

Read supplied source code/documentation and applicable instructions. Establish entity/grain, keys, source definitions, measurement timing, population, missingness and output questions. Record exact reference-repository commits and relevant paths. Do not inspect microdata, restricted caches or credentials merely to infer a schema. Source code is evidence of an intended contract, not confirmation of real data.

Distinguish approved choices from proposed choices. Ask about ambiguity that changes the estimand or population; otherwise state a reversible assumption. A prototype can run on mocks while questions remain open. Keep unresolved scientific definitions, source coverage and disclosure policy visibly pending; do not label real-data integration verified.

Restricted compute has no remote model access. No AI invocation or model API dependency belongs in the batch runner. Keep agent-readable local inputs to code, documentation, clearly fake data, and aggregate releases explicitly approved by a human for model access. An automatic disclosure pass alone never authorizes loading real outputs into model context, including through locally running tools connected to a hosted model. Never configure automatic GitHub synchronization. Git records history locally; add approved institutional remotes only when requested.

## Scaffold only what the first batch needs

Provide:
- Concise project instructions recording actual boundaries, chosen tools and commands.
- A documented source adapter, explicit mock/real configuration, and analysis/configuration files for one small batch.
- Markdown question tickets with YAML frontmatter and dependency links; use filesystem search and metadata, not a separate database.
- A way to package an explicit committed revision, run it incrementally with persistent restricted caches, validate outputs, and download them into human-only review storage before explicit approval for model access. Use bounded current code/output locations rather than accumulating remote run directories.
- A simple local HTML view of tickets, batch state, tables and evidence. Paper and slides are researcher-facing build targets too; prototype outputs must be visibly labelled mock and need not imitate a completed manuscript.
- A short runbook stating what was checked locally and what still requires restricted compute or the researcher.

Remote artifacts are declared aggregate data files, preferably CSV with a data dictionary and explicit suppression markers. Render tables and figures locally from those files; deterministic human-only previews may be generated before approval, but models must not see their contents until approval.

Routine execution includes only active processing, assertions and selected analyses. Completed investigations remain tracked and discoverable but do not run unless selected. Preserve original executable code and execution revision. Promote construction rules or analyses still needed by current research into maintained code rather than archiving live dependencies.

The mock generator is an external executable/CLI, not part of this skill or a new subsystem to build. Consume its declared outputs if available. For a trial without it, the brief may provide an independent wholly invented mock sample; keep it separate from unit fixtures and identify its limitations. Never derive unit fixtures from mock data or restricted records.

## Verification proportional to risk

Keep these jobs separate:
- Unit tests run locally only, on minimal hand-authored fixtures. Add a test only for a named failure not already protected. Prefer extending a focused test. Do not test every helper, mirror implementation details, or add large snapshots/combinatorial matrices.
- The actual pipeline must run locally on mock inputs before every remote handoff. The same data assertions run on mock and real inputs. Fix mock contracts rather than adding mock-only bypasses. Assertions protect design requirements; diagnostics answer open questions.
- When a transfer artifact exists, execute that exact artifact against mock data in an isolated directory. This can also satisfy the full mock-pipeline gate; avoid two identical full runs.
- Every remote batch must invoke the plugin-supplied automatic disclosure checker as its final computational stage, before any result becomes downloadable. This is mandatory on MONA and other servers; see the release contract. Keep real release disabled while the checker or project policy is missing. Local success does not establish real coverage, findings, performance or disclosure clearance.

Use relevant tests/targets while iterating; run the complete small suite and full mock gate on the final execution revision. Record commands, results and runtime. At batch completion consolidate obsolete/redundant tests, retaining meaningful regressions and independent scientific/disclosure checks. Do not impose coverage percentages or arbitrary test-count quotas.

## Finish with evidence

Commit executable inputs when the user has authorized a committed-code trial. Keep source/configuration/lockfiles free of individual data and stage specific paths. Run from the commit, not a dirty working directory. Record answers in later commits referencing that execution revision; a mock pass is not an empirical answer.

Demonstrate one complete local batch transfer roundtrip, a rejection case at the release boundary, and that a closed investigation is excluded from default execution yet can be selected at its original revision. Use Git plus ticket status and a recorded command; do not build an archival subsystem.

Report usable project path, skill version, execution commit, checks, and precise server prerequisites. Do not connect to restricted compute or run real inputs unless specifically authorized and compatible with the access boundary. Scaffolding for a real project permits source-led preparation; it does not authorize exposing microdata to the agent.
