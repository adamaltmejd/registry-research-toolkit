# Independent installation trial: education-returns descriptive pilot

Date: 2026-09-28. Outcome: complete local scaffold/workflow trial; restricted-server integration remains unverified and real execution/import are disabled.

## Setup and deliverables
A fresh agent with no inherited conversation received the installed skill and the standalone [brief](brief.md), plus an empty Git repository. Skill input revision: `39e7545` (after a wording clarification separating research-record storage from the DuckDB query engine). The agent built the project, received narrow independent review, fixed the findings and completed verification. This was a reviewed trial, not an unassisted success claim.

Final local project: `field-earnings-descriptives`. Local Git only, no remote. The education-returns and upstream reference repositories were read only; no individual data, cached targets, credentials or server were inspected.

Pilot: Python/DuckDB, two Markdown research questions, explicit provisional attained-degree definitions, safe lookup provenance, private R FST-to-Parquet bridge, committed single-file package, input/output integrity checks and an offline HTML workbench with paper/slides targets. There is no Polars conversion layer, RAG, research-record database, workflow framework or mock-generator subsystem. The supplied invented sample is ignored and portable separately from Git and unit fixtures.

## Observed checks
- Five focused fixture-only unit tests passed (0.143 s); Ruff format/check passed.
- Active execution commit: `4922ad8acae4327d47a0467d844e89adc9881af5`.
- Active zipapp SHA256: `6fca019676d1de4d330b7915509c48f3596505d5b123f9f2f3fcbeb294fcfaad`; rebuilding that revision produced the same bytes.
- Exact artifact executed in an isolated directory against approved invented inputs. Full active mock pipeline took 0.043 s; this tiny run is not a performance benchmark.
- Result ZIP validated/imported; offline workbench, paper and slides generated.
- Closed workflow ticket excluded by default; explicit rerun from original `1ae2565d70acbc9ac8f765bd3ac32d3847fb7a5c` worked. Empirical tickets remain open.
- Altered/unapproved mock inputs and malformed releases are rejected; real mode/import fail closed.
- R bridge syntax passed. FST/Arrow conversion was not run because the clean R environment lacked those packages.
- Browser visual review was blocked by the browser's local-file URL policy; report generation/static validation are the only UI checks.

Detailed run IDs, commands and hashes are in the pilot's `evidence/verification.md`. Final pilot evidence commit: `5f07119`.

## Findings fed back into the skill
1. Mock mode must be bound to approved invented inputs; a flag or label alone could accidentally allow real files into an unrestricted mock release. This pilot uses committed hashes, not committed mock records.
2. Archive exclusion means skipping question-specific computation, not merely hiding outputs. A workflow-only rerun must also report only checks it actually ran.
3. Source-validity assertions and analysis eligibility differ. Valid cohorts born after the outcome year must not fail the global birth-year assertion.

The generic instructions were amended after this trial. These amendments have been statically validated and reviewed against the pilot; a second fresh installation of the revised skill has not been run.

## Next real-data batch
Researcher confirmation of population and timing comes first. An operator must verify actual server schemas/versions, source coverage and the R bridge. Real disclosure support needs a concrete institution-approved policy, restricted candidate-versus-cleared output separation, review metadata and matching importer checks. These are required implementation tasks, not a switch silently enabled by this scaffold.

The actual Linux server and MONA compatibility remain untested. No real result, disclosure clearance, stack speed comparison or causal return estimate is claimed.

