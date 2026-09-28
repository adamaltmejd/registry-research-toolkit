# Execution and transfer

Use existing tools and the smallest runner with explicit inputs, failure propagation and rerun commands. Do not build a general workflow engine for a two-question trial.

## Local and remote responsibilities

Local: fixture tests, full mock pipeline/assertions, exact-artifact mock check, packaging, verified import, interpretation, figures/tables, HTML, paper and slides.

Restricted server: ingestion, persistent private intermediate caches, real assertions, investigations, aggregate data files and the mandatory final automatic disclosure check. MONA returns a checked ZIP; flexible servers return a checked file set through rsync. No model calls or automatic raw-data synchronization. Tables and figures are rendered locally.

Mode is explicit, never guessed from OS/path. Missing real sources halt; never fall back to mock. Verify that mock inputs belong to an approved invented input set before reading them; a mode flag or filename is not sufficient. For a small trial, a committed content-hash manifest can bind the ignored mock files without tracking their records. Keep that manifest distinct from unit fixtures. Separate caches/output by mode. Refuse input/output overlap. Source readers must not alter reference-project inputs or stores.

## MONA

Target one uploadable execution file and one result ZIP per batch. Python zipapp is a candidate when permitted; a supported plain-source format is another. Verify accepted formats and runtime before claiming compatibility. Preserve modular source locally; do not concatenate modules blindly. Include small required nonpersonal lookup/configuration assets.

Build from an explicit commit and source allowlist. Exclude .git, datasets, caches, logs, secrets and unrelated investigations. Record commit and file hashes. Dependencies belong in a prepared environment; never download during research execution. Zipapps cannot load native extension binaries directly from inside the archive.

Preflight runtime, dependencies, inputs and writable locations before expensive work. Exportable diagnostics must not print rows, arbitrary values or raw tracebacks.

Use manual MONA upload/export. A simulated local run establishes packaging only. Do not open the restricted desktop or use a remote model to inspect its data.

## Other servers

Use rsync over SSH for the flexible-server adapter. Send an allowlisted committed source snapshot plus manifest into a fixed code location; a package may be a staged directory rather than a zipapp. Download only the current automatically disclosure-checked result set into human-only local review storage. Use constrained transfer credentials (for example rrsync with direction/path restrictions plus filesystem permissions); an agent-facing credential must not expose raw inputs, private caches, logs or unreviewed result contents. Directory-restricted rsync alone does not enforce the human-review gate: its credential can still retrieve every file in that directory. For the MVP, have the human run result downloads under the review identity, or use a constrained helper whose credentials and destination are inaccessible to the agent. Expose only fixed status responses to the agent until explicit human approval. Download capability must not imply read access by the model. Do not infer credentials, connectivity or permission from a source path. No real transfer is authorized merely by scaffolding.

Use fixed code/, cache/, candidate/ and output/ locations, not a new retained directory for every run. Coding, Git history and research evidence stay local; do not build remote backup/history management. Preserve cache and build-tool metadata across deployments. Replace current code and outputs under an execution/transfer lock so a running batch cannot observe mixed source versions and a download cannot mix result sets. Bound temporary staging space, validate the manifest, then promote completed files; never expose partially written results. Scope any rsync deletion to the exact managed code/output subtree, never the source-data root or cache. Do not use in-place transfer into files being read by a running process. Invalidate the completion marker before a new run; a failed batch must not make stale results appear current.

Do not return a data-bearing Git working tree or route results through GitHub. Code may be published to GitHub later when requested; downloading results and admitting them to model context remain separate operations.

## Incremental builds

Use an established dependency-aware tool (for example targets in R or Snakemake for a Python/file-based pipeline) rather than writing a cache manager. A Makefile may expose convenient commands; if Make is the dependency engine, explicitly handle code/configuration/environment changes rather than relying on output existence alone. This skill does not mandate a particular engine.

Keep one current cached artifact per active target plus the small dependency state required by the engine. Model the dependencies between source versions, source adapters, processing scripts and imported helpers, configuration/parameters, dependency locks, analyses and output writers. Declare dependencies explicitly; do not assume a workflow engine discovers every imported helper or changed external dataset. Split processing and investigation code so changing one regression does not invalidate unrelated preprocessing. Preserve unchanged source metadata or use appropriate stage-specific fingerprints during deployment; do not make the global Git commit hash a dependency of every target. Record that commit in provenance separately.

A regression-only edit should reuse valid analysis data and rerun that regression and its downstream exports. An upstream data/processing change must invalidate affected descendants. Partial target files must never count as valid cache entries. Use separate mock/real caches, rebuild affected assertions with their data, and prune obsolete generated caches when their active dependencies disappear. Never retain a full historical cache per commit.

Always execute the automatic disclosure checker over the complete current candidate output set at the end, even when all analytical targets were reused. Reassemble only requested current outputs so stale files cannot leak into a new result set. Retain no old remote release history; use the locally tracked release record as input when the project policy needs prior-disclosure comparisons, without exposing unreviewed content to the model. Verify cache reuse and invalidation on mocks with a small targeted check before remote handoff.

## Local handoff gate

1. Validate ticket/batch dependencies and configuration.
2. Run the small fixture-only suite.
3. Package the committed execution revision.
4. Run that package on separate mock inputs in a fresh directory using the actual pipeline/assertions.
5. Verify and import the labelled mock ZIP, then build local reports.
6. Record revision, artifact digest, commands, runtimes and outcomes outside the execution snapshot. Real run/release remain pending until completed.

Exercise one minimal release-boundary rejection fixture (e.g. undeclared or altered file). Do not proliferate tests of command wording.

## Archive

Default builds select active tickets and skip closed question-specific computations, not just their output files. Shared processing remains active only while selected work needs it. Closed investigations remain in Git. Rerun their recorded commit and command in an isolated checkout. Do not create growing Git bundles or repository copies per investigation. Transfer artifacts can be deleted and rebuilt from retained commits. Reproducibility also requires permitted retention of data versions and dependencies within their environment.
