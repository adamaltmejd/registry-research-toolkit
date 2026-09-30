# Execution and transfer

Use existing tools and the smallest runner with explicit inputs, failure propagation and rerun commands. Do not build a general workflow engine for a two-question trial.

## Local and remote responsibilities

Local: fixture tests, full mock pipeline/assertions, exact-artifact mock check, packaging, verified import, interpretation, figures/tables, HTML, paper and slides.

Restricted server: ingestion, persistent private intermediate caches, real assertions, investigations, aggregate data files and the mandatory final automatic disclosure check. MONA returns a checked ZIP; flexible servers return a checked file set through rsync. No model calls or automatic raw-data synchronization. Tables and figures are rendered locally.

Configure real and mock input directories outside the project. Select real mode when the real directory exists; otherwise select mock when its directory exists; fail when neither exists. Select once before running. An unreadable directory, invalid input or failed assertion in the selected mode halts execution; never retry with another mode. Print the selected mode prominently. Verify that mock inputs belong to an approved invented input set before reading them; a mode flag or filename is not sufficient. For a small trial, a committed content-hash manifest can bind the ignored mock files without tracking their records. Keep that manifest distinct from unit fixtures. Derive separate cache, result and local-render paths from the selected mode. Keep generated paths and machine-specific configuration gitignored and outside code uploads. Mock runs must never overwrite real outputs; missing real results must never make a paper or slide build use mock results. Refuse input/output overlap. Source readers must not alter reference-project inputs or stores.

## MONA

Target one uploadable execution file and one result ZIP per batch. Python zipapp is a candidate when permitted; a supported plain-source format is another. Verify accepted formats and runtime before claiming compatibility. Preserve modular source locally; do not concatenate modules blindly. Include small required nonpersonal lookup/configuration assets.

Build from an explicit commit and source allowlist. Exclude .git, datasets, caches, logs, secrets and unrelated investigations. Record commit and file hashes. Dependencies belong in a prepared environment; never download during research execution. Zipapps cannot load native extension binaries directly from inside the archive.

Preflight runtime, dependencies, inputs and writable locations before expensive work. Exportable diagnostics must not print rows, arbitrary values or raw tracebacks.

Use manual MONA upload/export. A simulated local run establishes packaging only. Do not open the restricted desktop or use a remote model to inspect its data.

## Other servers

Use operator-run rsync over SSH with independent upload, run and download steps. The agent prepares source selection, placeholder configuration and reviewed commands; the researcher privately configures and runs the connection. Never ask for server aliases, credentials or private paths, and never inspect or invoke the operator's connection configuration. A local fake-data simulation can test the transfer sequence without a network connection.

Keep the same source layout locally and remotely. Upload an explicit committed source snapshot, or Git-selected tracked files after verifying the tracked working tree is clean and matches the recorded commit. Exclude .git and private/generated files; Git history stays local and remote Git is unnecessary. Handle removed source files within the managed source scope without deleting caches, results or environment files. Do not use .gitignore as the download selector: downloads select only the designated checked export.

The researcher uploads while no batch is running, inspects the uploaded code, then runs it manually (for a Python project, a simple command such as `uv run run.py`). Provision dependencies separately; do not sync local virtual environments. Preserve private cache and build-tool metadata across uploads. A pipeline lock prevents concurrent runs; operator procedure prevents upload/run overlap. Do not introduce a cross-step deployment lock or an all-in-one script that invokes remote execution by default.

Use fixed current paths for code, mode-specific caches/results and exports. Invalidate completion before a new run; a failure must not present stale output as current. Reassemble the export from declared current outputs, check its entire contents, and publish only after successful completion. Keep temporary staging bounded. The researcher reviews the checked export remotely before explicitly downloading it; alternatively download into private human-only storage and review there. Review need not happen twice. Keep raw logs, intermediates and caches outside the export; see [operator-review.md](operator-review.md).

Do not return a data-bearing Git working tree or route results through GitHub. Code may be published to GitHub later when requested. Only human-approved exports may become readable by the model/tool identity; an automatic check alone never grants that approval.

## Incremental builds

Use an established dependency-aware tool (for example targets in R or pytask for a Python/file-based pipeline) rather than writing a cache manager. A Makefile may expose convenient commands; if Make is the dependency engine, explicitly handle code/configuration/environment changes rather than relying on output existence alone. This skill does not mandate a particular engine.

Keep one current cached artifact per active target plus the small dependency state required by the engine. Model the dependencies between source versions, source adapters, processing scripts and imported helpers, configuration/parameters, dependency locks, analyses and output writers. Declare dependencies explicitly; do not assume a workflow engine discovers every imported helper or changed external dataset. Split processing and investigation code so changing one regression does not invalidate unrelated preprocessing. Preserve unchanged source metadata or use appropriate stage-specific fingerprints during deployment; do not make the global Git commit hash a dependency of every target. Record that commit in provenance separately.

A regression-only edit should reuse valid analysis data and rerun that regression and its downstream exports. An upstream data/processing change must invalidate affected descendants. Partial target files must never count as valid cache entries. Use separate mock/real caches, rebuild affected assertions with their data, and prune obsolete generated caches when their active dependencies disappear. Never retain a full historical cache per commit.

Always execute the automatic disclosure checker over the complete current candidate output set at the end, even when all analytical targets were reused. Reassemble only requested current outputs so stale files cannot leak into a new result set. Retain no old remote release history; use the locally tracked release record as input when the project policy needs prior-disclosure comparisons, without exposing unreviewed content to the model. Verify cache reuse and invalidation on mocks with a small targeted check before remote handoff.

## Local handoff gate

1. Validate ticket/batch dependencies and configuration.
2. Run the small fixture-only suite.
3. Prepare the committed execution snapshot (one file for MONA, source files for a flexible server).
4. Run that snapshot on separate mock inputs in a fresh directory using the actual pipeline/assertions.
5. Verify and import the labelled mock export, then build local reports.
6. Record revision, artifact digest, commands, runtimes and outcomes outside the execution snapshot. Real run/release remain pending until completed.

Exercise one minimal release-boundary rejection fixture (e.g. undeclared or altered file). Do not proliferate tests of command wording.

## Archive

Default builds select active tickets and skip closed question-specific computations, not just their output files. Shared processing remains active only while selected work needs it. Closed investigations remain in Git. Rerun their recorded commit and command in an isolated checkout. Do not create growing Git bundles or repository copies per investigation. Transfer artifacts can be deleted and rebuilt from retained commits. Reproducibility also requires permitted retention of data versions and dependencies within their environment.
