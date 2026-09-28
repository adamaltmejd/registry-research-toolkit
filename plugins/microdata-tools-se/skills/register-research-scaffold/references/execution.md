# Execution and transfer

Use existing tools and the smallest runner with explicit inputs, failure propagation and rerun commands. Do not build a general workflow engine for a two-question trial.

## Local and remote responsibilities

Local: fixture tests, full mock pipeline/assertions, exact-artifact mock check, packaging, verified import, interpretation, figures/tables, HTML, paper and slides.

Restricted server: ingestion, individual intermediates, real assertions, investigations, disclosure-controlled artifacts, release checks and approved output ZIP. No model calls or automatic raw-data synchronization.

Mode is explicit, never guessed from OS/path. Missing real sources halt; never fall back to mock. Separate caches/output by mode. Refuse input/output overlap. Source readers must not alter reference-project inputs or stores.

## MONA

Target one uploadable execution file and one result ZIP per batch. Python zipapp is a candidate when permitted; a supported plain-source format is another. Verify accepted formats and runtime before claiming compatibility. Preserve modular source locally; do not concatenate modules blindly. Include small required nonpersonal lookup/configuration assets.

Build from an explicit commit and source allowlist. Exclude .git, datasets, caches, logs, secrets and unrelated investigations. Record commit and file hashes. Dependencies belong in a prepared environment; never download during research execution. Zipapps cannot load native extension binaries directly from inside the archive.

Preflight runtime, dependencies, inputs and writable locations before expensive work. Exportable diagnostics must not print rows, arbitrary values or raw tracebacks.

Use manual MONA upload/export. A simulated local run establishes packaging only. Do not open the restricted desktop or use a remote model to inspect its data.

## Other servers

Use the same batch/output contracts. Existing approved SSH/internal Git can deliver sources, or use the single-file artifact. Do not infer credentials, connectivity or access permission from a source path. Provide exact operator commands and configurable input root. For source-only preparation, do not run real data.

Do not return a data-bearing Git working tree. Transfer only approved releases. Private GitHub is still external; do not create one or add a cloud remote. Local/institutional source backups are a separate choice.

## Local handoff gate

1. Validate ticket/batch dependencies and configuration.
2. Run the small fixture-only suite.
3. Package the committed execution revision.
4. Run that package on separate mock inputs in a fresh directory using the actual pipeline/assertions.
5. Verify and import the labelled mock ZIP, then build local reports.
6. Record revision, artifact digest, commands, runtimes and outcomes outside the execution snapshot. Real run/release remain pending until completed.

Exercise one minimal release-boundary rejection fixture (e.g. undeclared or altered file). Do not proliferate tests of command wording.

## Archive

Default builds select active tickets. Closed investigations remain in Git. Rerun their recorded commit and command in an isolated checkout. Do not create growing Git bundles or repository copies per investigation. Transfer artifacts can be deleted and rebuilt from retained commits. Reproducibility also requires permitted retention of data versions and dependencies within their environment.
