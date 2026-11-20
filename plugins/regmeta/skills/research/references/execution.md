# Execution and transfer

Use existing tools and the smallest runner with explicit inputs, failure propagation and
rerun commands. Do not build a general workflow engine for a two-question trial.

## Local and remote responsibilities

Local: fixture tests, full mock pipeline/assertions, exact-artifact mock check,
packaging, verified import, interpretation, figures/tables, HTML, paper and slides.

Restricted server: ingestion, persistent private intermediate caches, real assertions,
investigations, aggregate data files and the mandatory final export-gate check
([export-gate.md](export-gate.md)). MONA returns the gate's output folder as a ZIP;
flexible servers return the checked file set through rsync. No model calls or automatic
raw-data synchronization. Tables and figures are rendered locally.

The real input folder is outside the project, on the server that holds the data. Mock
inputs are in the project's Git-ignored `.regmeta/mock-data/`. A run is real unless
`REGMETA_MOCK` is set (`run.R`); it stops when its input folder is missing, and a mock
run stops where the real folder exists. Before anything that can fail, invalidate the
previous run's review copies and manifest; the gate removes the previous export. An
unreadable directory, invalid input or failed assertion in the selected mode halts execution; never retry with another mode.
Print the selected mode prominently. Verify that mock inputs belong to an approved
invented input set before reading them; a mode flag or filename is not sufficient:
`reg-mock` lists its files in `mock_manifest.json` and labels each with `reg_mock`
metadata, and a real input must not carry the label (`check_inputs()` in
`src/inputs.R`).
No separate hash manifest: the committed spec and seed fix the bytes. Derive separate
cache, result and local-render paths from the selected mode. Ignore caches, mock
outputs, private intermediates, logs, environments and machine-specific configuration.
Disclosure-checked, human-approved real aggregate results and local renders may be
tracked in Git. Mock runs must never overwrite real outputs; missing real results must
never make a paper or slide build use mock results. Refuse input/output overlap. Source
readers must not alter their inputs.

## MONA

The order of work (catalog, `project_data.json`, mock inputs, the delivery, the gate,
MONA's export) and who does each step is in [mona.md](mona.md). On MONA the real input
folder is the delivered files' folder, in the project's read-only `PXXXX_Data`.

A batch is every file in `src/`, uploaded through MONA's web form, and `run.R`
submitted to the batch runner with no arguments. The form takes text files only and
keeps no folders, so `src/` is flat and holds only what runs on MONA; `run.R` is
uploaded once and does not change between batches. The run's last step calls the pinned
`export_gate.R`, uploaded unchanged in `src/` with the chosen schema and policy, never
folded into the project's own code: the receipt's gate version must name those exact
bytes. A Python pipeline runs the gate as a separate `Rscript` step after it
([export-gate.md](export-gate.md)). What leaves is the zip of `output/export/` (the
schema's Parquet files and `receipt.json`), never `.diagnostics/`, the review copies or
staging. Verify accepted formats and runtime before claiming compatibility; do not
concatenate modules into one file. Small nonpersonal lookup files the pipeline needs
go in `src/` as text.

There is no Git on MONA. The run writes the hash of `run.R` and of every file in `src/`
to `output/run_manifest.json` and prints a short source id. A mock run from a clean
local checkout prints the same id for the same files, which names the commit that ran.
Missing packages install from MONA's own mirror in the run's preparation step; nothing
else is downloaded during a run.

`run.R` checks the mode, the input folder, the packages and the pinned files before any
expensive work. Diagnostics are not exported: the gate's report stays in
`output/.diagnostics/` on MONA. The batch log (`run.Rout`) also stays on MONA; it may be
verbose, and it is never exported or pasted to the agent.

Use manual MONA upload/export. A simulated local run establishes packaging only. Do not
open the restricted desktop or use a remote model to inspect its data.

## Other servers

Use operator-run rsync over SSH with independent upload, run and download steps. The
agent prepares source selection, placeholder configuration and reviewed commands; the
researcher privately configures and runs the connection. Never ask for server aliases,
credentials or private paths, and never inspect or invoke the operator's connection
configuration. A local fake-data simulation can test the transfer sequence without a
network connection.

Keep the same project layout locally and remotely. A short `sync.sh up|down [--dry-run]`
may wrap rsync in either direction using the root .gitignore as shared exclusions, plus
an explicit .git exclusion. Keep this file to simple rsync-compatible exclude patterns;
rsync does not implement full Git-ignore semantics. If advanced Git rules become
necessary, use Git to select files rather than writing a custom parser. Real results are
eligible only after disclosure checking and human review. Do not delete destination
files by default; explicitly remove obsolete files after review. Sync is directional
copying, not a merge: do not sync during a run or overwrite concurrent local edits.

Before upload, verify executable inputs match the locally recorded commit. A small
ignored execution manifest can carry that commit and source hashes so the remote runner
verifies source without .git. The researcher inspects uploaded code and runs it manually
(for Python, `uv run run.py`). Dependencies and caches remain machine-local. Keep
synchronization separate from execution, disclosure checking and approval; no remote
commands or cross-step deployment lock belong in the sync helper.

Use fixed current paths for code, mode-specific caches/results and exports. Invalidate
completion before a new run; a failure must not present stale output as current.
Reassemble the export from declared current outputs, check its entire contents, and
publish only after successful completion. Keep temporary staging bounded. Before
downloading the project, the researcher reviews the checked export and all other
transfer-eligible remote content. A failed gate blocks download. Keep raw logs,
intermediates and caches excluded; see [operator-review.md](operator-review.md).

Git history stays local; .git is never synchronized. Approved real aggregate results may
be committed locally. Hosted publication remains a separate explicit decision. Only
human-approved real outputs may become readable by the model/tool identity; an automatic
check alone never grants that approval.

## Incremental builds

The R template uses `targets`: `src/pipeline.R` declares the steps and the cache is in
`.regmeta/targets/`, so a refused gate costs only the steps that changed. For another
stack use an established dependency-aware tool (for example pytask for a
Python/file-based pipeline) rather than writing a cache manager. A Makefile may expose
convenient commands; if Make is the dependency engine, explicitly handle
code/configuration/environment changes rather than relying on output existence alone.
This skill does not mandate a particular engine.

Keep one current cached artifact per active target plus the small dependency state
required by the engine. Model the dependencies between source versions, source adapters,
processing scripts and imported helpers, configuration/parameters, dependency locks,
analyses and output writers. Declare dependencies explicitly; do not assume a workflow
engine discovers every imported helper or changed external dataset. Split processing and
investigation code so changing one regression does not invalidate unrelated
preprocessing. Preserve unchanged source metadata or use appropriate stage-specific
fingerprints during deployment; do not make the global Git commit hash a dependency of
every target. Record that commit in provenance separately.

A regression-only edit should reuse valid analysis data and rerun that regression and
its downstream exports. An upstream data/processing change must invalidate affected
descendants. Partial target files must never count as valid cache entries. Use separate
mock/real caches, rebuild affected assertions with their data, and prune obsolete
generated caches when their active dependencies disappear. Never retain a full
historical cache per commit.

Always execute the export gate over the complete current candidate output set at the
end, even when all analytical targets were reused. Reassemble only requested current
outputs so stale files cannot leak into a new result set. Retain no old remote release
history; use the locally tracked release record as input when the project policy needs
prior-disclosure comparisons, without exposing unreviewed content to the model. Verify
cache reuse and invalidation on mocks with a small targeted check before remote handoff.

## Local handoff gate

1. Run the small fixture-only suite, if the project has one.
2. Prepare the committed execution snapshot (the project folder's committed
   source files).
3. Run that snapshot on separate mock inputs in a fresh directory using the actual
   pipeline/assertions, ending with the export gate.
4. Once an import check exists, verify the labelled mock export against its
   `receipt.json` and import it.
5. Record revision, artifact digest, commands, runtimes and outcomes outside the
   execution snapshot. Real run/release remain pending until completed.

## Archive

Default builds select active tickets and skip closed question-specific computations, not
just their output files. Shared processing remains active only while selected work needs
it. Closed investigations remain in Git. Rerun their recorded commit and command in an
isolated checkout. Do not create growing Git bundles or repository copies per
investigation. Transfer artifacts can be deleted and rebuilt from retained commits.
Reproducibility also requires permitted retention of data versions and dependencies
within their environment.
