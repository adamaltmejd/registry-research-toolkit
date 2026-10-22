# Research and evidence contracts

These are semantic requirements, not a required directory tree or serialization
framework. Use simple files and existing tools.

## Questions

Each Markdown ticket has stable id, title, status, depends_on, and batch frontmatter.
Add evidence/run references and supersession links when available. Validate duplicate
IDs, dangling dependencies and cycles. Keep question, rationale, method, answer,
evidence and limitations in readable prose.

Use a small status vocabulary, e.g. proposed, ready, awaiting-results, reviewed, closed,
superseded. Distinguish answered, inconclusive and rejected outcomes in prose. Mock
verification is not an empirical answer. Closing hides a ticket from the active board,
not historical search. Changed source definitions should flag dependent findings for
review, not automatically invalidate them.

## Source contracts

Document relevant columns/types, source filename/format, entity/grain, uniqueness, year
coverage, units/encoding, sentinel values, cardinality, timing and evidence. Preserve
exact IDs; avoid floating-point conversion. Do not assume every Swedish source uses the
same encoding or identifiers. Distinguish absent rows, missing values, zero, conflicting
classifications and suppression. Keep source-validity bounds separate from analysis
eligibility; valid out-of-sample cohorts must not fail global assertions.

Preserve appropriate reference-project lookups, recording revision and provenance. Read
only needed columns/partitions. A bridge for an existing input format stays narrow and
inside restricted compute; never export individual-level bridge files.

## Batch and run

A batch names tickets and required processing/configuration. Each run has a unique
evidence ID and records source commit, batch/configuration/lock hashes, data mode,
input-release identifiers, software versions, completed checks and artifact hashes. Keep
research history and evidence tracking locally. A run ID does not require a persistent
remote directory: remote current code, candidate outputs and downloadable outputs may be
replaced each iteration. Retain only current cache/dependency state and bounded
temporary files remotely. Reference processing and investigation code within that
commit. Do not export raw paths, IDs, values or exception dumps as provenance.
Machine-specific paths can remain in private local configuration.

Execution source must be committed. Transfer only declared sources from the recorded
commit and verify runtime/dependencies rather than importing untracked helpers. The run
and later ticket answer can be different commits. Keep execution commits reachable in
history or a retained batch tag.

## Results and disclosure

Separate restricted intermediates from releasable output. A release explicitly
inventories aggregate files, schemas and content hashes, tied to a run and mode.
Recreate it cleanly; never merge old output into a new release. Unknown, extra, missing
or changed files must fail release/import validation. Validate archive paths, links and
size before extracting.

Names and identifier regex cannot establish disclosure safety. Use narrow writers
emitting declared aggregate fields and validate schemas/types/domains. Apply documented
project policy before writing. Cover unique-person support, nonmissing
counts/complements, related margins and differencing where relevant. Never invent
universal Swedish thresholds or claim a minimum count alone makes earnings statistics
safe. Provisional mock policy does not authorize real release. Record overlapping real
releases for review; do not build a universal disclosure solver.

The disclosure checker is the toolkit's export gate, versioned toolkit code and not
agent instructions; [export-gate.md](export-gate.md) has the pin and the call. Every
remote batch invokes it automatically as the final computational stage over the complete
candidate release: nothing the batch exports bypasses it. It runs entirely inside
restricted compute without model access, applies the chosen toolkit schema and policy,
and records its version, the schema's and policy's identity and the checked files'
hashes in `receipt.json`. A missing schema or policy, an undeclared or malformed file, a
gate error, a failed check or an incomplete analysis blocks publication; checking
partial outputs must never turn a failed run into a releasable one. The checked bytes
are frozen; any change requires rechecking. The gate's detailed report stays in
`.diagnostics/` on the data side; only the files and the receipt are exported, and a
refusal tells the agent only its category code. Exercise the same gate locally on mocks,
with one rejection case in the small fixture suite. Schemas and policies are toolkit
files the user chooses: never write, edit or loosen one in the project. Until the user
has chosen a schema and policy that declare the project's outputs, report disclosure
integration as pending and keep real release disabled.

Track automatic disclosure checks, any institution-required export approval, and manual
human approval for model access separately. A disclosure pass never authorizes model
ingestion. A human reviews the checked export remotely before download, or locally in
private storage inaccessible to the model/tool identity. Remote review does not require
a second local review. Whole-project downloads are permitted after reviewing all
transfer-eligible remote content; shared exclusions omit .git and private/generated
state. Exclusions are transfer mechanics, not disclosure approval. Approved real
aggregate results may be Git-tracked locally. Unreviewed local files require an actual
access boundary, not merely ignored paths or a prompt, and must not be previewed,
indexed or loaded into context by the agent. Keep the reviewed version unchanged through
download, or verify it against its reviewed hashes. Replacing bytes invalidates
approval. Keep raw logs, exceptions and unreviewed diagnostic values out of tool
responses. Human-only deterministic previews are permitted. Mock outputs, caches and
local renders are separate from real ones; never promote mock output by relabelling or
substitute it for missing real results. A blocked run can emit safe status metadata, not
raw failure text or unchecked artifacts.

Remote outputs are the aggregate Parquet files the chosen schema declares, with stable
names and typed columns, a suppressed cell null (never zero), and the gate's
`receipt.json`. Keep units and column definitions in the project's data dictionary, not
in extra exported files. Generate tables and figures locally from these files; do not
export model objects or arbitrary rendered artifacts in place of the data contract.
Local processing may use only quantities supported by cleared artifacts. Preserve
suppression as missing/annotated, never zero. Do not reconstruct hidden margins or
denominators. Presentation changes should not need remote runs; new samples, groupings
or unsupported statistical quantities do.

## Local interface

Generate offline-capable HTML from canonical ticket/result files, not a second state
store or server platform. Escape prose, filenames and labels. Show batch status,
question, answer/uncertainty, provenance, result table/figure and next action. Separate
remote output, local renders and paper/slides outputs.
