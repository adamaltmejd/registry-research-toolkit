# Research and evidence contracts

These are semantic requirements, not a required directory tree or serialization framework. Use simple files and existing tools.

## Questions

Each Markdown ticket has stable id, title, status, depends_on, and batch frontmatter. Add evidence/run references and supersession links when available. Validate duplicate IDs, dangling dependencies and cycles. Keep question, rationale, method, answer, evidence and limitations in readable prose.

Use a small status vocabulary, e.g. proposed, ready, awaiting-results, reviewed, closed, superseded. Distinguish answered, inconclusive and rejected outcomes in prose. Mock verification is not an empirical answer. Closing hides a ticket from the active board, not historical search. Changed source definitions should flag dependent findings for review, not automatically invalidate them.

## Source contracts

Document relevant columns/types, source filename/format, entity/grain, uniqueness, year coverage, units/encoding, sentinel values, cardinality, timing and evidence. Preserve exact IDs; avoid floating-point conversion. Do not assume every Swedish source uses the same encoding or identifiers. Distinguish absent rows, missing values, zero, conflicting classifications and suppression. Keep source-validity bounds separate from analysis eligibility; valid out-of-sample cohorts must not fail global assertions.

Preserve appropriate reference-project lookups, recording revision and provenance. Read only needed columns/partitions. A bridge for an existing input format stays narrow and inside restricted compute; never export individual-level bridge files.

## Batch and run

A batch names tickets and required processing/configuration. Each run has a unique evidence ID and records source commit, batch/configuration/lock hashes, data mode, input-release identifiers, software versions, completed checks and artifact hashes. Keep research history and evidence tracking locally. A run ID does not require a persistent remote directory: remote current code, candidate outputs and downloadable outputs may be replaced each iteration. Retain only current cache/dependency state and bounded temporary files remotely. Reference processing and investigation code within that commit. Do not export raw paths, IDs, values or exception dumps as provenance. Machine-specific paths can remain in private local configuration.

Execution source must be committed. Transfer only declared sources from the recorded commit and verify runtime/dependencies rather than importing untracked helpers. The run and later ticket answer can be different commits. Keep execution commits reachable in history or a retained batch tag.

## Results and disclosure

Separate restricted intermediates from releasable output. A release explicitly inventories aggregate files, schemas and content hashes, tied to a run and mode. Recreate it cleanly; never merge old output into a new release. Unknown, extra, missing or changed files must fail release/import validation. Validate archive paths, links and size before extracting.

Names and identifier regex cannot establish disclosure safety. Use narrow writers emitting declared aggregate fields and validate schemas/types/domains. Apply documented project policy before writing. Cover unique-person support, nonmissing counts/complements, related margins and differencing where relevant. Never invent universal Swedish thresholds or claim a minimum count alone makes earnings statistics safe. Provisional mock policy does not authorize real release. Record overlapping real releases for review; do not build a universal disclosure solver.

The plugin must supply a versioned executable disclosure checker, not just agent instructions. Every remote batch invokes it automatically as the final computational stage over the complete candidate release, including exportable diagnostics and metadata. The checker runs entirely inside restricted compute without model access, applies the configured project policy, and records its version, policy identity, result and checked artifact hashes. Missing policy, unsupported output, checker error, a failed check or an incomplete analysis blocks publication; checking partial outputs must never turn a failed run into a releasable one. Freeze the checked bytes; any change requires rechecking. Keep detailed failure reports private and export only explicitly safe status metadata. Exercise the same checker locally on mocks, with rejection cases in the small fixture suite. Until this executable and a concrete policy exist, the scaffold must report disclosure integration as unimplemented and keep real release disabled.

Track automatic disclosure checks, any institution-required export approval, and manual human approval for model access separately. A disclosure pass never authorizes model ingestion. A human reviews the checked export remotely before download, or locally in private storage inaccessible to the model/tool identity. Remote review does not require a second local review. Download only that export, never the remote project; .gitignore is not a release boundary. Unreviewed local files require an actual access boundary, not merely ignored paths or a prompt, and must not be previewed, indexed or loaded into context by the agent. Keep the reviewed version unchanged through download, or verify it against its reviewed hashes. Replacing bytes invalidates approval. Keep raw logs, exceptions and unreviewed diagnostic values out of tool responses. Human-only deterministic previews are permitted. Mock outputs, caches and local renders are separate from real ones; never promote mock output by relabelling or substitute it for missing real results. A blocked run can emit safe status metadata, not raw failure text or unchecked artifacts.

Remote outputs should be simple declared aggregate data files (CSV by default), with stable names, units, column definitions, suppression markers and a concise disclosure-check report that a human can inspect easily. Generate tables and figures locally from these files; do not export model objects or arbitrary rendered artifacts in place of the data contract. Local processing may use only quantities supported by cleared artifacts. Preserve suppression as missing/annotated, never zero. Do not reconstruct hidden margins or denominators. Presentation changes should not need remote runs; new samples, groupings or unsupported statistical quantities do.

## Local interface

Generate offline-capable HTML from canonical ticket/result files, not a second state store or server platform. Escape prose, filenames and labels. Show batch status, question, answer/uncertainty, provenance, result table/figure and next action. Separate remote output, local renders and paper/slides outputs.
