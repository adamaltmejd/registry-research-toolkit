# Operator transfer and manual model-access approval

Keep transfer commands transparent: standard rsync and a short runbook with separate upload, remote run and download steps. Configuration belongs to the researcher outside agent-readable storage. Ship only placeholders. Never add server connections or approval to an agent-executed default build.

## Transfer contract

For a flexible server, provide a small `sync.sh up|down [--dry-run]` around rsync. Both directions use the same simple root .gitignore exclusions and explicitly omit .git. Keep Git history local; approved real aggregate results may be tracked. Exclude mock results, caches, private intermediates, raw logs, environments and machine configuration. Raw and mock inputs live outside the project. Do not claim full Git-ignore support: use simple exclude patterns, or Git's file selection if advanced rules become necessary.

1. Test the committed source locally on approved mock inputs, then upload with `./sync.sh up`. The helper can record source hashes and the execution commit in a small ignored manifest for verification without remote Git. Do not upload during a run.
2. Inspect uploaded code and run it manually with `uv run run.py`. External inputs select real or mock mode with separate output/cache paths; see [execution.md](execution.md). Mocks need not exist remotely.
3. The runner invalidates old completion, builds requested outputs and automatically checks the entire fresh export. Failure blocks download. Keep unchecked data and detailed failure reports in excluded paths.
4. Review the checked results, execution revision and all other transfer-eligible remote content. Approve real outputs for model access, then run `./sync.sh down`. Keep reviewed files unchanged until copying finishes. Transfer failure is not a completed handoff; wait for a successful retry before consuming results.

The helper only copies files: no remote execution, automatic approval or deletion by default. Review and remove obsolete source/output files explicitly. A sync is not a merge; avoid concurrent runs or local edits that a download would overwrite. Fixed remote paths retain current data/cache only. MONA keeps its separate manual upload/export workflow.

## Alternative local review

If review happens after download, receive the export into human-only storage inaccessible to the model/tool identity, outside the Git checkout. Use OS/account permissions or an equivalent enforced boundary; .gitignore and same-user chmod do not prevent agent access. Do not auto-preview or index unreviewed files. Deterministic previews may run under the human identity without model calls. Expose only the reviewed version after approval; no duplicate review is required after a remote review.

[approve_release.py](../scripts/approve_release.py) is an optional operator-run helper for this local-review route. It accepts the reviewed ZIP's SHA-256 and execution commit, checks the bytes before copying them, and replaces `current.zip` and `approval.json` in model-readable storage. It copies one file, not siblings. Do not compute the approved hash inline from a potentially replaced candidate. Consumers using the receipt must verify the ZIP hash and execution commit before importing. The receipt records an operator action; it is not authentication, disclosure clearance or proof of access isolation.

Keep real execution/release/import disabled until the executable disclosure checker and project policy exist. Unreviewed real bytes must remain inaccessible to the agent throughout the chosen review route. Neither the optional helper nor a mock pass enables real results. Agent tests use hand-authored fixtures or separately approved invented mocks only.
