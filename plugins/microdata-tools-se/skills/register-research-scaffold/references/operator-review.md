# Operator transfer and manual model-access approval

Keep transfer commands transparent: standard rsync and a short runbook with separate upload, remote run and download steps. Configuration belongs to the researcher outside agent-readable storage. Ship only placeholders. Never add server connections or approval to an agent-executed default build.

## Transfer contract

For a flexible server, prepare these independent operator steps:

1. Test the committed source snapshot locally on approved mock inputs. Upload that snapshot, or tracked files from a verified clean checkout, to the existing remote source location. Omit .git, inputs, caches, results and local environments. Remove obsolete source files only within the managed source scope; preserve remote state. Do not upload while a batch runs.
2. Inspect the uploaded source on the server and run it manually. A small Python project can expose `uv run run.py` with an established build tool underneath. Configure external real/mock input paths and derive separate output/cache locations from the selected mode, as described in [execution.md](execution.md). Mocks need not exist remotely.
3. The runner invalidates the old completion marker, builds requested outputs and automatically checks the entire newly assembled export. Failure leaves no current releasable export. A pipeline lock prevents concurrent runs; a deployment/transfer service or lock spanning all operator steps is unnecessary for this MVP.
4. Review the completed export on the server, including the checker report and execution revision, and approve it for model access. Then explicitly rsync only the designated export into the local real-results destination. Never download caches, intermediates, logs or the whole project. Keep that export unchanged until transfer completes; a checksum can confirm it is the reviewed version. Receive into temporary storage and replace the current local export only after successful transfer and validation, without merging stale files.

Fixed locations overwrite current code/results; temporary staging is bounded. Git history and research records stay local. Runtime/native dependencies are provisioned separately. MONA retains its manual upload/export procedure and the same human-review boundary.

## Alternative local review

If review happens after download, receive the export into human-only storage inaccessible to the model/tool identity, outside the Git checkout. Use OS/account permissions or an equivalent enforced boundary; .gitignore and same-user chmod do not prevent agent access. Do not auto-preview or index unreviewed files. Deterministic previews may run under the human identity without model calls. Expose only the reviewed version after approval; no duplicate review is required after a remote review.

[approve_release.py](../scripts/approve_release.py) is an optional operator-run helper for this local-review route. It accepts the reviewed ZIP's SHA-256 and execution commit, checks the bytes before copying them, and replaces `current.zip` and `approval.json` in model-readable storage. It copies one file, not siblings. Do not compute the approved hash inline from a potentially replaced candidate. Consumers using the receipt must verify the ZIP hash and execution commit before importing. The receipt records an operator action; it is not authentication, disclosure clearance or proof of access isolation.

Keep real execution/release/import disabled until the executable disclosure checker and project policy exist. Unreviewed real bytes must remain inaccessible to the agent throughout the chosen review route. Neither the optional helper nor a mock pass enables real results. Agent tests use hand-authored fixtures or separately approved invented mocks only.
