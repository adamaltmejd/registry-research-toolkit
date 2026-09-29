# Operator transfer and manual model-access approval

Keep transfer commands transparent: standard SSH/rsync and a small project runbook, not a transport service. Configuration belongs to the researcher outside agent-readable storage. Ship only a placeholder example. Never add server connections or approval to an agent-executed default build.

## Transfer contract

For a flexible Linux server, prepare these operator steps:

1. Build the explicit committed allowlisted package in fresh temporary local staging. Run that exact package on approved mock inputs before upload. Never rsync a checkout or trust arbitrary files added alongside a package.
2. Acquire a work-root deployment/transfer lock distinct from pipeline locks. All supported upload/run/download commands use it. Invalidate the previous remote current result before code replacement. Keep the lock across code sync, batch execution and download. On failure or disconnect leave it for operator inspection; never automatically release it while a remote process might still run.
3. Scope rsync deletion to the dedicated code subtree only. Preserve the private cache and build metadata. Never send individual-level real data from the local checkout. Use only separately identified invented inputs for a mock transport trial.
4. Run the batch under the lock. Download only its final checked release, never cache/intermediates/logs. Clear the previous review candidate before attempting a new run/download, receive into a temporary private filename, and promote only after success. Leave diagnostics in operator-only storage.
5. Release the deployment lock after success. Fixed locations overwrite current code/results; temporary staging is bounded and cleaned. Runtime/native dependencies are provisioned separately by the operator.

MONA retains its manual upload/export procedure. The same human-review and model-access boundary applies.

## Review and approval

The researcher inspects the exact release, its execution commit, checker report and artifact hashes inside private storage. Deterministic local previews can run there under the human identity, without model calls. Record the SHA-256 of the inspected ZIP; changing the candidate requires a new review.

[approve_release.py](../scripts/approve_release.py) is an optional small standard-library helper run **by the operator**. It requires the explicitly reviewed SHA-256 and execution commit, reads at most 64 MiB, compares the hash before writing any bytes into model-readable storage, then replaces `current.zip` and `approval.json` there. It copies exactly one file, not sibling files or a directory. Do not compute the approved hash inline when invoking it: that would approve changed bytes without review.

The receipt records purpose `human-approved-for-model-access`, ZIP hash, human-supplied execution commit and time. It is evidence of the operator action, not authentication, a disclosure checker or proof of OS isolation. Consumers must check the actual ZIP hash and execution commit against the receipt. A receipt/ZIP mismatch after interruption or replacement must block import. Provision the approved destination with human write and agent read access, for example through a setgid shared-group directory; the helper publishes mode-0640 files. Approved local copies must not share writable files with private review storage.

Keep real execution/release/import disabled until the executable disclosure checker, project policy and actual access boundary exist. Neither this helper nor a mock pass enables real results. Agent tests use hand-authored fixtures or separately approved invented mocks only.
