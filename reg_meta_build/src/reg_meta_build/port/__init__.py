"""Temporary code for the Rust port of the builder (`PORT.md`).

`convert` (with `select`, the member selector, and `emit`, the TOML writer) turns
today's curation tree into the neutral verbs that the Rust builder reads
(`crates/reg-build-model/src/verbs.rs`). `equivalence` checks, per entry, that each
converted entry selects exactly the records the old entry acts on in today's Python
compile; `golden` checks the output against the verb golden corpus. All are deleted at
phase 4 (cutover), after the converted tree is committed.
"""
