# Design: `reg-export-gate`, the Rust export gate

The export gate's design (trust boundaries, the schema, policy and receipt formats, the
checks, the refusal codes, the rewrite and the case corpus) is in
[`reg_export_gate/DESIGN.md`](../../reg_export_gate/DESIGN.md). This file holds what is
specific to the Rust implementation, which is the reference: the R gate imitates it
(maintainer, 2026-10-25). Issue #1803.

## Shape

- **One crate, a library now and a binary later.** No dependency on `reg-core` or
  `reg-catalog`: the gate reads no catalog.
- **Dependencies.** `parquet` (arrow-rs) with `arrow-array` and `arrow-schema` reads and
  will write Parquet. Reading accepts the codecs R's arrow, pyarrow, DuckDB and Polars
  write: snappy, zstd, gzip and lz4; another codec is `unreadable`. `serde` and
  `serde_json` load the schema and policy as closed types. `bytes` hands the file bytes,
  read once, to the decoder.
- **Schema and policy loading** applies the setup rules in the R gate's order on the
  parsed JSON (unreadable, not the format, `format_version` exactly the integer 1, the
  reserved name), then deserializes into `deny_unknown_fields` types and checks names,
  versions and column roles.
- **Inventory** sees links and other non-regular entries on every platform (the R gate
  misses links on Windows).
- **Structure** decodes each file from its bytes in memory, with the author's embedded
  Arrow schema (`ARROW:schema`) ignored: column types come from the Parquet types alone,
  so the type check reads the same file the same way whatever the author's writer
  recorded. One consequence: a string column R's arrow writes from a factor is a
  dictionary to the R gate (refused, `column_type`) and plain `string` here (accepted).
  The R gate follows when it imitates this one. The same holds for other hints (a
  pyarrow `date64` stored as a Parquet date is `date` here), and a hint that contradicts
  the Parquet types is ignored rather than `unreadable`.
- **Reading** opens each staging file once, with `O_NOFOLLOW | O_NONBLOCK`, checks the
  open handle is a regular file and reads at most the byte cap plus one byte: a link or
  FIFO swapped in after the inventory is `unreadable`, and nothing is read through a
  link. (Off Unix the open follows links; the checks still run on the bytes read.)
- **Decoding** checks the footer's row counts against the row cap (each row group, and
  their sum without wrapping), then the columns against the schema, and only then
  decodes rows, so a column that would be refused, a nested one say, is never expanded.
  Rows are counted again as they decode, and their decoded Arrow size is summed against
  a 64 MiB bound (`too_large` too): the row cap alone does not stop one huge compressed
  string repeated. It counts finished batches only: the decoder's page buffers, sized by
  each page header (up to 2 GiB a page), are not counted, so memory is not strictly
  bounded. Running out of memory aborts the process, which leaves no export, since the
  export is written last.
- **Decoder panics** are caught and are `unreadable`. The default panic hook would print
  the message first, and a decoder message can quote an author's name or value, so the
  crate installs a hook that is silent while a thread decodes and defers to the previous
  hook otherwise. It is installed on the first check; the binary must not set a panic
  hook after that, or it replaces this one.
- **Setup loading is stricter than R** on toolkit files: `1.0` is not the integer 1, a
  non-string `description` is invalid, and a duplicate JSON key is `schema_invalid` or
  `policy_invalid`.

## Plan

Tracks the build of #1803; deleted when the binary ships. Decisions (maintainer,
2026-11-04): no review UI in v1 (the reviewer runs
`export --project X --receipt <sha256>`); approved files go to a shared folder the agent
VM sees read-only; staging files over 8 MiB or 100,000 rows are refused (`too_large`);
the binary ships as an extra asset in the `reg_meta/v*` release, versioned by this
crate.

1. **Library: loading, inventory, structure, the size caps** (this PR). The corpus
   runner (`tests/corpus/`) runs every refusal these checks decide and lists the rest,
   and every pass, as pending. Two corpus cases (`structure-too-many-*`) and the corpus
   format's `repeat` are new; the R runner materializes `repeat` and lists the two cases
   as pending until the R gate has the caps.
2. **Library: disclosure, backstop, the rewrite, the receipt, the report, clearing a
   prior export.** The rewrite builds its own Arrow schema (the decoded one can carry
   the author's file metadata). `na_not_null` leaves the rule list (Rust reads Arrow
   validity, so the values it caught are `nan_value` or `min_cell_count`), and the two
   NaN cases gain `unsupported_statistic`. No case pending.
3. **Binary: `check`**, with `--schema/--policy` or `--project` (config in
   `/etc/reg-export-gate/`), stdout `pass` or `refused: <code>` only, stable exit codes.
4. **`export` and delivery**: re-derive the bytes, match the receipt the reviewer named,
   copy to the destination, re-hash there.
5. **Ship**: the release asset; this section goes.

After 5, one R PR makes `export_gate.R` imitate the binary: the size caps, the
`na_not_null` change, and a duplicate column refused as `column_order` (today R ends in
`internal_error` there, since `GetFieldByName` gives NULL).
