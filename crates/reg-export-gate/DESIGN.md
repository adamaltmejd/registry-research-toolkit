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
- **The rewrite** builds a fresh Arrow schema of the export types (`integer` as `int64`)
  with no key-value metadata at all: arrow-rs's `ARROW:schema` entry is skipped, so the
  exported files carry none, where R's arrow always writes one. Pinned options: data
  page v1, no compression, no dictionary, no statistics, row groups of 1,048,576 rows,
  `created_by` `reg-export-gate <version>`. Byte identity holds per gate version.
- **The backstop** scans by hand over runs of ASCII digits (the `regex` crate has no
  lookaround). `tests/backstop/shapes.json` pins it to the R pattern: inputs and the R
  pattern's verdict, computed once by running it.
- **Clearing a prior export** treats a link or any non-regular entry in the output
  folder, and an output that is itself a link, as not the gate's
  (`output_not_gate_export`), including `output/` naming a link: the gate drops a
  trailing separator, which would make the OS follow it. The prior receipt is read as
  staging files are. It checks, then deletes, by path: a pipeline that swaps the output
  folder for a link between the two has the gate delete files of the listed names, if
  regular, in whatever folder the link points to. That gives a same-user pipeline no
  power it lacks already (it can delete those files itself), so it stays; closing it
  needs deletion relative to an open folder handle (`unlinkat`), which `std` does not
  offer without `unsafe`.
- **Paths are compared by identity on disk** (device and inode), not by spelling, so a
  case or Unicode variant on a case-insensitive volume is the same folder; the check
  creates `.diagnostics` first, so an output name the volume folds to it is seen. That
  output, one named `.diagnostics` in any ASCII case, or staging inside that folder, is
  `paths_overlap`.
- **The report** is written beside `output` only where it can touch neither staging nor
  the export, checked again once its folder exists, and not at all when staging exists
  but cannot be resolved. It goes into a real folder, as a new file (`O_NOFOLLOW`, never
  over an existing one; a planted link or hard link named like the report is unlinked
  first). The export's own files are written the same way.
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

1. **Library: loading, inventory, structure, the size caps** (#2176). The corpus runner
   (`tests/corpus/`) runs every refusal these checks decide and lists the rest, and
   every pass, as pending. Two corpus cases (`structure-too-many-*`) and the corpus
   format's `repeat` are new; the R runner materializes `repeat` and lists the two cases
   as pending until the R gate has the caps.
2. **Library: disclosure, backstop, the rewrite, the receipt, the report, clearing a
   prior export** (this PR). `export_gate()` is the whole gate. `na_not_null` leaves the
   rule list, and the two NaN cases gain `unsupported_statistic` (pending in the R
   runner). The whole corpus runs; nothing is pending here.
3. **Binary: `check`**, with `--schema/--policy` or `--project` (config in
   `/etc/reg-export-gate/`), stdout `pass` or `refused: <code>` only, stable exit codes.
4. **`export` and delivery**: re-derive the bytes, match the receipt the reviewer named,
   copy to the destination, re-hash there.
5. **Ship**: the release asset; this section goes.

After 5, one R PR makes `export_gate.R` imitate the binary: the size caps, the
`na_not_null` change, and a duplicate column refused as `column_order` (today R ends in
`internal_error` there, since `GetFieldByName` gives NULL).
