# Design: `reg-export-gate`, the Rust export gate

The export gate's design (trust boundaries, the schema, policy and receipt formats, the
checks, the refusal codes, the rewrite and the case corpus) is in
[`reg_export_gate/DESIGN.md`](../../reg_export_gate/DESIGN.md). This file holds what is
specific to the Rust implementation, which is the reference: the R gate imitates it
(maintainer, 2026-10-25). Issue #1803.

## Shape

- **One crate, library and binary.** No dependency on `reg-core` or `reg-catalog`: the
  gate reads no catalog. The binary only parses arguments and prints the result; tests
  drive the library, and the corpus runs through both.
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
  hook otherwise. It is installed on the first check; the binary sets its own silent
  hook before that, so no panic prints anything, and must not set one after, or it
  replaces this one. Any other panic in a check is `internal_error`, with no message.
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
  the export, checked again once its folder exists, and only when staging resolves (no
  report on `staging_missing`). It goes into a real folder, as a new file (`O_NOFOLLOW`,
  never over an existing one; a planted link or hard link named like the report is
  unlinked first). The export's own files are written the same way.
- **Setup loading is stricter than R** on toolkit files: `1.0` is not the integer 1, a
  non-string `description` is invalid, and a duplicate JSON key is `schema_invalid` or
  `policy_invalid`.

## The binary

- `reg-export-gate check --staging DIR --output DIR [--schema FILE] [--policy FILE]`:
  the gate as the R gate's `export_gate()` runs it, for the corpus and for a researcher
  running it by hand.
- `reg-export-gate check --project NAME --staging DIR`: server mode (below).
- `reg-export-gate export --project NAME --receipt SHA256`: the reviewer's export of a
  server-mode output (below). `SHA256` is 64 lowercase hex digits, as `sha256sum` prints
  them.

stdout gets one line, `pass` or `refused: <code>`. `check` prints nothing else, so the
pipeline learns only the category; findings go to the report. `export`, which only the
reviewer runs, also prints a refusal's findings on stderr, one per line (rule, then
file, column, position: never a value). Arguments are parsed by hand, as `reg-meta`
does, and a usage error prints fixed text, never an argument. Exit status: 0 pass, 2
usage, 10 setup, 11 inventory, 12 structure, 13 disclosure, 14 backstop, 15 delivery.

## Server mode

In server mode the pipeline names a project and its staging folder, nothing else. The
project's config, `/etc/reg-export-gate/projects/<name>.json`, names the rest:

```json
{
  "format": "reg-export-gate/project",
  "format_version": 1,
  "schema": "schemas/trial.schema.json",
  "policy": "schemas/trial.policy.json",
  "output": "/data/project-x/output",
  "destination": "/mnt/agent-inbox/project-x"
}
```

The config folder is a constant in the binary, not read from the environment or the
arguments, which the pipeline controls; tests pass another folder through the library
(`check_project`). The format is closed. Setup rules, Rust only:

- `config_missing`: no config folder, a name that is not a slug (so it cannot leave
  `projects/`), or no such file.
- `config_invalid`: the project file, or its schema or policy, is not a regular file
  reached from the config folder by plain relative names with no link at any step (a
  link in a folder someone else can write could be retargeted after the check; a FIFO
  would block the read); or the config is not a closed format-1 project config; or its
  output or destination is not absolute.
- `config_writable`: the config, the schema, the policy or any folder from them up to
  the config folder has a group or other write bit, or an owner other than the config
  folder's (or root), who could set one. Whoever can write those chooses the check the
  pipeline is held to. Off Unix there are no mode bits to read, so server mode always
  refuses there.
- `paths_overlap` extends to the config folder and the destination: staging, the output
  and the output's folder may not be, or lie inside, either; neither may lie inside
  staging or the output, or be the diagnostics folder beside the output. A destination
  that does not exist yet is apart. The check runs before the diagnostics folder is
  created.

A config refusal writes no report (the output is not known to be the deployer's yet);
stdout still says `refused: setup`. `check` in server mode is advisory: the pipeline
controls the run. The guard is `export`, which the reviewer runs under the same config.

Deployment invariants: the config folder and the destination are writable only by the
reviewer's user; the destination is the agent VM's read-only view of a shared folder;
the deployer copies the chosen schema and policy from `reg_export_gate/schemas/` into
the config folder, a reviewed step.

## Export and delivery

No review UI in v1 (maintainer, 2026-11-04). The output folder is the pipeline's to
write at any time, so the reviewer binds what they inspect to the hash they name:

1. Copy the output folder somewhere only the reviewer can write.
2. Hash the copy's `receipt.json` and check every file in the copy against the hash the
   receipt lists (`sha256sum`).
3. Inspect the copy with any tool (DuckDB, R, Python).
4. Run `export --project NAME --receipt <that hash>`.

`export` then succeeds only if the live output folder is byte for byte what was
inspected. Hashing the live folder instead leaves a window: the pipeline can show one
passing set during the review and swap in another before the hash or the export.
`export` runs under the same config as `check` and trusts nothing in the output:

1. The config loads as for `check`. The destination must be a real folder with no group
   or other write bit (`destination_writable`); the output's folder must exist
   (`output_parent_missing`) and be apart from the destination and the config folder
   (`paths_overlap`).
2. The output is read once, each file as staging files are: no link, never blocking,
   under the 8 MiB cap. It must be a real folder holding exactly the schema's files and
   `receipt.json`, all regular files, and the receipt's SHA-256 must be the one named
   (`receipt_mismatch`).
3. The files are checked again (structure, disclosure, backstop, under the configured
   schema and policy, with those codes) and rewritten. The fresh receipt must equal the
   one read, byte for byte, and each file read must hash to its rewrite
   (`receipt_mismatch`). A genuine export reproduces exactly (byte identity per gate
   version). The author's own Parquet does not, even under the genuine receipt (it
   decodes to the same values). A rewritten file over 8 MiB is `too_large` in `check` as
   well, so `export` accepts exactly the outputs `check` can write. The gate writes
   uncompressed, so a compressed staging file under the cap can grow past it. That costs
   capacity: at 100,000 rows, about ten non-null `int64` columns fill 8 MiB.
4. The destination and every folder above it must be safe from everyone but the user
   running `export`, or the pipeline could move the destination aside and put its own
   folder or a link there (`destination_writable`). The destination is owned by that
   user; each folder above it is owned by that user or root. None has a group or other
   write bit, except that a folder above may be sticky (`/tmp`). The config folder must
   be owned by that user or root, with the same rule for the folders above it
   (`config_writable`). `std` has no `geteuid` and the crate has no `unsafe`, so the
   user is read as the owner of a probe folder the gate creates and removes in the
   destination.
5. The gate writes its own bytes, never the folder's, to
   `<destination>/<first 16 hex digits of the receipt hash>/`, through a new folder and
   a rename as for an export, then reads them back and compares hashes (`hash_mismatch`,
   and the folder is removed). If that folder exists, the receipt was exported before:
   it is re-verified the same way and nothing is written.

Delivery rules, Rust only: `receipt_mismatch`, `destination_writable`, `hash_mismatch`.
`export` writes no report: its reader is the reviewer, on stderr. Ownership and mode
bits are what the gate reads. An ACL that grants the pipeline's user write access
passes, so the deployment must not set one on the config folder or the destination. The
probe folder (`.export-gate-probe-*`) and the new folder (`.export-gate-*`) appear in
the destination for a moment, and a killed export can leave the latter behind with
verified gate bytes in it. The agent's side ignores dot-folders.

## The R gate follows

`export_gate.R` stays the MONA implementation (MONA runs no executables) and imitates
this one in its own PR, naming the gate version it matches: the size caps (the rewrite's
too), the `na_not_null` change (a NaN beside a null count is `unsupported_statistic`),
and a duplicate column refused as `column_order` (today R ends in `internal_error`
there, since `GetFieldByName` gives NULL). From then on a corpus change lands with the
Rust change, and R follows.
