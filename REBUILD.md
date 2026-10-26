# Tracker: the catalog rebuild

**Deleted when milestone F (cutover) ships.** This is the one root-level tracker
AGENTS.md allows for a multi-PR refactor. It replaces `PORT.md`, the tracker of the
byte-exact Rust port, which stopped after phase 2. It coordinates milestones and exit
checks; the design is in [crates/DESIGN.md](crates/DESIGN.md), "The catalog builder".

## Why

The port reproduced today's builder byte for byte, including the parts that are wrong: a
common form that is not common, a delivery coordinate only SCB has, and curation that
compensates with provider checks. The rebuild starts from a clean-room design instead.
It has one identity rule for every provider, optional releases and six curation
statement kinds. A dry run of its default rules over all of today's metadata (no build)
sized the result: for example 4,863 split SCB keys, 53,975 SCB variables and 2,590
ambiguous SCB cells. The maintainer chose the rebuild on 2026-10-26.

## Rules for every milestone

- **Beside, not instead.** The new crates sit beside `reg_meta_build` until F.
  `reg_meta_build` gets release fixes only, no features, and is deleted at F, not
  before.
- **No byte identity.** Nothing is diffed against today's artifact for equality. The
  check is what users see: tables, columns, variables, labels, value sets and random
  full-register orders, compared by `scripts/rebuild_compare.py`. Every difference is
  classified as intended, a defect, or curation to carry over.
- **No curation during A-C.** The default rules stand alone. Today's curation is
  attached in D.
- **Heavy runs** (the full SCB read, full builds, real-artifact conformance) are
  scheduled by the Operator with the maintainer.
- **No new tests until cutover.** Proof is the comparison script and the dry-run
  figures. CI keeps one check for the new builder: build twice and compare bytes.
  Reader, server and conformance tests (`crates/`, `conformance/`) stay as the black-box
  check that the new catalog works for readers. The Python builder's tests stay while it
  ships releases and go with it at F.
- **Review loop.** A PR that adds or changes design (crates, frame records, resolver
  rules, curation layout, reader config) merges only after an independent review and the
  tracker session's drift review; the Operator routes it. Mechanical PRs skip the
  tracker. The tracker audits the whole milestone diff before the milestone is done.

## Milestones

  | M   | Content                                                                                            | Proves                                                                                            | Status      |
  | --- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------- |
  | A   | Frame, readers, resolve and emit for the thin providers and Socialstyrelsen; the comparison script | The pipeline runs end to end on providers without variable ids, and the frame is provider-neutral | in progress |
  | B   | The SCB export and the LISA workbook; the SCB ledger                                               | The identity and release rules hold at full scale                                                 | —           |
  | C   | The link stage, the full published catalog, `reg-catalog` over it, the server switch               | Readers and agents work against the new catalog                                                   | —           |
  | D   | Today's curation carried over; new ids minted                                                      | Today's curation survives in the six statement kinds                                              | —           |
  | E   | The holdings layer: design, then SWECOV's catalog, physical orders and the MONA order file         | Stewards and projects work without `reg_meta_build`                                               | —           |
  | F   | Cutover: the deletions                                                                             | Nothing depends on the old builder                                                                | —           |

The hosted global catalog switches in C, uncurated, and curation in D happens against
it. There are no users to protect yet, and one live pipeline is simpler than two.
"Parity" in C means the reader's API and conformance cases, not the same data.

### A: frame, thin providers and Socialstyrelsen

- **The first PR deletes the port's code**: `crates/reg-build-model` (the model of
  Python-prepared v16 records, the prepared reader, the guard digest golden and the verb
  types with their golden corpus, #1935 and #1937) and
  `reg_meta_build/src/reg_meta_build/port/` plus `reg_meta_build/port/` (the converter,
  its member selector, emitter, equivalence harness and golden check, #1938), with
  `dbdiff`'s `--ignore-file`. The rebuild reads raw inputs, has no prepared stage and
  has six statement kinds, so none of it is reused. D writes a new converter. The old
  converter's selection rules (an exact member, narrowed by `where = { name }` where
  only SCB's VarID separates two families) are its reference, read from git at
  `d51dcf1ce`.

- **New crates:**
  - `reg-frame`: the source frame records and their canonical JSONL;
  - `reg-readers`: one reader per input format. A starts with the transcription CSV, the
    Socialstyrelsen workbook (calamine) and the classification CSV;
  - `reg-resolve`: per-register resolution (identity, time rules, rules, segments,
    findings);
  - `reg-build`: the link stage, the emitter and the CLI.

  The period grammar, interval algebra and text folds stay in `reg-core`.

- **The new curation root `curation/`** holds reader config (`readers/`) and the
  per-register id ledgers (`registers/<provider>/<register>/ids.toml`), generated from
  today's slug pins. It holds no statements before D.

- **Left out of A**, each added by the milestone that needs it: the resolve cache and
  zstd intermediates (frames stay in memory; `reg-build read` dumps them as JSONL),
  releases and gaps (B), search and documents (C), classification binding, unit types
  and roles beyond the defaults (D). Known reader limits: calamine exposes no number
  formats, so a zero-padded numeric code loses its padding; the inline value-set reader
  accepts only plain `code=label` or bare-code lists; a code-list period it cannot read
  (`1990/91-1994`) applies to every period (reported).

- **Inputs.** Today's six thin-provider TOML files and `scb_canonical.toml` are
  transcribed once into the transcription format and committed to the inputs repository.
  The maintainer approves the pin.

- **`scripts/rebuild_compare.py`** (temporary, deleted at F) dumps, per register, what
  users see in both catalogs and classifies each difference:
  - tables, and columns per period;
  - the column-to-variable mapping;
  - labels;
  - value-set code lists, classification references and coverage.

**Exit:**

- CI builds a small committed input twice and the bytes are identical;
- the provider-vocabulary scan of `reg-frame`, `reg-resolve` and `reg-build` passes. It
  is a lint step in the gate, not a test;
- a real build of the thin providers and Socialstyrelsen, and a comparison report that
  classifies every difference. The dry run's figures are the expectation:
  - Socialstyrelsen: 23 ambiguous column pairs, 329 names in several tables (60 of them
    with different labels), and 32 variables without an existing id;
  - the thin providers: no ambiguity and no missing id.

### B: the SCB export and LISA

- **The SCB export reader:**
  - `Registerinformation`, with the population collapse and gap markers;
  - `Vardemangder`, streamed into the content-addressed value-set store (102M rows);
  - `VardemangderValidDates`, applied at read time;
  - `UnikaRegisterOchVariabler`, `Identifierare` and `Timeseries`.
- **The delivery-name grammar** in `curation/readers/scb.toml`.
- **The LISA workbook reader.**
- **SCB ledgers** generated from today's slug pins.

**Exit:**

- two full metadata builds are byte-identical;
- the dry-run figures are reproduced, or each difference is explained:
  - 4,863 split keys and 53,975 SCB variables;
  - 2,211 keys documented without a column, which are not variables;
  - 2,590 ambiguous cells in 82 registers;
  - no true release collision;
  - at most 0.6% of delivery names unparsed;
  - 4,481 variables without an existing id;
- the 43 uncurated keys where today's build has more variables are checked by hand, with
  the outcome recorded here;
- the time and memory of each stage measured against these targets: the SCB read in 2-4
  minutes, the resolve of all registers in under 60 s.

The full SCB read is a heavy run.

### C: link stage, published catalog, server switch

- **The link stage:**
  - cross-register `continues` and `derived-from` links;
  - tags;
  - the search index with code search;
  - parsed documents.
- **`reg-catalog`** rewritten over the new schema. The operations in
  `conformance/api/operations.toml` and the OpenAPI contract are kept where the model
  allows. Each contract change is reviewed and the SPA types are regenerated.
- **The order check**, which returns the logical order or its findings. An ambiguous
  window and an unnamed preliminary window block.
- **The conformance harness moves off `reg_meta_build`.** Fixtures are built by the new
  builder's CLI from readable sources. Artifact validation (the schema invariants) moves
  into `reg-build`, which becomes its single authority. The holdings and steward cases
  need the old builder; they leave the run in C and return, rewritten, in E.
- **Documents move into the catalog file.** The separate docs DB and its schema gate in
  `reg-catalog` go.
- **Release plumbing.** The release and container workflows read the new builder's
  schema versions, and the `release` skill is rewritten. The contract table in
  `crates/DESIGN.md` names the new homes.
- **A Rust `dbdiff` for the new schema,** grown from the comparison script. From the
  switch on, each release is checked by its `dbdiff` against the previous release, read
  by the builder agent to confirm that only intended changes occurred.
- **The server switch.** reg-meta.vqv.uk serves the new artifact. SWECOV's hosted
  catalog stays on the last old-schema release until E, so no reader handles both
  schemas.

**Exit:**

- the conformance suite, without the holdings and steward cases, is green against the
  new server on the synthetic artifacts and on the real one (the `release` step);
- random full-register orders are compared with today's server, and the differences
  classified;
- the SPA flows are green;
- the maintainer approves the switch release.

### D: curation carry-over

- **A one-shot converter** turns today's 17,299 entries into the six statement kinds,
  reading the new frames. It is deleted at F. For the 1,819 identity partitions the dry
  run gives the expected outcome:

  | Outcome      | Entries |
  | ------------ | ------- |
  | No statement | 1,461   |
  | `assign`     | 97      |
  | Merge        | 55      |
  | `set`        | 145     |
  | Regroup      | 61      |

  Of the 97 curated splits, 8 are case-only. They are dropped and listed.

- **Classification curation, sets and tags** are converted. Classification references
  are always curated.

- **Unminted ids** are minted, and the new ids are reviewed in the PR.

- **From D on, curation is edited only in `curation/`.** The old tree is frozen for the
  SWECOV build until E.

**Exit:**

- every old entry is converted, dropped with a named reason, or refused at its file and
  line in the converter's report;
- the curated strict build passes;
- no variable is unminted;
- the comparison is re-run, and every curated difference is explained.

### E: the holdings layer

- **Design first, for the maintainer's review.** The design covers:
  - steward and project holdings, and the three questions holdings answer: what is in my
    project, what my steward holds, and what exists with the providers;
  - the mapping from a logical order to files;
  - the MONA order file;
  - mock data;
  - today's holdings vocabulary as a checklist: partitions, projections, wide column
    series, column windows and key-only files.
- **Then the build.** SWECOV's holdings run on the new catalog, together with its
  steward catalog, its release asset and physical orders. `reg_mona_runner` reads the
  new order file.

**Exit:**

- SWECOV's catalog and release asset come from the new stack;
- the conformance holdings and order cases are green;
- one real order has run on MONA. The maintainer runs it.

### F: cutover

**Deleted:**

- `reg_meta_build`: the package, its curation tree, the prepared stage, the steward
  extension and the real-seed cache (`scripts/real_seed_cache.py` and its tests);
- `crates/reg-core-py`;
- `scripts/rebuild_compare.py`;
- the old builder's CI, container and release workflow steps;
- builder-only scripts such as `scripts/prototype_scb_inputs.py` and its test, and
  `reg_webapp/stewards/swecov/build_catalog.py`, once E replaces it;
- this tracker.

**Rewritten:**

- the `build-db`, `code-cleanup` and `dogfood` skills;
- the testing policy in `AGENTS.md`: a few black-box tests for the key guarantees, and
  the release `dbdiff` as the main release check;
- `AGENTS.md`, `ARCHITECTURE.md` and `README.md`;
- every other file that names `reg_meta_build`, edited to drop it: among others
  `scripts/gate.py`, `scripts/parse_lisa_docs.py`, `reg_webapp/dev/dev.sh`, the root
  `pyproject.toml` and `uv.lock`, `renovate.json`, `.panache.toml`, `.dockerignore`,
  `.gitignore`, `reg_webapp/`, `conformance/` and `scripts/tests/`.

**Exit:** `git grep reg_meta_build` finds nothing, `scripts/gate.py all` is green, and a
release is built by the new builder.

## Decisions already made

Recorded with their sources by the maintainer:

- one identity rule for every provider;
- variables register-scoped;
- optional releases, never synthesized;
- ambiguity blocks orders;
- classification references always curated;
- per-code validity dates applied at read time only;
- populations not a segment dimension;
- no series entity in the catalog;
- holdings as a separate layer.

The durable parts are in [crates/DESIGN.md](crates/DESIGN.md).
