# Tracker: the catalog rebuild

**Deleted when milestone F (cutover) ships.** This is the one root-level tracker
AGENTS.md allows for a multi-PR refactor. It replaces `PORT.md`, the tracker of the
byte-exact Rust port, which stopped after phase 2. It coordinates milestones and exit
checks; the design is in [crates/DESIGN.md](crates/DESIGN.md), "The catalog builder".

## Why

The port reproduced today's builder byte for byte, including the parts that are wrong: a
common form that is not common, a delivery coordinate only SCB has, and curation that
compensates with provider checks. The rebuild starts from a clean-room design instead.
It has one identity rule for every provider, optional releases and four curation
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
- **Gaps one or two registers hit are curation, not reader code.** From B on, a source
  defect or a reader gap that only one or two registers hit becomes a D statement
  (`set`, `assert`), never a new reader fallback. A reader rule must be general across
  its format.
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
  | A   | Frame, readers, resolve and emit for the thin providers and Socialstyrelsen; the comparison script | The pipeline runs end to end on providers without variable ids, and the frame is provider-neutral | done        |
  | B   | The SCB export and the LISA workbook; the SCB ledger                                               | The identity and release rules hold at full scale                                                 | done        |
  | C   | The link stage, the full published catalog, `reg-catalog` over it, the server switch               | Readers and agents work against the new catalog                                                   | done        |
  | D   | Today's curation carried over; new ids minted                                                      | Today's curation survives in the four statement kinds                                             | in progress |
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
  has four statement kinds, so none of it is reused. D writes a new converter. The old
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
  - `UnikaRegisterOchVariabler` and `Identifierare` (the flags). `Timeseries` moves to C.
- **The delivery-name grammar** in `curation/readers/scb.toml`.
- **The LISA workbook reader**, its rows a register of their own until attachment in D-8
  (LISA headings). Add a cross-register destination only if the D-4 converter needs it.
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
  minutes, the resolve of all registers in under 60 s;
- `scripts/rebuild_ledger.py` is deleted. B's SCB ledgers are its last run.

**Outcome.** The SCB exit figures, from the full build and `scripts/rebuild_compare.py`
against 0.64.0:

- variables 52,851, against the dry run's 53,975. The difference reconciles exactly: 256
  keys whose only deliveries have unparsed names, and 868 fewer split pieces;
- split keys 4,423, against 4,863: the dry run took a delivery's year from the first
  four characters of its name ("Läså", "Vårt"), which over-split;
- ambiguous segments 3,597, against 2,590 cells: a different unit, in 551 (table,
  column) groups against 491 pairs;
- 46 unparsed delivery names (0.53%), and no release collision;
- variables without an id 4,235, against 4,481, once ledger anchors name source keys;
- the read, resolve and emit of the full export take 73 s with a 9.0 GB peak.

The hand check found 65 uncurated keys, not 43, where today's build has more variables;
the 43 was the dry run's pin-based estimate. None is a true wrong merge:

- one needs a D `set variable` (Prisindex 23614: `prodgrpkod` is three code systems);
- in 28, one variable changes definition, classification or name over time, and stays
  one variable with F1;
- in 17, today's split is an artifact;
- in 19, the gap is unplaced spellings in unparsed releases (IoT `2021VX`, FoB1985, UHR
  "Läsåren", Öar 2013).

The per-key list is in the PR that recorded it (rebuild B, PR 9).

The full SCB read is a heavy run.

### C: link stage, published catalog, server switch

- **Carried from B's audit:**
  - B2: before the server switch, one sampled pass over the value-set differences the
    comparison still leaves unexplained (920 "codes differ", 70 only in the old catalog,
    74 only in the new one). Take about 30 of the 920 plus the 70 and bucket them by
    cause: the ValidDates filter or its whole-set fallback; curation in the old catalog;
    a reused ItemId's dates; other. Record the buckets here. Re-open a rule only if a
    bucket is large and wrong. Also check r118 kommun codes 0197 and 0199 per segment.
    - **Outcome**, from a full build at c0be3db37 against 0.64.0. The comparison now
      gives 924 "codes differ", 75 only in old and 79 only in new. Buckets: (a) the
      ValidDates filter, (b) the whole-set fallback, (c) curation in old, (d) a reused
      ItemId's dates, (e) other, (f) a source error for a D `set`.
    - "codes differ", 30 sampled with seed 0: 9 (a), 13 (d), 1 (b), 1 (c), 6 (e).
      - Of the 9 (a), 4 drops are right: code 86 and the historical kommuner end before
        the release, and Europabolag starts after it.
      - The other 5 (a) and all 13 (d) are wrong. Each drops real answer codes (Ja, Nej,
        Vet ej, grades A-F, kommun 0114) because their ItemIds are dated only from
        2008-2014, after the release. These dates record when the item entered the
        metadata, not when the code was valid.
      - The filter guards against this only when no item is kept. Here a sentinel such
        as "." or "Uppgift saknas" carries an older date and is kept.
      - The 6 (e) are 4 columns that one catalog merges and the other splits, 1
        ambiguous segment, and 1 code (Nej) the old build left out.
    - Only in old, all 75: (e).
      - 56 are ambiguous segments (F2): the occurrences state different sets, so the
        segment keeps none and the comparison, which reads segments, sees no codes. In 5
        of these the date filter causes the disagreement.
      - 2 are population alternatives (F6), 6 are CVIDs holding only an uncoded marker,
        5 are CVIDs with no rows in `Vardemangder`, and 6 are SOS transcriptions that
        state no set. In 10 of these 11 SCB cases the old variable merged another
        column's set.
      - #1949 tag: 5 involve a reused ItemId's interval, all among the F2 cases. In the
        2 `studiekommun` cases the dates end before the release and the drop is right;
        in `avslutunderaret` and `terminstuderande` (ht, vt) they start after it and it
        is wrong. A filter drop never empties a set (the fallback), so the ruling's
        evidence is in "codes differ", not here.
    - Only in new, 79: 49 (c), 17 (b), 13 (e).
      - (c) is the old `errata:steward-holdings`, `curated:representations` and
        `curated:coding.uncoded`.
      - (b) is 16 undated ULF sets and one r235 set, which the old build dropped.
      - (e) is 9 SOS transcriptions, 3 free-text marker sets (`Alfanumerisk`,
        `Kontinuerlig`) missing from the reader's uncoded list, and 1 Ja/Nej set the old
        build left out.
    - r118: the "Taxeringsår 2013" delivery names CVID 320512, the 21 Län codes, which
      is bucket (f). Check that F1 `domain-change` is raised there (c-f1 adds it).
    - r118 `kommun` has no segment from 2017 on. All seven "Deklarationsår YYYY
      (beskattningsår YYYY-1)" deliveries (2014-2020) are unparsed, so every r118
      variable ends in 2013. 0197 and 0199 are open-ended in the 2017, 2018 and 2021 LKF
      sets those deliveries name, so the cause is the delivery-name grammar, not a
      value-set rule.
    - Verdict: one bucket is large and wrong. The filter's start dates, which record
      metadata entry rather than code validity, drop real codes in 18 of the 30 sampled
      cases (13 on reused ItemIds, 5 on single-use ones; about 550 of the 924 by
      extrapolation). Reuse widens it. This re-opens D30 and the #1949 ruling and is
      escalated to the tracker. The rule is not changed here. The per-case table is in
      the PR that recorded it.
    - **After the ValidTo rule** (the tracker's ruling: drop an item only when all its
      intervals are closed and the latest ValidTo is before the period; ignore
      ValidFrom), from a full build at bc3e972c5 against 0.64.0.
      - Counts: "codes differ" 924 → 967, only in old 75 → 19, only in new 79 → 80. Two
        causes. The comparison now reads the occurrence sets of ambiguous (F2) segments;
        that empties "only in old" of its 56 F2 cases and adds one to "only in new". The
        rule moves 604 cases out of "codes differ" (553 to no difference, 51 to the
        blank code only) and 647 in.
      - The 30 sampled cases: of the 18 wrong drops, 16 now match and 1 differs only by
        the blank code. The 18th (ULF `ha1822`) gets back 3 of its 4 codes. Its fourth,
        `0`, now drops by the new rule, the class below. The 4 right drops: code 86
        (twice) and the historical kommuner still drop. Europabolag and code 55 are kept
        for the 1997 release, the cost the ruling named. The other 8 are unchanged (b,
        c, e), and the F2 case differs only by the blank code.
      - Only in old: the 56 F2 cases now show 44 no difference, 10 blank code only and 2
        `studiekommun` codes differ (1,151 → 295, retired kommuner, right). The other 19
        are unchanged. Only in new: the 79 are unchanged, plus `cfl/kurskod` (an F2
        segment with 7,177 codes, no set in the old catalog).
      - The 647 entrants: 646 lose only codes whose every interval is closed and ends
        before the release. The old "every item dated" and "some overlap" guards kept
        them, and the ruling removed those guards. 524 lose code `0`, a missing-value
        sentinel ("Värde saknas", "Okänt värde") whose three ItemIds end in 1993-1994.
        These are in ULF and the work-related-disorders survey from 2004 on, where `.`
        "Värde saknas" is open-ended. The rest are retired codes: SU, YU, ZR, TP, kommun
        1504, sector codes 11 and 15. Under the ruling these are right drops. Whether a
        sentinel that SCB closed in 1994 should drop from 2004 releases that still list
        it is for the tracker. The 647th, `ep-val-resultat/riksdagsvalkrets-kod`, loses
        `02`: its occurrences carry F6 although every population's rows name the same
        set. That is escalated as a possible resolver defect.
      - r118 (`scb/taxeringsutfallet`) `kommun`: 0197 and 0199 are absent 2014-2017 and
        present from the delivery named 2018 (beskattningsår 2017). The 2014-2016
        deliveries name the LKF 2013 set, and the 2017 delivery the LKF 2016 set. Both
        sets have 290 items and neither code. From 2018 the LKF 2017, 2018 and 2021 sets
        hold both, open-ended. This is the source, bucket (f): a D `set` if the
        2014-2017 data hold them.
      - Verdict: the ruling's check passes. The wrong drops are gone and the right ones
        stay. The guards' removal adds one large class, 646 ended codes that are right
        by the rule, most of them a legacy missing-value sentinel. It is reported to the
        tracker, not ruled here. The per-case table is in the PR that recorded it;
      - Ruling (tracker, drift-review-b2-recheck §2): the 646 codes newly dropped under
        the end-of-validity rule are "expected: validity end stated by the source"
        (bucket a). 524 of them are the sentinel `0`, superseded by the open-ended `.`.
        A marker row beside a coded row in one population is not a second value set
        (c-marker-rows), which clears the F6 on `riksdagsvalkrets-kod`. After #1975 the
        F6 'states different value sets across its rows' findings fell from 53 to 43;
        the remaining ones spot-checked are real disagreements (e.g. `scb/aku/syssokt8`:
        one population names two sets where 0 = 'Nej' vs 'Ej svar').
  - B3: when `reg-catalog` is rewritten, slim the resolve `Report`
    (`crates/reg-resolve/src/lib.rs`, 26 counters) to what the catalog cannot answer:
    the ledger lists and `split_keys`. The comparison script derives the rest with SQL;
  - B4: when C or D next changes `crates/reg-resolve/src/lib.rs` (1,569 lines), move
    gaps, cadence and stretch into `gaps.rs` and `assign_ids` and `Anchor` into
    `ledger.rs`. Code move only, no behaviour change, not a standalone PR;
  - for `reg-catalog` and the SPA: a blank code (`""`, SCB's "Uppgift saknas") is a real
    item until D marks it a sentinel. It must render and be searchable, and never be
    confused with "no value set".
- **The link stage:**
  - cross-register `continues` and `derived-from` links;
  - the SCB export's `Timeseries` events (series breaks, ends, replacements) and release
    notices;
  - tags;
  - the search index with code search;
  - parsed documents.
- **`reg-catalog`** rewritten over the new schema. The operations in
  `conformance/api/operations.toml` and the OpenAPI contract are kept where the model
  allows. Each contract change is reviewed against the server snapshot.
- **The order check**, which returns the logical order or its findings. An ambiguous
  window blocks. A window only a preliminary release covers orders with a
  `preliminary_data` warning; final wins where both exist. Choosing an edition or
  release per source is deferred until after curation: researchers never want
  preliminary data when final exists, and curation may remove the need.
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
- the maintainer approves the switch release.

**Outcome.** The maintainer approved C-2, C-3, C-14 and the switch release on
2026-10-27. `reg_meta/v0.65.0` was published 2026-10-27 from bump `629975af6` (inputs
`9186fcb`), built without `--release` (5,128 unminted variables). `verify` exited 0;
release conformance passed 4/4. The one catalog asset is `reg_meta.db.zst`.
reg-meta.vqv.uk serves generation
`0967d0cb920fbfb43de5e5562c72a01d93acb5d0662d11b13aec4ee8c9624507`, contract 5.0.0. The
recorded full real-seed build at `5380260f` (after #1992 and #1993) took 108 s, peaked
at 7.1 GB RSS and produced a 663 MB catalog; `reg-build verify` exited 0. Its
`meta.generation` is `0967d0cb920fbfb43de5e5562c72a01d93acb5d0662d11b13aec4ee8c9624507`,
identical to the build at `110134aa` (#1990): byte-identical output across reader-only
changes.

- Exit 1: release-tier conformance using `gate.py release`'s flags against that real
  artifact passed, 4 of 4 (`-m release`). The synthetic suite in `g0 rust` passed at
  #1993: 194 passed, 2 skipped. Classifications remain dark until D; holdings and
  steward cases return in E.
- Exit 2: `scripts/rebuild_compare.py orders` against 0.64.0, n=20 for each of seeds 0
  and 1, covered 40 sampled register cases (39 distinct registers). Every defect class
  has zero rows on both seeds. Diagnosis found no builder defect in either seed; #1990
  and #1992 fixed the comparator over nine review rounds.

Seed 0 (`.local/rebuild-c/rc/orders-0/summary.md`, Classes):

  | class                | rows | refusals | register-only records | registers | owner      |
  | -------------------- | ---: | -------: | --------------------: | --------: | ---------- |
  | blocked window       | 4211 |        6 |                     0 |         6 | intended   |
  | preliminary data     |    0 |        0 |                     0 |         0 | intended   |
  | old blocked          |    0 |        0 |                     0 |         0 | intended   |
  | identity change      |    0 |        0 |                     0 |         0 | intended   |
  | old curation         |    1 |        0 |                     0 |         1 | D curation |
  | unplaced release     |    0 |        0 |                     0 |         0 | D curation |
  | clipped period       |    0 |        0 |                     0 |         0 | intended   |
  | form boundary        |   99 |        0 |                     0 |         5 | intended   |
  | spelling             |   13 |        0 |                     0 |         5 | intended   |
  | representation only  |    0 |        0 |                     0 |         0 | intended   |
  | table split or merge |    0 |        0 |                     0 |         0 | defect     |
  | missing in new       |    0 |        0 |                     0 |         0 | defect     |
  | missing in old       |    0 |        0 |                     0 |         0 | defect     |
  | other                |    0 |        0 |                     0 |         0 | defect     |

Seed 1 (`.local/rebuild-c/rc/orders-1/summary.md`, Classes):

  | class                | rows | refusals | register-only records | registers | owner      |
  | -------------------- | ---: | -------: | --------------------: | --------: | ---------- |
  | blocked window       | 3437 |        6 |                     0 |         6 | intended   |
  | preliminary data     |    0 |        0 |                     0 |         0 | intended   |
  | old blocked          |    0 |        0 |                     0 |         0 | intended   |
  | identity change      |  330 |        0 |                     0 |         4 | intended   |
  | old curation         |  606 |        0 |                     0 |         2 | D curation |
  | unplaced release     |   33 |        0 |                     0 |         1 | D curation |
  | clipped period       |    0 |        0 |                     0 |         0 | intended   |
  | form boundary        |  833 |        0 |                     0 |         7 | intended   |
  | spelling             |   77 |        0 |                     0 |         6 | intended   |
  | representation only  |    0 |        0 |                     0 |         0 | intended   |
  | table split or merge |    0 |        0 |                     0 |         0 | defect     |
  | missing in new       |    0 |        0 |                     0 |         0 | defect     |
  | missing in old       |    0 |        0 |                     0 |         0 | defect     |
  | other                |    0 |        0 |                     0 |         0 | defect     |

- Blocked window: the new order refuses an ambiguous window with genuine rival SCB
  variables; D curation, per the maintainer's ruling of 2026-10-27.
- Identity change: the same column and days appear under a different variable id.
- Old curation: rows the old catalog has from its curated layer (period families and
  errata); carried over in D.
- Unplaced release: undated LISA releases (Avlidna, Födelseland, In- och utvandrade)
  remain unplaced; D.
- Form boundary: the new order splits at type or value-set changes.
- Spelling: latest spelling within a resolved run versus historical case.

Whole-catalog sizing found 3,788 ambiguous data segments in 99 of 291 registers; a
full-register order on any of those 99 refuses. Of the segments, 3,385 involve different
SCB VarIds claiming one column. Blocking only the actual overlap still leaves all 99
registers; exempting same-VarId type-only disagreements leaves 93. These are sizing
estimates, not executed order requests; D's ruling below adopts the exemption only.
Sources: `.local/rebuild-c/runs/diag-orders-1.txt`, `diag-orders-2.txt` and
`ambiguity-sizing-1.txt`.

The remaining C checklist:

- B2 shipped: value-set audit #1968, fixes and ValidTo rule #1970/#1971, recheck #1974,
  marker-row resolution #1975/#1976; buckets and r118 checks are recorded above.
- B3 shipped: #1957 slimmed `Report` to the ledger lists and `split_keys`.
- B4 shipped: #1957 moved gaps/cadence/stretch to `gaps.rs` and ids/anchors to
  `ledger.rs`.
- Blank codes render and are searchable in the reader (#1981/#1984); SPA rendering
  remains E, with the v4 SPA archived by #1991.
- Link stage shipped: cross-register links, Timeseries events and notices #1960; search
  and code search #1958/#1962; parsed documents #1964. Tags did not ship: they need D
  curation, as do sets.
- `reg-catalog` rewrite shipped: v5 contract #1963, admission #1973, operations
  #1978–#1985; the maintainer approved the contract on 2026-10-27.
- Logical order check shipped in #1985: ambiguity blocks, preliminary-only coverage
  warns and final wins; edition/qualifier selection remains deferred after curation.
- Conformance moved off `reg_meta_build` in #1969; `reg-build verify` owns artifact
  invariants (#1953), with harness cleanup in #1993. Holdings/steward cases await E.
- Documents in one catalog file shipped in #1964/#1973/#1983; the separate docs DB and
  reader gate are gone.
- Release/container schema plumbing and the release skill shipped in #1986/#1989, with
  deployment cleanup in #1993.
- Rust `dbdiff` shipped as `reg-build diff` in #1955; post-switch releases compare with
  their predecessor. The switch has no comparable v4 predecessor.
- Server switch shipped with `reg_meta/v0.65.0` on 2026-10-27; reg-meta.vqv.uk serves
  contract 5.0.0. SWECOV stays on 0.64.0 until E.

### D: curation carry-over

- **Maintainer rulings** (2026-10-27):
  - D-R: keep edition and qualifier. Address a release with `where release = '<handle>'`
    or `release:<table-slug>/<handle>`. Order-time release selection is deferred.
    Releases stay optional and are never synthesized for tables without one;
  - ambiguity: occurrences sharing a source key and folded spelling are not ambiguous
    when they disagree only in type, or type and case (#1998). Label, value-set and
    rival-variable disagreements still block. Overlap-only blocking is not adopted; the
    rival check marks the whole run (99 → 93 registers);
  - D-3: no source priority or frame-level merges. Selectors that split occurrence
    periods are refused; chained moves conflict. `set variable` changes exactly the
    selected occurrences;
  - contract 5.1.0 is approved within D for D-10's `follow_successor`.

- **F1 `domain-change` rule**: it is not emitted when only the classification version
  differs (tracker F1 spec). Moot in C, since class versions start in D; implement it
  with V1, or every version bump fires a domain-change.

- **A one-shot converter** turns today's 17,299 entries into the four statement kinds,
  reading the new frames. It is deleted at F. For the 1,819 identity partitions the dry
  run gives the expected outcome:

  | Outcome        | Entries |
  | -------------- | ------- |
  | No statement   | 1,461   |
  | `set variable` | 97      |
  | Merge          | 55      |
  | `set`          | 145     |
  | Regroup        | 61      |

  Of the 97 curated splits, 8 are case-only. They are dropped and listed.

- **Statements A deferred**, each found by A's comparison report and the tracker's audit
  of A:
  - EKB: a `set` of `EKB_AR`'s coverage. The tables sheet bounds it 1990–1997, but its
    variable rows run to 2010–2017, so 397 variables are withheld. The source bound is
    wrong; no rule widens a table from its occurrences. `EKB_MANAD`'s clamps are the
    same pattern;
  - `assert` for tables the variables sheet cites but the tables sheet does not declare
    (or `set table` onto an existing table): LOVA `A_LOVA_*`, DORS `DORS-COV`, LMED
    `VARA/LMED`, LSS. The ledger's old table ids name the declared tables;
  - per-occurrence `set identifier = false` where `Kopplingsvariabel` marks a column
    that is not an identifier: LMED `ATC` and `VARUNR`, LVM `BESDATUM`;
  - `set` value set (or `assert`) for the six code lists the reader leaves unbound: BU
    personnummerkvalitet and vartbarn, LMED kon and transtyp, LVM avslut, SOL pomvtrygg;
  - LVM `besdatumn`: a `set variable`, or a second ledger anchor.

- **Proposals, built before new curation** (review findings that carry a proposed
  statement, so they need D's statement grammar): F3 continuation proposals (a proposed
  `link continues` or `set variable`; the only way renames under successive keys are
  found) and V1 classification-version proposals (they rank, never bind). F4 `same-text`
  proposals are built when a curator asks.

- **D's link stage drops no successions in the real corpus** (`succession_dropped`
  remains zero), so no successor-choice or `reject` form exists. A curated link that
  closes a cycle or gives a table a second successor on one day is a located refusal.
  The existing source-only dropping rules remain unchanged. Add a choice form only for a
  demonstrated real case.

- **Release builds refuse unplaced and split ledger ids** once D's curation exists;
  until then they are only counted in the build report.

- **LISA section headings** yield reviewable set candidates from the reader's heading →
  rows map, and adopted sets are ordinary curation.

- **Classification curation, sets and tags** are converted. Classification references
  are always curated.

- Set/tag presentation metadata (axes, filters, value labels, coordinates, tag
  rank/star/notes) is useful for the webapp and is incorporated later; source:
  reg_meta_build/curation at 99de15a6a908c032972e5b32dea97c7b1808ab07.

- **`follow_successor`**, an optional binding field in D-10, is additive; the default
  does not follow.

- **Classification pins** (`classification` in a binding) resolve once D curates class
  versions. Until then every pin is `fqid_unresolved`.

- **Unminted ids** are minted, and the new ids are reviewed in the PR.

- [x] **D-13: From D on, curation is edited only in `curation/`.** The old tree is
  frozen for the SWECOV build until E.

**Exit:**

- [x] every old entry is converted, dropped with a named reason, or refused at its file
  and line in the converter's report;
- [x] the curated strict build passes;
- [x] no variable is unminted;
- [ ] the comparison is re-run, and every curated difference is explained.

**D-X outcome (2026-10-28): blocked on the comparison; no release.** Checks used the
committed tree `8cc73d5fe` and clean inputs `9186fcbceeb2283a50c7f684764869e4dda5765e`,
with the pinned SCB archive. The release binary's second strict build exited 0 in
393.923 s, peaking at 10,340,057,088 bytes RSS. The first took 363.41 s; its
`/usr/bin/time -l` wrapper failed on a sandbox-denied `sysctl` after writing the
catalog, so its builder exit and peak RSS were not captured. Independent
`reg-build verify` passed (exit 0, 27.617 s). Both catalog files and JSON reports were
byte-identical. Catalog size: 1,181,515,776 bytes; SHA-256:
`23ddf88880a0821b1d50693bf718c82781c37c7649fb91ec300e34fcd4268aa2`.

- Retained catalog: this worktree's `.local/d-exit/strict/reg_meta.db`.
- Generation: `6b0b6bc63bef882a6dafb4d5fdd453d5dbdcf7b56147a9f3afd4b4fd22c07083`.
- Curation digest: `dd835cefa6c133f6fe75cd4cd7463f560177d88088cfabaf92ed255df2d6e598`.
- Unminted, unplaced, split and merged ledger ids: all zero. Skipped statements: zero;
  redundant-set notices: 45; source successions dropped: zero.
- F1 counts: definition-change 20,677; domain-change 16,040; label-differs 47;
  label-drift 181; type-change 6,713; unit-change 5,977; population-change zero.
- Source disposition evidence remains in `scripts/rebuild_convert_report.md`,
  `scripts/rebuild_convert_classifications_report.md` and
  `scripts/rebuild_convert_sets_tags_report.md`. These are overlapping stage reports,
  not additive final counts: base conversion 5,603 converted / 12,538 named drops / zero
  refused; classification conversion 288 / 218 / six; set/tag report 551 converted
  membership rows, 115 missing-ID refusals, 11 deferred rows and 729 drops. The reports
  retain source locations and reasons; later D work supersedes some early deferrals. The
  converters were not rerun.
- Rust `reg-build diff` against the downloaded 0.65.0 asset exited 1 (differences), with
  no schema change. It reports 28 changed table counts, 6,687 added and 5,149 removed
  public ids, 29 shared table changes, 5,411 shared variable changes and eight added
  value sets. Aggregate/provenance review reconciles these to D's statements, minted
  identities, sets/tags, classifications, LISA attachment and descriptions; no
  unexplained dbdiff category was found. This does not independently certify every
  changed text or selector.

The full random-order comparison against downloaded 0.64.0 ran with n=20 for each of
seeds 0 and 1. Both commands exited 0, but their classifications fail the exit
criterion:

  | Class            | C seed 0 | D seed 0 | C seed 1 | D seed 1 |
  | ---------------- | -------: | -------: | -------: | -------: |
  | Blocked window   |     4211 |     2939 |     3437 |     8453 |
  | Identity change  |        0 |        5 |      330 |      155 |
  | Old curation     |        1 |        0 |      606 |      538 |
  | Unplaced release |        0 |        0 |       33 |       33 |
  | Form boundary    |       99 |      130 |      833 |      861 |
  | Spelling         |       13 |       13 |       77 |       72 |
  | Other            |        0 |       24 |        0 |      720 |

All other classes have zero rows. Blocked-window refusals: seven and six (C: six each);
register-only difference records: zero. Retiring the standalone workbook register
changes the sampling pool from 291 to 290; only 13 and 14 sampled registers respectively
overlap C, so these totals are not a fixed-register trend.

**Comparator-classified `other` rows: 744.** Seed 0 has 24 on `scb/moms/sateskommun`;
seed 1 has 720 across seven registers. The comparator's `form_boundary()` checks type
and value-set changes but omits classification versions. A read-only audit of the
complete saved responses and catalog explains all 501 unsupported interior boundaries by
classification-version transitions (eight in seed 0, 493 in seed 1). No unexplained
boundary remains; no catalog defect was established. The comparator defect remains
unfixed, and its corrected replay is unrun. No code was changed. D's comparison box
remains open.

Stopped at the defect as instructed. Release-tier conformance, full gates, synthetic
suites, legacy real-seed builds, converter replay, `gate.py regen`, release publication
and PR merge were not run. The replay catalog and downloaded comparison artifacts are
removed after reporting; the strict catalog is retained for a later release decision.

**Outside:** G0 runs ~160 s against its 60 s budget; fixed in a separate task.

- **2026-10-28 ruling:** curate evidenced integer/decimal source mislabels only on
  affected occurrences, leave unresolved cases uncurated for quieter notices; N1 general
  logical-type inference stays at E.

### E: the holdings layer

- **Design first, for the maintainer's review.** The design covers:
  - steward and project holdings, and the three questions holdings answer: what is in my
    project, what my steward holds, and what exists with the providers;
  - the mapping from a logical order to files;
  - the MONA order file;
  - mock data;
  - today's holdings vocabulary as a checklist: partitions, projections, wide column
    series, column windows and key-only files;
  - N1 logical-type inference: review findings where it disagrees with the reader's
    `[types]` map. Its consumers are mock data and output types, which are E's.
- **Then the build.** SWECOV's holdings run on the new catalog, together with its
  steward catalog, its release asset and physical orders. `reg_mona_runner` reads the
  new order file.
- **The SPA** is archived from main at the switch and rewritten in E on the v5 API from
  `archive/reg-webapp-spa` and `claude/rebuild-s-shell-search`, after curation and
  holdings. The work covers shell and search, entity pages, values/graph/docs, project
  and order, and flows. Until E the SPA is not part of any milestone exit.

**Exit:**

- SWECOV's catalog and release asset come from the new stack;
- the conformance holdings and order cases are green;
- the SPA flows are green;
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
  `scripts/gate.py`, `scripts/parse_lisa_docs.py`, the root `pyproject.toml` and
  `uv.lock`, `renovate.json`, `.panache.toml`, `.dockerignore`, `.gitignore`,
  `reg_webapp/`, `conformance/` and `scripts/tests/`.

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
- holdings as a separate layer;
- sensitivity follows the source per segment (design v4 §4.6), not a variable-wide
  ratchet (maintainer yes, 2026-10-26). 73 variables become sensitive only in some
  segments (57 in LISA r34); D corrects source errors with `set sensitive`.

The durable parts are in [crates/DESIGN.md](crates/DESIGN.md).
