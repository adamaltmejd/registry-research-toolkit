# D recovery fix-03 accounting

Temporary conversion evidence; deleted at milestone F (cutover).

Current statement accounting is 827 additions: 571 assertions and 256 setters. Four FoU
identity setters are withdrawn to retain the published STRAT family. All 23 recovered
identity setters are deferred. This fix restores 33 published r257 IDs to their 0.66.0
native targets and replaces two hash anchors with dates. It names the 38 recovered
CIS2002 columns separately: eight free legacy names and 30 D-12 label-plus-column names.
The branch still mints 42 IDs total. The fix-02 claim of retained populations used the
wrong reference and is superseded. The full published-ID comparison and new names are in
[the recovery report](rebuild_recover_report.md).

# Historical D recovery fix-02 counts (superseded)

Temporary conversion evidence; deleted at milestone F (cutover).

Current branch additions are 831 statements: 571 assertions and 260 setters. Four FoU
identity setters are restored; 19 remain deferred (16 D-11, three ID-emptying). Retained
source entries by kind are 299 delivered, 239 parallel, 176 partition, 44 column, four
column-owner and two description, plus one tag membership.

The fix adds explicit shared keys to 107 recovered assertions and mints 42 IDs: r2 one,
r193 three replacing its 34 branch-added IDs, r257 38. Baseline public ID names and LISA
bindings/lineage remain unchanged; 35 r257 anchors gain disambiguating source keys.
Current setter dispositions and checks are recorded in [the recovery
report](rebuild_recover_report.md). Earlier counts below are historical.

# Historical D recovery fix-01 counts (superseded)

Temporary conversion evidence; deleted at milestone F (cutover).

Current branch additions are 827 statements (571 assertions and 256 setters), including
the tag membership. All 23 recovered variable-assignment setters are dropped: 18 violate
D-11 and five empty the minted FoU `stratum-strat` identity. The 34 legitimate r193
assertion identities now have dated ledger anchors. Source-level review, all new ID
names, dropped subjects and final validation are in [the recovery
report](rebuild_recover_report.md). Earlier counts below are historical.

# D close-out recovery

Temporary conversion evidence; deleted at milestone F (cutover).

The catalog-backed recovery retains locked additional outputs from 774 legacy entries:
299 `errata.delivered`, 239 `representation.parallel`, 183 `identity.partition`, 44
`errata.column`, seven `identity.column_owner` and two `enrichment.description`. There
are 849 added statements (571 assertions, 278 setters), plus the independent set/tag
replay's one IoT `familjetyp` tag membership. These are additional outputs; they do not
replace the historical stage counts below.

The prior 806 absent-everywhere / 125 other-register ledger-target drops now read 775 /
100, plus two mixed missing-ID/unplaced entries. Missing/undated-release drops are 49
after rechecking the prior 426 missing-release drops against exact published releases.
The output-level inventory, located refusals and final validation status are in [the D
recovery report](rebuild_recover_report.md); all remaining cohorts have a home in
REBUILD.md §D deferrals.

Earlier conversion measurements below are historical.

# D-4b fix round 01: readable selectors

Output committed at `bda3ce88fdfed654e74fda9bd7e0b7a943d43b85`. Rerun only to regenerate
before D-13 freezes the old tree.

Collapse setters from the same legacy entry only after comparing exact occurrence
membership against raw and asserted facts. Prefer spelling alone, then a normalized
calendar union; remove key guards only with the same membership. Preserve whole
occurrences and refuse ambiguity between overlapping source keys/labels. Assertion
subjects construct facts, so their key/release guards remain unchanged. Legacy source
file:line stays first in evidence.

  | Measure        | Before | After  |
  | -------------- | -----: | -----: |
  | Statements     | 13,746 |  6,113 |
  | Setters        | 11,044 |  3,411 |
  | TOML lines     | 83,227 | 37,320 |
  | Release guards | 10,607 |    660 |
  | Key guards     | 11,345 |    743 |

Remaining release guards: 545 assertion constructors, 98 setters distinguishing
calendar-overlapping editions, and 17 setters whose undated editions cannot be selected
faithfully by a calendar period. Assertions (2,599) and links (103) are unchanged. All
244 statement/lock files reproduced byte for byte with the final converter. Both
corrected locks passed with 45 redundant-set notices and no refusals.

**The before/after catalog files are not byte-identical.** Collapsing statements changes
the published statement rows, authoring locations, curation digest and generation ID.
The full-file SHA-256 comparison was:

- before: `f3a47622164b5a61140f3455a30f5f089b0051ed0cda1f279b207dc4bc3dbb99`
- after: `b524f8dfb2f01ffec5ef6ff0141a1e032ea64b27aac63da817fb85a8a4850fa2`

`reg-build diff` reports only statement row-count and two meta-value changes. A full
logical-table comparison found all 51 catalog tables equal, preserving raw inputs,
source locators and entity ID mappings. It normalizes declared text references and
statement references (kind, evidence, caveat), excludes the statement table and the
`generation_id`/`curation_digest` meta values, and separately identifies SQLite's
changed `sqlite_stat1`/`sqlite_stat4` planner statistics. The initial full comparison
returned 1 solely for those two physical statistics tables; the 51 catalog-table hashes
all match. This proves unchanged applied catalog effects, not literal file identity.
Published statements: 13,569 → 6,068 (2,599 assert, 3,366 set, 103 link).

Release binary; inputs remain at `9186fcbceeb2283a50c7f684764869e4dda5765e` and the SCB
asset SHA-256 at `a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`.
Heavy commands ran sequentially.

  | Command           | Exit | Wall seconds |
  | ----------------- | ---: | -----------: |
  | release-build     |    0 |        0.208 |
  | read              |    0 |       56.720 |
  | before-build      |    0 |      263.184 |
  | before-verify     |    0 |       23.615 |
  | convert           |    0 |       70.675 |
  | convert-lock      |    0 |      185.866 |
  | rerun             |    0 |       71.890 |
  | rerun-lock        |    0 |      185.065 |
  | final-replay      |    0 |       73.687 |
  | final-replay-lock |    0 |      185.572 |
  | after-build       |    0 |      261.613 |
  | after-verify      |    0 |       23.601 |
  | diff              |    1 |        0.815 |
  | effects           |    1 |       84.812 |

The first two conversions straddled a case-folding correction found in review; their
replay mismatch was not accepted as reproducibility evidence. The final two corrected
conversions/locks are identical. Independent final source review passed. No new tests.

`g0 rust` passed in 21.644 seconds: lint/type/schema/vocabulary checks, Rust
Clippy/tests/dependency checks, 116 script tests, byte-identical synthetic builds, and
194 conformance cases (2 skipped). Its first attempt caught an undeclared report
attribute; initialization fixed the type check without changing generated output.

Not run this round: legacy real-seed build, release gate, curated release build, R
suites, `gate.py regen`, or an additional uncurated build. No new tests. Final Markdown
formatting/lint and `git diff --check` passed. Task-owned scratch is removed after
reporting.

Earlier conversion measurements below are historical.

# D-4b: committed curation

Output committed at `8471d6c3b297c14ce904bd39a3164358a9e2d745`. Rerun only to regenerate
before D-13 freezes the old tree.

The converter's explicit `--write-curation` mode writes into this checkout's
`curation/`; its default still writes to a new scratch root. Each statement includes its
legacy source file:line in evidence. There are 122 register statement/lock pairs, with
no generated comments or empty statement files. Two conversions and locks on inputs
`9186fcbceeb2283a50c7f684764869e4dda5765e` reproduced all 244 files byte for byte. The
SCB asset digest remains
`a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`.

Both locks passed with 177 redundant-set notices and zero refusals. The committed-tree
dev build and explicit verify passed. Applied: 2,599 assert, 10,867 set, 103 link, zero
ack. Relative to uncurated: +1,912 variables, +2,370 segments, -67 ambiguous segments,
+155 withheld occurrences. The dev report retains 6,927 unminted variables; this does
not certify D's release/minting exit gate.

  | Command      | Exit | Wall seconds |
  | ------------ | ---: | -----------: |
  | read         |    0 |       65.964 |
  | uncurated    |    0 |      132.013 |
  | convert      |    0 |       79.184 |
  | convert-lock |    0 |      187.085 |
  | rerun        |    0 |       68.820 |
  | rerun-lock   |    0 |      187.572 |
  | curated      |    0 |      266.903 |
  | verify       |    0 |       23.423 |
  | diff         |    1 |        1.170 |

The diff's exit 1 means expected catalog differences. Rust data commands ran
sequentially. Initial Python environment synchronization briefly overlapped the dev
build, so its wall time is not an isolated performance benchmark. The first release
compile failed in the host sccache; disabling RUSTC_WRAPPER succeeded.

CI's Rust build and conformance use their own synthetic curation roots; the legacy suite
reads `reg_meta_build/curation`. No inspected CI step applies this real tree. Default
build paths, fixtures and tests are unchanged.

`uv run --no-project scripts/gate.py g0 rust` passed in 62.404 seconds:
lint/type/schema/vocabulary checks, Rust Clippy/tests/dependency checks, 116 script
tests, byte-identical synthetic rebuilds, and 194 conformance cases (2 skipped).
Independent converter and generated-file review passed. No new tests were added.

Not run: legacy real-seed build, release gate, R suites, `gate.py regen`, or curated
release build. Task-owned scratch artifacts are removed after reporting.

The earlier converter reports below are historical measurements.

## D-3c fix round 01: current finding codes and a smaller link stage

Deleted at milestone F (REBUILD.md).

The regenerated converter root now locks directly. All 2,296 legacy acknowledge source
declarations are dropped with the exact reason
`legacy finding code; no new-model finding`. The previous 1,046 normalized ack proposals
become **zero**. There are no current-code acknowledge declarations in this corpus; the
converter retains the translation branch for any such declaration. The current code
vocabulary comes from the resolver's finding-code literals.

Source accounting: **5,603 converted / 12,538 dropped / 0 refused**, across 18,141
declarations. Other proposals remain unchanged: 2,599 assertions, 11,044 setters, and
103 `label_of` links.

Removed predecessor/day successor choice, source-edge displacement, displaced-edge
`override` cells and JSON aggregation, the unpruned source-link mode and extra relation
locator, source-candidate premise snapshots, a duplicate full-catalog resolve, and the
trivial `ack::apply` wrapper. Curated links add relation rows with provenance and
optional validity. The existing source link stage owns endpoint matching and source-only
dropping. A curated cycle or second table successor on one day is a located refusal.
Exact endpoint and finding selection, F5 marker matching, finding premises, identity
rekeying and conflict guards remain because the explicit plan needs them. No
successor-choice or reject form exists.

Implementation/docs/converter diff, excluding this report: **+811/−73**, down from
**+1057/−77**. `link/curated.rs`: **523 → 323 lines**; `curation/ack.rs`: **162 → 156**.
The generated report below replaces the previous round's snapshot and removes its stale
ack proposal counts. The full PR diff, including this report refresh, is reported in the
PR.

Measurements use the runtime and converter sources in this report's commit. Inputs
remain at `9186fcbceeb2283a50c7f684764869e4dda5765e`; the SCB asset is pinned by
`a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`. It was downloaded
inside this checkout. The main checkout and accepted inputs were not modified. All heavy
commands ran sequentially. Wall time and Darwin child peak RSS were recorded with
monotonic time and `getrusage(RUSAGE_CHILDREN)`.

  | Command                     | Exit | Wall seconds | Peak RSS bytes |
  | --------------------------- | ---: | -----------: | -------------: |
  | Release binary build        |    0 |        7.569 |    704,987,136 |
  | Pinned input read           |    0 |       54.617 |  7,666,188,288 |
  | Converter regeneration      |    0 |       67.105 |  5,519,065,088 |
  | Real lock, regenerated root |    0 |      184.839 |  7,941,341,184 |
  | Curated dev build           |    0 |      256.252 |  8,306,671,616 |
  | Explicit verify             |    0 |       23.123 |     81,772,544 |

The real lock accepted **13,746 premises in 122 files**, with **zero stale-ack
refusals**. No proposals were removed after regeneration and no locks were copied from
another root. The dev build and explicit `reg-build verify` passed. Applied statements:
**2,599 assert / 10,867 set / 103 link / 0 ack**. All 103 curated relation rows are
`label_of`; `continues`, `derived_from` and `equivalent` have zero applied rows in this
corpus. The build report has **0 located refusals, 0 stale acks and 0 missing
premises**. `succession_dropped` remains **0**; the previous round's uncurated
comparison also had zero. The current corpus needs no curated choice or reject
operation.

Curated digest: `2bb6876508555e7fc61570b6df8466532fd5f5e152c9f41721f8a5eeb0289285`.

`g0 rust` passed: formatting, Ruff, type/schema/vocabulary checks, Rust Clippy/tests and
dependency checks, 116 script tests, byte-identical fixture rebuilds, and 194 runtime
conformance cases passed (2 skipped). Independent read-only review found no remaining
issue. No new tests were added. Final Markdown formatting/lint also passed.

Not run this round: legacy real-seed build, release gate, `gate.py regen`, R suites,
additional uncurated or pre-change byte-comparison builds, or new ack/successor
synthetic cases. No rebase, force push, PR merge, or main-checkout mutation occurred.
Generated frames, curation, catalogs and logs used task-local `.local/d-link-ack-r1/`
and are removed after reporting.

# One-shot curation conversion report

Proposals only. Lock records current premises; old fingerprints have not been certified.

Scoped declarations: 18141 = 17299 register curation entries + 750 table declarations +
92 classification declarations/bindings. Slug pins and inline payload rows are not
additional statements.

  | Outcome   | Entries |
  | --------- | ------: |
  | converted |    5603 |
  | dropped   |   12538 |

  | Statement / field / subtype         | Count |
  | ----------------------------------- | ----: |
  | `assert`                            |  2599 |
  | `assert.entity.occurrence`          |  2552 |
  | `assert.entity.release`             |    47 |
  | `assert.fields.definition`          |  2552 |
  | `assert.fields.identifier`          |   663 |
  | `assert.fields.label`               |  2552 |
  | `assert.fields.length`              |   510 |
  | `assert.fields.sensitive`           |   663 |
  | `assert.fields.type`                |  2552 |
  | `assert.fields.unit`                |    37 |
  | `link`                              |   103 |
  | `link.relation.label_of`            |   103 |
  | `set`                               | 11044 |
  | `set.fields.coded`                  |   144 |
  | `set.fields.definition`             |   264 |
  | `set.fields.description`            |  4947 |
  | `set.fields.format`                 |     1 |
  | `set.fields.identifier`             |     8 |
  | `set.fields.label`                  |  4141 |
  | `set.fields.notice`                 |     1 |
  | `set.fields.operational_definition` |   104 |
  | `set.fields.period`                 |   114 |
  | `set.fields.sensitive`              |     7 |
  | `set.fields.sets`                   |   243 |
  | `set.fields.spelling`               |   146 |
  | `set.fields.unit`                   |   280 |
  | `set.fields.value_set`              |   174 |
  | `set.fields.variable`               |   491 |

Explicit legacy set-field requests, including entries whose selectors or payloads still
need review (counts are source entries, not expanded statements):

  | Field                | Entries |
  | -------------------- | ------: |
  | `classification`     |       6 |
  | `definition`         |      65 |
  | `description`        |     395 |
  | `format`             |       1 |
  | `identifier`         |       8 |
  | `label`              |    1744 |
  | `period`             |     151 |
  | `sensitive`          |       7 |
  | `source_attribution` |      12 |
  | `spelling`           |      16 |
  | `table`              |      20 |
  | `unit`               |     166 |
  | `variable`           |    6088 |

## D-3b field scope

Counts below include mechanically mapped deferred categories. These are proposals, not
lock-certified applications.

  | Set field                | Proposed statements |
  | ------------------------ | ------------------: |
  | `coded`                  |                 144 |
  | `definition`             |                 264 |
  | `description`            |                4947 |
  | `format`                 |                   1 |
  | `identifier`             |                   8 |
  | `label`                  |                4141 |
  | `notice`                 |                   1 |
  | `operational_definition` |                 104 |
  | `period`                 |                 114 |
  | `sensitive`              |                   7 |
  | `sets`                   |                 243 |
  | `spelling`               |                 146 |
  | `unit`                   |                 280 |
  | `value_set`              |                 174 |
  | `variable`               |                 491 |

The counts include translated parallel grouping (`sets`) and delivered donor wording
(`operational_definition`). Field application is tested with the d-set-fields
integration. Calendar corrections use `period`; the 33 noncalendar declarations stay
dropped as D-7. Classification clearing and parallel metadata semantics are resolved
below.

## Identity and case audit

Historical set variable: 12,151 statements from 6,088 owner entries. Current proposed
set variable: 491. The current pass also projects partitions previously skipped by the
approximate census; its pre-filter population differs from the historical one.

Projection audit:
`{"after": 491, "before": 21362, "coalesced_duplicate_effects": 32, "deferred_concurrent_folds": 18, "gap_marker": 1, "missing_local.absent-everywhere": 2830, "missing_local.only-other-register": 827, "orphan_gap_marker": 1, "removed_default_restatements": 16889, "unplaced": 273}`.

Case safety:
`{"different_or_unproven_outcomes": 42, "distinct_source_coordinates": 880, "same_outcome_folded_selector": 94}`.
Remaining cases are listed in report.json.

Cached proposal audit: `{}`. This is a static reconstruction, not a new real-frame lock.

Missing ledger causes: `{}`. A same-named id in another register is not authority to
borrow it; changed groups without local ids wait for D-12.

  | Partition outcome | Historical | Observed from frames | Four-kind translation                                     |
  | ----------------- | ---------: | -------------------: | --------------------------------------------------------- |
  | No statement      |       1461 |                 1382 | skip: projected default identity (not resolver-certified) |
  | set variable      |         97 |                  108 | set variable (case-only splits dropped)                   |
  | Merge             |         55 |                   45 | set variable onto one ledger id                           |
  | set               |        145 |                  222 | set sets (simultaneous columns stay distinct)             |
  | Regroup           |         61 |                   62 | set variable per curated owner                            |

The historical program/output are missing. This extends the recovered partitions.py
census; observed counts use current source periods, without edition shadowing. They are
not a resolver equivalence proof.

## Dropped reasons

  | Reason                                                                                                                                                                    | Entries |
  | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------: |
  | D-11: identity assignment merges concurrent different folded spellings in one table; retain separate variables                                                            |       2 |
  | D-11: identity assignment merges concurrent different folded spellings in one table; retain separate variables; projected default identity already has the legacy outcome |       3 |
  | D-12: delivered occurrence has no unique existing unqualified ledger anchor                                                                                               |      60 |
  | D-12: grouping has no proved placed final variable with a local ledger id                                                                                                 |      45 |
  | D-12: target has no local ledger id (absent-everywhere)                                                                                                                   |     806 |
  | D-12: target has no local ledger id (absent-everywhere); D-7: identity assignment targets an unplaced occurrence                                                          |       1 |
  | D-12: target has no local ledger id (only-other-register)                                                                                                                 |     125 |
  | D-12: target has no local ledger id (only-other-register); D-7: identity assignment targets an unplaced occurrence                                                        |       1 |
  | D-7                                                                                                                                                                       |      33 |
  | D-7: identity assignment targets an unplaced occurrence                                                                                                                   |      85 |
  | D-7: identity assignment targets an unplaced occurrence; projected default identity already has the legacy outcome                                                        |     104 |
  | D-7: parallel legacy window disagrees with new-reader release placement                                                                                                   |       4 |
  | asserted column classification needs reviewed family/version mapping                                                                                                      |       6 |
  | asserted column needs a reviewed existing table                                                                                                                           |      21 |
  | asserted column needs an explicit expressible period                                                                                                                      |      48 |
  | assertion needs one existing table; table declaration requires review                                                                                                     |     109 |
  | case aliases covered by folded spellings; no statement                                                                                                                    |     211 |
  | case-folded variants have different or unproven legacy outcomes                                                                                                           |      42 |
  | case-only split: folded spelling cannot distinguish owners                                                                                                                |       8 |
  | classification binding lacks an explicit family/version mapping                                                                                                           |       9 |
  | classification correction needs reviewed family/version mapping                                                                                                           |       6 |
  | classification metadata and sentinel definitions have no complete four-kind mapping                                                                                       |      83 |
  | classification_timeline: requires reviewed translation of legacy payload/guards                                                                                           |       1 |
  | code list has no unique existing frame value set; needs handwritten assertion                                                                                             |      62 |
  | coding choice needs a reviewed selected source value-set hash                                                                                                             |     186 |
  | coding.omit: requires reviewed translation of legacy payload/guards                                                                                                       |       5 |
  | coding.sentinel: requires reviewed translation of legacy payload/guards                                                                                                   |      15 |
  | coding.support: requires reviewed translation of legacy payload/guards                                                                                                    |       8 |
  | delivered donor facts disagree in length; reviewed edition-specific mapping required                                                                                      |      13 |
  | delivered donor facts disagree in operational_definition; reviewed edition-specific mapping required                                                                      |      50 |
  | delivered donor facts disagree in type; reviewed edition-specific mapping required                                                                                        |      15 |
  | delivered occurrence needs one dated existing release; convert the missing release first                                                                                  |     426 |
  | delivery metadata alternatives disagree or lack the requested value                                                                                                       |       3 |
  | delivery metadata window does not cover a whole source occurrence                                                                                                         |       1 |
  | documentary.binding: requires reviewed translation of legacy payload/guards                                                                                               |      21 |
  | documentary.retained: requires reviewed translation of legacy payload/guards                                                                                              |      16 |
  | enrichment.alias: requires reviewed translation of legacy payload/guards                                                                                                  |      44 |
  | errata.occurrence_carry: requires reviewed translation of legacy payload/guards                                                                                           |       4 |
  | errata.support: requires reviewed translation of legacy payload/guards                                                                                                    |      35 |
  | four-kind fields cannot clear a value                                                                                                                                     |       4 |
  | gap marker has no variable identity; retain its finding; identity assignment would orphan a default gap marker; retain its finding                                        |       1 |
  | group: requires reviewed translation of legacy payload/guards                                                                                                             |      48 |
  | grouping identity may change after asserted occurrences; reviewed final membership required                                                                               |      11 |
  | identity split needs a reviewed non-type selector                                                                                                                         |      31 |
  | identity.edition_split: requires reviewed translation of legacy payload/guards                                                                                            |      37 |
  | identity.rename: requires reviewed translation of legacy payload/guards                                                                                                   |       1 |
  | identity.route: requires reviewed translation of legacy payload/guards                                                                                                    |      20 |
  | legacy diagnostic/link endpoint has no placed final local variable; reviewed current finding/link required                                                                |       2 |
  | legacy field has no four-kind field                                                                                                                                       |      12 |
  | legacy finding code; no new-model finding                                                                                                                                 |    2296 |
  | legacy period cuts a whole source occurrence                                                                                                                              |       8 |
  | new reader binds no source classification                                                                                                                                 |      52 |
  | no exact new-frame occurrence endpoint                                                                                                                                    |     257 |
  | noncontiguous occurrence coverage cannot be expressed by set period                                                                                                       |       2 |
  | occurrence period requires an explicit parent coverage correction; no scope is shrunk                                                                                     |       2 |
  | overlapping delivered releases need an assertion-conflict design decision                                                                                                 |      18 |
  | parallel declared occurrence absent from new frames; reviewed assertion required                                                                                          |      33 |
  | partition includes case-only owners beside other spellings; needs reviewed regrouping                                                                                     |      13 |
  | projected default identity already has the legacy outcome                                                                                                                 |    6188 |
  | qualified ledger endpoint needs reviewed key/day selection                                                                                                                |       5 |
  | release name needs reviewed coverage interpretation; do not infer coverage from its token shape                                                                           |       4 |
  | representation.alias_window: requires reviewed translation of legacy payload/guards                                                                                       |       4 |
  | representation.matrix: requires reviewed translation of legacy payload/guards                                                                                             |       7 |
  | representation.period_family: requires reviewed translation of legacy payload/guards                                                                                      |       8 |
  | source spelling cannot be expressed by address grammar                                                                                                                    |       7 |
  | table identity already in ledger; panel metadata has no four-kind field                                                                                                   |     750 |

## Case-only splits

- reg_meta_build/curation/registers/scb/ftr.toml:223
- reg_meta_build/curation/registers/scb/ftr.toml:230
- reg_meta_build/curation/registers/scb/ftr.toml:237
- reg_meta_build/curation/registers/scb/ftr.toml:244
- reg_meta_build/curation/registers/scb/ftr.toml:251
- reg_meta_build/curation/registers/scb/ftr.toml:279
- reg_meta_build/curation/registers/scb/ftr.toml:286
- reg_meta_build/curation/registers/scb/tradbransle-torv.toml:52

## Lock

Status: accepted by the separately measured real lock above; zero located refusals.

  | Lock error | Located refusals |
  | ---------- | ---------------: |

## Statements A deferred

The converter covers none of these A comparison repairs; they stay hand-written:

- EKB_AR/EKB_MANAD coverage: source-bound corrections are absent from supported legacy
  fields; hand-written set coverage.
- LOVA/DORS/LMED/LSS undeclared tables: hand-written assert table or set table; no
  guessed routing.
- LMED ATC/VARUNR and LVM BESDATUM identifier flags: no explicit legacy errata.flags or
  errata.field repair targets these columns; reader flags are not inverted by inference.
- BU personnummerkvalitet/vartbarn, LMED kon/transtyp, LVM avslut, SOL pomvtrygg code
  lists: hand-written assert/set after documentary endpoint review.
- LVM besdatumn: the legacy BESDATUM/BESDATUMN identity.split entries at
  sos/lvm.toml:173 and :198 use non-type selectors and are dropped; a reviewed set
  variable or second ledger anchor stays hand-written.

## Open decisions

Resolved by the operator in fix round 02:

- **classification clearing (52):** new reader binds no source classification; dropped.
- **parallel metadata (592):** 211 case aliases need no statement; 236 co-delivered and
  145 ordinary declarations use set sets, preserving each column's own source metadata;
  disagreements use F6/ambiguity.
- **operational_definition (191 previously deferred declarations):** emit set
  operational_definition; d-set-fields supplies field application in the validation
  integration; missing endpoints or conflicting donors remain separately listed.
- **noncalendar placement (33):** dropped with reason D-7.

Remaining genuinely open items:

- Source-attribution provenance has no reviewed four-kind field mapping.
- Ambiguous donor facts, unsupported release-name coverage, partial source windows, and
  indistinguishable case-only owners still require documentary selection.
- Missing authored tables/releases and changed variable targets without local IDs remain
  prerequisites; no routing or ID is guessed. Noncalendar placement is already assigned
  to D-7.
- Overlapping release assertions remain blocked by the current assertion-conflict
  policy.

Changed targets awaiting D-12: 486. Register/variable names and original source lines
are retained in report.json.

The field application is supplied by d-set-fields; the model decision is resolved. The
report records the exact runtime revision used for validation.

  | Category                           | Source declarations | Converted | Refused by observed lock | Dropped/no statement | Mapping                                                                                                         |
  | ---------------------------------- | ------------------: | --------: | -----------------------: | -------------------: | --------------------------------------------------------------------------------------------------------------- |
  | `errata.delivered`                 |                 732 |       145 |                        0 |                  587 | assert occurrence plus set operational_definition where uniquely stated; preserve source key and release guards |
  | `representation.parallel`          |                 592 |       293 |                        0 |                  299 | case aliases: no statement; other declarations: set sets only; source metadata stays on each occurrence         |
  | `errata.version`                   |                 155 |        47 |                        0 |                  108 | assert release where coverage and an existing table are known                                                   |
  | `errata.occurrence_period`         |                 151 |       114 |                        0 |                   37 | set period for calendar coverage; noncalendar: D-7                                                              |
  | `representation.delivery_metadata` |                  74 |        64 |                        0 |                   10 | set description/label from agreeing pinned alternatives                                                         |
  | `errata.classification_reference`  |                  52 |         0 |                        0 |                   52 | new reader binds no source classification; no statement                                                         |

Converted means emitted without an observed located refusal. If the whole-root lock
fails before evaluation completes, other proposals remain uncertified; unreported errors
are not assumed absent.
