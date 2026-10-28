## Round 04 verified on merged main

Merged `origin/main` at `33e975de1731500c655e531d23ccb5e5f7355cc3` (#2005) with a normal
merge, `d79d3a058fedfb7fad07922425bdeb72114f9dd3`. No temporary dependency merge
remains. The release binary was rebuilt from that merged head. Both comparison catalogs
use the same binary and inputs worktree/manifest commit
`9186fcbceeb2283a50c7f684764869e4dda5765e`. This is a curated **dev** build, not a
release or activation.

The regenerated proposals and every curation/config file are byte-identical to round 03.
No converter emission changes were needed for the merged review fixes. No type or length
setters are requested by the retained proposals; the sole legacy data-type correction
still lacks an exact frame endpoint. No sentinel, role or unit-type setters are emitted.
The one format setter remains valid. The 243 sets statements now produce memberships
through resolve: **235 sets and 250 members**, unchanged from round 03.

All heavy runs were sequential. Process listing remains unavailable (`pgrep` cannot
access sysmond); this operator-scheduled run did not overlap another heavy job from this
worker. `/usr/bin/time -l` was invoked, but its detailed output is blocked by
`sysctl kern.clockrate`. Exact Rust-child exits, wall times and peak RSS were captured
separately with `wait`/monotonic time/`getrusage(RUSAGE_CHILDREN)`; Darwin RSS is bytes.
The outer timing wrapper returns 1 for its sandbox error even when the recorded Rust
child returns 0.

  | Run                        | Rust exit | Wall seconds | Peak RSS bytes | Round 03 wall | Wall delta |
  | -------------------------- | --------: | -----------: | -------------: | ------------: | ---------: |
  | Final lock                 |         0 |      174.452 |  8,148,647,936 |       175.973 |     -1.521 |
  | Curated dev build          |         0 |      246.673 |  8,793,587,712 |       246.339 |     +0.334 |
  | Explicit curated verify    |         0 |       22.945 |     81,281,024 |        22.911 |     +0.034 |
  | Uncurated comparison build |         0 |      112.849 |  8,989,655,040 |       112.420 |     +0.429 |

All reported catalog counts, statement/override counts, memberships, source outcomes,
and diagnostic families are unchanged from round 03. Restored type/length application
has no retained setters to affect. Removing format from blocking forms/type-change
findings causes no aggregate difference on these inputs. Moving memberships into resolve
preserves the observed memberships. Timing/RSS variation is measured host/run variation,
not evidence of a performance change.

The final lock wrote **169 files containing 14,792 reviewed premises** and had **zero
refusals**. The curated catalog passed its built-in post-write checks and the explicit
`reg-build verify` command. It published **13,466 applied statement rows: 2,599 assert
and 10,867 set**, with **zero missing-premise diagnostics**. The uncurated comparison
has zero statements and zero curated overrides.

The remaining 1,326 locked proposals are 177 redundant sets, 103 links and 1,046
acknowledgements. Links/acks are premise-only until D-3c; they are not reported as
applied. There are zero acknowledged findings in this catalog.

### D-3b field scope: applied set statements and winning override cells

Counts below come from the published SQLite `statement` and `override` boundaries. One
applied statement may affect several cells; these are not source-entry counts. Seven
sensitive setters were wholly redundant and produced no winning setter override.

  | Field                    | Proposed sets | Applied set statements | Set override cells |
  | ------------------------ | ------------: | ---------------------: | -----------------: |
  | `coded`                  |           144 |                     32 |                 32 |
  | `definition`             |           264 |                    264 |                264 |
  | `description`            |         4,947 |                  4,947 |              4,948 |
  | `format`                 |             1 |                      1 |                  2 |
  | `identifier`             |             8 |                      1 |                  1 |
  | `length`                 |             0 |                      0 |                  0 |
  | `label`                  |         4,141 |                  4,127 |              4,129 |
  | `notice`                 |             1 |                      1 |                  1 |
  | `operational_definition` |           104 |                    104 |                104 |
  | `period`                 |           114 |                    114 |                114 |
  | `sensitive`              |             7 |                      0 |                  0 |
  | `sets`                   |           243 |                    243 |                243 |
  | `spelling`               |           146 |                    146 |                146 |
  | `type`                   |             0 |                      0 |                  0 |
  | `unit`                   |           280 |                    280 |                280 |
  | `value_set`              |           174 |                    116 |                116 |
  | `variable`               |           491 |                    491 |              3,654 |

Period setters also detached 11 source-release references; those 11 `release` override
cells are side effects, not a new set field.

All curated override cells, including asserted source-shaped facts:

  | Field                    | Cells |
  | ------------------------ | ----: |
  | `coded`                  |    32 |
  | `definition`             | 3,009 |
  | `description`            | 4,948 |
  | `edition`                |    47 |
  | `format`                 |     2 |
  | `grain`                  |    47 |
  | `identifier`             |   841 |
  | `label`                  | 6,874 |
  | `length`                 |   510 |
  | `name`                   |    47 |
  | `notice`                 |     1 |
  | `operational_definition` |   104 |
  | `period`                 | 2,361 |
  | `population_valuesets`   | 2,745 |
  | `populations`            |    47 |
  | `provider`               | 2,792 |
  | `register`               | 2,792 |
  | `release`                |   603 |
  | `sensitive`              |   840 |
  | `sets`                   |   243 |
  | `source_key`             | 2,745 |
  | `spelling`               | 2,891 |
  | `table`                  | 2,792 |
  | `type`                   | 2,745 |
  | `unit`                   |   317 |
  | `value_set`              |   116 |
  | `variable`               | 3,654 |

### Catalog-level deltas

  | Measure              | Uncurated | Curated | Delta  |
  | -------------------- | --------: | ------: | -----: |
  | Variables            |    56,009 |  57,921 | +1,912 |
  | Segments             |   169,825 | 172,195 | +2,370 |
  | Clear segments       |   166,182 | 168,619 | +2,437 |
  | Ambiguous segments   |     3,627 |   3,560 |    -67 |
  | Preliminary segments |        16 |      16 |     +0 |
  | Withheld occurrences |       397 |     552 |   +155 |

Withheld is an occurrence status; no count of unpublished “withheld variables” is
inferred. Both catalogs have zero withheld-gap rows.

### Remaining diagnostics

Curation: **0 missing premises; 177 redundant-set notices; 0 other located curation
errors**. Reader warnings: **50,349**. The following are retained catalog findings, not
missing-premise failures:

  | Family                         | Findings |
  | ------------------------------ | -------: |
  | `C0:curated-notice`            |        1 |
  | `F1:definition-change`         |   20,678 |
  | `F1:domain-change`             |   17,314 |
  | `F1:label-differs`             |       50 |
  | `F1:label-drift`               |      181 |
  | `F1:type-change`               |   15,544 |
  | `F1:unit-change`               |    5,977 |
  | `F2:ambiguous`                 |    3,560 |
  | `F5:documented-without-column` |    2,211 |
  | `F6:source-conflict`           |    1,174 |
  | `F7:edition-disagreement`      |       12 |
  | `F9:unparsed-delivery`         |       39 |
  | `S0:source-notice`             |      140 |
  | `T1:clamp`                     |      657 |

### Emission repairs and deferrals

- **Gap marker:** one `identity.unassigned` proposal for the `ant6` family is deferred,
  preserving the default marker and its finding
  (`kommunalekonomisk-utjamning.toml:403`). No gap guard is weakened.
- **65 unsupported-year proposals:** 64 LOVA period setters plus one label setter, from
  66 source entries, are retained using exact source guards without the unrepresentable
  1893 period qualifier. The table/fold/key peers have identical whole-source spans. No
  year is clamped.
- **Variable-wide grouping:** 1,597 per-occurrence grouping proposals normalize to 243
  proved local-variable statements, preserving positive memberships/evidence. Unproved
  groups are deferred with source records (731 lack a proved placed local variable; 18
  have assertion-dependent identity). No IDs are guessed.
- **Absent endpoints:** 200 legacy acknowledgements and two code-label links are
  deferred with named endpoint reasons.
- **Scalar scope:** release guards remain on scalar corrections; authored names and
  literal label guards distinguish the three ambiguous corrections. This avoids five
  assertion/unit duplicate effects.
- **Period shape:** two disjoint corrections are deferred because `set period` needs one
  contiguous period; one EKB correction needs an explicit parent-coverage correction. No
  scope is shrunk.
- **D-11:** 18 assignments merging concurrent different spellings in one table are
  deferred with original source accounting. They cover exactly the 11 variables
  responsible for the observed structural failure. Existing source facts and ambiguity
  rules remain.

In round 03, the first lock failed after 183.62 seconds with 9,331 located diagnostics
(929 empty matches, 8,395 overlap diagnostics, and seven other errors). After those
fixes, a lock passed, but the first curated build failed after 232.289 seconds at the
occurrence/segment invariant. A diagnostic rerun retained the rejected stage: 38 fold
mismatches across 11 variables; zero variable/table/period/value-set/dtype mismatches. A
tiny real-CLI fixture reproduced the invalid concurrent-fold assignment. Temporary
diagnostic retention was restored before the final measured run; verifier checks and
publication guards were never changed. Failed stages were not published.

Every deferred proposal retains origins/reason in report.json, including partially
converted source entries. “Converted” below means a source entry retained at least one
accepted proposal, not that every legacy feature was expressible. Full legacy
fingerprint certification is not claimed.

Final source outcomes: **7,004 converted / 11,137 dropped or no-op / 0 refused** across
18,141 scoped declarations; register curation alone is **7,004 / 10,295 / 0** across
17,299 entries.

Detailed JSON, final locks, both catalogs and run metrics are retained locally under
`.local/d-converter-round04/` (round 03 evidence is retained separately). Generated TOML
and catalogs stay out of Git. Frames, rejected stages and temporary tools are removed
after reporting. No PR was merged, no release gate or regen ran, and no legacy real-seed
build ran.

# One-shot curation conversion report

Deleted at milestone F (REBUILD.md).

Proposals only. Lock records current premises; old fingerprints have not been certified.

Scoped declarations: 18141 = 17299 register curation entries + 750 table declarations +
92 classification declarations/bindings. Slug pins and inline payload rows are not
additional statements.

  | Outcome   | Entries |
  | --------- | ------: |
  | converted |    7004 |
  | dropped   |   11137 |

  | Statement / field / subtype                               | Count |
  | --------------------------------------------------------- | ----: |
  | `ack`                                                     |  1046 |
  | `ack.finding.ambiguous_sibling_shape`                     |    46 |
  | `ack.finding.conflicting_code_memberships`                |   106 |
  | `ack.finding.conflicting_coding_labels`                   |     4 |
  | `ack.finding.conflicting_occurrence_facts`                |    24 |
  | `ack.finding.conflicting_variable_fact`                   |     1 |
  | `ack.finding.missing_coding_period`                       |   374 |
  | `ack.finding.no_supported_states`                         |    41 |
  | `ack.finding.unknown_code_membership`                     |     8 |
  | `ack.finding.unknown_data_type`                           |     2 |
  | `ack.finding.unresolved_column_representation`            |    13 |
  | `ack.finding.unresolved_flag`                             |     3 |
  | `ack.finding.unresolved_lineage_ambiguous_source_variant` |     3 |
  | `ack.finding.unresolved_native_identity`                  |   343 |
  | `ack.finding.unsupported_occurrence`                      |    64 |
  | `ack.finding.unsupported_representation_coding`           |    14 |
  | `assert`                                                  |  2599 |
  | `assert.entity.occurrence`                                |  2552 |
  | `assert.entity.release`                                   |    47 |
  | `assert.fields.definition`                                |  2552 |
  | `assert.fields.identifier`                                |   663 |
  | `assert.fields.label`                                     |  2552 |
  | `assert.fields.length`                                    |   510 |
  | `assert.fields.sensitive`                                 |   663 |
  | `assert.fields.type`                                      |  2552 |
  | `assert.fields.unit`                                      |    37 |
  | `link`                                                    |   103 |
  | `link.relation.label_of`                                  |   103 |
  | `set`                                                     | 11044 |
  | `set.fields.coded`                                        |   144 |
  | `set.fields.definition`                                   |   264 |
  | `set.fields.description`                                  |  4947 |
  | `set.fields.format`                                       |     1 |
  | `set.fields.identifier`                                   |     8 |
  | `set.fields.label`                                        |  4141 |
  | `set.fields.notice`                                       |     1 |
  | `set.fields.operational_definition`                       |   104 |
  | `set.fields.period`                                       |   114 |
  | `set.fields.sensitive`                                    |     7 |
  | `set.fields.sets`                                         |   243 |
  | `set.fields.spelling`                                     |   146 |
  | `set.fields.unit`                                         |   280 |
  | `set.fields.value_set`                                    |   174 |
  | `set.fields.variable`                                     |   491 |

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
  | legacy diagnostic subject has no public ledger endpoint                                                                                                                   |     688 |
  | legacy diagnostic/link endpoint has no placed final local variable; reviewed current finding/link required                                                                |     209 |
  | legacy field has no four-kind field                                                                                                                                       |      12 |
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

Status: accepted. See report.json for located refusals and their source origins.

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
  operational_definition; merged d-set-fields supplies field application; missing
  endpoints or conflicting donors remain separately listed.
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

## Final gate and reproducibility

`uv run --no-project scripts/gate.py g0 rust` passed on the merged tree: lint,
format/type/schema checks, Rust fmt/clippy/tests and dependency audit, 116 script tests,
byte-identical synthetic builds, and 194 conformance tests (2 skipped). Independent
review found no material converter merge blocker. No permanent tests were added. The
first standalone formatter invocation could not create its default uv tool directory;
the formatter and full gate then succeeded with a checkout-local tool directory.

The measured converter sources are unchanged from `aea16f0b1`; runtime validation uses
the normal main merge `d79d3a058`. The JSON evidence records curation digests, exact
commands and child results. The locked-curation file manifest SHA-256 (sorted relative
paths mapped to content SHA-256, excluding locks) is
`5631703867465d59d1b0b0273b44e8ccd6f903f449e72e62b197c00121db16ca`. Final locks, both
catalogs and metrics remain local. The PR diff contains only the six converter scripts
and this report. No PR merge, release gate, regen or legacy real-seed build was run.
