# One-shot curation conversion report

Deleted at milestone F (REBUILD.md).

Proposals only. Lock records current premises; old fingerprints have not been certified.

Scoped declarations: 18141 = 17299 register curation entries + 750 table declarations +
92 classification declarations/bindings. Slug pins and inline payload rows are not
additional statements.

  | Outcome   | Entries |
  | --------- | ------: |
  | converted |    7368 |
  | dropped   |   10772 |
  | refused   |       1 |

  | Statement / field / subtype                               | Count |
  | --------------------------------------------------------- | ----: |
  | `ack`                                                     |  1246 |
  | `ack.finding.ambiguous_sibling_shape`                     |    46 |
  | `ack.finding.conflicting_code_memberships`                |   106 |
  | `ack.finding.conflicting_coding_labels`                   |     4 |
  | `ack.finding.conflicting_occurrence_facts`                |    24 |
  | `ack.finding.conflicting_variable_fact`                   |     1 |
  | `ack.finding.missing_coding_period`                       |   376 |
  | `ack.finding.no_supported_states`                         |   139 |
  | `ack.finding.unknown_code_membership`                     |     8 |
  | `ack.finding.unknown_data_type`                           |     2 |
  | `ack.finding.unresolved_column_representation`            |    13 |
  | `ack.finding.unresolved_flag`                             |     3 |
  | `ack.finding.unresolved_lineage_ambiguous_source_variant` |     3 |
  | `ack.finding.unresolved_native_identity`                  |   343 |
  | `ack.finding.unsupported_occurrence`                      |   164 |
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
  | `link`                                                    |   105 |
  | `link.relation.label_of`                                  |   105 |
  | `set`                                                     |  6765 |
  | `set.fields.coded`                                        |   118 |
  | `set.fields.definition`                                   |   213 |
  | `set.fields.description`                                  |   880 |
  | `set.fields.format`                                       |     1 |
  | `set.fields.identifier`                                   |     8 |
  | `set.fields.label`                                        |  2722 |
  | `set.fields.notice`                                       |     1 |
  | `set.fields.operational_definition`                       |   104 |
  | `set.fields.period`                                       |   117 |
  | `set.fields.sensitive`                                    |     7 |
  | `set.fields.sets`                                         |  1597 |
  | `set.fields.spelling`                                     |    64 |
  | `set.fields.unit`                                         |   280 |
  | `set.fields.value_set`                                    |   164 |
  | `set.fields.variable`                                     |   510 |

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
  | `coded`                  |                 118 |
  | `definition`             |                 213 |
  | `description`            |                 880 |
  | `format`                 |                   1 |
  | `identifier`             |                   8 |
  | `label`                  |                2722 |
  | `notice`                 |                   1 |
  | `operational_definition` |                 104 |
  | `period`                 |                 117 |
  | `sensitive`              |                   7 |
  | `sets`                   |                1597 |
  | `spelling`               |                  64 |
  | `unit`                   |                 280 |
  | `value_set`              |                 164 |
  | `variable`               |                 510 |

The counts include translated parallel grouping (`sets`) and delivered donor wording
(`operational_definition`). The latter is emitted for the upcoming d-set-fields
implementation. Calendar corrections use `period`; the 33 noncalendar declarations stay
dropped as D-7. Classification clearing and parallel metadata semantics are resolved
below.

## Identity and case audit

Historical set variable: 12,151 statements from 6,088 owner entries. Current proposed
set variable: 510. The current pass also projects partitions previously skipped by the
approximate census; its pre-filter population differs from the historical one.

Projection audit:
`{"after": 510, "before": 21362, "coalesced_duplicate_effects": 32, "gap_marker": 1, "missing_local.absent-everywhere": 2830, "missing_local.only-other-register": 827, "removed_default_restatements": 16889, "unplaced": 273}`.

Case safety:
`{"different_or_unproven_outcomes": 42, "distinct_source_coordinates": 940, "same_outcome_folded_selector": 94}`.
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

  | Reason                                                                                                             | Entries |
  | ------------------------------------------------------------------------------------------------------------------ | ------: |
  | D-12: delivered occurrence has no unique existing unqualified ledger anchor                                        |      60 |
  | D-12: target has no local ledger id (absent-everywhere)                                                            |     806 |
  | D-12: target has no local ledger id (absent-everywhere); D-7: identity assignment targets an unplaced occurrence   |       1 |
  | D-12: target has no local ledger id (only-other-register)                                                          |     125 |
  | D-12: target has no local ledger id (only-other-register); D-7: identity assignment targets an unplaced occurrence |       1 |
  | D-7                                                                                                                |      33 |
  | D-7: identity assignment targets an unplaced occurrence                                                            |      85 |
  | D-7: identity assignment targets an unplaced occurrence; projected default identity already has the legacy outcome |     104 |
  | D-7: parallel legacy window disagrees with new-reader release placement                                            |       4 |
  | asserted column classification needs reviewed family/version mapping                                               |       6 |
  | asserted column needs a reviewed existing table                                                                    |      21 |
  | asserted column needs an explicit expressible period                                                               |      48 |
  | assertion needs one existing table; table declaration requires review                                              |     109 |
  | case aliases covered by folded spellings; no statement                                                             |     211 |
  | case-folded variants have different or unproven legacy outcomes                                                    |      42 |
  | case-only split: folded spelling cannot distinguish owners                                                         |       8 |
  | classification binding lacks an explicit family/version mapping                                                    |       9 |
  | classification correction needs reviewed family/version mapping                                                    |       6 |
  | classification metadata and sentinel definitions have no complete four-kind mapping                                |      83 |
  | classification_timeline: requires reviewed translation of legacy payload/guards                                    |       1 |
  | code list has no unique existing frame value set; needs handwritten assertion                                      |      62 |
  | coding choice needs a reviewed selected source value-set hash                                                      |     186 |
  | coding.omit: requires reviewed translation of legacy payload/guards                                                |       5 |
  | coding.sentinel: requires reviewed translation of legacy payload/guards                                            |      15 |
  | coding.support: requires reviewed translation of legacy payload/guards                                             |       8 |
  | delivered donor facts disagree in length; reviewed edition-specific mapping required                               |      13 |
  | delivered donor facts disagree in operational_definition; reviewed edition-specific mapping required               |      50 |
  | delivered donor facts disagree in type; reviewed edition-specific mapping required                                 |      15 |
  | delivered occurrence needs one dated existing release; convert the missing release first                           |     426 |
  | delivery metadata alternatives disagree or lack the requested value                                                |       3 |
  | delivery metadata window does not cover a whole source occurrence                                                  |       1 |
  | documentary.binding: requires reviewed translation of legacy payload/guards                                        |      21 |
  | documentary.retained: requires reviewed translation of legacy payload/guards                                       |      16 |
  | enrichment.alias: requires reviewed translation of legacy payload/guards                                           |      44 |
  | errata.occurrence_carry: requires reviewed translation of legacy payload/guards                                    |       4 |
  | errata.support: requires reviewed translation of legacy payload/guards                                             |      35 |
  | four-kind fields cannot clear a value                                                                              |       4 |
  | group: requires reviewed translation of legacy payload/guards                                                      |      48 |
  | identity split needs a reviewed non-type selector                                                                  |      31 |
  | identity.edition_split: requires reviewed translation of legacy payload/guards                                     |      37 |
  | identity.rename: requires reviewed translation of legacy payload/guards                                            |       1 |
  | identity.route: requires reviewed translation of legacy payload/guards                                             |      20 |
  | legacy diagnostic subject has no public ledger endpoint                                                            |     688 |
  | legacy field has no four-kind field                                                                                |      12 |
  | legacy period cuts a whole source occurrence                                                                       |       8 |
  | new reader binds no source classification                                                                          |      52 |
  | no exact new-frame occurrence endpoint                                                                             |     167 |
  | overlapping delivered releases need an assertion-conflict design decision                                          |      18 |
  | parallel declared occurrence absent from new frames; reviewed assertion required                                   |      33 |
  | partition includes case-only owners beside other spellings; needs reviewed regrouping                              |      13 |
  | projected default identity already has the legacy outcome                                                          |    6188 |
  | qualified ledger endpoint needs reviewed key/day selection                                                         |       5 |
  | release name needs reviewed coverage interpretation; do not infer coverage from its token shape                    |       4 |
  | representation.alias_window: requires reviewed translation of legacy payload/guards                                |       4 |
  | representation.matrix: requires reviewed translation of legacy payload/guards                                      |       7 |
  | representation.period_family: requires reviewed translation of legacy payload/guards                               |       8 |
  | source spelling cannot be expressed by address grammar                                                             |       7 |
  | table identity already in ledger; panel metadata has no four-kind field                                            |     750 |

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

Status: refused. See report.json for located refusals and their source origins.

  | Lock error                             | Located refusals |
  | -------------------------------------- | ---------------: |
  | unknown field `operational_definition` |                1 |

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
  operational_definition; d-set-fields adds the field; missing endpoints or conflicting
  donors remain separately listed.
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

The d-set-fields addition of operational_definition is an implementation prerequisite,
not an unresolved model decision.

  | Category                           | Source declarations | Converted | Refused by observed lock | Dropped/no statement | Mapping                                                                                                         |
  | ---------------------------------- | ------------------: | --------: | -----------------------: | -------------------: | --------------------------------------------------------------------------------------------------------------- |
  | `errata.delivered`                 |                 732 |       144 |                        1 |                  587 | assert occurrence plus set operational_definition where uniquely stated; preserve source key and release guards |
  | `representation.parallel`          |                 592 |       344 |                        0 |                  248 | case aliases: no statement; other declarations: set sets only; source metadata stays on each occurrence         |
  | `errata.version`                   |                 155 |        47 |                        0 |                  108 | assert release where coverage and an existing table are known                                                   |
  | `errata.occurrence_period`         |                 151 |       118 |                        0 |                   33 | set period for calendar coverage; noncalendar: D-7                                                              |
  | `representation.delivery_metadata` |                  74 |        64 |                        0 |                   10 | set description/label from agreeing pinned alternatives                                                         |
  | `errata.classification_reference`  |                  52 |         0 |                        0 |                   52 | new reader binds no source classification; no statement                                                         |

Converted means emitted without an observed located refusal. If the whole-root lock
fails before evaluation completes, other proposals remain uncertified; unreported errors
are not assumed absent.

## Round 02 run result

Merged origin/main including #2003 (`dc5ffd12`), then built
`cargo build --release -p reg-build` with the sccache workaround. The inputs worktree
and manifest both name `9186fcbceeb2283a50c7f684764869e4dda5765e`. A fresh reg-build
read exported the frames; the converter regenerated the scratch root.

The requested release-binary lock exited 1 while loading curation: **one observed
located unknown-field refusal**, `curation/registers/scb/r195/curation.toml:277`, from
`reg_meta_build/curation/registers/scb/ksju.toml:1507`. The family is
`unknown field operational_definition`; d-set-fields has not added that field to the
current closed schema. The loader stopped before reading real inputs or evaluating
premises. Other error families were not evaluated and are not claimed absent.

The entire process-check/lock/postprocessing invocation took 0.394 seconds. Exact lock
time was not retained because a report postprocessing step failed on an empty statement
file; postprocessing was repaired from the retained log without repeating the lock. This
is not a real-scale performance measurement.

`pgrep -f 'reg-build (lock|build)'` exited 3: process listing is unavailable in this
sandbox (`sysmond service not found` / `Cannot get process list`). Concurrency was not
certified. A synthetic check and the lock entry point verified that the unsupported
field causes refusal before readers run, so this attempt started no heavy resolution. A
full real-frame lock still requires d-set-fields and an available concurrency check or
operator confirmation.

Register curation (17,299 entries): **7,368 converted proposals, 9,930 dropped/no-op, 1
observed refusal**. Including 750 table and 92 classification declarations: 7,368 /
10,772 / 1. Every remaining emitted proposal is uncertified after the root-level loader
failure.

The 191 formerly field-blocked delivery declarations now split into 18 emitted without
an observed refusal, one observed refusal, and 172 dropped for other prerequisites: 91
missing dated releases, 50 conflicting donor definitions, 16 missing identity anchors,
10 overlapping release assertions, four conflicting lengths, and one conflicting type.
They produced 103 operational_definition setters; one additional authored-column
declaration contributes the 104th.

Parallel grouping: 344 declarations emitted, 211 case aliases are no-ops, 33 need
declared occurrences absent from the new frames, and four have legacy windows that
disagree with reader release placement (D-7). These prerequisites do not reopen the
operator's grouping decision.

## Validation

`g0 rust` passed: lint/type checks, Rust checks, 116 script tests, byte-identical
synthetic builds, and 194 conformance tests (2 skipped). Focused synthetic ruling checks
and independent review passed. No tests were added. No real-seed build, release gate or
regen ran. Generated TOML remains outside Git and is removed after recording the report;
detailed report.json is retained.
