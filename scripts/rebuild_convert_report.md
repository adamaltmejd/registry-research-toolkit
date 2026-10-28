# One-shot curation conversion report

Deleted at milestone F (REBUILD.md).

Proposals only. Lock records current premises; old fingerprints have not been certified.

Scoped declarations: 18141 = 17299 register curation entries + 750 table declarations +
92 classification declarations/bindings. Slug pins and inline payload rows are not
additional statements.

  | Outcome   | Entries |
  | --------- | ------: |
  | converted |    7006 |
  | dropped   |   11135 |

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
  | `assert`                                                  |  2496 |
  | `assert.entity.occurrence`                                |  2449 |
  | `assert.entity.release`                                   |    47 |
  | `assert.fields.definition`                                |  2449 |
  | `assert.fields.identifier`                                |   560 |
  | `assert.fields.label`                                     |  2449 |
  | `assert.fields.length`                                    |   421 |
  | `assert.fields.sensitive`                                 |   560 |
  | `assert.fields.type`                                      |  2449 |
  | `assert.fields.unit`                                      |    35 |
  | `link`                                                    |   105 |
  | `link.relation.label_of`                                  |   105 |
  | `set`                                                     |  5719 |
  | `set.fields.coded`                                        |   118 |
  | `set.fields.definition`                                   |   213 |
  | `set.fields.description`                                  |   880 |
  | `set.fields.format`                                       |     1 |
  | `set.fields.identifier`                                   |     8 |
  | `set.fields.label`                                        |  2722 |
  | `set.fields.notice`                                       |     1 |
  | `set.fields.period`                                       |   117 |
  | `set.fields.sensitive`                                    |     7 |
  | `set.fields.sets`                                         |   655 |
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

  | Set field     | Proposed statements |
  | ------------- | ------------------: |
  | `coded`       |                 118 |
  | `definition`  |                 213 |
  | `description` |                 880 |
  | `format`      |                   1 |
  | `identifier`  |                   8 |
  | `label`       |                2722 |
  | `notice`      |                   1 |
  | `period`      |                 117 |
  | `sensitive`   |                   7 |
  | `sets`        |                 655 |
  | `spelling`    |                  64 |
  | `unit`        |                 280 |
  | `value_set`   |                 164 |
  | `variable`    |                 510 |

Mechanically expressible mappings also blocked by endpoint prerequisites remain in
scope: `period` (118 calendar declarations; 33 noncalendar declarations need D-7),
`description`/`label` (74 delivery-metadata declarations). `errata.delivered` maps to
assert occurrence, and `errata.version` to assert release; neither adds a set field.

Open decisions: classification clearing syntax (52); parallel metadata
preservation/sharing (592); noncalendar placement (33); donor disagreement and missing
tables/releases/ledger ids. These are not guessed setters.

## Identity and case audit

Historical set variable: 12,151 statements from 6,088 owner entries. Current proposed
set variable: 510. The current pass also projects partitions previously skipped by the
approximate census; its pre-filter population differs from the historical one.

Projection audit:
`{"after": 510, "before": 21362, "coalesced_duplicate_effects": 32, "gap_marker": 1, "missing_local.absent-everywhere": 2830, "missing_local.only-other-register": 827, "removed_default_restatements": 16889, "unplaced": 273}`.

Case safety:
`{"different_or_unproven_outcomes": 42, "distinct_source_coordinates": 926, "same_outcome_folded_selector": 14}`.
Original 982 case drops:
`{"emitted": 272, "still dropped: D-12: target has no local ledger id (absent-everywhere)": 149, "still dropped: D-12: target has no local ledger id (absent-everywhere); D-7: identity assignment targets an unplaced occurrence": 1, "still dropped: D-12: target has no local ledger id (only-other-register)": 35, "still dropped: D-7: identity assignment targets an unplaced occurrence": 3, "still dropped: case-folded variants have different or unproven legacy outcomes": 42, "still dropped: projected default identity already has the legacy outcome": 480}`.
Remaining cases are listed in report.json.

Cached proposal audit:
`{"gap marker": 1, "missing local ledger": 2987, "overlap directed diagnostics": 184, "overlap pairs": 92, "unplaced occurrence": 151}`.
This is a static reconstruction, not a new real-frame lock.

Missing ledger causes:
`{"no ledger id anywhere": 2421, "slug exists only in another register": 566}`. A
same-named id in another register is not authority to borrow it; changed groups without
local ids wait for D-12.

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
  | D-12: delivered occurrence has no unique existing unqualified ledger anchor                                        |      44 |
  | D-12: target has no local ledger id (absent-everywhere)                                                            |     806 |
  | D-12: target has no local ledger id (absent-everywhere); D-7: identity assignment targets an unplaced occurrence   |       1 |
  | D-12: target has no local ledger id (only-other-register)                                                          |     125 |
  | D-12: target has no local ledger id (only-other-register); D-7: identity assignment targets an unplaced occurrence |       1 |
  | D-7: identity assignment targets an unplaced occurrence                                                            |      85 |
  | D-7: identity assignment targets an unplaced occurrence; projected default identity already has the legacy outcome |     104 |
  | D-7: noncalendar occurrence coverage needs a design decision                                                       |      33 |
  | asserted column classification needs reviewed family/version mapping                                               |       6 |
  | asserted column needs a reviewed existing table                                                                    |      21 |
  | asserted column needs an explicit expressible period                                                               |      48 |
  | assertion needs one existing table; table declaration requires review                                              |     109 |
  | case-folded variants have different or unproven legacy outcomes                                                    |      42 |
  | case-only split: folded spelling cannot distinguish owners                                                         |       8 |
  | classification binding lacks an explicit family/version mapping                                                    |       9 |
  | classification clearing needs a design decision; no replacement family is declared                                 |      52 |
  | classification correction needs reviewed family/version mapping                                                    |       6 |
  | classification metadata and sentinel definitions have no complete four-kind mapping                                |      83 |
  | classification_timeline: requires reviewed translation of legacy payload/guards                                    |       1 |
  | code list has no unique existing frame value set; needs handwritten assertion                                      |      62 |
  | coding choice needs a reviewed selected source value-set hash                                                      |     186 |
  | coding.omit: requires reviewed translation of legacy payload/guards                                                |       5 |
  | coding.sentinel: requires reviewed translation of legacy payload/guards                                            |      15 |
  | coding.support: requires reviewed translation of legacy payload/guards                                             |       8 |
  | delivered donor facts disagree in length; reviewed edition-specific mapping required                               |       9 |
  | delivered donor facts disagree in type; reviewed edition-specific mapping required                                 |      14 |
  | delivered donor operational_definition has no four-kind field                                                      |     191 |
  | delivered occurrence needs one dated existing release; convert the missing release first                           |     335 |
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
  | no exact new-frame occurrence endpoint                                                                             |     167 |
  | overlapping delivered releases need an assertion-conflict design decision                                          |       8 |
  | parallel metadata sharing/preservation needs reviewed per-field translation; grouping alone is not equivalent      |     592 |
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

Status: not run. See report.json for located refusals and their source origins.

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

## Fix round 01

No real-frame lock, real-seed build, or reader run was performed this round. The static
audit reads the retained d-lock-perf frame/proposal cache. The previous full lock log
was deleted with scratch; the reconstruction below matches its aggregate diagnostic
counts but is not a fresh CLI acceptance result.

  | Historical refusal        | Static reconstruction | Cause                                                                              |
  | ------------------------- | --------------------: | ---------------------------------------------------------------------------------- |
  | Missing local ledger slug |                 2,987 | 2,421 absent everywhere; 566 same-named only in another register                   |
  | curation-overlap          |        184 (92 pairs) | duplicate partition/column-owner effects; converter noise                          |
  | Unplaced occurrence       |                   151 | 76 partition assignments, 75 column-owner assignments; release placement needs D-7 |

The missing-ledger reconstruction observes the lock validation order: 3,023 proposed
targets are absent locally, but 36 first fail placement validation. A same-named slug
elsewhere is not an authorized rename. No cross-register ID is borrowed.

  | Historical missing-ledger disposition after projection                                                             | Statements |
  | ------------------------------------------------------------------------------------------------------------------ | ---------: |
  | D-12: target has no local ledger id (absent-everywhere)                                                            |       1719 |
  | D-12: target has no local ledger id (only-other-register)                                                          |        405 |
  | D-12: target has no local ledger id (only-other-register); D-7: identity assignment targets an unplaced occurrence |         27 |
  | projected default identity already has the legacy outcome                                                          |        836 |

There are 486 distinct changed `(register, variable)` targets without a local ledger ID
in the expanded pass. They wait for D-12; their names and source origins are retained in
report.json. Unplaced assignments are counted separately and are not assumed to need
minting.

`set variable`: **12,151 before; 510 after**. The expanded pass projects every
expressible partition, including ones skipped by the earlier heuristic. Whole default
groups with obsolete unminted owner names are no-ops; explicit different existing ledger
IDs remain assignments.

Of the original 982 case drops, **940 are harmless**: 926 are already separated by
source-coordinate guards, and 14 permit actual folded-selector widening to the same
outcome. **42 remain dropped**: 40 have distinct explicit owners; two retain an
explicitly excluded/unassigned sibling outcome (FoU NgSredo/NgsRedo and school-cost
Sform/SForm). The complete register/variable/column list is in report.json.

  | Deferred category                  | Source declarations | Emitted declarations | Four-kind form / remaining decision                                                                                                                                |
  | ---------------------------------- | ------------------: | -------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
  | `errata.delivered`                 |                 732 |                  126 | assert occurrence from unique new-frame donor facts; existing identity/table/release required; no invented key, inherited unit, or implicit donor coding           |
  | `representation.parallel`          |                 592 |                    0 | 236 co-delivered, 211 case aliases, 145 ordinary declarations; sharing vs preserving per-column metadata needs a field-source decision; sets alone is insufficient |
  | `errata.version`                   |                 155 |                   47 | assert release; unsupported names and missing authored tables stay listed; GDB January tokens describe the preceding year end                                      |
  | `errata.occurrence_period`         |                 151 |                  118 | set period for 118 calendar declarations; 33 year-independent declarations need D-7                                                                                |
  | `representation.delivery_metadata` |                  74 |                   64 | set description and, for five declarations, label; pinned alternatives must agree and cover whole source occurrences                                               |
  | `errata.classification_reference`  |                  52 |                    0 | 52 clear erroneous classification references; needs explicit clear syntax, not coded=false or a fabricated class ID                                                |

Current static proposal audit:
`{"overlap directed diagnostics": 0, "overlap pairs": 0}`. Empty results do not certify
lock acceptance.

Remaining decisions: classification clearing; operational_definition (191 delivery
declarations have no lossless current field); parallel metadata field sources;
noncalendar/unparsed release placement; missing table/release/ID prerequisites;
ambiguous donor facts and overlapping release assertions. The raw legacy
source_attribution requests also need a provenance-field decision.

## Validation

`g0 rust` passed: lint/type checks, Rust checks, 116 script tests, synthetic
byte-identical builds, and 194 conformance tests (2 skipped). The initial gate attempt
caught a missing attribute declaration; the full rerun passed. A synthetic lock passed
for period/label/description, release and guarded occurrence assertions. Inline
identity, case-safety and nested-selector checks passed; independent review completed.
No tests were added. No real-frame lock, real-seed build, release gate or regen ran this
round. The base was fetched and remains included in this branch.
