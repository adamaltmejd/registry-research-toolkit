# Golden corpus: the neutral curation verbs

Each directory is one old verb at its hardest real case.

- `before.toml` is the entry verbatim from `reg_meta_build/curation/registers/**`. Line
  1 is `# case: <case id>`.
- `after.toml` is its neutral form, which `reg_build_model::verbs::Verbs` reads with
  unknown fields denied.
- `tests/verbs_golden.rs` is a schema check: every old verb has a pair, every
  `after.toml` parses, and each carries one entry. Its semantic oracle is PR C's
  per-entry record-set equivalence in today's Python compile, which also checks each
  converted `head` against the provenance Python publishes for the same entry.
- PR C's converter must produce each `after.toml`, compared as parsed values, not bytes.
- Counts come from the pinned prepared root `.local/build-2026-10-25/prepared` (manifest
  `030c4709…`, `records.sqlite`, read-only). "Old" is the record set the old entry acts
  on. "New" is what the after-form members select in the same root. Counts are
  occurrences.
- Changing an `after.toml` is a content decision reviewed in the diff.

## Conventions

- **Case ids are 1-based.** Every loop in `curation_compile.py` is `enumerate(..., 1)`
  (`#/identity.partition/{i}`, `#/errata.{table}/{index}`), and the build fixtures
  agree. The matrix case id is `#/matrix/<edition name>`.
- **Head** is the first line of the provenance the old entry publishes, from the code
  path that publishes it (`crates/reg-build-model/DESIGN.md` §11): `errata:<class>`,
  `errata:<upstream>` (delivered), `errata:<source>` (column), `curated:matrix-answer`,
  `curated:alias-window`, `curated:coding.<kind>`,
  `curated:representation.period_family`. The identity verbs, `errata.version`,
  `parallel` and `delivery_metadata` publish none, so `head` is absent.
- **Member rule.** Translate the coordinates the old entry names: variant to `table`,
  edition or `versions` to `deliveries`, SoS `deldatamangd` to `table`, SoS bare
  `variable` to `column`. Drop the provider ids (VarID, family key). Add `where` only to
  make the selection exact, usually `where = { name = ... }`. Entries that name no
  variant (partition, unassigned, split) get no `table`.
- **SoS table slugs.** A Deldatamängd token resolves to a variant name through
  `identity.route`, or is the name itself, as in PAR. The pinned `[[variant]]` native id
  is `mint("sos", <register key>, <variant name>)` (`authored_naming_id`), so
  `LOVA PERSON` becomes `lova-person`.
- **Delivery names.** Whitespace folding changes none of the 8,608 edition names in the
  root, so every delivery is the edition name as stated.
- Native ids inside guards (`expected_records`, `authority`, `expected_selector`) are
  evidence of what the source stated, not addresses.

## Pairs

### identity.partition

`scb/lbr.toml#/identity.partition/10`

- Hardest: it has `unassigned_columns`, a family digest and `data_warning`. Its
  unassigned literal `Lamm` is also VarID 42420's literal, on the same table (RegVarID 72)
  in the same delivery (2017). `table` and `deliveries` do not separate the two (table
  plus 2017 gives 2). Only `where = { name = "Majs" }` does.
- Owner `10.42421.majs` becomes `majs` (`lbr.toml` `[variable]`).
- Old: family 42421 = 25 (24 `majs`, 1 `Lamm`). New: `{column = "majs"}` = 24,
  `{column = "Lamm", where = {name = "Majs"}}` = 1. The sets are equal (25).
  `{column = "Lamm"}` alone = 27.

### identity.unassigned

`scb/energi-flerbostad.toml#/identity.unassigned/1`

- Hardest: three literals (`Energiid`, `Kvant`, `Uppvid`), each also delivered under 8
  or 9 other VarIDs on the same table and deliveries.
- Old: family 75.2330 = 12. New: three `owner = false` members, each
  `where = {name = "Vedanvändning"}`, 4 each. The sets are equal (12). Without `where`: 108.
  With table and deliveries but no `where`: 100.
- `kommunalekonomisk-utjamning.toml#/identity.unassigned/1` (117.22209) has 10 of 16
  family records with `column_name` stated `negative`. Such columnless occurrences are
  selected only by a member without `column` (see below).

### identity.column_owner

`scb/iot.toml#/identity.column_owner/129`

- Hardest: complete `expected_fields` (the five-field group with unit and source
  attribution, the largest allowed). The same column in 2017–2024 belongs to another
  owner (`/130`), so `deliveries` is the only discriminator.
- `25.763` becomes `individer-och-dodsbon`. Owner `25.659.belopp` becomes `akyrk`
  (`iot.toml`). `source_editions` becomes `deliveries`. `expected_fields` becomes
  `guard`.
- Old: 72 (VarID 659, RegVarID 763, AKYRK, 24 editions). New: 72, the same set. AKYRK
  without deliveries: 120.

### identity.split

`sos/par.toml#/identity.split/14`

- Hardest `by = "name"` case: four parts, each one record of `TYP` on `PAR_TV`.
- Each part becomes one link, `{column = "TYP", where = {name = <part>}}`. Owners
  `5891427617861710725.TYP.<k>` become `typ`, `markor-yttre-orsakskod`,
  `markor-avvikning`, `markor-permission` (`par.toml` `[variable]`).
- Old: 4. New: 1 + 1 + 1 + 1 = 4. No split entry with `by = name` or `description` has
  `expected_records`, so this pair carries no guard.

### identity.rename

`sos/lova.toml#/identity.rename/1`

- The only rename. `A_LOVA_PERSON` routes to `LOVA PERSON`, then `lova-person`. Owner
  `7847722537922273761.INVARN9` becomes `invandringsdatum-9-numerisk`.
- The compile selects the source literal (`INVARN8`) by name and rewrites `column_name`
  to `INVARN9` (`CheckedFieldChange` in `curation_compile.py`). So the member is
  `INVARN8` and `column_name = "INVARN9"` is the effect.
- Old: 1. New: 1. Without `where`: 2 (the true INVARN8 shares the table).

### identity.edition_split

`scb/komvux.toml#/identity.edition_split/3`

- Hardest: RegVarID 498 has three splits (autumn, spring, academic year), so
  `source_editions` (115) holds the other two splits' editions too. It is the
  complement, not what the parent keeps.
- `249.498` becomes `kursdeltagare`. `249.498.academic-year` becomes
  `kursdeltagare-lasar`. `guard.expected_deliveries = source_editions`.
- Old: 330 (14 editions). New: `{table = "kursdeltagare", deliveries = editions}` = 330.
  `editions` plus `source_editions` is all 129 editions (3,100 records).

### identity.route

`sos/lmed.toml#/identity.route/1`

- The only ambiguous token (D7 drops the 1:1 routes): `LMED VARA/LMED` routes to `lmed`
  and `lmed-vara`.
- Old: 1 record (FDDD). New: `source_token` selects it and `members` is empty, because
  no table slug addresses an unrouted token.

### variant.name

`scb/gdb.toml` `[[variant]]` entry 4

- A canonical-band variant (`70.2861754316974127173`) that names itself. It becomes
  `[[errata.table]] {table = "individer", name = "Individer"}` with no legacy. The
  `before.toml` header `#/variant/4` is not an artifact case id. `description` and the
  panel keys stay on the naming pin.

### errata.field

`sos/dors.toml#/errata.field/9`

- Hardest: `expected_records`, `authority_records` and the evidence digest (a guarded
  name correction with same-family authority), on a routed token.
- `DORS-COV` routes (1:1) to `COV_DORS_HERMES`, then `cov-dors-hermes`. `field` and
  `value` become `set = {name = "Pseudonummer"}`. All guards are carried verbatim.
- Old: 1. New: `{table = "cov-dors-hermes", column = "PSEUDO"}` = 1. Without `table`: 2
  (PSEUDO is also on `dors`).

### errata.data_type

`sos/lova.toml#/errata.data_type/1`

- The only entry. `A_LOVA_HOSP` becomes `lova-hosp`. `set = {data_type = "date"}`.
  `expected_type` and `expected_representation` go to `guard`.
- Old: 1. New: 1.

### errata.classification_reference

`sos/lova.toml#/errata.classification_reference/4`

- Hardest: `EXAMAR` is delivered on three tables (and split by type,
  `identity.split/1`), so `table` must restrict.
- The effect is `classification_declared` with status `unknown`, written as
  `unknown = ["classification_declared"]`.
- Old: 1. New: `{table = "lova-hosp", column = "EXAMAR"}` = 1. Without `table`: 3.

### errata.flags

`scb/ksju.toml#/errata.flags/2`

- All seven entries have the same shape. This one has the irregular delivery name
  `2011 kv2- kv4` (edition 11393).
- `195.275` becomes `sjukfranvaro-sjukloneperioden`. `edition` becomes `deliveries`.
  `expected_period_text` is re-keyed `{"2011 kv2- kv4" = "2011 kv2- kv4"}`. The flags
  become `set = {sensitivity = true, identifier = false}`, keyed by source field names.
- Old: 1. New: 1.

### errata.occurrence_period.scb

`scb/rtb.toml#/errata.occurrence_period/1`

- Hardest: edition 13829 (`Kvartal 1-3 fr.o.m. 2009`) of RegVarID 53 is the delivery
  `identity.edition_split/1` moves to `2.53.folkbokforda-personer-kvartal`. The member
  names the table after `table.assign`: `folkbokforda-personer-kvartal`.
- `edition_scope` and `edition_period_scope` become `scope` and `period`. The period
  text is re-keyed to the delivery name.
- Old: 1. New: 1 (RegVarID 53, DistriktsKod, that delivery).

### errata.occurrence_period.sos

`sos/lova.toml#/errata.occurrence_period/26`

- Hardest: an `authority` record, and the occurrence is one of two `INVARN8` records on
  `lova-person`, the one `identity.rename/1` renames. Only
  `where = {name = "Invandringsdatum 9 numerisk"}` separates them.
- Old: 1. New: 1. Without `where`: 2. The member names the source literal `INVARN8`.

### errata.occurrence_carry

`sos/par.toml#/errata.occurrence_carry/1`

- All four entries have the same shape. They become `[[errata.delivered]]` with
  `member.period = carry_scope`, no donor, head `errata:occurrence_carry`.
- Original (guarded): `{table = "par-ov", column = "DIA"}` = 1. Without `table`: 2 (DIA
  is also on PAR_SV). The member's period is the added coverage (2005–2014), which no
  record holds.

### errata.delivered

`scb/hreg.toml#/errata.delivered/5`

- Hardest: `donor_variant`, `data_type`,
  `upstream = additional-physical-column-in-version` and `native_variable_id`.
- Member `{table = "grundutbildning-poang", column = "LInr", deliveries = versions}`
  selects 0: it is the addition. Donor
  `{table = "grundutbildning-1993", column = "LInr"}` = 1, and that record is VarID
  29874, the entry's `native_variable_id`. So here the donor names the owner. Entries
  with no donor (467) name it through `owner`.

### errata.version

`scb/gdb.toml#/errata.version/1`

- A delivery of the authored `individer` table (the `variant.name` pair), which has no
  source records at all. It becomes
  `[[errata.delivery]] {table = "individer", delivery = "1991-01-01"}`. Old and new
  select 0 records. It adds a delivery.

### errata.column.scb

`scb/lisa.toml#/errata.column/1`

- Hardest: the only `errata.column` form that names deliveries (`versions`). It also
  states `classification` and both flags.
- Variant `individer-15plus` is the pinned slug of `34.153`. `versions` 2010–2013 become
  `deliveries`. All four are edition names on 34.153.
- The member selects 0 occurrences, which matches the old entry: the column is
  undocumented. The register has 0 `Ssyk3_J16` occurrences in `scb-registerinformation`.
  The one occurrence in `scb-lisa-variable-availability` has no delivery, so a member
  with `deliveries` does not select it.

### errata.column.sos

`sos/ekb.toml#/errata.column/1`

- Hardest: the non-SCB form with `holdings_period`, both flags and a `data_warning`, and
  its variant id is hashed.
- Token `EKB_MANAD` becomes `ekb-manad`, because `mint("sos","ekb","EKB_MANAD")` equals
  the pinned `5352018376318353790.5596904919895902135`.
- `holdings_period = "2015-2023"` becomes
  `member.period = {kind = "pooled", label, pooled_start = 2015-01-01, pooled_end = 2023-12-31}`,
  as in `holdings_scope`.
- 0 occurrences of `P1105_LopNr_EKB` (any case) in the EKB source, against 125
  `EKB_MANAD` occurrences. That matches the old entry.

### errata.support

`scb/it-anvandning.toml#/errata.support/7`

- Hardest: a `nonphysical_projection` (`column = ""`, and the guards are all 40 fields
  with `column_name` negative), with an authority at another edition on the same native
  variable.
- Variant `258.556` becomes `it-anvandning-i-foretag`. Edition `5763` becomes delivery
  `2014`, and the authority's `11568` becomes `2017`. `expected_period_text` is keyed by
  delivery. `expected_coding_sha256` is carried in the guard.
- Support: `{table, deliveries = ["2014"], where = {name}}` selects 2 occurrences,
  exactly the old (17575, 556, 5763, column "") selection.
- Authority: `{table, column = "AWSVALPCT", deliveries = ["2017"]}` selects 2
  occurrences, equal to the old selection.
- The same-native authority rule is checked against the owners the two members resolve
  to.

### representation.parallel

`scb/klk.toml#/representation.parallel/1`

- Hardest: it combines `co_delivered`, per-column metadata and per-column coding
  (`expected_codings`).
- `89.50` becomes owner `manad`, and `89.164` becomes table `_default`.
  `source_editions = ["2009"]` becomes `deliveries`. Edition labels are unfolded names,
  and folding is a no-op here.
- `Man` selects 3 occurrences and `Manad` selects 3, each equal to the old selection.
  `expected_evidence_sha256` is carried in `guard`.

### representation.alias_window

`scb/it-anvandning.toml#/representation.alias_window/1`

- The four entries have the same shape. This one sits on a sequential rename: `AEBUY`
  covers 2013–2015 and `E_AWBUY` covers 2017–2018, under one partition owner. In the
  2018 alias delivery, the source documents the other spelling.
- The FQID becomes its slug, and the variant is already a slug. The entry becomes
  `representation.parallel` with `alias_window = true` and one column. The head is
  `curated:alias-window`.
- The old entry selects the owner's records in 2018: 2 occurrences, 1 semantic record,
  under `E_AWBUY`. There are 0 occurrences under `AEBUY` in 2018. So the window selects
  records by owner, table and deliveries, and `column` is only the spelling being
  asserted. The schema has no member here.

### representation.period_family

`scb/lisa.toml#/representation.period_family/2`

- Hardest: it has `expected_definitions` for all twelve months, plus an in-entry
  comment.
- `register = "scb/lisa"` is dropped, because the file implies it.
  `expected_definitions` moves to `guard`.
- The stem selects 60 occurrences: 12 `AGI1LonFink<month>` columns times 5 editions, on
  34.153.

### representation.delivery_metadata

`sos/lova.toml#/representation.delivery_metadata/2`

- Hardest: it has `support` projections, its three columns are in three routed tables,
  and two of the columns carry a `source_scope`.
- `variant_key` holds the routed variant name, and `mint` maps each name to its pin:
  `lova`, `lova-examen`, `lova-hosp`. `records` moves to `guard.expected_records`, and
  `support` stays as is.
- Each `{table, column = "YRKESKOD"}` selects 1 occurrence: `A_LOVA`, `A_LOVA_EXAMEN`
  and `A_LOVA_HOSP` after `identity.route`. Together those are the 3 `records` semantic
  keys.
- `source_scope` is the period-corrected coverage pin (stated 1977- and 1900-, corrected
  to 1995- by `errata.occurrence_period`). Columns 2 and 3 carry it as
  `guard.expected_scope`, not as `member.period`, because members select on stated
  values.

### representation.matrix

`scb/innovation-foretag.toml#/matrix/2014 - 2016`

- Hardest: the CIS2016 `named` form, and its selector covers 54 columns.
- The whole selector goes into `guard.expected_selector`. RegVerID `11529` becomes
  delivery `2014 - 2016`.
- The old selector selects 324 occurrences of var 15662: 54 `CO*` columns times 6.
  `{table = "_default", deliveries, where = {name}}` selects exactly those 324. Table
  plus delivery alone selects 954.

### coding.*

Common rules:

- Each member is `{table, column}`.
- Every selected occurrence has one native variable, so no entry needs an `owner`. That
  holds even for the split owners, because the partition or split separates them by
  column or by Deldatamängd.
- `periods` are kept in their original order. An empty `periods = []` is left out, which
  is equivalent.
- `expected_evidence_sha256` goes in `guard` on the entry (warning, uncoded).
  `keep_members_sha256` stays in `payload` beside `keep_members`. Everything else goes
  verbatim into `payload`.

### coding.warning

`scb/fasit.toml#/coding.warning/1`. The only one. Table `individregister`, 15
occurrences.

### coding.choice

`scb/innovation-foretag.toml#/coding.choice/40`. Hardest: `source_authority` with
`label_equivalences`, `keep_members` and `data_warning`. Table `_default`, 24
occurrences.

### coding.uncoded

`scb/prisindex.toml#/coding.uncoded/1`. Hardest: `stored_role`, a digest and 14 periods,
with split owner `296.119.kn4namn` (partition by column). Table `ppi`, 28 occurrences.

### coding.omit

`scb/fou.toml#/coding.omit/2`. Hardest: 4 periods, and the native key is reassigned by
`identity.column_owner`. Table `lokala-regionala-fou`, 4 occurrences.

### coding.extend

`sos/mfr.toml#/coding.extend/1`. SoS, hashed variant id, `source_authority`. Table
`mfr-fok` (`MFR_FOK`), 1 occurrence.

### coding.documented

`sos/lova.toml#/coding.documented/8`. Hashed variant id, split owner by Deldatamängd,
enumeration authority, `source_scope`, no periods. Table `lova-hosp` (routed
`A_LOVA_HOSP`), 1 occurrence.

### coding.sentinel

`scb/lisa.toml#/coding.sentinel/1`. Split owner `34.19054.org-sni92`, 6 periods. Table
`foretag`, 12 occurrences.

### coding.support

`sos/mfr.toml#/coding.support/1`. All eight association pins. Table `mfr-fok`, 1
occurrence.

## Resolved schema questions

Schema fields that carry what the old entries stated:

- `ErrataField.unknown` states status `unknown`, which `clear` (absent) cannot.
  `errata.classification_reference` uses `unknown = ["classification_declared"]` (52
  entries).
- `ErrataDelivered.owner` names the variable of an additional physical column, which has
  no donor and no source link (467 entries carry `native_variable_id` without a donor).
- `Guard.expected_coding_sha256` carries the coding digest. `errata.support` uses it.
- `RepresentationParallel.guard` carries `expected_evidence_sha256`.
- `CodingEntry.guard` carries `expected_evidence_sha256` for warning and uncoded.
  `keep_members_sha256` stays in `payload` beside `keep_members`.
- `DeliveryMetadataColumn.guard` carries `source_scope` as `guard.expected_scope`
  (columns 2 and 3 of `representation.delivery_metadata`).

Decided without a schema change:

- The matrix member uses `where = { name }`. It is exact (324 = 324).
- Columnless occurrences (`column_name` negative) are selected only by a member without
  `column`, as `Member`'s doc says.
- `Member.column` is the source literal, before any `column_name` effect.
- `source_token` selects the token's records, and `members` narrows them.
- `errata.field` keys are source field names (`sensitivity`, `identifier`).
- `expected_deliveries` is the complement the source table keeps.
- `errata.support`'s same-native authority rule is checked against the owners that the
  two members resolve to.
