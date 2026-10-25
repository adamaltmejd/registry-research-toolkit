# Design: reg-build-model normalize mapping

This is the mapping the phase-3 Rust *normalize* stage implements
([../../PORT.md](../../PORT.md)). The types are in `src/model.rs`; the crate's role,
reader and digest basis are in [../DESIGN.md](../DESIGN.md), "The builder model". The
mapping is temporary: phase 5 moves decoding into Rust adapters and folds it into them.
Normalize reads the occurrence and payload rows of a Python-prepared root
(`PRAGMA user_version=16`, `prepared_sources.py:160-214`) and emits Register, Table,
Delivery, Column, ColumnLink and Evidence. The target schema and link rules are
[../../reg_meta_build/DESIGN.md](../../reg_meta_build/DESIGN.md), "Target design: the
common form".

All paths are relative to `reg_meta_build/src/reg_meta_build/`. "compile" means
`curation_compile.py`. Counts come from the prepared root accepted on 2026-10-25
(prepared manifest `030c4709…`), read with aggregate queries only.

## 0. What the v16 occurrence table holds

Payload rows are interned canonical JSON (`_PayloadWriter.put`,
`prepared_sources.py:272-275`, `exclude_defaults=True`). An absent key in a payload body
means the model default. The decode is `_read_record` (`prepared_sources.py:727-760`).
`fields` is a map of field name to a `field` payload id (`:277-287`). `parents` is a
list of `parent` payload ids, and each parent nests `coordinate` payload ids
(`:289-315`).

Five sources carry `provider = "scb"`. Only some reach compile. The pipeline compiles
sources with `record_usage == "occurrence"` (`pipeline.py:1173-1177`, `:1394`); usage is
set by role in `prepared_catalog.py:413-424`.

  | Source (v16 `source`)                                                   | Rows    | Role / usage                    | Normalize output                                     |
  | ----------------------------------------------------------------------- | ------- | ------------------------------- | ---------------------------------------------------- |
  | `scb-registerinformation`                                               | 996,840 | `scb_records` / occurrence      | Register, Table, Delivery, Column, stated ColumnLink |
  | `scb-unikaregisterochvariabler`                                         | 76,422  | `scb_auxiliary` / field_support | stated fields joined onto SCB Columns (§2.3)         |
  | `scb-identifierare`                                                     | 375     | `scb_auxiliary` / field_support | `identifier` joined onto SCB Columns by VarID (§2.3) |
  | `scb-lisa-variable-availability`                                        | 898     | `lisa_workbook` / support       | Evidence only; not compiled (§3)                     |
  | 13 × `Socialstyrelsen/Metadata…xlsx`                                    | 2,706   | `sos_workbook` / occurrence     | Register, Table, Column, default ColumnLink          |
  | `providers/{fk,fohm,lakemedelsverket,pliktverket,riksarkivet,umu}.toml` | 968     | `thin_provider` / occurrence    | Register, Table, Column, default ColumnLink          |
  | `scb_canonical/scb_canonical.toml`                                      | 31      | `thin_provider` / occurrence    | as thin, under provider `scb`                        |

Each occurrence has exactly one `locator` row. `delivered_cell` holds 36.8M rows over
1,077,342 occurrences.

## 1. Rules that apply to every provider

**Evidence.** Every occurrence row becomes one Evidence value holding the full decoded
record: `record_id`, `source`, `revision_id`, the locator, `delivered_cell`s and every
payload. Evidence is opaque to rules and curation. It must stay byte-exact, because
`record_id` hashes the whole record (`source_records.py:318-351`), SoS inline value
descriptors are keyed by `record_id` (`inline:<record_id>`, see §4), and the
`expected_*_sha256` curation guards hash the Python canonical JSON of records
([../DESIGN.md](../DESIGN.md), "Guard digest basis").

**Record reference.** Curation and guards address a record by `(source, semantic_key)`
(`source_curation.py:1400-1401`, `:2063-2069`). The semantic key is not unique. SCB has
160,046 keys shared by 2-10 occurrences (481,164 surplus rows); SoS has 13 (DORS 1, LOVA
1, BU 1, PAR 10 surplus rows). A reference therefore names a set of Evidence values,
never one.

**Column grain.** A Column is one (table, delivery or none, physical name) under one
link target. It is never one occurrence. Several occurrences can feed one Column (SCB:
one row per population × object type of the edition). One semantic key can feed several
Columns: in 3,124 SCB keys (same RegVerID, VarID and CVID) the rows disagree on
`column_name` (2,880 different literals, 244 case-only). Rows in a shared key also
disagree on `data_type` (2,348 keys) and `data_length` (2,090 keys). Normalize keeps
each differing claim as a separate stated-field observation with its Evidence. The field
ladder in rules decides between them. It does not pick one.

**Physical name.** Column.physical name is `fields.column_name.value` when its status is
`value`. It is optional. 27,979 SCB rows carry status `negative`: a delivered blank
Kolumnnamn means no physical column (`scb_records.py:404-418`). Compile omits these on
purpose (`source_intervals.py:530-535`). Normalize emits no Column for them and keeps
the Evidence so the omission stays visible.

**Coverage (one slot).** Every consumer reads `edition_period_scope` unless its kind is
`not_applicable`, and otherwise `edition_scope` (`source_intervals.py:123-126`,
`:537-540`, compile L1303-1306, `source_curation.py:1587-1591`). Normalize applies that
rule once and stores the result in Column.coverage. The slot each provider uses in the
data:

  | Provider        | `scope_payload` (edition_scope) kinds            | `period_payload` (edition_period_scope) kinds | Coverage slot                |
  | --------------- | ------------------------------------------------ | --------------------------------------------- | ---------------------------- |
  | SCB records     | intervals, pooled, unknown                       | intervals, pooled, unknown                    | period_payload               |
  | SCB auxiliaries | not_applicable                                   | not_applicable                                | none                         |
  | LISA            | intervals, year_independent                      | not_applicable                                | scope_payload (not compiled) |
  | SoS             | intervals, unknown, not_applicable (parent rows) | always not_applicable                         | scope_payload                |
  | thin, canonical | always not_applicable                            | intervals, pooled, unknown, not_applicable    | period_payload               |

**Stated fields.** Column.stated fields are the `SourceFields` names present in
`fields_payload` (`source_records.py:128-168`), each with status, value and raw value.
Parent facts carry their own `SourceFields`; they feed Register, Table or Delivery
fields, never Column fields.

## 2. SCB export (`sources/scb_records.py`, `scb-registerinformation`)

### 2.1 Mapping

  | v16 part                     | Body in the data                                                                                                                                                                          | Neutral target                                                                                                             |
  | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
  | `provider`                   | `scb`                                                                                                                                                                                     | Register.provider                                                                                                          |
  | `register_payload`           | `{native_id: int RegisterId, name}`; 238 distinct, one name per id                                                                                                                        | Register.key = RegisterId (decimal); Register.name = name. The register `name` parent fact wins for display (§2.2).        |
  | `variant_payload`            | `{native_id: int RegVarID, name}`; 597 distinct, one name per id                                                                                                                          | Table.key = RegVarID; Table.name = name                                                                                    |
  | `variant_references_payload` | always `[]`                                                                                                                                                                               | dropped (empty)                                                                                                            |
  | `population_payload`         | always `unknown` (`scb_records.py:375-378`)                                                                                                                                               | dropped; population comes from parents                                                                                     |
  | `variable_payload`           | `{native_id: int VarID, name}`                                                                                                                                                            | ColumnLink(source = Stated). VarID is evidence for the link, not a key (§7). `name` duplicates `fields.name`.              |
  | `member_payload`             | `{native_id: int CVID, name}`                                                                                                                                                             | Column value-set binding token = CVID; joins `Vardemangder.csv` by `member.native_id` (`source_value_bindings.py:356-394`) |
  | `native_payload`             | all five ids (`:389-395`)                                                                                                                                                                 | evidence only. `edition_id` equals the edition parent's `native_id` on every row.                                          |
  | `scope_payload`              | year projection of the period: 964,245 rows differ from `period_payload` only by taking the 4-character year of each bound; 32,595 rows are equal                                         | Delivery.reference period (year form). Read directly by preliminary guards and errata (§8).                                |
  | `period_payload`             | day-precision period from the edition name (`source_periods.py:120-271`)                                                                                                                  | Delivery coverage and Column.coverage                                                                                      |
  | `fields_payload`             | `availability, column_name, name, data_type, data_length, reference_period` on every row; `definition, measurement_unit, source_attribution, operational_definition, description` on some | Column.stated fields; `column_name` → physical name                                                                        |
  | `fields.source_attribution`  | `VariabelRegister_Källa`, the variable's source register (`scb_records.py:436-440`)                                                                                                       | Column stated field, meaning "source register" (see §10)                                                                   |
  | `language`                   | always NULL                                                                                                                                                                               | dropped                                                                                                                    |
  | `original_period_text`       | raw `Registerversionnamn`; equals `context[2]` except 568 rows (raw vs `.strip()`)                                                                                                        | evidence only                                                                                                              |
  | `context_payload`            | 9 strings (`:457-467`): register, variant, edition name (strip only), population name/definition/comment/date, object type name/definition                                                | evidence only, except `context[2]`, which today is the release-status input (§2.2)                                         |
  | `parents_payload`            | always 5 parents: register, variant, edition, population, object_type (`_parent_facts`, `:237-300`)                                                                                       | see the parent table below                                                                                                 |
  | `family_payload`             | `(source, scb, register, native-int, RegisterId, variable, native-int, VarID)`                                                                                                            | not read; recomputed (§7)                                                                                                  |
  | `locator`                    | `Registerinformation.csv`, `row:N`                                                                                                                                                        | Evidence                                                                                                                   |
  | `delivered_cell`             | every CSV cell, including five uninterpreted columns (`RegistervariantSekretess` among them)                                                                                              | Evidence                                                                                                                   |

Parents:

  | Parent kind | Coordinate                                        | Fields                                                                                   | Neutral target                                                                                 |
  | ----------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
  | register    | RegisterId + Registernamn                         | name, purpose                                                                            | Register.name, Register.description (purpose)                                                  |
  | variant     | RegVarID + Registervariantnamn                    | name, description                                                                        | Table.name, Table description                                                                  |
  | edition     | RegVerID + Registerversionnamn (`normalize_text`) | name, description, measurement_information, documentation_status, first/last_approved_at | Delivery: key, name, version_label and fields                                                  |
  | population  | name only, no id                                  | name, population_definition, population_comment, population_date                         | Delivery population (edition-scoped: `catalog_resolution.py:376-384` keys it by `edition_key`) |
  | object_type | name only                                         | name, definition                                                                         | Delivery object type (`catalog_resolution.py:385-391`)                                         |

The target types put `population` on Table. For SCB the source states it per edition, so
normalize fills Delivery.populations and Delivery.object_types, and a rule derives the
Table value if one is needed.

### 2.2 Delivery coordinate and release status

The delivery is the SCB edition (RegVerID). It is spelled three ways in v16:

- `native.edition_id` (`scb_records.py:389-395`);
- the edition parent's coordinate `{native_id: RegVerID, name: normalize_text(name)}`
  (`:282-298`). Its `native_id` equals `native.edition_id` on all rows;
- `context[2]`, `Registerversionnamn` with `.strip()` only (`:109`, `:460`).

In the data, RegVerID and `context[2]` are 1:1 within a variant (8,608 editions, no
collisions either way). The parent name differs from `context[2]` in 180 rows, because
`normalize_text` folds internal whitespace.

Normalize sets Delivery.key = the edition parent's `name`, which `_edition_label` reads
(compile L916-922), and version_label the same. RegVerID and its name are 1:1 within a
variant, so the key is unique in its Table. The RegVerID itself, the spelling
`source_occurrence` uses (`source_occurrences.py:69-80`), stays in Evidence: the
converter re-addresses every `edition = "18774"` to the delivery name before a Rust
build reads the tree, so Rust never matches an integer edition. Delivery.period is
`period_payload` and Delivery.period_years is `scope_payload` (§8, "SCB year-projection
scope").

Release status today: `compile_scb_preliminary` (compile L4026-4130) fullmatches
`(\d{4}), (preliminär|slutlig) version` on `context[2]` (L4051-4055). In the data 27,585
rows match `preliminär`, 63,197 `slutlig`, and 906,058 neither. A preliminary edition is
superseded where a final edition of the same variant and year exists and shares the
VarID (L4060-4076). Normalize sets Delivery.release_status from that regex on
`context[2]`, strip-only form: `preliminary`, `final`, otherwise `unstated`. Using the
folded parent name instead could flip a fullmatch in the 180 divergent rows.

### 2.3 SCB support sources

These rows are not Columns. They attach stated fields to Registerinformation Columns
through the declared `SourceSupportJoin`s (`sources/scb_auxiliary.py:47-85`, matching in
`source_support.py:182-229`).

  | Source        | Join key                                                                                        | Discriminator                                                                                                                | Fields supplied                                  |
  | ------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
  | Unika         | register name, variant name, variable name, column literal (all `.name` or `column_name.value`) | `coverage_from`/`coverage_to` (VersionForsta/Sista) against the candidates' edition names, when the key names several VarIDs | identifier, sensitivity, conditional_sensitivity |
  | Identifierare | VarID (`variable.native_id`)                                                                    | none; applies across registers                                                                                               | identifier                                       |

Payload notes: Unika `register`/`variant`/`variable`/`member` are name-only; scopes are
`not_applicable`; `native` is empty; 3,332 rows have `column_name` unknown (keyless,
diagnosed as `blank_column`). Identifierare has `register` and `variant`
`not_applicable`, so `family_payload` is NULL for all 375 rows.

Identifierare joins on VarID. With `NativeCoordinates` gone, the Rust join must reach
the VarID through the stated ColumnLink's Evidence. This is the one place outside the
link itself where a provider id is read; the join runs inside normalize, which may read
Evidence.

## 3. LISA workbook (`sources/lisa.py`, `scb-lisa-variable-availability`)

The workbook role is `lisa_workbook`, usage `support`. No compile path reads these
records (`pipeline.py:1173-1177`). Only `source_inspection.py:1297-1340` reads them, and
`inspect-source-records` is deleted at cutover ([../../PORT.md](../../PORT.md)). Slice 5
"LISA" concerns SCB register 34 from Registerinformation (row pins, the validator anchor
at `validate.py:568-573`), not this workbook.

  | v16 part                             | Body                                                                                                                  | Neutral target                                                                                                                       |
  | ------------------------------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
  | `register_payload`                   | `{name: "LISA"}`, no id (`lisa.py:705-708`)                                                                           | evidence only. The hardcoded 34 (`scb_records.py:37`) is used only by `source_inspection.py:1327` to select SCB rows for comparison. |
  | `variant_payload`                    | `{name: table_key}` (4 values)                                                                                        | evidence only                                                                                                                        |
  | `population_payload`                 | population text on 139 rows, unknown on 759                                                                           | evidence only; the only provider that fills the subject population                                                                   |
  | `variable_payload`, `member_payload` | column token as `native_id`+`name` / `name`                                                                           | evidence only                                                                                                                        |
  | `native_payload`                     | empty                                                                                                                 | dropped                                                                                                                              |
  | `scope_payload`                      | intervals (833), year_independent (65)                                                                                | evidence only (would be coverage)                                                                                                    |
  | `period_payload`                     | not_applicable                                                                                                        | dropped                                                                                                                              |
  | `fields_payload`                     | availability, column_name, description, source_attribution (= register text "LISA"), base_register, sensitivity (759) | evidence only                                                                                                                        |
  | `original_period_text`               | 833 rows                                                                                                              | evidence only                                                                                                                        |
  | `context_payload`                    | 2 strings (897 rows: title, section label), 4 on 1 row (continuation note)                                            | evidence only                                                                                                                        |
  | `parents_payload`                    | empty                                                                                                                 | dropped                                                                                                                              |
  | `family_payload`                     | `(…, register, name, LISA, variable, native-str, column)`                                                             | not read                                                                                                                             |

Normalize may skip this source for the compiled build. If phase 3 keeps an inspection
path, it reads the Evidence directly.

## 4. Socialstyrelsen (`sources/sos_records.py`)

Two record kinds share each workbook source. Variable records (2,314) come from
`clean_sos_variable` (L362-452). Metadata records (392) come from `_metadata_record`
(L496-582): 349 with a `register` parent (general sheet, plus one per DCAT cell and
language) and 43 with a `variant` parent (Deldatamängder rows).

  | v16 part                                   | Body                                                                                                                                                                                                                                                                                                                                             | Neutral target                                                                                                                                                                                                            |
  | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `provider`                                 | `sos`                                                                                                                                                                                                                                                                                                                                            | Register.provider                                                                                                                                                                                                         |
  | `register_payload`                         | `{name: Datamängd}` from the general sheet (`_register_name`, L334-343); no id                                                                                                                                                                                                                                                                   | Register.key is NOT this. Register.key = lowercased last parenthesized code in the workbook filename (compile L524-529): lmed, ekb, can, dors, lova, lss, hsl, bu, mfr, par, lvm, sol, thr. One workbook is one register. |
  | `variant_payload`, variable rows           | `{name: Deldatamängd token}` (L374-376, L394-398)                                                                                                                                                                                                                                                                                                | Table.key = the token. Resolution to a subset is not done here (below).                                                                                                                                                   |
  | `variant_payload`, subset rows             | `{name: subset name}`, or `"<name> / <label>"` when the name repeats (L515-528)                                                                                                                                                                                                                                                                  | Table.key and Table fields from the variant parent                                                                                                                                                                        |
  | `variant_payload`, register rows           | `not_applicable`                                                                                                                                                                                                                                                                                                                                 | none                                                                                                                                                                                                                      |
  | `variant_references_payload`               | `[]`                                                                                                                                                                                                                                                                                                                                             | dropped                                                                                                                                                                                                                   |
  | `population_payload`                       | unknown                                                                                                                                                                                                                                                                                                                                          | dropped; Table.population unknown                                                                                                                                                                                         |
  | `variable_payload`                         | `{native_id: column token, name: Variabeletikett}` on variable rows; `not_applicable` on metadata rows                                                                                                                                                                                                                                           | default ColumnLink (§7); `name` duplicates `fields.name`                                                                                                                                                                  |
  | `member_payload`                           | `{name: column token}`                                                                                                                                                                                                                                                                                                                           | value-set binding token: member name (`member_target = "member_name"`, `prepared_catalog.py:772-778`; `source_value_bindings.py:395-420`), plus `inline:<record_id>` descriptors                                          |
  | `native_payload`                           | empty                                                                                                                                                                                                                                                                                                                                            | dropped                                                                                                                                                                                                                   |
  | `scope_payload`                            | variable and subset rows: own Data från/Data till (`_coverage_scope`, L210-304): intervals 2,307, unknown 50; register rows: not_applicable                                                                                                                                                                                                      | Column.coverage (variable rows); Table coverage (subset rows)                                                                                                                                                             |
  | `period_payload`                           | always not_applicable                                                                                                                                                                                                                                                                                                                            | dropped                                                                                                                                                                                                                   |
  | `fields_payload`, variable rows            | availability, column_name, name, description, data_type, sensitivity (constant true, L424-428), identifier, coverage_from/to, representation, classification_declared (2,291; suppressed for a same-workbook sheet pointer, L436-445), source_attribution                                                                                        | Column.stated fields                                                                                                                                                                                                      |
  | `fields.source_attribution`, variable rows | the `Specificera källa` cell (`source_detail`, L446-449; `sos.py:970`): 1,850 value, 464 unknown                                                                                                                                                                                                                                                 | Column stated field, meaning "source detail"                                                                                                                                                                              |
  | `fields_payload`, metadata rows            | empty; the facts are in the parent                                                                                                                                                                                                                                                                                                               | —                                                                                                                                                                                                                         |
  | `language`                                 | `sv`/`en` on DCAT register rows only (11-12 of each per workbook); NULL elsewhere                                                                                                                                                                                                                                                                | language tag on the Register field observation; rules pick                                                                                                                                                                |
  | `original_period_text`                     | NULL                                                                                                                                                                                                                                                                                                                                             | dropped                                                                                                                                                                                                                   |
  | `context_payload`                          | `[]`                                                                                                                                                                                                                                                                                                                                             | dropped                                                                                                                                                                                                                   |
  | `parents_payload`, register                | fields from `_GENERAL_FIELDS` (source_version, source_date, and name or dataset_label) and `_DCAT_FIELDS` (name = DCAT Titel, description, coverage, geographic_coverage, population_definition, update_frequency, source_attribution = publisher, documentation_url, landing_page, access_url, access_rights, legislation) (L465-482, L585-660) | Register.name (DCAT Titel wins; Datamängd is `dataset_label` when a DCAT sheet exists, L594-601), Register.description, other register fields. `source_attribution` here means publisher (L478).                          |
  | `parents_payload`, variant                 | `_SUBSET_FIELDS`: name, description, coverage_from/to, update_frequency, aggregation_level (L483-492)                                                                                                                                                                                                                                            | Table.name, Table fields                                                                                                                                                                                                  |
  | `family_payload`                           | `(source, sos, register, name, Datamängd, variable, native-str, token)` on variable rows; NULL on 392 metadata rows                                                                                                                                                                                                                              | not read; recomputed (§7)                                                                                                                                                                                                 |
  | `locator`                                  | workbook path, sheet, `row:N`                                                                                                                                                                                                                                                                                                                    | Evidence                                                                                                                                                                                                                  |
  | `delivered_cell`                           | every cell of the row, including unmapped Objekttyp, Kopplingsbeskrivning and others                                                                                                                                                                                                                                                             | Evidence                                                                                                                                                                                                                  |

**Table tokens.** Variable-row tokens and subset names join in 7 workbooks (CAN, EKB,
HSL, MFR, PAR, SOL, THR). They do not join in LOVA (15 of 15 tokens unmatched) and LVM
(3 of 3). DORS has 1 unmatched token (26 rows) and LMED 1 (1 row). LSS and BU have no
Deldatamängder sheet. In the v16 target normalize emits Table.key = the bare token and
leaves the join to later stages: `[[identity.route]]` (compile L4486-4581) and
`_sos_places_in_default` / `_sos_variant_key` (compile L4429-4448), which needs the
curation tree and so is not a normalize step. D7 moves unambiguous token resolution into
the adapter; that is not in the v16 shape.

SoS has no Delivery.

## 5. Thin providers (`sources/curated_records.py`)

fk, fohm, lakemedelsverket, pliktverket, riksarkivet and umu. One TOML source per
provider. Record kinds: one per `[[register]]` (register parent), one per
`[[register.variant]]` (variant parent), and one per `[[register.variable]]` (no
parents) (`_record`, L268-357).

  | v16 part                                              | Body                                                                                                                                                                                       | Neutral target                                                                                                             |
  | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
  | `register_payload`                                    | `{native_id: authored key, name}`                                                                                                                                                          | Register.key = authored key (compile L531: `str(register[-1])`); Register.name                                             |
  | `variant_payload`                                     | variant rows: `{native_id, name}`; all other rows `not_applicable`                                                                                                                         | Table.key, Table.name (variant rows only)                                                                                  |
  | `variant_references_payload`                          | variable rows: list of `{native_id: variant key}`, 0-6 entries                                                                                                                             | the Tables the Column belongs to. Empty → every declared variant; no variants → one `_default` Table (compile L4832-4843). |
  | `population_payload`                                  | unknown                                                                                                                                                                                    | dropped                                                                                                                    |
  | `variable_payload`, `member_payload`                  | `{native_id: column, name}` on variable rows; `not_applicable` elsewhere                                                                                                                   | default ColumnLink (§7). No value-set binding by member.                                                                   |
  | `native_payload`                                      | empty                                                                                                                                                                                      | dropped                                                                                                                    |
  | `scope_payload`                                       | always not_applicable (L328)                                                                                                                                                               | dropped                                                                                                                    |
  | `period_payload`                                      | `valid_from`/`valid_to` (`_scope`, L225-256): intervals, `pooled` only with `period_scope = "pooled"`, unknown with one bound                                                              | Column.coverage (variable rows); Table/Register coverage (parent rows)                                                     |
  | `fields_payload`, variable rows                       | availability, column_name, name, data_type, definition, sensitivity, identifier (omitted flag = explicit false, L174-182), optional classification_declared, coverage_from/to, description | Column.stated fields                                                                                                       |
  | `fields_payload`, parent rows                         | empty; facts in the parent (`fields` copied into the parent, L333-354)                                                                                                                     | —                                                                                                                          |
  | `language`, `original_period_text`, `context_payload` | NULL, NULL, `[]`                                                                                                                                                                           | dropped                                                                                                                    |
  | `parents_payload`                                     | register rows: one register parent; variant rows: one variant parent; variable rows: none                                                                                                  | Register and Table fields, including coverage_from/to                                                                      |
  | `family_payload`                                      | `(source, provider, register, native-str, key, variable, native-str, column)`                                                                                                              | not read                                                                                                                   |
  | `delivered_cell`                                      | one cell per TOML key, excluding child tables (L184-206)                                                                                                                                   | Evidence, but `data_warning` is read by name (§8)                                                                          |

Compile does not use the variable's `period_payload` as its coverage.
`_compile_thin_register` (compile L4779-4965) mints the occurrence with start =
max(variable `coverage_from` or register `coverage_from`, variant `coverage_from`) and
end = min of the `_to` values (L4854-4885). It uses the variant's `period_payload` only
to keep a `pooled` kind and label (L4886-4900). Phase 1 b1 replaces this intersection
with the neutral coverage rule. Until then, normalize emits Column.coverage =
`period_payload` and the stated coverage_from/to, and the rule stage does the
intersection.

## 6. SCB canonical (`scb_canonical/scb_canonical.toml`)

The same reader as §5, with `provider = "scb"` (`prepared_catalog.py:785-789`). Mapping
is §5. Differences: 2 registers and 1 variant; 3 variable rows declare
`value_set_declared` (value-set binding token = that name,
`member_target = "declared_list"`, `source_value_bindings.py:409-413`);
`_require_canonical_lists` checks them at prepare time. Naming recovers the format from
the source-name prefix (compile L511-513), so these registers take the canonical id
band, not RegisterId.

## 7. Grouping columns into variables (`family_payload` recomputed)

`family_payload` is `native_variable_key(record)` (`source_coordinates.py:43-50`),
written at `prepared_sources.py:351-353`. The Rust normalize does not read it. It forms
links instead:

1. **Stated link (SCB records only).** Columns from rows with the same RegisterId and
   VarID get one link target, `ColumnLink { source: Stated(evidence) }`. The target is
   register-scoped, as `family_payload` is.
2. **Default link (all other occurrence sources).** Where no stated link exists, columns
   with the same physical name within one Register are one variable,
   `ColumnLink { source: Default }`. Compare `normalize_token` output exactly
   (`normalization.py:15-17`: NFC and strip, no case fold).
3. A default link never merges across stated links. Curated links (`identity.*`) apply
   after normalize.

Check against today's `family_payload`:

  | Provider              | Result                                                                                                                                                                                                                                                                                                                                            |
  | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | SoS, thin, canonical  | `variable.native_id` equals `column_name.value` on every variable row. No column literal maps to two variables, and no literals differ by case only. The default rule reproduces `family_payload` exactly.                                                                                                                                        |
  | SCB records           | 3,958 (register, literal) groups hold more than one VarID; 10,695 VarIDs carry more than one literal; 3,643 literals differ from another only by case; 4,113 VarIDs occur in more than one register. The default rule would disagree on all of these. The stated link reproduces `family_payload` exactly, since both key on (RegisterId, VarID). |
  | SCB auxiliaries, LISA | not grouped; not Columns                                                                                                                                                                                                                                                                                                                          |

The 27,979 SCB `negative` rows have a family but no Column. Their link has no column to
attach to; keep them as Evidence under the stated target so curation guards still count
them.

`iter_naming_families` and `iter_partition_families` iterate families in
`family_payload` *payload id* order, then by ordinal (`prepared_sources.py:1189-1193`,
`:1225`). Payload ids follow intern order at prepare time. A recomputed grouping must
reproduce that order, or the slice must show the result is order-independent.

## 8. Oddities a byte-exact port must reproduce

- **Provider as format switch.** `prepared_sources.py:898` and `:1200` read only
  register, variant and parents for `{"scb","sos"}`, and the full subject plus scopes
  for every other provider. Compile builds thin-only expectations and peer guards from
  the fuller projection (compile L3494-3534). SCB canonical has provider `scb` but is
  detected by source prefix (compile L511-513).
- **Register key per provider.** SCB: int RegisterId into the SCB naming band; SoS:
  filename code (compile L524-529); thin and canonical: authored key, canonical band for
  `scb_canonical/` (compile L511-544). `curation_tree.py:2567-2575` requires a decimal
  `native_id` for `{"scb","sos"}` only.
- **`_PROJECTION_REGISTERS = {310}`** (`scb_records.py:40`) strips
  `pooled_start`/`pooled_end` at adapter time (`scb_edition_scopes`, L303-323). The v16
  scopes already carry the result; normalize reads them as they are. b3 moves the flag
  to curation; it does not change v16.
- **LISA register 34** is hardcoded at `scb_records.py:37` and `validate.py:573`. It
  selects SCB rows; the LISA workbook itself names its register `"LISA"`.
- **`source_attribution` has three meanings**: SCB variable source register
  (`scb_records.py:451-455`), SoS variable source detail (`sos_records.py:446-449`), SoS
  register publisher from DCAT (`sos_records.py:478`). LISA puts the register text
  there. Lineage matches the field against register names
  (`catalog_lineage.py:166-279`).
- **Two edition strings.** Release status reads `context[2]` (strip only);
  `_edition_label` and the Delivery name read the parent name (`normalize_text`). 180
  rows differ.
- **SCB year-projection scope.** `scope_payload` is read on its own by the preliminary
  `PeerGuard.edition_scopes` (compile L4099-4106), `guard.edition_scopes`
  (`source_curation.py:1459`), `_edition_label` callers' window bounds (compile L908),
  `scb_errata.py:917-931`, `:1428`, and `cis2016_matrix.py:292`. Keep both scopes on the
  Delivery, or the guard digests change.
- **Thin `data_warning`** is a `delivered_cell` read by name (compile L4927-4932), not a
  typed field. Normalize exposes it as a stated Column fact.
- **Thin flags.** An omitted `is_sensitive`/`is_identifier` is an explicit false claim
  with no raw value (`curated_records.py:174-182`).
- **SoS constant sensitivity** = true on every variable row (`sos_records.py:424-428`).
- **SCB `measurement_unit = "Okänd"`** becomes `unknown` (`scb_records.py:442-447`).
- **Value-set binding tokens differ by provider**: SCB CVID (int `member.native_id`),
  SoS member name plus `inline:<record_id>`, canonical `value_set_declared`, other thin
  providers `classification_declared` only.
- **Shared semantic keys** (§1). One reference names several Evidence values and
  possibly several Columns.

## 9. Delta after phase-1 b2 (release status in the SCB adapter)

b2 makes the SCB adapter parse `preliminär`/`slutlig` into a typed field and turns
`compile_scb_preliminary` into a neutral supersession rule (phase-1 b2).

Payload changes, `scb-registerinformation` only:

- The edition parent's `fields` (in `parents_payload`) gains a typed release-status
  field, parsed from the strip-only `Registerversionnamn` that is `context[2]` today,
  with the same fullmatch. Parsing the folded parent name would change 180 rows.
- `context_payload` may keep its 9 strings. If b2 removes `context[2]`, the tuple
  shrinks and every SCB `record_id` changes, which forces new guard digests.
- `record_id`, and with it every guard digest over SCB records, changes because the
  parent facts change.

Mapping rows that update:

- §2.1 `context_payload`: evidence only, with no exception.
- §2.2: Delivery.release_status = the edition parent's typed field; the regex leaves
  normalize.
- §8 "Two edition strings": the release-status half is gone.
- Supersession matches on the shared stated link (VarID today, L4068-4076) between
  deliveries of one Table and year. The rule reads ColumnLink, not `native.variable_id`.

## 10. Delta after phase-1 b4 (SoS register identity set by the adapter; `source_attribution` one meaning)

Payload changes, SoS sources:

- `register_payload` gains `native_id` = the lowercased filename code (today computed at
  compile L524-529), keeping `name` = Datamängd. This changes the payload body for all
  2,706 SoS rows, every SoS `record_id`, and the SoS `family_payload` (`name` becomes
  `native-str`).
- The `register` coordinate inside every SoS parent changes the same way.
- `source_attribution` is split or renamed. On SoS variable rows the `source_detail`
  value moves to the named field b4 chooses. On DCAT register parents the publisher
  moves to a publisher field. SCB keeps `source_attribution` = source register. LISA is
  unaffected in practice (not compiled).

Mapping rows that update:

- §4 `register_payload`: Register.key = `register.native_id`; the filename rule leaves
  normalize, and §8 "Register key per provider" loses its SoS line.
- §4 `fields.source_attribution` and the register parent row: each maps to its own named
  field.
- §2.1 `fields.source_attribution`: unchanged in meaning, possibly renamed.
- §8 "`source_attribution` has three meanings": becomes one meaning per field.
- Lineage (`catalog_lineage.py:166-279`) reads the register-source field only.

## Decided in phase 2

- Delivery carries population and object-type lists and both scopes (§2.1, §8).
- Delivery.key is the edition name; RegVerID stays in Evidence (§2.2).
- No compiled slice needs the LISA workbook records (§3). Slice 5 ports SCB register
  34's curation, row pins and validator anchor.
