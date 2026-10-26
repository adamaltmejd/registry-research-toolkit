# Tracker: the Rust port of reg_meta_build

**Deleted when phase 4 (cutover) ships.** This is the one root-level tracker AGENTS.md
allows for a multi-PR refactor. It coordinates phases and slices; the design lives in
[reg_meta_build/DESIGN.md](reg_meta_build/DESIGN.md) ("Target design: the common form")
and [crates/DESIGN.md](crates/DESIGN.md) ("The builder model").

## Why

`reg_meta_build`'s common form is not common. `subject.variable` is SCB's VarID for SCB
and the column for every other provider, only SCB has a delivery coordinate, and variant
and coverage are spelled three ways. Curation compensates with 38 provider checks and
step 2 has 38 provider-dependent rules. The port rebuilds the builder in Rust around one
neutral schema, an explicit column → variable link and a neutral verb set, and proves
each step by a zero dbdiff against the Python builder.

## Phases

  | Phase | Content                                                                                                                                                                                                                             | Where         | Proof                                                                  | Status                                   |
  | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ---------------------------------------------------------------------- | ---------------------------------------- |
  | 0     | Cross-table `identity.assign` for LOVA, located errors for wrong-provider verbs, #1907 names                                                                                                                                        | Python        | LOVA slice dbdiff, then the next curation real-seed build              | waits; the LOVA entry waits for evidence |
  | 1     | b1 coverage rule, b2 release status in the SCB adapter, b3 literals to curation, b4 SoS register identity in the adapter                                                                                                            | Python        | slice dbdiff each; one shared real-seed build                          | not scheduled                            |
  | 2     | Spec: `reg-build-model`, the verb schema and golden corpus, the converter with per-entry record-set equivalence, the dbdiff ignore list, the slice specs, the design docs, this tracker                                             | Rust + Python | record-set equivalence on slices 1-3; review                           | in progress                              |
  | 3     | Rust resolve over the Python-prepared SQLite, slice by slice (below)                                                                                                                                                                | Rust          | per-slice `--registers` dbdiff = 0                                     | after phase 2                            |
  | 4     | Cutover: full Rust build at the phase-1 Python release's pins and tree against that release's `reg_meta.db`; the legacy-reference rename (below); commit the regenerated tree; delete the converter and the Python resolve pipeline | Rust          | full dbdiff = 0 under the ignore list, plus conformance                | maintainer schedules the run             |
  | 5     | Raw decoders and preparation in Rust, then a new acceptance cycle; `dbdiff` and `build-docs` port                                                                                                                                   | Rust          | prepared-record equivalence on the pinned inputs, then full dbdiff = 0 | —                                        |
  | 6     | Neutral contract ids (`provider_key`, `register_id`, canonical band), identity across links, role/temporality/unit type, organization-wide definitions                                                                              | Rust          | reviewed dbdiffs; contract changes with the reader                     | —                                        |

Phase 1 b2 and b4 change the prepared payloads, so they need a new `prepare-sources` run
and an acceptance cycle; the maintainer schedules them. The normalize mapping targets
today's v16 shape and names each one's delta in one place
([crates/reg-build-model/DESIGN.md](crates/reg-build-model/DESIGN.md), §9 and §10).

## Settled decisions

- **D1** Hybrid start: Rust reads the Python-prepared SQLite; a Rust normalize does the
  provider-to-neutral translation from the start; raw decoders port last (phase 5).
- **D2** A fold's owner is an existing member's naming id, so its slug survives; it
  becomes the fixed pin key.
- **D3** SCB curation stops addressing by integers, once, at the port; the pin key is
  fixed at first pin and never re-derived from links.
- **D4** `provider_key`, `register_id` and the canonical band are neutralized after the
  port, batched with the legacy-reference rename.
- **D5** Identity across source links (a re-minted VarID) stays curation, with the
  succession worklist.
- **D6** Coverage ⊆ table delivery coverage is a rule (phase-1 b1); the merged
  `errata.delivered` warning stays optional.
- **D7** The SoS adapter resolves unambiguous Deldatamängd tokens; 1:1 routes are
  dropped; `table.assign source_token` stays for ambiguous ones.
- **D8** The LOVA fold waits for an evidence review; it blocks the LOVA entry, not the
  verb.
- **D9** Variables stay register-scoped; an organization-wide definition layer is
  optional, after the port.
- The digest basis is reproduced in Rust, not re-pinned
  ([crates/DESIGN.md](crates/DESIGN.md), "Guard digest basis").
- The steward extension ports before cutover as slice 7; `inspect-source-records` and
  `parse-sos` are deleted at cutover; `entity-key-pins` is deleted in slice 7;
  `classification-residue` becomes part of the Rust build report.

## Legacy references in the artifact

The artifact stores old verb names in two places. Provenance heads (`errata:<class>`,
`errata:<upstream>`, `errata:<source>`, `curated:<table>`, `curated:alias-window`,
`curated:matrix-answer`; the full table is in
[crates/reg-build-model/DESIGN.md](crates/reg-build-model/DESIGN.md) §11) are in
`variable_state.provenance`. Curation case ids (`<file>#/<table>/<n>[/period/<k>]`) are
in `data_warning.evidence_json`: 745 `case_id` values and 1,418 `acknowledged_by` refs
in 0.64.0. Converted entries carry both verbatim (`legacy = { ref, head }`). The
post-cutover rename PR rewrites heads and case ids together, as one reviewed dbdiff, so
neither stays behind.

## Known non-neutral addresses

Some SCB VarID families share a column literal on the same table and delivery, so only
SCB's VarID separates them. The converter tells them apart by the stated name
(`where = { name = ... }`), which is exact today but rests on source wording. Phase 6
and D5 revisit them. Known so far:

- `scb/lbr.toml#/identity.partition/10`: `Lamm` under VarID 42421 and 42420 (2017).
- `scb/energi-flerbostad.toml#/identity.unassigned/1`: VarID 2330's literals, also
  delivered under eight or nine other VarIDs.

The converter lists every member it narrows with `where` in its report
(`convert-report.json`, `non_neutral`), so the complete list is generated with each
conversion rather than kept here. Over the 67 registers converted for PR C (slices 1-3
and the golden corpus's registers) it emits 1,047, almost all `where = { name }`:
`scb/it-anvandning` 737, `iot` 117, `fasit` 55, `innovation-foretag` 34,
`energi-flerbostad` 23, `hreg` 15, `prisindex` 15, `rtb` 13, `lbr` 9, `fou` 8,
`ksju` 5, `gdb` 4, `komvux` 3, `lisa` 1, `sos/lova` 4, `sos/bu` 2, `sos/par` 2. Two
further cases are not VarID families:

- alternative records that share one source ref (SoS `INVARN8` on `lova-person`, PAR's
  `ATC` by data type), where `where` picks the alternative the old entry's
  `expected_fields` describe;
- a column delivered under several case spellings on one table (gdb `fastigheter`
  `Kommun`/`kommun`/`KOMMUN`), which Python folds: the member drops `column` and is
  narrowed by `where` instead, since `column` is an exact literal.

## What the conversion makes explicit

Python infers some addresses that the neutral verbs state. The converter freezes each
inference into data, computed with Python's own rule, so the Rust build infers none:

- **Split pins.** A `[variable]` pin `<reg>.<var>.<split>` on an SCB family with no
  `identity.partition` entry still re-keys it today: each literal whose derived slug
  (`derive_variable_slug`) is the split's suffix goes to that owner
  (`curation_compile._column_partition_plan`). The converter writes one
  `identity.assign` per such family with the exact literals, `legacy.ref =
  <file>#/variable/<reg>.<var>` and no head. A Rust split-pin rule was rejected:
  ownership by spelling is inference. Size: the tree has 2,593 such families with 9,830
  split pins in 159 SCB registers (an upper bound; families with only `column_owner`
  splits bind no literal). The 67 registers converted so far give 191 families and 622
  members.
- **Consequence after cutover.** Once the converted tree is committed, a new delivery
  that brings a new literal into a split family gets no owner: it lands unassigned and
  must be curated. That is the intended behavior, an exact list with nothing inferred,
  but it adds curation work. The Rust build report and `check-curation` must list those
  unassigned literals per family (phase 3, slice 4).
- **Literals a column owner already owns.** A partition's `unassigned_columns` literal
  whose records an `identity.column_owner` entry owns (RTB `Personnr`) is owned in
  Python, so the `owner = false` link leaves those records out.
- **SoS lookup sheets.** A Deldatamängd routed into a variant with no `[[variant]]` pin
  (LOVA's `A_LOVA_STYR_*`) is support-only in Python: its rows are on no table.
- **Old defaults.** `errata.support` without `kind` states `contradictory_question`.
- **Coding on added occurrences.** Three coding entries address occurrences that only an
  `errata.column` or `errata.delivered` entry creates (`scb/fou` `coding.documented/1`
  and `/2`, `scb/gdb` `coding.extend/1`). They have no prepared record, so the
  converter names them as stated (`table`, `column`) and the record-set check cannot
  reach them; the report lists them (`added`). Phase 3 must select coding members after
  the additions.

## Temporary code and its gates

  | Code                                                         | Deleted at                             |
  | ------------------------------------------------------------ | -------------------------------------- |
  | The curation converter (old tree → neutral verbs), in Python: `reg_meta_build/src/reg_meta_build/port/` (`convert`, `select`, `emit`) | phase 4, after its output is committed |
  | The converter's record-set equivalence harness and golden check: `port/equivalence.py`, `port/golden.py` | phase 4 |
  | `crates/reg-build-model/examples/parse_verbs.rs` (reads converted files with the verb types) | phase 4 |
  | `crates/reg-build-model/tests/digest/generate.py`            | phase 5, with Python preparation       |

## Phase 2

  | PR  | Content                                                                | State          |
  | --- | ---------------------------------------------------------------------- | -------------- |
  | A   | this tracker, design docs, `reg-build-model` types and prepared reader | merged (#1935) |
  | B   | verb schema and golden corpus                                          | merged (#1937) |
  | C   | converter, record-set equivalence on the slices, dbdiff ignore file    | open           |

### The equivalence oracle (provisional)

The slice 1-3 equivalence result is provisional: it re-runs on the next accepted
prepared root. The pinned root records `prepare_code_sha256` `3aac71fe…`, and main's
builder refuses it (`prepared_code_stale`) since #1926 changed the `Cargo.toml` pins of
`reg-core` and `reg-core-py`, which the prepare fingerprint hashes. So the Python side
of the check runs from a scratch tree whose prepare fingerprint is still `3aac71fe…`:

- base `dda9b72f5` (`b6b87c2ff^`, the commit before #1926);
- cherry-pick `82c563689` (#1936, repeatable `--registers`, which slice 3 needs);
- all of `1a08ff362` (#1930) excluded, because `_curation.py`, which it touches, is in
  the prepare fingerprint.

It reads this checkout's curation tree (`--curation-dir`). Its `reg_meta_build/src`
differs from main only by #1930, in two places that change no output:

- `concept_groups._common_prefix` instead of `_curation.common_prefix`: the same
  character-wise longest common prefix, differing only on an empty list, which no
  caller passes.
- `prepared_sources._locator_values` calls `os.path.commonprefix` instead of
  `common_prefix`: the same result on a list of strings, and only `prepare-sources`
  runs it.

### PR C results (provisional)

On the pinned root, with the oracle above. Each converted entry's selection equals the
record set its old case acts on in Python's `check-curation --dump-decisions`, and its
`legacy.head` equals the provenance head Python publishes:

  | Slice                 | Registers | Converted entries | Equal | Python run | Peak RSS |
  | --------------------- | --------- | ----------------- | ----- | ---------- | -------- |
  | 1 (thin + canonical)  | 39        | 1                 | all   | 48 s       | 1.7 GB   |
  | 2 (`scb/rtb`)         | 1         | 147               | all   | 179 s      | 8.8 GB   |
  | 3 (Socialstyrelsen)   | 13        | 385               | all   | 56 s       | 1.7 GB   |

Identity is compared per record: every record Python re-keys is covered, each owner
matches Python's (SCB by naming id, SoS by a one-to-one grouping, since SoS keys by
discriminator). All 33 golden pairs equal the converter's output as parsed values
(`python -m reg_meta_build.port.golden`), every converted file parses with
`reg_build_model::verbs::Verbs` (`examples/parse_verbs.rs`), and two conversions of
slice 3 are byte-identical. The full-tree conversion and its equivalence run are
scheduled with the maintainer; they re-run with slices 1-3 on the next accepted root.

To re-run a slice: `check-curation --dump-decisions DUMP --registers ...` in the oracle
tree, `python -m reg_meta_build.port.convert ... --registers ... --out DIR`, then
`python -m reg_meta_build.port.equivalence ... --registers ... --converted DIR --dump
DUMP`.

## Phase 3 slices

  | Slice | Scope             | State |
  | ----- | ----------------- | ----- |
  | 1     | thin providers    | —     |
  | 2     | `scb/rtb`         | —     |
  | 3     | SoS, 13 registers | —     |
  | 4     | the rest of SCB   | —     |
  | 5     | LISA              | —     |
  | 6     | global files      | —     |
  | 7     | steward extension | —     |

Each slice's spec follows. File:line references are to `f67117c72` and drift; re-find
them by name. Counts are 0.64.0 references; each slice's Python oracle DB is the
authority.

### Preface: shared rules

1. **Re-check against the current tree.** Every run reads the curation tree as it is
   that day, not a frozen copy. Re-run the verb counts below before starting a slice.
   The real-seed cache key holds `curation_tree_sha256` and the `--registers` set
   (`scripts/real_seed_cache.py`, build key), so the Python oracle rebuilds itself when
   curation moves. Copy the oracle DB out of the cache at once: `KEEP = 8` evicts
   entries within hours.
2. **The converter runs before every Rust build.** The Rust side reads only the
   converter's output tree (`--curation-dir`); the Python side reads the tracked tree.
3. **Gate = zero dbdiff under the committed ignore file**, nothing else exempt. A slice
   is done when its own set is zero **and** every earlier slice's set still is (re-run
   them).
4. **Nothing content-changing rides with a slice.** A divergence that needs a Python
   behavior change goes to the maintainer (park criterion), lands in Python first, and
   the slice re-runs on the new oracle.
5. **Strict mode by default.** The 0.64.0 strict full build completed with warnings
   only. In subset mode stale entries become `not_evaluated_in_subset` and out-of-slice
   references become `deferred_reference` warnings (`curation_compile.py` `subset=`,
   `pipeline.py` L1480-1560), and `real_seed_cache.incomplete()` accepts a strict subset
   without `publication_ready`. Fall back to `--diagnostic` on both sides only if the
   Python strict subset build blocks; record why.
6. **Pins.** Prepared root `.local/build-2026-10-25/prepared` (manifest sha256
   `030c4709…` verified 2026-10-25), acceptance commit `6ca94676…`. The manifest's own
   `input_commit` (`de44a837…`) is the raw bundle commit; `6ca94676…` is the acceptance
   commit. The scripts in `.local/build-2026-10-25/build-*.sh` used the older
   `build-2026-10-24` root; ignore them. When phase-1 b2 or b4 lands, a new prepared
   root replaces these pins and every slice re-runs on it.
7. **Toolchain.** `RUSTUP_TOOLCHAIN=1.99.1` for real-seed builds (the key hashes the
   `reg_core_py` `.so` bytes).

#### Facts that apply to every slice

- **Classification books are global and written by every `--registers` build.**
  `compile_curation` builds `books` from all `tree.classifications`
  (`curation_compile.py:8201-8242`), `_resolve` resolves each from the value sources
  (`pipeline.py:1591-1660`), and the writer stores them (`materialize.py:820-834`). So
  `classification`, `classification_code`, `classification_replaced_by`,
  `classification_derived_from`, the classification `concept_group` rows and the derived
  `classification_chain`/`classification_family` are non-empty in slice 1's oracle.
  **Slice 1 must port book resolution**, or its dbdiff cannot be zero. This moves part
  of slice 6 into slice 1; slice 6 keeps the variable-level global edges.
- **Global edges are compiled, then filtered, in subset builds.**
  `compile_declared_metadata` (`curation_compile.py:314`) translates every edge and
  records unmatched ones; `resolve_variable_edge_groups`,
  `resolve_metadata_dependencies` and `resolve_catalog_lineage` take `unselected` and
  `slice_registers` and drop edges with an endpoint outside the slice. Rust must
  reproduce that subset filter to pass slices 2-5. It is dead code after cutover; mark
  it `simplify:` with that gate.
- **Partial-build manifest** (`materialize.py:756-795`): `builder_commit` and
  `generation_id` are removed, `catalog_publishable = false`,
  `catalog_completeness = incomplete`, `search_pin` and `search_synonym` are empty and
  their digests hash empty rows. Search pins and synonyms are loaded (and can fail) in
  every mode but stored only by a complete build.
- **Ignore file.** `dbdiff.py` has no `--ignore-file` yet (CLI L1247-1262: `-n`,
  `--no-default-ignore`, `--json`) and `reg_meta_build/port/` does not exist. Phase 2 PR
  C adds both. For slices 1-5 the keys that differ are `import_date` (default ignore)
  and `curation_tree_sha256` (the Rust side reads the converted tree). Slices 6-7 add
  `builder_commit` and `generation_id`. Use the committed file, not this list.
- **Slug freeze.** `slug_state.toml` sets every zone to `curating`, so only that branch
  of `fqid_slugs.load_freeze_states` (L786) and `freeze_state` (L830) is exercised. The
  `churning` and `frozen` paths are not proven by any slice; port them to the Python
  behavior and say so in the PR.
- **Validation is a gate, not content.** dbdiff does not see it. Run the Python
  validator on the Rust DB as a cross-check each iteration:
  `uv run python -c 'import sys; from reg_meta_build.validate import validate_built_db as v; r=v(sys.argv[1]); print(r.passed, r.failures)' <rust.db>`.
- **Write path common to all slices:** `materialize.write_resolved_catalog` (L653):
  `_storage_id` (`_resolved_common.py:96`, built on `id.mint` `id.py:42`), dense
  `variable_id` in (provider, register, slug) order (L716-728), `seed_providers`
  (`db.py:2132`), `_write_value_sets` (L392), `_write_classifications` (L511),
  `prepare_resolved_metadata` (L121), `write_data_warnings` (`db.py:1895`), then
  `derive/` (`derive/__init__.py:47`: states, browse, chains, schema, search_index),
  `ANALYZE`, `VACUUM`, `validate_built_db(corpus=False)` (`validate.py:165`).
- **Guard digests** keep their basis: `evidence_sha256` / `canonical_sha256`
  (`source_evidence.py:54-71`).

#### Oracle template

```sh
REG=(--registers '<spec>' --registers '<spec>' ...)  # one flag per SPEC; quote SoS names
P=.local/build-2026-10-25/prepared
C=6ca9467669b0838c7af6d04576cdb7c99abb48d5
M=030c470955ff4394e95198ce99d6ddaa1115f898e91aae70242275fb61854008
W=<scratch dir>
RUSTUP_TOOLCHAIN=1.99.1 uv run --no-project scripts/real_seed_cache.py build \
  --prepared $P --input-commit $C --input-manifest-sha256 $M \
  "${REG[@]}" > $W/py.json            # add --diagnostic only per rule 5
cp "$(jq -r .database $W/py.json)" $W/py.db  # strict: <entry>/catalog/reg_meta.db
<convert "${REG[@]}" --out $W/tree>
<rust build-db --prepared $P --input-commit $C --input-manifest-sha256 $M \
  --curation-dir $W/tree "${REG[@]}" --db $W/rust>
uv run python -m reg_meta_build.dbdiff $W/py.db $W/rust/reg_meta.db \
  --ignore-file reg_meta_build/port/dbdiff_ignore.json --json > $W/diff.json
```

`--registers` grammar (`pipeline._selected_scopes` L266,
`source_coordinates._coordinate_key` L17): a SPEC is the last element of the prepared
register coordinate, so an SCB integer register id, a thin-provider `native_id` string,
or an SoS register **name** (SoS coordinates carry no `native_id`), or
`SOURCE:<that value>`. `[register].slug` and `provider/slug` are not accepted
(`--registers sos/lova` fails with `pipeline_registers_unknown`). `--registers` is
repeated once per SPEC, so a name may contain a comma (#1936).

#### Park criterion (all slices)

Stop and write up when either holds:

- three consecutive fix iterations leave the dbdiff's differing-row total (sum over
  tables, both directions) unchanged or larger; or
- the time cap is reached: 3 working sessions for slices 1-3 and 5, 6 for slices 4 and
  6, 4 for slice 7.

The write-up goes into the slice's row in `PORT.md` with a linked issue
(`fix(reg_meta_build): …`, labels `reg_meta_build` + `bug`) and contains:

- the pins, the curation tree sha256, the `--registers` set and both mode flags;
- the `--json` dbdiff, and per table the first differing rows in each direction;
- the smallest SPEC set that still reproduces it;
- the Python function believed responsible, with file:line, and what it does that Rust
  does not;
- a stated guess: port bug, or Python behavior to change first in Python (with the
  content change it would make).

The maintainer decides. The slice does not continue past a parked divergence.

----------------------------------------------------------------------------------------

### Slice 1: thin providers (and the SCB canonical pair)

**`--registers` set.** 37 thin registers, by prepared `native_id`:

```
arbetsskadeersattning,assistansersattning,bilstod,foraldrapenning,tillfallig-foraldrapenning,sjuk-aktivitetsersattning,sjukpenning,merkostnadsersattning,omvardnadsbidrag,lakarintygsregistret,tandvardsstodet,underhallsstod,aktivitetsstod,barnbidrag,bostadsbidrag,bostadsersattning,bostadstillagg,ersattning-karens,ersattning-riskgrupper,ersattning-sjuklonekostnader,etableringsersattning,etableringstillagg,graviditetspenning,handikappersattning,narstaendepenning,smittbararpenning,utvecklingsersattning,vardbidrag,sminet,nvr,covidwgs,biverkningar,insark,insiprov,diagnos,inskrivning,hogskoleprovet
```

(fk 28, fohm 3, lakemedelsverket 1, pliktverket 3, riksarkivet 1, umu 1; 1,028 prepared
occurrences.) **Gate 1b** adds `utrikeshandel-tjanster,agi-huvud`: the source
`scb_canonical/scb_canonical.toml` is also role `thin_provider` (31 records) and is
compiled by the same `_compile_thin_register`, but under provider `scb` with the
canonical id band. It is the cheapest place to prove the band. If the band proves
costly, move 1b into slice 4 and say so in `PORT.md`.

**Tables populated** (0.64.0 release counts for these registers, as a reference only;
the slice oracle DB is the authority):

  | Table                                        | Thin                                                                                                                                           | Canonical (1b)            |
  | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- |
  | `register` / `register_variant`              | 37 / 76                                                                                                                                        | 2 / 2                     |
  | `variable` / `variable_state`                | 880 / 1,290                                                                                                                                    | 29 / 29                   |
  | `variable_alias`, `data_warning` (+ `_text`) | yes, 16 warnings                                                                                                                               | yes, 0                    |
  | `state_classification`                       | 27 (binding rules, no value sets)                                                                                                              | 0                         |
  | `value_set` and members                      | none                                                                                                                                           | 3 states bind a value set |
  | global in every build                        | `provider` (8, seeded), all classification tables (see preface), `tag` (check), `import_manifest`                                              | same                      |
  | derived                                      | `expanded_state`, `browse_delivery`, `delivery_window`, `resolver_column`, `coded_variable_stats`, `variable_search_text`, the four FTS tables | same                      |

Empty, left for later slices: `register_version`, `population`, `object_type`,
`variable_alias_window`, `alias_window_classification`, `source_relationship`,
`variable_same_as`, the variable/variant/register/representation `*_replaced_by`,
`variable_state_lineage*`, variable `concept_group*`, `tag_member`, `search_pin`,
`search_synonym`, `holding_*`.

**Curation in scope.**

  | Subtree / file                                                                                  | Verbs (tomllib walk, `.auto.toml` = slug pins)                                                                                                                                                                                                                                                                                                                                               |
  | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `registers/{fk,fohm,lakemedelsverket,pliktverket,riksarkivet,umu}/` (37 files)                  | `[[variant]]` 76, `[variable]` pins 279, `errata.column` 1 (`fk/tillfallig-foraldrapenning.toml`); `.auto.toml` pins 601. `riksarkivet/inskrivning` and `fohm/covidwgs` have no `.auto.toml` (pins inline).                                                                                                                                                                                  |
  | `providers/*.toml` (6)                                                                          | authored source records; already in the prepared root (`prepared_catalog.AUTHORED_SOURCES`), read again only by `check_authored_sources`                                                                                                                                                                                                                                                     |
  | 1b: `registers/scb/{agi-huvud,utrikeshandel-tjanster}.toml`, `scb_canonical/scb_canonical.toml` | `[[variant]]` 2, `[variable]` 16, `.auto.toml` 13                                                                                                                                                                                                                                                                                                                                            |
  | global, loaded in every build                                                                   | `classifications/` (83 books: 490 sentinel codes, 111 `binding.value_set_labels`, 9 `binding.variable`, 7 aliases, 3 family aliases), `classification_groups.toml` (3), `relations.toml` (only `derived_from` 1 reaches output), `slug_state.toml`, `tags.toml` (6), `search_pins.toml` and `search_synonyms.toml` (loaded, not stored), `lineage.toml` (1 entry, `scb/rtb`, unmatched here) |

**Python reproduced exactly.**

- Entry and scope: `materialize.build_catalog` (L1137) → `pipeline.resolve_catalog`
  (L1034) → `_resolve` (L1130); `_selected_scopes` (L266);
  `curation_tree.load_curation_tree` (L3039); `curation_compile.tree_sha256` (L239).
- Naming and slugs: `compile_native_naming` (`curation_compile.py:3443`); the naming id
  branch at L500-535 (`authored_naming_id`, `source_naming.py:303`; for 1b the
  `canonical_scb` test at L512); `source_naming.convert_naming` (L591),
  `refuse_slug_collisions` (L524); `fqid_slugs.load_slug_dir` (L1042),
  `_load_register_auto_file` (L1218), `load_freeze_states` (L786), `_name_slug` (L2118),
  `_uniquify` (L2160), `_fallback_slug` (L2176), `populate_variable_slugs` (L2366).
- Thin compile: `compile_provider_declarations` (L4133) → `_compile_thin_register`
  (L4779-4965), including its register/variant coverage intersection and the
  `accepted-authored:` case ids.
- Per scope: `pipeline._resolve_scope` (L585) → `source_scope.resolve_source_scope`
  (L157), `source_formation`, `source_effects`, `source_intervals`,
  `catalog_resolution`, `data_warnings.keep_warning` (L130).
- Classification books: `pipeline.py:1591-1660`,
  `source_classifications.resolve_canonical_codes` (L154),
  `curation_compile.validate_sentinels` (L286), `prepared_values` (value-source
  sessions), `resolve_classification_successions` (`catalog_dependencies.py:72`); label
  and variable bindings: `source_classification_bindings`,
  `finalize_classification_bindings` (`curation_compile.py` after L8700).
- Catalog stage: `resolve_panel_dependencies` (`catalog_dependencies.py:1328`),
  `resolve_variable_edge_groups` (L983), `resolve_month_groups` (L881),
  `resolve_metadata_dependencies` (L1123), `catalog_lineage.resolve_catalog_lineage`
  (L117), `resolve_variable_successions` (L130), `check_delivery_coverage` (L306).
- 1b only: the canonical band, `id.mint_canonical_scb` (L58), `id.is_canonical_scb`
  (L73), `fqid_slugs.authored_scb_variant` (L388), `_validate_authored_variant_name`
  (L408), `slug_dir_curates_canonical_scb` (L132), `validate._check_minted_id_bands`.
- The common write path (preface).

**Oracle.** Template with the set above, one `--registers` per id; then again with 1b's
two ids appended. Strict: thin scopes carry no acknowledgements and the full build is
strict-clean.

**Park.** Common criterion, cap 3 sessions. Slice 1 is the skeleton, so a park here
blocks every later slice; raise it at once.

----------------------------------------------------------------------------------------

### Slice 2: `scb/rtb`

**`--registers` set.** `2` (unambiguous; `scb-registerinformation:2` also works). RTB
has 675 prepared occurrences.

**Tables populated** (0.64.0 reference): `register` 1, `register_variant` 29, `variable`
308, `variable_state` 7,453, `register_version` 580, `population` 609, `object_type`,
`value_set` / `value_set_member` / `value_code` (4,623 states bind a value set),
`code_variable_map`, `state_classification` 1,394, `classification_conformance`
(+`_code`), `data_warning` 42, variable `concept_group` 30 (+`_variable`, `_axis`,
`_variable_facet` as derived from edges and tokens), `variant_replaced_by` (one RTB
edge), plus everything slice 1 writes. `variable_same_as` is expected empty: 6 of the 7
RTB `same_as` edges leave the slice, and the one edge with both ends in RTB has no row
in 0.64.0 either. Find out why before porting (candidates: `steward_same_as` skipping it
in `compile_declared_metadata` near L396, or an endpoint that does not resolve); Rust
must drop it for the same reason. Also check whether the `lineage.toml` default
`scb/rtb` yields `variable_state_lineage` rows inside RTB (0.64.0 has none for RTB). The
oracle decides.

**Curation in scope.** `registers/scb/rtb.toml` and `rtb.auto.toml` (174 pins):

  | Verb                       | Count | Verb                     | Count |
  | -------------------------- | ----- | ------------------------ | ----- |
  | `identity.column_owner`    | 37    | `errata.column`          | 8     |
  | `identity.partition`       | 31    | `errata.delivered`       | 1     |
  | `identity.edition_split`   | 14    | `coding.sentinel`        | 3     |
  | `errata.field`             | 13    | `coding.extend`          | 2     |
  | `errata.occurrence_period` | 11    | `enrichment.description` | 1     |
  | `[[variant]]`              | 29    | `group`                  | 2     |
  | `[variable]` pins          | 160   | `acknowledge`            | 0     |

RTB and LISA hold all 44 SCB `errata.occurrence_period` entries; no other SCB file uses
the period verb. Global files as in slice 1, plus `relations.toml` (RTB: 7 `same_as`, 1
`replaced_by`) and `lineage.toml`.

**Python reproduced exactly** (beyond slice 1):

- SCB naming: `source_naming.native_scb_naming_id` (L284) via
  `curation_compile.py:515-524`, `fqid_slugs._parse_register_id` / `_parse_variant_id` /
  `_parse_variable_id` (L1272-1315), `_era_column_slugs` (L2313) if RTB has era-split
  columns.
- Identity: `compile_partitions` (L2307; exact literal coverage, member digests,
  `_partition_owned_naming_entry`, `_partition_family_relevant`), `_partitioned_naming`
  (L8084), column_owner inside `compile_errata` (L7029) and `_compile_provider_columns`
  (L6814), `compile_edition_splits` (L4252).
- Errata: `compile_errata` (L7029) with `scb_errata.py`;
  `compile_occurrence_corrections` (L6002) for `errata.occurrence_period`;
  `_curated_head` (L683-690), whose `errata:<class>` / `curated:<table>` provenance
  heads the converter carries verbatim.
- `compile_scb_preliminary` (L4026): runs for every SCB scope until phase-1 b2 lands.
  State in the PR which payload shape (v16 or post-b2) the slice reproduces.
- `compile_enrichment` (L7780); `compile_coding_register` (L4985) with
  `source_coding.py`, `source_coding_choices.py`, `coding_entry_windows` (L4967).
- Support joins, first exercised here: `SourceSupportBindings` (`source_support.py`)
  over `scb-unikaregisterochvariabler` and `scb-identifierare` (identifier and
  sensitivity flags), read whole, never per scope (`pipeline.py:1662-1680`).
- SCB values: `prepared_values`, `source_values`, `source_value_bindings`,
  `source_value_periods`, `edition_bounds`, `source_periods`, `source_curation` guards;
  `catalog_resolution` for `register_version`, `population`, `object_type`.
- Value-set and conformance writers: `_write_value_sets`, `_write_conformance`
  (`materialize.py:563`).

**Oracle.** Template with `--registers 2`. Strict.

**Park.** Common criterion, cap 3 sessions. A divergence in partition or column_owner
guard semantics also blocks slice 4; flag it as such.

----------------------------------------------------------------------------------------

### Slice 3: Socialstyrelsen, 13 registers

**`--registers` set.** SoS SPECs are the register names from the workbooks, one
`--registers` per name:

- Läkemedelsregistret
- Registret över ekonomiskt bistånd
- Cancerregistret
- Dödsorsaksregistret
- Legitimerade omsorgs- och vårdyrkesgruppers arbetsmarknadsstatus (LOVA)
- Insatser enligt lagen om stöd och service till vissa funktionshindrade
- Insatser inom kommunal hälso- och sjukvård
- Registret över insatser till barn och unga
- Medicinska födelseregistret
- Patientregistret
- Registret över tvångsvård enligt lagen om vård av missbrukare i vissa fall, LVM
- Socialtjänstinsatser till äldre och personer med funktionsnedsättning
- Tandhälsoregistret

LVM's name contains a comma; the repeatable `--registers` flag (#1936) selects it, and
the Rust CLI mirrors that flag.

**Tables populated** (0.64.0 reference for all 13): `register` 13, `register_variant`
45, `variable` 1,740, `variable_state` 2,404, `source_relationship` 21 (documentary
bindings, the only provider with rows), `data_warning` 327, variable `concept_group` 51,
`state_classification` 203, `value_set` (349 states), `tag_member` 2, plus slice 1's
globals. Empty: `register_version`, `population`, `object_type` (SoS has no editions),
`variable_alias_window`, every variable relation table (no relation edge touches SoS).

**Curation in scope.** `registers/sos/` (13 files; `.auto.toml` pins 612):

  | Verb                               | Count | Verb                                                                                      | Count |
  | ---------------------------------- | ----- | ----------------------------------------------------------------------------------------- | ----- |
  | `acknowledge`                      | 321   | `documentary.binding`                                                                     | 21    |
  | `errata.occurrence_period`         | 107   | `identity.route`                                                                          | 20    |
  | `representation.delivery_metadata` | 74    | `documentary.retained`                                                                    | 16    |
  | `errata.field`                     | 70    | `coding.support`                                                                          | 8     |
  | `errata.classification_reference`  | 52    | `errata.occurrence_carry`                                                                 | 4     |
  | `identity.split`                   | 36    | `errata.column`                                                                           | 3     |
  | `coding.documented`                | 26    | `coding.extend` 2, `identity.rename` 1, `errata.data_type` 1, `classification_timeline` 1 |       |
  | `[[variant]]`                      | 45    | `group` 20, `[variable]` pins 1,171                                                       |       |

102 of the 107 period entries are LOVA's; phase-1 b1 deletes them, and phase 0 adds
LOVA's `identity.assign`. Re-count on the day. Global files as in slice 1, plus
`tags.toml` (2 SoS members) and `search_pins.toml` (`sos/par`, not stored in a subset).

**Python reproduced exactly** (beyond slice 1):

- `compile_provider_declarations` (L4133) → `_compile_sos_register` (L4451): table
  tokens, coverage, routing, and the SoS errata pass after L4203 (`data_type`,
  `classification_reference`).
- SoS naming id parsed from the workbook filename's last parenthesized code
  (`curation_compile.py:525-530`; phase-1 b4 moves it to the adapter).
- `compile_occurrence_corrections` (L6002) for `occurrence_period` and
  `occurrence_carry`; `identity.split`, `identity.rename`, `identity.route` handling in
  `compile_errata` (L7029) and `compile_partitions` (L2307).
- `compile_delivery_metadata` (L1231); `source_documentary.compile_documentary_bindings`
  and the evidence loop in `pipeline.py:1450-1580` (`owner_bound_literal`,
  `retained_unattached`, the `#1436` row key `"{table}:{evidence_sha256}"`).
- `_classification_timelines` (`pipeline.py:237`); acknowledgement matching in
  `source_scope` (`_window_within` L1279, `_windows_meet` L1294).
- Coding verbs as in slice 2.

**Oracle.** Template with the 13 names, one `--registers` each. Strict.

**Park.** Common criterion, cap 3 sessions.

----------------------------------------------------------------------------------------

### Slice 4: the rest of SCB

**`--registers` set.** The 236 remaining SCB register ids:

```
1,7,8,10,13,22,24,25,26,29,32,42,47,50,51,52,55,60,70,71,72,73,74,75,76,77,86,89,90,91,97,99,102,103,104,105,106,108,110,111,114,115,116,117,118,121,122,123,124,125,127,128,130,131,132,133,134,135,139,140,141,142,143,145,147,149,151,152,155,156,157,158,159,161,162,164,165,166,167,168,176,179,181,182,183,186,187,188,189,190,192,193,194,195,196,197,198,199,200,201,203,208,209,211,215,216,217,219,221,223,225,226,227,228,229,231,232,233,234,235,237,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,257,258,259,260,263,264,269,270,271,272,273,276,282,283,284,285,286,289,290,292,293,294,295,296,299,300,301,302,304,305,307,310,311,312,315,317,318,319,320,321,322,323,325,326,330,333,334,335,336,339,340,342,344,346,348,349,350,351,352,353,355,356,357,358,359,360,361,366,367,368,369,371,372,374,375,376,377,378,379,382,383,387,389,390,391,392,393,395,396,397,400,404,405,407,408,409,410,411,414
```

Regenerate it on the day: the distinct `register_payload` native ids of source
`scb-registerinformation` in `files/records/files/records.sqlite`, minus 2 and 34. All
238 prepared ids have a curation file. If 1b moved here, append
`utrikeshandel-tjanster,agi-huvud`.

This set is about 95% of the corpus, so its Python oracle is close to a full compile
(the
> 43 GB memory wall). **Iterate on sub-batches** of a few registers chosen by verb (one
> batch per verb family below, picked with the tomllib walk), and run the full 236-id
> gate once, scheduled with the maintainer.

**Tables populated** (0.64.0 reference, these 236): `register` 236, `register_variant`
600, `variable` 50,799, `variable_state` 193,610, `variable_alias_window` 2,080 (+
`alias_window_classification`), `register_version` 7,923, `population` 10,790,
`object_type`, `data_warning` 2,945, variable `concept_group` 2,815,
`state_classification` 13,691, `variable_same_as` (1,536 rows scb↔scb, stored both
directions), `variable_replaced_by`, `register_replaced_by` 1,
`representation_replaced_by` (frida, ksju), `variant_replaced_by` (ulf),
`variable_state_lineage`, `tag_member` 19, plus slice 1's globals. Edges to RTB and LISA
are dropped as out of slice.

**Curation in scope.** `registers/scb/*.toml` less `rtb` and `lisa` (238 files incl. the
canonical pair; `.auto.toml` pins 31,225):

`identity.column_owner` 5,948; `errata.column` 2,037; `acknowledge` 1,975;
`errata.field` 1,942; `identity.partition` 1,708; `errata.delivered` 717;
`representation.parallel` 592; `enrichment.description` 268; `coding.extend` 185;
`coding.choice` 162; `errata.version` 155; `code_label_pair` 105; `coding.documented`
54; `enrichment.alias` 44; `identity.unassigned` 38; `errata.support` 35;
`identity.edition_split` 23; `coding.uncoded` 22; `group` 18; `errata.flags` 7;
`coding.sentinel` 7; `representation.matrix` 7; `coding.omit` 5;
`representation.period_family` 4; `representation.alias_window` 4; `coding.warning` 1;
`[[variant]]` 603; `[variable]` pins 24,231.

`representation.matrix` is only in `scb/innovation-foretag.toml`. Global:
`relations.toml` (scb-only edges), `tags.toml`, the rest as slice 1.

**Python reproduced exactly** (beyond slices 1-2):

- `compile_parallel_representations` (L1416), `compile_alias_windows` (L1817) and the
  `curated:alias-window` head (`source_annotations.py:368`), `compile_period_families`
  (L1008), `source_representations.py`.
- `compile_matrix_repr` (L1984) with `cis2016_matrix.py` (the per-survey CIS module, all
  five ids compared at L271-292).
- `errata.version`, `errata.flags`, `errata.support` (`source_effects.py:399`,
  `use = "support"`), `identity.unassigned` (the `owner = false` competition rule) in
  `compile_errata` / `compile_partitions`; `code_label_pair`
  (`curation_compile.py:8248-8279`).
- Release status: `compile_scb_preliminary` (or its post-b2 supersession rule).
- Source events: `source_event_resolution.py` over `scb-timeseries`, the event bindings
  and `skipped_events` (`pipeline.py` after L2210).
- `variable_same_as.py`, `relations.py`, `concept_groups.py` edge and token groups,
  `catalog_dependencies.check_delivery_coverage`.
- `_UNIT_PAIRS` (`source_intervals.py`), `PROVIDER_HEADER_SUFFIXES` and
  `_CODE_LABEL_PREFIXES` (`_curation.py:278`) until phase-1 b3 moves them.

**Oracle.** Template per sub-batch; the full set once. Strict; the 0.64.0 strict full
build carried all 1,975 acknowledgements without stale errors.

**Park.** Common criterion, cap 6 sessions. Park per verb family: one parked family does
not stop the others.

----------------------------------------------------------------------------------------

### Slice 5: LISA

**`--registers` set.** `34`.

**Tables populated** (0.64.0 reference): `register_variant` 7, `variable` 814,
`variable_state` 2,146, `variable_alias_window` 240, `register_version` 105,
`population` 105, `state_classification` 618, value sets (1,076 states), variable
`concept_group` 39 (the month families), `variable_state_lineage` 112, `tag_member` 15,
`variant_replaced_by`, `representation_replaced_by` (2), `variable_replaced_by` (the
LISA-internal `replaced_by` edges: 46 of the 53 LISA edges have both ends in LISA), plus
slice 1's globals. `variable_same_as` within LISA: none in 0.64.0 (all 48 LISA `same_as`
edges leave LISA).

**Curation in scope.** `registers/scb/lisa.toml` and `lisa.auto.toml` (319 pins):
`[variable]` pins 576, `enrichment.description` 92, `identity.partition` 80,
`errata.column` 33, `errata.occurrence_period` 33, `identity.column_owner` 29,
`errata.field` 20, `errata.delivered` 14, `group` 8, `[[variant]]` 7, `coding.extend` 6,
`coding.documented` 5, `coding.sentinel` 5, `representation.period_family` 4.
`[register]` carries `source_labels`, the only SCB file that does. Global:
`relations.toml` (53 `replaced_by`, 48 `same_as` touch LISA), `tags.toml`,
`search_pins.toml` (`scb/lisa`, not stored in a subset).

**Python reproduced exactly** (beyond slices 1-2):

- Resolve-side LISA behavior: `fqid_slugs._era_column_slugs` (L2313, disjoint-era
  `<slug>-<year>`), `compile_period_families` (L1008), `concept_groups.py:59` month
  families, `relations.py` `replaced_by` with vintage lift, `source_labels` handling.
- Prepare-side LISA behavior is **not** reproduced: the ~80 row pins in
  `sources/lisa.py`, `LISA_REGISTER_ID = 34` in `sources/scb_records.py:37` and
  `_PROJECTION_REGISTERS` live before the D1 seam and port in phase 5. Rust reads their
  output from the prepared root.
- Open item to trace first: the prepared root holds 898 records of source
  `scb-lisa-variable-availability` (role `lisa_workbook`, usage `support`, plus
  `WorksheetContextEvidence`). `pipeline._resolve` visits only `occurrence` sources and
  an `rg` found no resolve-path reader of them outside `source_inspection.py`. Confirm
  whether they reach the catalog before porting anything for them.
- `validate.py:568-578` LISA code-membership anchor runs in subset builds too (it is not
  behind `corpus`); the Rust validator must keep it until phase-1 b3 moves it to release
  conformance.

**Oracle.** Template with `--registers 34`. Strict.

**Park.** Common criterion, cap 3 sessions.

----------------------------------------------------------------------------------------

### Slice 6: global files and the full build

**`--registers` set.** None: a full build of every scope (290 in 0.64.0: 238 SCB, 2
canonical, 13 SoS, 37 thin). It is publishable, so the Python side needs a clean builder
checkout at HEAD and the build is a heavy run the maintainer schedules.

**Tables populated.** All catalog tables except `holding_*`. New relative to slices 1-5:
`search_pin` (2) and `search_synonym` (0 today), cross-slice edges (`variable_same_as`
between SCB, RTB and LISA; `tag_member` across registers; cross-register `concept_group`
edge groups), and the manifest keys `builder_commit`, `generation_id`,
`catalog_publishable = true`, `catalog_completeness = complete`. 0.64.0 reference
totals: `variable` 54,570, `variable_state` 206,932, `variable_same_as` 1,640,
`variable_replaced_by` 245, `concept_group` 2,938, `data_warning` 3,330,
`value_set_member` 4,840,917, `code_variable_map` 4,384,977.

**Curation in scope.** The whole tree. Global files: `classifications/` (83),
`classification_groups.toml` (3), `relations.toml` (1,009 edges: 827 `same_as`, 181
`replaced_by`, 1 `derived_from`), `lineage.toml` (1), `tags.toml` (6),
`search_pins.toml` (2), `search_synonyms.toml` (empty), `slug_state.toml` (8 zones, all
`curating`).

**Python reproduced exactly** (beyond slices 1-5):

- Complete-mode branches: `stale_curation_entry` errors for unmatched entries
  (`curation_compile.py:8443-8453`) and stale bindings
  (`finalize_classification_bindings`), the "scopes cover the complete occurrence-source
  selection" check (`pipeline.py:1405`).
- `_write_search_pins` (`materialize.py:620`), `load_search_pins` (`_curation.py:657`),
  `load_search_synonyms`, `search_rows_sha256` and `generation_id`
  (`artifact_identity.py`), `builder_commit` capture.
- `validate_built_db(corpus=True)`: `_check_sos_sanity` and the corpus floors.
- The `classification-residue` content as part of the Rust build report.

**Oracle.** Python: the template without `--registers`
(`real_seed_cache.py build --prepared $P --input-commit $C --input-manifest-sha256 $M`),
strict, publishable; maintainer-scheduled. Rust: `<convert>` on the whole tree, then
`<rust build-db … --curation-dir $W/tree --db $W/rust>`. dbdiff with the ignore file,
which now also covers `builder_commit` and `generation_id`. This is a dry run of the
phase-4 cutover gate; phase 4 itself compares against the phase-1 release artifact.

**Park.** Common criterion, cap 6 sessions. A divergence that appears only in the full
build (it passed every slice) points at the subset filter or at cross-slice edges; say
which in the write-up.

----------------------------------------------------------------------------------------

### Slice 7: steward extension (SWECOV)

**`--registers` set.** None. `extend-db` over a complete base DB, one steward directory.

**Base.** Both sides extend the **same base file**: the SWECOV candidate pins
`global_db_sha256` and `extend_db` refuses any other base (`extend_db.py:445-460`,
`extend_base_db_mismatch`). Today that is `.local/release-0.64.0/ev/global/reg_meta.db`
(sha256 `3b51fa59…`, equal to the pin in `registry-catalog-inputs/swecov/manifest.json`
at `5ae1fcd1`, tag `swecov/accepted-reg_meta-0.64.0`). Re-check the pin on the day.

**Tables populated.** `holding_table`, `holding_period`, `holding_column`,
`holding_mapping`, `holding_key` (all empty in the global DB); steward-only `provider`,
`register`, `register_variant`, `variable`, `variable_state` rows; steward `same_as`
(`_insert_steward_same_as`, `extend_db.py:214`); the steward manifest keys
(`STEWARD_GENERATION_KEYS`, `artifact_identity.py:25-32`); the derived tables rebuilt
(`derive_holdings`, `derive/__init__.py:61`). The 2026-10-25 extend wrote 45 registers,
54 variants, 1,331 variables, 1,358 states, 1,432 holding tables, 38,070 holding
columns, 37,500 mappings.

**Curation and inputs in scope.** `reg_webapp/stewards/swecov/` (`steward.json`,
`scope.toml`, `series/`); the accepted holdings candidate
`registry-catalog-inputs/swecov/` (`manifest.json`, `providers/`,
`policy/inventory.toml`, `slugs/`, `code/`); global `relations.toml` (steward edges,
`steward_same_as`) and the committed slug pins.

**Content-neutral precondition.** The port replaces auto-derived steward slugs with the
global pin rule and deletes `entity-key-pins` and `_check_entity_key_vars_curated`
(`validate.py`, flavored only). Today `extend-db` derives about 1,230 steward variable
slugs on each run. So, before the Rust dbdiff: generate pins equal to today's derived
slugs once, commit them to the candidate's `slugs/` (a reviewed inputs-repo commit), and
re-run the Python extend on them to show zero change. Only then delete auto-derivation.
Otherwise the slice changes content.

**Python reproduced exactly.** `extend_db.extend_db` (L314) and its helpers
(`_load_provider_ir` L84, `_insert_providers` L165, `_assert_steward_rows_slugged` L188,
`resolve_steward_slug_dir` L279, `read_private_holdings_input` L693);
`holdings_compile`, `holdings_accounting`, `holdings_census`,
`holdings_validation.validate_compiled_holdings`, `steward_spec`, `steward_storage`,
`inventory*`; `sources/curated.CuratedAdapter` and `ir/`; `fqid_slugs` steward pins
(`steward_register_pins` L1586); `validate_built_db` in flavored mode
(`extend_db.py:661`).

**Oracle.**

```sh
BASE=.local/release-0.64.0/ev/global/reg_meta.db
H=.local/registry-catalog-inputs/swecov
RUSTUP_TOOLCHAIN=1.99.1 uv run reg-meta-build --db $W/py extend-db --base-db $BASE \
  --steward swecov --holdings-input $H --input-commit <inputs commit> \
  --input-manifest-sha256 <sha256 of $H/manifest.json>
<rust extend --base-db $BASE --steward-dir <swecov steward dir> --holdings-input $H \
  --input-commit … --input-manifest-sha256 … --db $W/rust>
uv run python -m reg_meta_build.dbdiff $W/py/reg_meta.db $W/rust/reg_meta.db \
  --ignore-file reg_meta_build/port/dbdiff_ignore.json --json
```

Strict (extend-db has no diagnostic mode). The cutover proof against the phase-1
release's `reg_meta_swecov.db` is phase 4, not this slice. Never print holdings rows in
the write-up; name tables, counts and keys only.

**Park.** Common criterion, cap 4 sessions. A divergence in holdings accounting touches
the MONA confinement guards; hand it to the maintainer without a proposed fix.
