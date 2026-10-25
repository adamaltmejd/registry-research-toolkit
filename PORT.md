# Tracker: the Rust port of reg_meta_build

**Deleted when phase 4 (cutover) ships.** This is the one root-level tracker AGENTS.md
allows for a multi-PR refactor. It coordinates phases and slices; the design lives in
[reg_meta_build/DESIGN.md](reg_meta_build/DESIGN.md) ("Target design: the common form")
and [crates/DESIGN.md](crates/DESIGN.md) ("The builder model").

## Why

`reg_meta_build`'s common form is not common. `subject.variable` is SCB's VarID for SCB
and the column for every other provider, only SCB has a delivery coordinate, and
variant and coverage are spelled three ways. Curation compensates with 38 provider
checks and step 2 has 38 provider-dependent rules. The port rebuilds the builder in Rust
around one neutral schema, an explicit column → variable link and a neutral verb set,
and proves each step by a zero dbdiff against the Python builder.

## Phases

  | Phase | Content                                                                                                                                                                                      | Where         | Proof                                                                                | Status                                     |
  | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------ | ------------------------------------------ |
  | 0     | Cross-table `identity.assign` for LOVA, located errors for wrong-provider verbs, #1907 names                                                                                                | Python        | LOVA slice dbdiff, then the next curation real-seed build                            | waits; the LOVA entry waits for evidence   |
  | 1     | b1 coverage rule, b2 release status in the SCB adapter, b3 literals to curation, b4 SoS register identity in the adapter                                                                    | Python        | slice dbdiff each; one shared real-seed build                                        | not scheduled                              |
  | 2     | Spec: `reg-build-model`, the verb schema and golden corpus, the converter with per-entry record-set equivalence, the dbdiff ignore list, the slice specs, the design docs, this tracker | Rust + Python | record-set equivalence on slices 1-3; review                                         | in progress                                |
  | 3     | Rust resolve over the Python-prepared SQLite, slice by slice (below)                                                                                                                        | Rust          | per-slice `--registers` dbdiff = 0                                                    | after phase 2                              |
  | 4     | Cutover: full Rust build at the phase-1 Python release's pins and tree against that release's `reg_meta.db`; the provenance-head rename; commit the regenerated tree; delete the converter and the Python resolve pipeline | Rust | full dbdiff = 0 under the ignore list, plus conformance | maintainer schedules the run |
  | 5     | Raw decoders and preparation in Rust, then a new acceptance cycle; `dbdiff` and `build-docs` port                                                                                         | Rust          | prepared-record equivalence on the pinned inputs, then full dbdiff = 0              | —                                          |
  | 6     | Neutral contract ids (`provider_key`, `register_id`, canonical band), identity across links, role/temporality/unit type, organization-wide definitions                                    | Rust          | reviewed dbdiffs; contract changes with the reader                                   | —                                          |

Phase 1 b2 and b4 change the prepared payloads, so they need a new `prepare-sources` run
and an acceptance cycle; the maintainer schedules them. The normalize mapping targets
today's v16 shape and names each one's delta in one place
([crates/DESIGN.md](crates/DESIGN.md), "Normalize mapping").

## Settled decisions

- **D1** Hybrid start: Rust reads the Python-prepared SQLite; a Rust normalize does the
  provider-to-neutral translation from the start; raw decoders port last (phase 5).
- **D2** A fold's owner is an existing member's naming id, so its slug survives; it
  becomes the fixed pin key.
- **D3** SCB curation stops addressing by integers, once, at the port; the pin key is
  fixed at first pin and never re-derived from links.
- **D4** `provider_key`, `register_id` and the canonical band are neutralized after the
  port, batched with the provenance-head rename.
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

## Temporary code and its gates

  | Code                                                         | Deleted at                       |
  | ------------------------------------------------------------ | -------------------------------- |
  | The curation converter (old tree → neutral verbs), in Python | phase 4, after its output is committed |
  | The converter's record-set equivalence harness               | phase 4                          |
  | `crates/reg-build-model/tests/digest/generate.py`            | phase 5, with Python preparation |

## Phase 2

  | PR | Content                                                              | State |
  | -- | -------------------------------------------------------------------- | ----- |
  | A  | this tracker, design docs, `reg-build-model` types and prepared reader | open  |
  | B  | verb schema and golden corpus                                        | —     |
  | C  | converter, record-set equivalence on the slices, dbdiff ignore file | —     |

## Phase 3 slices

  | Slice | Scope                 | State |
  | ----- | --------------------- | ----- |
  | 1     | thin providers        | —     |
  | 2     | `scb/rtb`             | —     |
  | 3     | SoS, 13 registers     | —     |
  | 4     | the rest of SCB       | —     |
  | 5     | LISA                  | —     |
  | 6     | global files          | —     |
  | 7     | steward extension     | —     |

SLICE_SPECS
