# PROGRESS — #747 SCB whole-slug curation (catalog-wide concept→consistent-slug)

> **Temporary overnight run tracker. DELETE before `gh pr ready`** (repo forbids
> permanent root-level trackers — CLAUDE.md Governance; Codex flags it). Lasting record
> = PR description + commit messages + reg_meta_build/DESIGN.md.

Branch `feat/scb-slug-consistency` off `main` (87d2b6b6). PR: `Closes #747`, refs
#209/#444/#660/#418/#732/#546. Continues #471 (PR #746). Scratch under
`/tmp/scb-cur/n747/`.

## Convention (locked — obey verbatim)

Swedish, ASCII-folded (å/ä→a, ö→o). No English/coined contractions. Abbrev only via
established short-forms (nr, orgnr, id, antal) or dropping filler. ≤24 target, ≤20
prefer, ≤40 hard. One canonical slug per DISTINCT concept (name AND delivery-column
semantics) across ALL registers incl. canonical-SCB. `-N` ONLY for true twins. Locked
families: personnr / personnr-mor/-far/-barn/-sambo / eget-personnr / personnr-urval /
uppgiftslamnarens-personnr / personnr-motsvarande / personnr-bakgrund; person-orgnr /
person-samordnings-orgnr / person-orgnr-{arbetsgivare,anstalld,anordnare,bolag}; orgnr /
orgnr-foretaget / orgnr-10 / orgnr-12; cfar-nummer / cfar-nummer-arbetsstalle;
hushallsid; fnr (+fnr-far/-mor); fastighetsbeteckning; lghnr; scbid;
foretagsenhetsnummer / verksamhetsenhetsnummer.

## Pin surfaces

- regular SCB: `[variable."<reg>.<provider_key>"]` slug override in scb.toml (pin ALL
  changed).
- canonical-SCB (#444): `[variable."<minted_reg_id>.<Column>"]` in scb.toml
  (provider_key = delivery COLUMN, not var_id — minted regs unique-column so 2-part
  key). NO scb_canonical.toml code change needed (adapter sets slug="" → populate_slugs
  fills from override).
- refs: panel_entity_key/panel_time_key (bare + composite JSON-array) scoped to variant
  reg; classification_links.toml; curation/relations.toml; concept_groups.toml.

## Phases

- [x] P0a branch + scratch + baseline slug-dir copy
- [ ] P0b baseline real-seed DB → /tmp/scb-cur/n747/db_base (bg id: b2qft2ngy)
- [ ] P0c mine concept-inconsistency worklist (folded name, base-variance across regs +
  canonical-SCB alignment) → per-concept batches; counts here
- [ ] P1 pilot 2–3 concept families; iterate prompt to clean
- [ ] P2 full fan-out (Workflow, resumable) round-1 + round-2 escalation; runIds here
- [ ] P3 apply (pin all) + repoint refs + rebuild + dbdiff vs db_base + base→concept
  audit → fixpoint (0 worklist mismatch / 0 unintended ripple / 0 collision);
  refresh snapshot
- [ ] P4 canonical-SCB seed fold-in (verify minted pins resolve in rebuild)
- [ ] P5 gates: ruff/ty/pytest(Docker UP)/panache; real-seed build-db on PR head +
  dbdiff (attach); /code-review high until clean; gh pr ready; Codex poll;
  stale-head; summary
- [ ] DELETE PROGRESS.md before ready

## Key numbers

(filled as phases complete)

## Workflow runIds

(filled at fan-out)

## Gotchas carried from #471 (do not relearn)

- PIN EVERY changed slug (curated vars excluded from Pass-3 pending → un-pinned drift).
- Split-sibling source_id disc ≠ variable slug; map refs by auto_slug.
- Free-slot-collapse over-merge → audit base→single-concept.
- Workflow sandbox has NO FS → agents Read/Write own scratch; bake batch list into
  script.
- variable_state_lineage matches by slug identity (−edges expected, advisory →
  #660/#418).
