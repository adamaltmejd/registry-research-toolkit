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
- [x] P0b baseline real-seed DB → /tmp/scb-cur/n747/db_base (built, validated)
- [x] P0c mine concept worklist → 2,776 concepts / 14,191 inst; batches + chunk files
- [x] P1 pilot kon/kommun/person-orgnr/civilstand → RULES tightened (MINIMIZE CHURN: no
  demote prose→cryptic, no invented `<prose>-<columncode>`); re-piloted clean. RULES
  frozen.
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

- baseline DB built (exit 0, validated) → db_base; scb.auto.toml written.
- mining (folded name + base-variance across regs, canonical always surfaced):
  - 48,900 SCB vars; 31,673 concepts; **worklist 2,776 concepts / 14,191 instances**.
  - 29 canonical-SCB instances; 38% of worklist instances are cryptic column-codes.
  - **2,600/2,776 (94%) span >1 delivery-column set** → over-merge risk is pervasive.

## Scope decision (autonomous; surfaced, not blocked — issue #747 open question)

The worklist's bulk is the cryptic COLUMN-code population #471 left untouched (name-arm
only). #471 itself **tried a broad sweep and dropped it** ("PERSORG/bare-N filters swept
in thousands of meaningful series — dropped"; canon_build.py). Over-merge is the
dangerous failure mode (mission). So this pass is CONSERVATIVE-by-correctness,
catalog-wide-by-reach:

- DO (locked, low-risk, high-value): canonical-SCB seed alignment; identifier/entity-key
  families across the catalog (locked forms); converge same-ROLE instances of a concept
  to its canonical prose slug.
- KEEP DISTINCT (default when unsure): generic names reused for distinct roles split by
  column (kommun→Kommun/MantKommun/FtgKommun; kon vs KonSamh); size-2 distinct-fold
  collisions; anything ambiguous. Over-merge > under-merge in cost.
- Agents decide per-concept (unify/split/keep) with column+definition context; hard
  validation (grammar/uniqueness) + base→concept audit + round-2 on flagged merges;
  dbdiff guard. Genuinely-debatable tail unifications left as documented follow-up,
  surfaced in PR.

## Workflow runIds

- round-1 fan-out (310 chunks, sonnet): `wf_feb94680-d0c` (task w81yfcak3). 310 chunk
  files /tmp/scb-cur/n747/chunks/cNNN.json; proposals →
  /tmp/scb-cur/n747/proposals/<bid>.json. Resume: Workflow({scriptPath:
  …scb-slug-consistency-fanout-wf_feb94680-d0c.js, resumeFromRunId: "wf_feb94680-d0c"})
  — agents idempotent (skip written proposals).
- round-1 hit weekly rate-limit at \~199/310 chunks (1,791 valid proposals recovered).
- round-1 RESUME (110 incomplete chunks, sonnet): `wf_41688b4a-da2` (task wp1khgw5e);
  incomplete_chunks.json lists them; agents idempotent so partial chunks finish cleanly.
- baseline DB: /tmp/scb-cur/n747/db_base; slug-dir copy: /tmp/scb-cur/n747/slugdir_base.

## Gotchas carried from #471 (do not relearn)

- PIN EVERY changed slug (curated vars excluded from Pass-3 pending → un-pinned drift).
- Split-sibling source_id disc ≠ variable slug; map refs by auto_slug.
- Free-slot-collapse over-merge → audit base→single-concept.
- Workflow sandbox has NO FS → agents Read/Write own scratch; bake batch list into
  script.
- variable_state_lineage matches by slug identity (−edges expected, advisory →
  #660/#418).
