# Value-set co-delivery plan (stacked follow-up)

**Scope + self-delete gate:** this is the cross-package tracker (governance
exception, per `CLAUDE.md` "Governance") for the value-set-version work, split
across two stacked PRs. **Delete this file when Phase 2 merges.**

- **Phase 1 (this PR): retire `@version` + re-key the ambiguity check.** Done.
- **Phase 2 (stacked on top): enforce one value set per `(variable, variant,
  period)` at build time.** Designed here; not yet implemented. Both PRs merge
  together once Phase 2 is green.

This closes the deferred A5.3c-ii "value-set-version pinning" follow-up — but the
investigation showed the right fix is *not* a binding-side pin (see Phase 1) and
*is* a build-time invariant (Phase 2).

## The model decision (settled)

A binding FQID names a **variable**; its value set is determined by the resolved
`(variable, variant, period)` — **not** pinned on the FQID. A value set is an
anonymous, content-hashed code-list (`value_set.member_hash` + members); it has
**no** validity interval of its own — validity lives on `variable_state`
(`valid_from`/`valid_to`). Invariant target: **`(variable, variant, period)`
resolves to exactly one value set.**

## Phase 1 — done (this PR)

- Retired the `@version` binding-FQID pin everywhere: `reg_schema.structural`
  (+ the `reg_monabundle` runtime mirror), the `reg_webapp` path guard
  (`catalog_fqid`), the catch-all reconcile + `_parsed_binding`/`catalog_index`/
  `semantic`/`order_export` threading. A `@` in a binding leaf is now an
  `invalid_fqid`. Dropped the `binding_value_set_version_mismatch` rule.
- Re-keyed `binding_value_set_version_ambiguous` onto **`value_set_id`** (not the
  free-text `value_set_version_label`): the webapp now flags only *genuine*
  distinct-value-set co-delivery, not the ~71% label-noise.
- The read-only `?value_set_version` **browse** filter is kept (useful for
  classified *and* anonymous value sets — see Phase 2 (3)).
- Regenerated `openapi.json` + `api-types.ts`. All gates green.

The semantic `binding_value_set_version_ambiguous` is now a **defensive
backstop**: once Phase 2 enforces the build invariant, a clean catalog never
trips it.

## Phase 2 — to build (stacked PR)

### Root cause (why co-delivery exists)

The SCB coalescer (`reg_meta_build/sources/scb.py`) groups `variable_instance`
rows by a shape-tuple (incl. `value_set_id`) and derives each group's window as
**`[min, max]` of the editions it was observed in** (`_StateGroup.regver_min/
max`). That single contiguous span **cannot express gaps**: if `vs A` was
delivered 1998–2009 and 2011–2025 (with `vs B` in 2010), the coalescer still
emits `vs A` = `1998–2025`, overlapping the real 2010 `vs B` state. The overlap
is a `[min,max]` artifact — the 2010 exception is legitimate ("a status appeared
then vanished").

### The data (scratch SCB-only build, 2026-06-03; instrumented coalescer)

3014 overlapping distinct-non-null-value-set `variable_state` PAIRS on the shipped
(`[min,max]`) DB. But the **real** signal is per `(variable, variant, year)`
occupancy from the *observed editions* (`_StateGroup.regyears`, not the
`[min,max]` span). Two findings reshaped the plan:

- **Most "overlaps" are `[min,max]` phantoms.** `civilfar` is delivered one
  cvid/year 1998–2025; 2010's cvid → exception value set `vs2834`, so the `vs2`
  group has a **real gap at 2010** the `[min,max]` collapse paves over. Likewise
  the 5,944 multi-label "folds" (e.g. `kommun` `LKF 1980-01-01 … 2025-01-01`) are
  **sequential annual vintages** — one label/year — that only overlap via
  `[min,max]`. Per-year occupancy lays both out correctly with no overlap.
- **Genuine same-year co-delivery = 1,584 cells** (years where ≥2 distinct value
  sets were *both* observed). A deterministic CASCADE resolves 63%:

  | layer | resolves | signal |
  |---|---|---|
  | **authority** | 452 | `registerversionnamn`: final > plain > sub-annual > old |
  | **recency** | 249 | latest `registerversion_senastgodkanddatum` supersedes |
  | **cosmetic** | 296 | symmetric code-diff ≤ 2 → keep larger |
  | **GENUINE** | 587 cells / **129 (variable,variant)** | true parallel co-delivery |

  Genuine splits two ways: **distinct-column** parallels (`Åldersgrupp`
  `agrupp`/`agrupp2`) → auto-split into siblings; **same-column** dual-coding
  (`rv=448` historical old/new; two ~800-code classifications) → curate.

### Plan (settled)

1. **Per-`(variable, variant)` per-year value-set timeline (build time).** Replace
   the per-group `[min,max]` emit. For each `(variable, variant)` that has
   OVERLAPPING distinct-value-set groups, build per-year occupancy from
   `regyears`, resolve each year to a winner via the cascade
   (authority → recency → cosmetic), and RLE each group's *won* years into
   contiguous `variable_state` runs. Non-overlapping `(variable, variant)` keep
   the fast `[min,max]` path (preserves benign-gap coverage + most output). This
   is option **(a)** (interval schema, multi-interval-per-shape) — chosen over a
   period-list (b): the period grammar is sub-annual, the whole query layer is
   interval-overlap, and SOS already emits multi-interval-per-shape, so (a) needs
   no schema/consumer change. Cascade signals tracked on `_StateGroup`:
   `regyears`, `year_authority` (`_edition_authority`), `year_approval`.
2. **`validate.py` invariant (done).** `_check_one_value_set_per_period` FAILS the
   build when any `(variable, variant)` resolves a period to >1 value set. After
   (1) the only survivors are the genuine residual.
3. **Auto-split distinct-column genuine parallels** into sibling variables
   (extend §5.7 split: co-delivered distinct columns are distinct variables now
   that `@version` is retired — a binding can't pick a grain otherwise).
4. **Same-column genuine residual → STRICT FAIL → curation TOML** (modeled on
   `fqid_slugs`): build fails listing each; a TOML entry pins the canonical value
   set (or requests a split). Nothing ships until every same-column genuine case
   is curated.
5. **Browse-by-`value_set_id` timeline (frontend/backend, nice-to-have).** Key the
   `?value_set_version` browse on `value_set_id` + show each distinct set's
   contiguous span. Composes on (1)'s partitioned windows.

### Verification

Rebuild SCB-only to a scratch path (read-only on the input; never the live DB).
`--db` is a GLOBAL flag (before the subcommand); the default is the live DB, so it
must always be passed:

```sh
reg-meta-build --db /tmp/regmeta-scratch build-db \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data \
  --providers scb --skip-slugs --validate
```

Invariant passes (or fails only on uncurated same-column genuine); spot-check that
`scb/rtb/civilfar` resolves to one value set per period (vs2834 owns 2010, vs2
1998–2009 + 2011–2025); the live DB at `~/.local/share/reg_meta/reg_meta.db` is
**never** rebuilt in place.
