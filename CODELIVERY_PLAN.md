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

### The data (stable DB, 2026-06-02 build; SCB)

3033 overlapping distinct-non-null-value-set `variable_state` PAIRS:

| kind | count | meaning |
|---|---|---|
| **different window** | 1819 | broad "default" coding + a narrower override (exception year / transition). 1655 strict containment; ~164 transition off-by-ones (likely a coalescer boundary bug). **Artifacts** — should partition cleanly. |
| **identical window** | 1214 | genuinely two codings for the *same* range. 835 cosmetic (≤2-code off-by-one); 216 divergent (`pin/branschgrupp` Br92 vs Br07; grain variants like "Ja nej 1" vs "Ja nej 3"). |

Only 204/1226 violating `(variable,variant)` pairs carry a classification family;
the bulk is anonymous value sets — so a classification-based rule does **not**
suffice.

### Plan

1. **Fix window derivation (build time).** Stop collapsing to `[min,max]`; emit
   per the actual observed editions so the timeline is **partitioned** (one value
   set per `(variable, variant, period)`). Open design choice:
   - **(a) multiple intervals per shape** — keep the `valid_from/valid_to` schema
     + its sub-annual granularity + all consumers; just split a shape's window
     around gaps (RLE of the period set). Minimal blast radius.
   - **(b) explicit period/edition lists** — replace the interval with a period
     set (e.g. a `variable_state_period` junction). Simpler/bug-proof build (no
     interval arithmetic, no boundary off-by-ones, trivial co-delivery detection)
     but a v1 schema + query-layer rewrite (`resolve_at`, period-grammar
     expansion, every reg_meta/webapp/bundle consumer). Justifiable pre-v1.
   - Lean: (a) unless we want the bigger simplification. Decide before cutting —
     it changes the storage contract.
2. **`validate.py` invariant + report.** Add a `build-db --validate` check that
   FAILS when any `(variable, variant)` has two states with **overlapping**
   validity and **distinct** `value_set_id`. After (1) the only survivors are
   **identical-window** genuine co-delivery → these must be resolved or curated.
3. **Resolve / curate the identical-window 1214.** Auto-resolve cosmetic
   (off-by-one) + classification-in-effect cases; the genuinely-divergent ~216
   → `UNRESOLVED` (curation TOML, modeled on `fqid_slugs`) → build fails until
   resolved. Open question: genuinely-distinct co-codings (Br92/Br07, grain)
   may instead warrant **splitting into sibling variables** (distinct FQIDs), the
   way §5.7 splits distinct columns.
4. **Browse-by-`value_set_id` timeline (frontend/backend, nice-to-have).** The
   `?value_set_version` browse keys on the free-text label today — reliable for
   classifications, weak for anonymous sets. Key it on `value_set_id` + show each
   distinct value set's contiguous span (e.g. `sysselsattningsstatus`: set A
   2000–2011, set B 2012–2025). Composes cleanly on (1)'s partitioned windows.

### Verification

Rebuild SCB-only to a scratch path (read-only on the input; never the live DB):

```sh
reg-meta-build build-db \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data \
  --db /tmp/regmeta-scratch/ --providers scb --validate
```

Invariant passes (or fails only on `UNRESOLVED`); spot-check that representative
bindings (`scb/rtb/kommun`, `scb/komvux/folkbokforingskommun`,
`scb/rtb/civilgrel`) resolve to a single value set per period; the live DB at
`~/.local/share/reg_meta/reg_meta.db` is **never** rebuilt in place.
