<script lang="ts">
// The #615/#671 availability-aware local period slider — the subject page's
// DEFAULT period control (#611 → Period model). A year-grain dual-thumb slider
// (mirrors the header's YearWindowSlider mechanics) over the project WINDOW,
// drawn on a track that shows the subject's data COVERAGE UP FRONT (#671): the
// out-of-coverage region is a greyed NON-SELECTABLE band rendered immediately
// (no drag needed), and the thumbs are hard-clamped to coverage (via the shared
// primitive's `selectableMin/Max`) so a true data span like 1995–2008 is visible
// at a glance instead of the full 1960–vintage track reading as available. The
// not-delivered GAP (inside the selection but outside coverage) is GREYED, and
// two amber deviation hints fire:
//   • USER deviation — the active selection ≠ the project window → "deviates
//     from project window · reset to project window" (reset = clear `?period`,
//     which falls back to the window; the parent owns that URL write).
//   • AVAILABILITY deviation — coverage doesn't cover the active selection →
//     the greyed gap + a "not delivered before/after Y" note. SOFTENED when no
//     project window is set (browsing without a window is not a "deviation",
//     just an FYI), per the spec.
//
// Self-contained presentation (props in, callbacks out — no store import) so
// it's unit-testable in isolation; the PeriodPicker owns the wire seam (it
// shapes the slider's `{from,to}` to/from the `?period` grammar) and the URL
// write flows through BindingLeafView's onsubmit/onclear, unchanged.

import { untrack } from "svelte";
import DualThumbTrack from "./DualThumbTrack.svelte";
import { type Coverage, notDeliveredGaps, sameYearWindow } from "./period";
import type { StudyWindow } from "./project_data";

interface Props {
  // The slider bounds (inclusive years) — the union of the window + coverage,
  // computed by the picker so both fit on one track.
  min: number;
  max: number;
  // The active selection to seed the thumbs from (the resolved precedence
  // `?period` > window > full history, already year-shaped by the picker).
  selection: StudyWindow;
  // The project window (#614), or null = none set. Drives the user-deviation
  // hint + the "reset to project window" affordance.
  window: StudyWindow | null;
  // The subject's data-availability span (derived from embedded states), or
  // null = unknown. Sides are INDEPENDENTLY bounded — a null side is open
  // (start/end unknown). The open END ("still delivered") projects to the
  // catalog VINTAGE (`vintageYear`), not the track edge: the band stops at the
  // vintage and a selection past it gaps as "not delivered after <vintage>"
  // (#631). The open START stays open (band runs to `min`, never gaps). Drives
  // the greyed not-delivered track + the availability hint.
  coverage: Coverage | null;
  // The catalog VINTAGE year (#631) — the ceiling an OPEN-ENDED coverage end
  // projects to ("delivered as far as the catalog knows = the vintage"). It
  // bounds the coverage BAND and the availability GAP, NOT the slider bounds
  // (the picker computes those). Optional — when a caller doesn't cap (the
  // standalone tests) the open end falls back to the track edge `max`.
  vintageYear?: number;
  // The active `?period` wire when it is set but NOT year-representable (a
  // sub-annual token / segment list / `_default` / text), else null. When
  // non-null the slider's `selection` is the project-window PROJECTION, not the
  // active selection — so we suppress the (misleading) user-deviation hint and
  // show an honest cue pointing at the real value in the (already-open) "more"
  // expander, instead of presenting the window as if it were selected.
  subAnnualPeriod: string | null;
  // false = no `?period` nor window chosen — suppress the not-delivered
  // availability GAP (the selection-vs-coverage deviation), which would otherwise
  // flag a span the user never chose (#639). Independent of the #671 up-front
  // `unavailable` band, which renders regardless (a passive "no data here", not a
  // selection warning) — that's how #671 surfaces coverage without resurrecting
  // the #639 alarm. The coverage band still renders too (an FYI of where data
  // exists, not a deviation claim).
  hasSelection: boolean;
  // Whether the user has ACTUALLY chosen a year-window value distinct from the
  // project window (an explicit year `?period`, or a live thumb drag) — gates the
  // USER-deviation hint (Fix B, refined by Fix 3). FALSE for the untouched
  // coverage-clamped default seed; combined with the `seedWithinWindow` carve-out
  // below, a default-seed deviation is silenced ONLY when the seed is a true
  // within-window narrowing (window∩coverage). A DISJOINT window/coverage default
  // seed snaps OUTSIDE the window, so the hint fires even without `userChosen`.
  // Distinct from `hasSelection`, which is true whenever a window is set and so
  // can't tell the default from a real choice.
  userChosen: boolean;
  // Emitted with the live selection as the thumbs move (the picker holds it and
  // submits the wire on Apply).
  onchange: (next: StudyWindow) => void;
  // Emitted when the user clicks "reset to project window" (the picker clears
  // `?period`, falling back to the window).
  onreset: () => void;
}
let {
  min,
  max,
  selection,
  window: projectWindow,
  coverage,
  vintageYear,
  subAnnualPeriod,
  hasSelection,
  userChosen,
  onchange,
  onreset,
}: Props = $props();

// The live thumb values, owned/clamped/re-synced by the DualThumbTrack primitive
// (seeded + re-synced from the `selection` prop) and bound back here so the
// geometry + deviation derivations below react to them. The initial value is a
// one-shot placeholder (`untrack` — the primitive owns the real seed via `bind:`).
let from = $state(untrack(() => selection.from));
let to = $state(untrack(() => selection.to));

// EMIT-PER-TICK (#615): unlike the header's commit-on-release, this slider emits
// the live selection on EVERY input tick — the picker holds it and submits the
// wire on Apply, so there's no per-tick store cost here. The primitive has
// already updated the clamped `from`/`to` when `onLiveInput` fires.
function onLiveInput(): void {
  onchange({ from, to });
}

// ── Track geometry (left/width % of the [min,max] span) ──────────────────────
const span = $derived(max - min || 1);
// A YEAR is a unit, not a point: render the [year, year+1) cell so a single-year
// selection has visible width and the right edge reaches `max`'s far side.
function leftPct(year: number): number {
  return ((year - min) / (span + 1)) * 100;
}
function widthPct(fromYear: number, toYear: number): number {
  return ((toYear - fromYear + 1) / (span + 1)) * 100;
}

// The selected span fill.
const fillLeft = $derived(leftPct(from));
const fillWidth = $derived(widthPct(from, to));

// The EFFECTIVE coverage for the band + gaps: an open END ("still delivered")
// projects to the catalog VINTAGE (#631) — the catalog only knows delivery up to
// its vintage, so the band stops there and a selection past it gaps as "not
// delivered after <vintage>", instead of reading as covered to the track edge.
// Falls back to `max` when no `vintageYear` is supplied (standalone callers). The
// open START is left open (null → band runs to `min`, never gaps). The READOUT
// below keeps the RAW `coverage` so the open end still shows an ellipsis, not a
// year.
const effectiveCoverage = $derived<Coverage | null>(
  coverage === null
    ? null
    : { from: coverage.from, to: coverage.to ?? vintageYear ?? max },
);

// The resolved band edges (#671) — the single source both the solid coverage band
// and its greyed `unavailable` complement read, so they can never drift (Fix E).
// Open sides resolve to the track edges (open start → `min`, open end → `max`,
// after `effectiveCoverage` has already vintage-projected a finite open end).
// Fix D (defensive): an INVERTED effective coverage (`from > to` — e.g. a register
// first delivered 2025 on a 2024-vintage catalog → effectiveCoverage {2025, 2024})
// would give the band a negative width and make the unavailable regions overlap /
// the hard-clamp degenerate. Treat it as "no band" (null) so the geometry degrades
// cleanly to no-band / no-clamp rather than emitting negative-width cells.
const bandEdges = $derived.by<{ from: number; to: number } | null>(() => {
  if (effectiveCoverage === null) {
    return null;
  }
  const bandFrom = effectiveCoverage.from ?? min;
  const bandTo = effectiveCoverage.to ?? max;
  return bandFrom > bandTo ? null : { from: bandFrom, to: bandTo };
});

// The coverage (available-data) band — a solid track segment; the not-delivered
// gaps draw OVER the selection fill as greyed cells. An open start extends the
// band to the track edge (`min`); the open end stops at the vintage ceiling (via
// `bandEdges`).
const coverageBand = $derived(
  bandEdges === null
    ? null
    : {
        left: leftPct(bandEdges.from),
        width: widthPct(bandEdges.from, bandEdges.to),
      },
);

// The UNAVAILABLE regions (#671): the track spans OUTSIDE the coverage band —
// left of the band start, right of the band end — rendered ALWAYS (no drag), as
// greyed NON-SELECTABLE cells so a variable's true data coverage reads up front.
// The thumbs are hard-clamped to coverage (`selectableMin/Max` below), so the
// default selection never reaches these regions; they're a passive "no data here"
// backdrop, NOT a "your selection is bad" warning (that distinction is how this
// supersedes the #639 gap-suppression without resurrecting its alarm — see `gaps`).
// Skipped while a sub-annual `?period` projects the window (the band itself is then
// just an FYI; nothing is selectable on this slider anyway). Reads the shared
// `bandEdges` so it can't drift from the solid band (Fix E) and inherits the
// inverted-coverage guard (Fix D — a null band yields no unavailable regions).
const unavailable = $derived.by(() => {
  if (bandEdges === null || subAnnualPeriod !== null) {
    return [];
  }
  const regions: { left: number; width: number }[] = [];
  if (bandEdges.from > min) {
    regions.push({
      left: leftPct(min),
      width: widthPct(min, bandEdges.from - 1),
    });
  }
  if (bandEdges.to < max) {
    regions.push({
      left: leftPct(bandEdges.to + 1),
      width: widthPct(bandEdges.to + 1, max),
    });
  }
  return regions;
});

// The not-delivered gaps of the ACTIVE selection (fires relative to the active
// selection, per the spec), as greyed cells over the fill — against the vintage-
// projected `effectiveCoverage`, so a selection past an open-ended coverage's
// vintage ceiling gaps too (#631). With the thumbs hard-clamped to coverage
// (#671), the DEFAULT seed never overruns coverage, so this fires only for an
// explicit out-of-coverage `?period` the URL carries (the consumer's source of
// truth, rendered honestly past the clamp). SUPPRESSED for a sub-annual `?period`:
// the shown span is then the window PROJECTION, not the real selection, so a gap
// against it is meaningless (a genuinely-covered `HT2020` would otherwise show
// unrelated "Not delivered" warnings) — the sub-annual cue already points the
// user at the real value (Codex P2). Also SUPPRESSED on the no-op full-history
// default (`!hasSelection`): no `?period`/window means the thumbs sit at the seed
// the user never chose, so a residual gap there would flag a span they never
// selected (#639 — the up-front `unavailable` band carries the "no data" message
// instead, without the selection-gap alarm).
const gaps = $derived(
  subAnnualPeriod !== null || !hasSelection
    ? []
    : notDeliveredGaps({ from, to }, effectiveCoverage).map((g) => ({
        left: leftPct(g.from),
        width: widthPct(g.from, g.to),
        from: g.from,
        to: g.to,
      })),
);

// ── Deviation states ─────────────────────────────────────────────────────────
// USER deviation: a user-CHOSEN selection different from the project window. Only
// meaningful WHEN a window is set (no window → nothing to deviate from) AND the
// shown span IS the active selection — for a sub-annual `?period` the span is
// the window PROJECTION, so `same==window` here is an artefact, not a real
// "no deviation"; the sub-annual cue below speaks for that case instead.
//
// Fix B suppression, refined (Fix 3): the default coverage-clamped seed must not
// fire a spurious hint — but ONLY when that seed is a true NARROWING, i.e. it sits
// WITHIN the window (`seedWithinWindow`). The common window∩coverage case (window
// 2000–2010 narrowed to coverage 2000–2008) is within-window: the data
// constrained it, not the user, so the hint (and its bare-window reset, which
// would re-render the not-delivered gap #671 avoids) stays silent until the user
// picks an explicit `?period` or drags a thumb. But when the window and coverage
// are DISJOINT the seed SNAPS to the nearest coverage edge, landing OUTSIDE the
// window (e.g. window 2012–2018, coverage 1995–2008 → seed 2008): that mismatch
// IS worth reporting, so the hint fires even on the default seed. Hence: a
// user-chosen deviation always shows; a default-seed deviation shows only when the
// seed falls outside the window.
const seedWithinWindow = $derived(
  projectWindow !== null &&
    from >= projectWindow.from &&
    to <= projectWindow.to,
);
const userDeviation = $derived(
  projectWindow !== null &&
    subAnnualPeriod === null &&
    !sameYearWindow({ from, to }, projectWindow) &&
    (userChosen || !seedWithinWindow),
);

// AVAILABILITY deviation: coverage doesn't cover the active selection. SOFTENED
// (advisory, not amber) when no window is set — the spec's "softened when no
// window is set". `hasGaps` drives both the greyed track and the note; it is
// empty for a sub-annual `?period` (gaps are suppressed at the source above).
const hasGaps = $derived(gaps.length > 0);
const availabilitySoft = $derived(hasGaps && projectWindow === null);

/** The human note for a not-delivered gap relative to the EFFECTIVE coverage
 * (before / after the band, or both) — so an open-ended coverage's "after" note
 * names the VINTAGE ceiling (`effectiveCoverage.to`), not the raw open end (#631).
 * `effectiveCoverage` is non-null whenever `hasGaps`, and a gap can only fire
 * against a FINITE side (after vintage-projection the end is always finite), so a
 * `before`/`after` note implies that side is non-null. */
const availabilityNote = $derived.by(() => {
  if (!hasGaps || effectiveCoverage === null) {
    return "";
  }
  const before =
    effectiveCoverage.from !== null && from < effectiveCoverage.from;
  const after = effectiveCoverage.to !== null && to > effectiveCoverage.to;
  if (before && after) {
    return `Not delivered before ${effectiveCoverage.from} or after ${effectiveCoverage.to}`;
  }
  if (before) {
    return `Not delivered before ${effectiveCoverage.from}`;
  }
  return `Not delivered after ${effectiveCoverage.to}`;
});

// "Coverage through <year>" (M21/#671): when coverage is OPEN-ended ("still
// delivered"), the raw readout shows an ellipsis — so name the catalog's delivery
// ceiling (the vintage-projected `effectiveCoverage.to`) explicitly, so the bound
// reads as an explained corpus ceiling rather than an unbounded span. For a FINITE
// coverage the `data <from>–<to>` readout already names the end, so this stays
// quiet (no redundant note). Empty when there's no coverage.
// Fix 7: gated on `bandEdges !== null` — the SAME non-inverted condition the band
// and the thumb seed use (Fix D). An INVERTED coverage (e.g. `{from:2025, to:null}`
// on a 2024 vintage → effective 2025..2024) is discarded as unusable (no band, no
// clamp), so naming a "coverage through 2024" ceiling next to "data 2025–…" would
// be a contradiction — the note stays quiet there too.
const coverageThrough = $derived(
  bandEdges !== null &&
    coverage !== null &&
    coverage.to === null &&
    effectiveCoverage?.to != null
    ? effectiveCoverage.to
    : null,
);
</script>

<div class="period-window">
  <div class="readout-row">
    <span class="readout" aria-live="polite">{from}–{to}</span>
    {#if coverage}
      <!-- An open (null) side reads as an ellipsis, not a bound — the start/end
           is unknown / still-delivered, never a literal year. -->
      <span class="coverage-readout muted"
        >data {coverage.from ?? "…"}–{coverage.to ?? "…"}</span
      >
      {#if coverageThrough !== null}
        <!-- Open-ended coverage: name the delivery ceiling so the ellipsis end
             reads as an explained corpus bound, not an unbounded span (M21). -->
        <span class="coverage-through muted"
          >coverage through {coverageThrough}</span
        >
      {/if}
    {/if}
  </div>

  <!-- The shared dual-thumb track (#632), wrapped to carry this slider's group
       role/label + sizing. `onLiveInput` emits per tick (#615) — no `onCommit`
       (this slider doesn't commit on release). The coverage band / fill / gap
       cells are this slider's own in-track decoration (year-as-cell geometry),
       drawn behind the primitive's thumbs via the children snippet. -->
  <div class="track-host" role="group" aria-label="Period window (years)">
    <DualThumbTrack
      {min}
      {max}
      selectableMin={bandEdges?.from ?? min}
      selectableMax={bandEdges?.to ?? max}
      {selection}
      bind:from
      bind:to
      {onLiveInput}
    >
      <!-- Unavailable (no-data) regions outside coverage: greyed NON-SELECTABLE
           backdrop, shown UP FRONT (#671). The thumbs can't enter them (hard
           clamp), so they read as "no data here", not a selection warning. -->
      {#each unavailable as region (`${region.left}:${region.width}`)}
        <div
          class="unavailable"
          style="left: {region.left}%; width: {region.width}%;"
        ></div>
      {/each}
      <!-- The coverage (available-data) band: a solid segment of the rail. -->
      {#if coverageBand}
        <div
          class="coverage"
          style="left: {coverageBand.left}%; width: {coverageBand.width}%;"
        ></div>
      {/if}
      <!-- The selected span fill. -->
      <div class="fill" style="left: {fillLeft}%; width: {fillWidth}%;"></div>
      <!-- Not-delivered gaps (inside the selection, outside coverage): greyed
           (amber-tinted unless softened) cells over the fill. -->
      {#each gaps as gap (`${gap.from}:${gap.to}`)}
        <div
          class="gap"
          class:soft={availabilitySoft}
          style="left: {gap.left}%; width: {gap.width}%;"
          title="Not delivered {gap.from}–{gap.to}"
        ></div>
      {/each}
    </DualThumbTrack>
  </div>

  {#if subAnnualPeriod !== null}
    <!-- The active `?period` is sub-annual (or otherwise not year-grain): the
         slider above shows the project-window default as a year projection, NOT
         the active value. Be honest about it and point at the real value, which
         the picker's "more" expander already opens to. -->
    <p class="deviation sub-annual" role="status">
      Active period <code>{subAnnualPeriod}</code> isn't year-grain — the slider shows
      the project-window default; edit it in More options.
    </p>
  {:else if userDeviation}
    <p class="deviation user" role="status">
      Deviates from project window ({projectWindow?.from}–{projectWindow?.to})
      <button type="button" class="reset" onclick={() => onreset()}>
        reset to project window
      </button>
    </p>
  {/if}

  {#if hasGaps}
    <!-- AMBER when a window frames the selection; advisory (muted) when softened
         (no window set) — an FYI, not a deviation from anything. -->
    <p class="deviation availability" class:soft={availabilitySoft} role="status">
      {availabilityNote}
    </p>
  {/if}
</div>

<style>
  .period-window {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    flex: 1 1 18rem;
    min-width: 0;
  }
  .readout-row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
  }
  .readout {
    font-variant-numeric: tabular-nums;
    font-weight: 600;
  }
  .coverage-readout,
  .coverage-through {
    font-size: 0.8rem;
    font-variant-numeric: tabular-nums;
  }
  /* The dual-thumb track is the shared DualThumbTrack primitive; this slider sets
     its own taller sizing (1.4rem track / 4px rail / 0.95rem thumb) via the vars,
     keeping the full-width default. The vars inherit into the primitive's
     `.track`; `.track-host` is the role/label group wrapper around it. */
  .track-host :global(.track) {
    --track-height: 1.4rem;
    --rail-height: 4px;
    --thumb-size: 0.95rem;
  }
  /* The unavailable (no-data) regions outside coverage (#671) — a faint hatched
     backdrop shown up front so the variable's true coverage is obvious before any
     interaction. Distinct from `.gap` (a selection-vs-coverage deviation): this is
     a passive "no data here", neutral grey, never amber. */
  .unavailable {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 8px;
    border-radius: 2px;
    background: repeating-linear-gradient(
      45deg,
      var(--muted) 0,
      var(--muted) 2px,
      transparent 2px,
      transparent 5px
    );
    opacity: 0.35;
  }
  /* The available-data band — a muted solid segment so the user sees where data
     actually exists relative to their selection. */
  .coverage {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 8px;
    border-radius: 4px;
    background: var(--border);
    opacity: 0.7;
  }
  .fill {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 4px;
    border-radius: 4px;
    background: var(--accent);
  }
  /* Not-delivered gap inside the selection: greyed (amber-tinted) cell. The
     `soft` variant (no window set) drops the amber for a plain grey FYI. */
  .gap {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    height: 8px;
    border-radius: 2px;
    background: repeating-linear-gradient(
      45deg,
      #d9a441 0,
      #d9a441 3px,
      transparent 3px,
      transparent 6px
    );
    opacity: 0.75;
  }
  .gap.soft {
    background: repeating-linear-gradient(
      45deg,
      var(--muted) 0,
      var(--muted) 3px,
      transparent 3px,
      transparent 6px
    );
    opacity: 0.55;
  }
  .deviation {
    margin: 0;
    font-size: 0.8rem;
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .deviation.user,
  .deviation.availability,
  .deviation.sub-annual {
    color: #92600a; /* amber — matches the picker's advisory tone */
  }
  .deviation.sub-annual code {
    font-size: 0.95em;
  }
  .deviation.availability.soft {
    color: var(--muted); /* softened: advisory FYI, not amber */
  }
  .reset {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.1rem 0.45rem;
    border: 1px solid currentColor;
    border-radius: 999px;
    background: none;
    color: inherit;
    cursor: pointer;
  }
  .reset:hover {
    background: var(--surface);
  }
</style>
