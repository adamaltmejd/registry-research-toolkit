<script lang="ts">
import PeriodWindowSlider from "./PeriodWindowSlider.svelte";
import {
  type Coverage,
  clampYearPeriodWire,
  clampYearWindow,
  coverageBandEdges,
  intersectCoverageWindow,
  resolveYearEntry,
  sameYearWindow,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";
import type { StudyWindow } from "./project_data";
import { Button } from "./ui";

// WINDOW-ANCHORED period selection (#615): a year-grain availability slider
// seeded from the project window and the subject's data-coverage track. The
// server-side `?period` wire grammar still accepts richer values (terms,
// `_default`, comma lists), but this UI no longer authors them. A non-year active
// value renders as read-only text with Clear and is never silently rewritten.
//
// Two authoring paths, ONE pending selection (Y-16). The slider's thumbs cannot
// express a range whose bounds must move THROUGH each other — DualThumbTrack's
// non-crossing clamp means 2019..2020 → 2022 only works if the To thumb moves
// first, and the other order silently submits 2020..2022. The exact YEAR FIELDS
// beside it read both bounds together, so a single year or a range lands in one
// Apply whichever bound the user types first, and an entry that doesn't parse,
// crosses, or falls outside the selectable years is REFUSED with the reason
// instead of being clamped into a different requested range. Both paths write the
// same `pending` window, which the slider (and its aria-live readout) shows, and
// Apply submits that DISPLAYED selection — so a range authored against selectable
// years that have since moved re-seeds instead of riding out behind the screen.
let {
  period,
  window = null,
  coverage = null,
  windowMinYear = 1960,
  vintageYear = undefined,
  enforcePeriodBounds = false,
  onsubmit,
  onclear,
}: {
  /** The active `?period` from the URL (null = full history). */
  period: string | null;
  /** The global project window (#614), or null = none set. */
  window?: StudyWindow | null;
  /** The subject's data-availability span, or null = unknown. */
  coverage?: Coverage | null;
  /** The outer floor for the year slider; steward deployments may narrow it. */
  windowMinYear?: number;
  /** The catalog vintage year (#631), used as the open-ended coverage ceiling. */
  vintageYear?: number;
  /** Treat the outer bounds as hard steward limits rather than global fallbacks. */
  enforcePeriodBounds?: boolean;
  /** Emitted with the chosen wire value on submit. */
  onsubmit: (period: string) => void;
  /** Emitted when the clear button is pressed (drop `?period`). */
  onclear: () => void;
} = $props();

/** Instance-scoped ids so the year fields keep real `<label for>` pairs even with
 * more than one picker mounted. */
const uid = $props.id();

// The slider's open-ended ceiling (#631). Undefined only before `/api/context`
// resolves; fall back to wall-clock so a pre-context leaf still renders.
const ceilingYear = $derived(vintageYear ?? new Date().getFullYear());
const boundedPeriod = $derived(
  enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, ceilingYear)
    : period,
);

/** Subject coverage clipped to hard steward bounds only. The global 1960 floor is
 * a fallback for unknown/empty cases, so real pre-1960 coverage must still widen
 * the track in non-steward deployments. */
const boundedCoverage = $derived.by<Coverage | null>(() => {
  if (coverage === null) {
    return null;
  }
  const bounded = {
    from:
      coverage.from === null
        ? null
        : enforcePeriodBounds
          ? Math.max(coverage.from, windowMinYear)
          : coverage.from,
    to:
      coverage.to === null
        ? null
        : enforcePeriodBounds
          ? Math.min(coverage.to, ceilingYear)
          : coverage.to,
  };
  if (enforcePeriodBounds) {
    const boundedFrom = bounded.from ?? windowMinYear;
    const boundedTo = bounded.to ?? ceilingYear;
    if (boundedFrom > boundedTo) {
      return null;
    }
  }
  return bounded;
});

const periodWindow = $derived<StudyWindow | null>(
  yearWindowFromWire(boundedPeriod),
);

const boundedWindow = $derived<StudyWindow | null>(
  window === null || !enforcePeriodBounds
    ? window
    : clampYearWindow(window, windowMinYear, ceilingYear),
);

const boundedPeriodWindow = $derived<StudyWindow | null>(
  periodWindow === null || !enforcePeriodBounds
    ? periodWindow
    : clampYearWindow(periodWindow, windowMinYear, ceilingYear),
);

/** The year window the slider treats as the active selection: a
 * year-representable `?period` wins; else the project window; else null. */
const activeYearSelection = $derived<StudyWindow | null>(
  boundedPeriodWindow ?? boundedWindow,
);

/** The active `?period` wire when it is set but NOT year-representable. */
const subAnnualPeriod = $derived<string | null>(
  boundedPeriod !== null && !yearWindowRepresentable(boundedPeriod)
    ? boundedPeriod
    : null,
);

/** Bounds that fit what is drawn: project window, active selection, and coverage. */
const sliderBounds = $derived.by(() => {
  const years: number[] = [];
  for (const w of [boundedWindow, activeYearSelection]) {
    if (w) {
      years.push(w.from, w.to);
    }
  }
  if (boundedCoverage) {
    if (boundedCoverage.from !== null) {
      years.push(boundedCoverage.from);
    }
    years.push(boundedCoverage.to ?? ceilingYear);
  }
  const max = years.length > 0 ? Math.max(...years) : ceilingYear;
  const min = Math.min(windowMinYear, ...years, max);
  return { min, max };
});

/** Thumb seed precedence (#671): explicit year `?period` > window∩coverage >
 * coverage > window > full bounds. */
const seededSelection = $derived<StudyWindow>(
  boundedPeriodWindow ??
    intersectCoverageWindow(
      boundedCoverage,
      boundedWindow,
      sliderBounds.min,
      ceilingYear,
    ),
);

/** The subject's coverage band over this track — the span the slider passes to
 * the thumbs' hard clamp (#671), null when there is no coverage or it inverts. */
const coverageBand = $derived(
  coverageBandEdges(
    boundedCoverage,
    sliderBounds.min,
    sliderBounds.max,
    ceilingYear,
  ),
);

/** The years a selection may name: that band, else the full rendered track. ONE
 * legal span behind every authoring path — the thumbs' hard clamp (#671), the
 * exact fields' refusal, and the re-arm below — is what preserves the coverage /
 * steward-bound / vintage constraints. The fields are a second way to author the
 * same selection, not a way around it. */
const selectableYears = $derived<StudyWindow>(
  coverageBand ?? { from: sliderBounds.min, to: sliderBounds.max },
);

// The picker's PENDING selection, written by a thumb drag AND by a valid exact
// year entry (null until the user changes something, so an untouched Apply still
// submits the seeded default). Re-armed by the effect below; read for the wire
// only through `sliderSelection`, so Apply can never send a value the user was
// not shown.
let pending = $state<StudyWindow | null>(null);

/** The clamped selection shown to the user — the readout, the thumbs and the
 * fields all follow it, and Apply submits it: the pending value while one is
 * live, else the seeded default. */
const sliderSelection = $derived<StudyWindow>(
  clampYearWindow(
    pending ?? seededSelection,
    sliderBounds.min,
    sliderBounds.max,
  ),
);

/** Whether the slider shows a real user-meaningful selection. */
const hasSliderSelection = $derived(
  activeYearSelection !== null || pending !== null,
);

/** Whether the user has actually chosen a year-window value distinct from the
 * project window. The untouched coverage-clamped default seed is not a user
 * deviation. */
const userChosen = $derived(periodWindow !== null || pending !== null);

/** What the thumbs and the fields are currently armed against, kept as a VALUE.
 * The inputs are all props, but two of them are rebuilt as fresh objects by
 * consumers that recompute for unrelated reasons — the leaf's
 * `coverageFromStates(node.states)`, the group's `unionCoverage` over its
 * selectable bands, a window-store write of the same span. Those hand this
 * component new identities without moving anything on screen, so an identity test
 * would re-arm and silently wipe a year half-typed. Plain, not `$state`: nothing
 * renders from it. */
let armedSeed: {
  period: string | null;
  active: StudyWindow | null;
  ceiling: number;
  seed: StudyWindow;
  selectable: StudyWindow;
} | null = null;

$effect(() => {
  // Re-arm the pending buffer and the year fields when the URL, window, coverage
  // or ceiling actually MOVES the seeded selection or the years a selection may
  // name — and only then. This prevents a stale dragged/typed value from
  // surviving a re-seed and being submitted instead of the newly displayed
  // selection, without letting a re-render that changes nothing throw away work
  // in progress.
  //
  // The SELECTABLE span is part of that key because it moves on its own: coverage
  // narrowing by a year, or hard steward bounds arriving (#1037), leaves the URL,
  // the window, the ceiling and the seed all equal while the years the user may
  // pick shift under an already-authored value. That value is no longer authorable
  // — and clamping it into the new span would submit a range nobody asked for,
  // the silent clamp this control exists to remove — so it re-seeds.
  const next = {
    period,
    active: activeYearSelection,
    ceiling: ceilingYear,
    seed: seededSelection,
    selectable: selectableYears,
  };
  if (
    armedSeed !== null &&
    armedSeed.period === next.period &&
    armedSeed.ceiling === next.ceiling &&
    sameYearWindow(armedSeed.active, next.active) &&
    sameYearWindow(armedSeed.seed, next.seed) &&
    sameYearWindow(armedSeed.selectable, next.selectable)
  ) {
    return;
  }
  armedSeed = next;
  pending = null;
  entry = null;
  entryCommitted = false;
});

// ── Exact year entry (Y-16) ──────────────────────────────────────────────────

/** The raw text of the two year fields, or null while they MIRROR the pending
 * selection. Kept as text so a refused entry stays on screen exactly as typed
 * rather than being rewritten into some other range. */
let entry = $state<{ from: string; to: string } | null>(null);
/** Whether the entry has been committed (blur, Enter, or Apply) — a half-typed
 * year must not announce a refusal on every keystroke. */
let entryCommitted = $state(false);

const entryFrom = $derived(entry?.from ?? String(sliderSelection.from));
const entryTo = $derived(entry?.to ?? String(sliderSelection.to));

/** The entry resolved: the year window it names, or why it names none — with the
 * field(s) that refusal is about, so the hairline marks the year at fault rather
 * than both. Null while the fields still mirror the pending selection. The
 * shared `resolveYearEntry` (period.ts) is passed this picker's selectable band,
 * so a four-digit year outside it reports as out of range rather than badly
 * typed — `SourceEditor`'s cart card calls the same resolver without one. */
const entryResolution = $derived(
  resolveYearEntry(entry?.from ?? null, entry?.to ?? null, {
    selectableYears,
    yearRule: `four-digit year, like ${selectableYears.from}.`,
  }),
);

// The two arms of that union, so the narrowing is spelled once for the three
// places that read it.
const entryProblem = $derived(
  entryResolution !== null && "problem" in entryResolution
    ? entryResolution
    : null,
);
const entryWindow = $derived(
  entryResolution !== null && "years" in entryResolution
    ? entryResolution.years
    : null,
);

/** The refusal to show, once the entry has been committed. */
const entryRefusal = $derived(entryCommitted ? entryProblem : null);
/** The refusal line is only described-by while it actually says something. */
const problemId = $derived(
  entryRefusal === null ? undefined : `${uid}-problem`,
);

function editEntry(side: "from" | "to", value: string): void {
  entry =
    side === "from"
      ? { from: value, to: entryTo }
      : { from: entryFrom, to: value };
  // A valid entry becomes the pending selection at once, so the slider and its
  // readout show the range Apply would submit. A refused one leaves the pending
  // selection alone — nothing is applied behind the user's back.
  if (entryWindow !== null) {
    pending = entryWindow;
  }
}

function apply(): void {
  if (entryProblem !== null) {
    // Explain the refusal instead of submitting some other range.
    entryCommitted = true;
    return;
  }
  if (pending === null) {
    // A token/list/default active period is valid URL state but not represented
    // by the year slider. Rendering it must not rewrite the URL just because the
    // user accepts the fallback slider projection; only an actual thumb move or
    // exact entry replaces it with a year-window wire.
    if (subAnnualPeriod !== null) {
      return;
    }
    // Nothing to narrow to at all: no selection, no window, no usable coverage.
    if (
      activeYearSelection === null &&
      window === null &&
      coverageBand === null
    ) {
      return;
    }
  }
  // Submit the DISPLAYED selection, never the raw buffer behind it. The readout,
  // the thumbs and the fields all render `sliderSelection`, so the wire is the
  // range the user was looking at — a bound the current track has moved cannot
  // ride out under a stale `pending`.
  onsubmit(yearWindowToWire(sliderSelection));
}

function resetToWindow(): void {
  if (boundedWindow !== null) {
    onsubmit(yearWindowToWire(boundedWindow));
  } else {
    onclear();
  }
}

function submit(event: SubmitEvent): void {
  event.preventDefault();
  apply();
}
</script>

<form class="period-picker" onsubmit={submit}>
  <div class="head">
    <span class="title micro-label" id="period-label">Period</span>
  </div>

  <div class="slider-row">
    <!-- NOTHING in this card is remounted when the applied `?period` changes
         (Y-65). The thumbs are a CONTROLLED buffer — DualThumbTrack re-seeds
         `from`/`to` from `selection` whenever it moves — so the `{#key period}`
         that used to wrap them re-seeded what was already re-seeding itself, and
         cost the keyboard focus of whoever pressed Enter on a thumb to apply. The
         exact fields were never inside it for the same reason in reverse: they
         hold their own state, which the effect above re-arms deliberately. -->
    <PeriodWindowSlider
      min={sliderBounds.min}
      max={sliderBounds.max}
      selection={sliderSelection}
      window={boundedWindow}
      coverage={boundedCoverage}
      vintageYear={ceilingYear}
      {subAnnualPeriod}
      hasSelection={hasSliderSelection}
      {userChosen}
      onchange={(next) => {
        pending = next;
        // The thumbs are now the pending selection, so the fields go back to
        // mirroring it (and drop any refusal they were carrying).
        entry = null;
        entryCommitted = false;
      }}
      onreset={() => resetToWindow()}
    />
    <div class="exact" role="group" aria-label="Exact years">
      <label class="micro-label" for="{uid}-from">From</label>
      <input
        id="{uid}-from"
        class="year"
        type="text"
        inputmode="numeric"
        autocomplete="off"
        value={entryFrom}
        aria-invalid={entryRefusal?.at.from === true}
        aria-describedby={problemId}
        oninput={(event) => editEntry("from", event.currentTarget.value)}
        onchange={() => (entryCommitted = true)}
      />
      <label class="micro-label" for="{uid}-to">To</label>
      <input
        id="{uid}-to"
        class="year"
        type="text"
        inputmode="numeric"
        autocomplete="off"
        value={entryTo}
        aria-invalid={entryRefusal?.at.to === true}
        aria-describedby={problemId}
        oninput={(event) => editEntry("to", event.currentTarget.value)}
        onchange={() => (entryCommitted = true)}
      />
    </div>
    <div class="actions">
      <Button type="submit" size="sm" aria-label="Apply period">
        Apply
      </Button>
      {#if period !== null}
        <Button type="button" size="sm" onclick={() => onclear()}>
          Clear
        </Button>
      {/if}
    </div>
  </div>

  <!-- The refusal line. Rendered ALWAYS, so a refusal lands in a live region
       that already existed rather than one that appears with it (empty, it
       holds no line box — just its own top margin). Plain err-role text with a
       leading glyph, matching the slider's own advisory lines directly above it
       rather than minting a banner inside the card. -->
  <p
    class="problem"
    class:refused={entryRefusal !== null}
    id="{uid}-problem"
    role="status"
  >
    {#if entryRefusal !== null}
      <span aria-hidden="true">✕</span>
      {entryRefusal.problem}
    {/if}
  </p>
</form>

<style>
  .period-picker {
    margin: 1.25rem 0;
    padding: var(--space-3) 0.9rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
  }
  .head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: 0.45rem;
  }
  .slider-row {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: var(--space-3);
  }
  .exact {
    display: flex;
    align-items: center;
    gap: var(--space-2);
  }
  .exact label {
    white-space: nowrap;
  }
  .actions {
    display: flex;
    gap: var(--space-2);
  }
  /* The shared `.year`/`.problem`/`.problem.refused` rules live in
     ui/utilities.css (Y-100) — this card's own top margin, spacing it under the
     slider row, is the only local part (SourceEditor's copy sits in a grid whose
     own row-gap already spaces it, so it sets none). */
  .problem {
    margin: var(--space-2) 0 0;
  }
</style>
