<script lang="ts">
// The shared DUAL-THUMB-OVER-A-TRACK primitive (#632) behind both year sliders
// — YearWindowSlider (the header's project-window control, #614/#629) and
// PeriodWindowSlider (the subject page's availability-aware period control,
// #615/#631). Both are two overlaid native `<input type="range">` thumbs (one
// "from", one "to") clamped so they can't cross; this owns that mechanic ONCE so
// a fix to it (a coincident-thumb z-index trick, a browser thumb-rendering tweak)
// lands in both at once.
//
// What's SHARED here (the primitive): the clamp + non-crossing onFrom/onTo, the
// controlled `from`/`to` buffer (seeded from `selection`, re-synced on prop
// change — the controlled-component doctrine), the two thumbs with their real
// `slider` ARIA roles + keyboard support, and the overlaid thumb/rail CSS (native
// rail hidden so the consumer's own track decoration shows through).
//
// What stays in the CONSUMER: everything the two sliders DIVERGE on — the track
// geometry (YearWindowSlider's plain max−min vs PeriodWindowSlider's year-as-cell
// model), the visual decoration (fill / coverage band / not-delivered gaps), the
// readout + deviation hints, and the emit policy (see below). The consumer draws
// its decoration via the `children` snippet (rendered inside the track, BEHIND
// the thumbs) and reads the live thumbs via `bind:from` / `bind:to`.
//
// DIVERGENT EMIT SEMANTICS — the primitive serves BOTH without baking in either:
//   • `onLiveInput` fires on every native `input` tick (smooth drag). The bound
//     `from`/`to` are already updated to the clamped live values when it runs.
//     PeriodWindowSlider wires this (it emits into the picker's apply-buffer per
//     tick).
//   • `onCommit` fires on the native `change` event (pointer release / keyboard
//     commit) only. YearWindowSlider wires this as its COMMIT (one store write
//     per drag, not one per tick — #629 item 2).
// A consumer wires the one(s) it needs; both are optional. The primitive never
// decides what a change MEANS — it only reports the two native signals.
//
// SIZING is CSS-var driven so each consumer keeps its exact pixels (the geometry
// must not unify): `--track-height`, `--rail-height`, `--thumb-size` on the host.

import type { Snippet } from "svelte";
import { untrack } from "svelte";

interface Props {
  // The thumb bounds (inclusive years). `step` is 1 (year grain). These stay the
  // FULL track geometry — the native `<input min/max>` is kept at [min, max] so
  // the browser's value→pixel mapping matches the consumer's [min, max] track.
  min: number;
  max: number;
  // OPTIONAL hard-selectable sub-range (#671): the thumbs are CLAMPED to
  // [selectableMin, selectableMax] in the handlers AND the controlled re-sync, so
  // a thumb can't enter the region outside it by drag OR keyboard — while the
  // native input keeps the full [min, max] range (narrowing the input's own
  // min/max would desync the native value→pixel mapping from the track). Default
  // to the full bounds, so a consumer that doesn't pass them (YearWindowSlider) is
  // unchanged. PeriodWindowSlider passes the coverage edges to lock dragging into
  // the delivered span.
  selectableMin?: number;
  selectableMax?: number;
  // The selection to seed the thumbs from. The buffer re-syncs to this whenever
  // it (or the bounds) change — a controlled component: the consumer's prop is
  // the source of truth, `from`/`to` is a smooth-drag buffer over it.
  selection: { from: number; to: number };
  // The live thumb values, exposed so the consumer can drive its own geometry +
  // readout + deviation off them reactively (no mirror). Bindable; the primitive
  // owns the writes (seed, re-sync, clamp).
  from?: number;
  to?: number;
  // Fired on every native `input` tick (live drag) — `from`/`to` already hold the
  // clamped live values. The consumer that emits per-tick (PeriodWindowSlider)
  // wires this.
  onLiveInput?: () => void;
  // Fired on the native `change` event (release / keyboard commit) only. The
  // consumer that commits on release (YearWindowSlider) wires this.
  onCommit?: () => void;
  // The in-track decoration (fill / coverage band / gap cells), rendered BEHIND
  // the thumbs so clicks still reach a thumb. The consumer owns its geometry.
  children?: Snippet;
}
let {
  min,
  max,
  selectableMin,
  selectableMax,
  selection,
  from = $bindable(),
  to = $bindable(),
  onLiveInput,
  onCommit,
  children,
}: Props = $props();

function clamp(year: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, year));
}

// The hard-selectable range for USER input (#671): the optional [selectableMin,
// selectableMax] when supplied, else the full bounds (the unconstrained default).
// USER writes (drag/keyboard via the handlers) clamp to THIS, so a thumb can't
// enter a not-selectable region by drag OR keyboard (a keyboard step fires
// `oninput` → the same handler). The SEED / controlled re-sync clamp only to the
// full [min, max] bounds, NOT this range: the consumer's `selection` prop is the
// source of truth, and an explicit out-of-range value it sets (e.g. an URL
// `?period` outside coverage) must still render honestly — the consumer flags it
// (PeriodWindowSlider's not-delivered gap); only the user's own moves are barred
// from the region. The native input min/max stay [min, max] (see Props) so the
// geometry doesn't desync.
const selLo = $derived(selectableMin ?? min);
const selHi = $derived(selectableMax ?? max);

// Seed the buffer once (untracked so it's a one-shot init, not a tracked dep —
// the $effect below owns re-seeding on prop change).
from = untrack(() => clamp(selection.from, min, max));
to = untrack(() => clamp(selection.to, min, max));
// Controlled re-sync: re-seed whenever the selection (or bounds) change, so the
// thumbs follow when the value is set elsewhere. Reading the props here registers
// the dependency.
$effect(() => {
  from = clamp(selection.from, min, max);
  to = clamp(selection.to, min, max);
});

// LIVE input: update the buffer (smooth drag/keyboard step), clamped to the
// selectable range so a thumb can't enter the not-selectable region, and
// non-crossing.
function onFrom(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    selLo,
    selHi,
  );
  from = Math.min(v, to ?? selHi); // never cross past `to`
  onLiveInput?.();
}
function onTo(event: Event): void {
  const v = clamp(
    Number((event.currentTarget as HTMLInputElement).value),
    selLo,
    selHi,
  );
  to = Math.max(v, from ?? selLo); // never cross before `from`
  onLiveInput?.();
}
</script>

<div class="track">
  {@render children?.()}
  <!-- Two overlaid native range thumbs (real slider role + keyboard for free);
       the `from` thumb sits above so a coincident pair stays grabbable apart.
       `oninput` updates the LIVE buffer; `onchange` commits (consumer-policy). -->
  <input
    class="thumb thumb-from"
    type="range"
    {min}
    {max}
    step="1"
    value={from}
    aria-label="From year"
    oninput={onFrom}
    onchange={() => onCommit?.()}
  />
  <input
    class="thumb thumb-to"
    type="range"
    {min}
    {max}
    step="1"
    value={to}
    aria-label="To year"
    oninput={onTo}
    onchange={() => onCommit?.()}
  />
</div>

<style>
  /* Sizing is var-driven so each consumer keeps its own exact pixels — the width
     differs (a fixed header chip vs a fluid panel control), so does it. */
  .track {
    position: relative;
    width: var(--track-width, 100%);
    height: var(--track-height, 1.25rem);
  }
  .track::before {
    /* The base rail. */
    content: "";
    position: absolute;
    left: 0;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    height: var(--rail-height, 3px);
    border-radius: var(--rail-height, 3px);
    background: var(--border);
  }
  /* Both range inputs stack on the same track; only their thumbs are visible (the
     native rail is hidden so the consumer's decoration + our `.track::before`
     show through). `pointer-events: none` on the input lets clicks reach whichever
     thumb is under the cursor; the thumbs re-enable pointer events. */
  .thumb {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    margin: 0;
    background: none;
    pointer-events: none;
    -webkit-appearance: none;
    appearance: none;
  }
  .thumb-from {
    /* Above the `to` thumb so a coincident pair stays draggable apart. */
    z-index: 2;
  }
  .thumb::-webkit-slider-runnable-track {
    background: none;
  }
  .thumb::-moz-range-track {
    background: none;
  }
  .thumb::-webkit-slider-thumb {
    -webkit-appearance: none;
    appearance: none;
    pointer-events: auto;
    width: var(--thumb-size, 0.85rem);
    height: var(--thumb-size, 0.85rem);
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
  .thumb::-moz-range-thumb {
    pointer-events: auto;
    width: var(--thumb-size, 0.85rem);
    height: var(--thumb-size, 0.85rem);
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
    cursor: pointer;
  }
</style>
