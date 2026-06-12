<script lang="ts">
import { untrack } from "svelte";
import PeriodRangeInput from "./PeriodRangeInput.svelte";
import {
  PERIOD_GRAINS,
  type PeriodGrain,
  periodRangeEndpoints,
  periodTokenBounds,
} from "./period";

// The #338 interrupted-series input: a LIST of period segments (each a single
// token or uniform-grain range, picked with the shared grain-aware
// PeriodRangeInput) serialized as the #307 comma-joined wire
// (`2005..2010,2015..2020`). A thin wrapper — the range input owns the grain
// UX; this only accumulates segments. Segments are kept sorted by their start
// (the schema's ascending-order list rule), so pick order never produces a
// guaranteed `invalid_period`; OVERLAP stays the server's call (merging
// adjacent/overlapping segments has semantics this advisory layer shouldn't
// guess at).
//
// Emit contract (mirrors PeriodRangeInput): the comma-joined wire for the
// current list, or null while the list is EMPTY — the surface decides what
// that means (the editor shows its amber "incomplete" hint).
//
// Local state is seeded ONCE at mount from `value` (the PeriodEditor
// doctrine); the picker controls KEEP their values after Add — adjusting one
// endpoint for the next segment beats re-picking the grain every time —
// with Add disabled until the controls re-emit (no double-click duplicates).
const {
  value = null,
  grains = PERIOD_GRAINS,
  onchange,
}: {
  /** Comma-joined wire to seed the list from; null/blank seeds empty. */
  value?: string | null;
  /** Grains offered by the embedded range input. */
  grains?: PeriodGrain[];
  /** The comma-joined wire for the current list, or null when it's empty. */
  onchange: (wire: string | null) => void;
} = $props();

let segments = $state<string[]>(
  untrack(() => {
    const wire = (value ?? "").trim();
    return wire === ""
      ? []
      : wire
          .split(",")
          .map((s) => s.trim())
          .filter((s) => s !== "");
  }),
);

/** The range input's latest emit — the candidate next segment. Cleared on
 * Add (the controls keep their values; the next emit re-arms the button). */
let pending = $state<string | null>(null);

/** ADVISORY ascending-sort key: the ISO start day of the segment's FROM
 * endpoint. A non-grammar segment (possible only via a seeded stored value —
 * the range input emits grammar wires) falls back to its own text, which just
 * sorts it imperfectly; the server stays the canonical list-order authority. */
function sortKey(segment: string): string {
  const from = periodRangeEndpoints(segment)?.[0] ?? segment;
  return periodTokenBounds(from)?.from ?? segment;
}

function emit(): void {
  onchange(segments.length === 0 ? null : segments.join(","));
}

function add(): void {
  if (pending === null) {
    return;
  }
  segments = [...segments, pending].sort((a, b) =>
    sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0,
  );
  pending = null;
  emit();
}

function remove(index: number): void {
  segments = segments.filter((_, i) => i !== index);
  emit();
}
</script>

<div class="list-input">
  {#if segments.length > 0}
    <ul class="segments">
      {#each segments as segment, i (`${i}:${segment}`)}
        <li>
          <code>{segment}</code>
          <button
            type="button"
            class="remove"
            aria-label="Remove {segment}"
            onclick={() => remove(i)}
          >
            ✕
          </button>
        </li>
      {/each}
    </ul>
  {/if}
  <div class="picker">
    <PeriodRangeInput value={null} {grains} onchange={(w) => (pending = w)} />
    <button
      type="button"
      class="add"
      disabled={pending === null}
      onclick={add}
    >
      Add segment
    </button>
  </div>
</div>

<style>
  .list-input {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .segments {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .segments li {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.15rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    font-size: 0.8rem;
    background: var(--surface);
  }
  .segments code {
    font-size: 0.95em;
  }
  .remove {
    font: inherit;
    font-size: 0.75rem;
    line-height: 1;
    padding: 0.1rem 0.2rem;
    border: none;
    background: none;
    color: var(--accent);
    cursor: pointer;
  }
  .picker {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 0.75rem;
  }
  .add {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.3rem 0.7rem;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--surface);
    color: var(--accent);
    cursor: pointer;
  }
  .add:disabled {
    border-color: var(--border);
    color: var(--border);
    cursor: default;
  }
</style>
