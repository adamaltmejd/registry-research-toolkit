<script lang="ts" module>
// Per-instance control-id sequence (unique label `for`s across instances).
let _rangeSeq = 0;
</script>

<script lang="ts">
import { untrack } from "svelte";
import {
  type PeriodGrain,
  PERIOD_GRAINS,
  grainOfToken,
  periodRangeEndpoints,
} from "./period";

// The #308 range-first period input, shared by the catalog PeriodPicker and
// the editor's PeriodEditor: a grain select (year/term/quarter/month/day) +
// from/to controls AT that grain. No token grammar knowledge required — the
// wire grammar stays the SERIALIZATION (`onchange` emits the wire string), not
// the UX. Month/day use the native inputs, whose values ARE the wire tokens
// (and which make calendar-invalid days unenterable — moots #239 for UI input).
//
// Emit contract: a complete FROM emits; a blank TO means to = from (a single
// year/term/… is the one-step path). An incomplete FROM emits null — the
// surface decides what that means (the picker disables nothing and simply has
// nothing to apply; the editor shows its amber "incomplete" hint).
//
// Local state is seeded ONCE at mount from `value` (the PeriodEditor doctrine:
// stable each-block keys remount a genuinely different instance; an unrelated
// edit must not snap typed state back). A surface that wants re-seed-on-change
// keys the component on its value (the PeriodPicker does, for back/forward).
const {
  value = null,
  grains = PERIOD_GRAINS,
  onchange,
}: {
  /** Wire period to seed from (single token or a uniform-grain `a..b` range);
   * null/blank/unparseable seeds blank controls at the first grain. */
  value?: string | null;
  /** The grains offered (pre-narrowed by the caller — e.g. to those a
   * variable's states actually exhibit). Single-grain hides the select. */
  grains?: PeriodGrain[];
  /** The wire string for the current selection (single token when from = to),
   * or null while FROM is incomplete. */
  onchange: (wire: string | null) => void;
} = $props();

const uid = `pri-${_rangeSeq++}`;

/** One endpoint's controls: the year spinner (year/term/quarter grains) or
 * the native input value (month/day grains), plus the term/quarter pick. */
interface Endpoint {
  year: string;
  term: "VT" | "HT";
  quarter: "Q1" | "Q2" | "Q3" | "Q4";
  native: string; // YYYY-MM (month) / YYYY-MM-DD (day)
}

function blankEndpoint(): Endpoint {
  return { year: "", term: "VT", quarter: "Q1", native: "" };
}

/** Seed one endpoint's controls from a wire token at `grain`. */
function endpointFromToken(token: string, grain: PeriodGrain): Endpoint {
  const e = blankEndpoint();
  if (grain === "year") {
    e.year = token;
  } else if (grain === "term") {
    const m = /^([HV]T)(\d{4})$/.exec(token);
    if (m) {
      e.term = m[1] as "VT" | "HT";
      e.year = m[2];
    } else {
      // The -H1/-H2 twins (accepted on input, never emitted).
      const h = /^(\d{4})-H([12])$/.exec(token);
      if (h) {
        e.term = h[2] === "1" ? "VT" : "HT";
        e.year = h[1];
      }
    }
  } else if (grain === "quarter") {
    const m = /^(\d{4})-(Q[1-4])$/.exec(token);
    if (m) {
      e.year = m[1];
      e.quarter = m[2] as Endpoint["quarter"];
    }
  } else {
    e.native = token;
  }
  return e;
}

/** The wire token for an endpoint at the active grain, or null if incomplete. */
function tokenOf(e: Endpoint, g: PeriodGrain): string | null {
  if (g === "month" || g === "day") {
    return e.native !== "" ? e.native : null;
  }
  if (e.year.trim() === "") {
    return null;
  }
  const year = e.year.trim();
  if (g === "year") {
    return year;
  }
  return g === "term" ? `${e.term}${year}` : `${year}-${e.quarter}`;
}

// One-time seed from `value`: a single token seeds from = to at its grain; a
// uniform-grain range seeds both; anything else (blank, `_default`, mixed
// grains) seeds blank at the first offered grain.
const seed = untrack(() => {
  const wire = (value ?? "").trim();
  const endpoints = periodRangeEndpoints(wire) ?? [wire, wire];
  const gFrom = grainOfToken(endpoints[0]);
  const gTo = grainOfToken(endpoints[1]);
  if (wire !== "" && gFrom !== null && gFrom === gTo && grains.includes(gFrom)) {
    return {
      grain: gFrom,
      from: endpointFromToken(endpoints[0], gFrom),
      to: endpointFromToken(endpoints[1], gFrom),
    };
  }
  return { grain: grains[0] ?? "year", from: blankEndpoint(), to: blankEndpoint() };
});

let grain = $state<PeriodGrain>(seed.grain);
let from = $state<Endpoint>(seed.from);
let to = $state<Endpoint>(seed.to);

/** Recompute + emit the wire value (TO falls back to FROM when blank). */
function emit(): void {
  const fromTok = tokenOf(from, grain);
  if (fromTok === null) {
    onchange(null);
    return;
  }
  const toTok = tokenOf(to, grain) ?? fromTok;
  onchange(fromTok === toTok ? fromTok : `${fromTok}..${toTok}`);
}

function setGrain(next: PeriodGrain): void {
  grain = next;
  emit();
}

const GRAIN_LABELS: Record<PeriodGrain, string> = {
  year: "Year",
  term: "Term (VT/HT)",
  quarter: "Quarter",
  month: "Month",
  day: "Day",
};
</script>

{#snippet endpointControls(
  which: "from" | "to",
  e: Endpoint,
  label: string,
)}
  <div class="endpoint">
    <span class="endpoint-label" id="{uid}-{which}-label">{label}</span>
    {#if grain === "month"}
      <input
        type="month"
        aria-labelledby="{uid}-{which}-label"
        value={e.native}
        oninput={(ev) => {
          e.native = ev.currentTarget.value;
          emit();
        }}
      />
    {:else if grain === "day"}
      <input
        type="date"
        aria-labelledby="{uid}-{which}-label"
        value={e.native}
        oninput={(ev) => {
          e.native = ev.currentTarget.value;
          emit();
        }}
      />
    {:else}
      <div class="pair">
        {#if grain === "term"}
          <select
            aria-label="{label} term"
            value={e.term}
            onchange={(ev) => {
              e.term = ev.currentTarget.value as "VT" | "HT";
              emit();
            }}
          >
            <option value="VT">VT (spring)</option>
            <option value="HT">HT (autumn)</option>
          </select>
        {:else if grain === "quarter"}
          <select
            aria-label="{label} quarter"
            value={e.quarter}
            onchange={(ev) => {
              e.quarter = ev.currentTarget.value as "Q1" | "Q2" | "Q3" | "Q4";
              emit();
            }}
          >
            <option>Q1</option>
            <option>Q2</option>
            <option>Q3</option>
            <option>Q4</option>
          </select>
        {/if}
        <input
          type="number"
          class="year"
          aria-labelledby="{uid}-{which}-label"
          placeholder="e.g. 2018"
          value={e.year}
          oninput={(ev) => {
            e.year = ev.currentTarget.value;
            emit();
          }}
        />
      </div>
    {/if}
  </div>
{/snippet}

<div class="range-input">
  {#if grains.length > 1}
    <label class="grain">
      <span class="grain-label">Granularity</span>
      <select
        value={grain}
        onchange={(ev) => setGrain(ev.currentTarget.value as PeriodGrain)}
      >
        {#each grains as g (g)}
          <option value={g}>{GRAIN_LABELS[g]}</option>
        {/each}
      </select>
    </label>
  {/if}
  <div class="endpoints">
    {@render endpointControls("from", from, "From")}
    {@render endpointControls("to", to, "To")}
  </div>
  <p class="hint muted">Leave “To” blank for a single {grain}.</p>
</div>

<style>
  .range-input {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .grain {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    font-size: 0.8rem;
  }
  .grain-label {
    font-weight: 600;
  }
  .endpoints {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 0.75rem;
  }
  .endpoint {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: 0.8rem;
  }
  .pair {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .endpoint-label {
    font-weight: 600;
  }
  .endpoint input,
  .endpoint select,
  .grain select {
    font: inherit;
    padding: 0.3rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .endpoint input.year {
    width: 6.5rem;
  }
  .hint {
    margin: 0;
    font-size: 0.75rem;
  }
</style>
