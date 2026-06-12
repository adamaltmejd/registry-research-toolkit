<script lang="ts" module>
// A per-instance sequence so each PeriodEditor's radio group gets a UNIQUE name.
// Otherwise multiple editors on one page form ONE document-wide radio group, and
// selecting a mode in one source unchecks the others' radios (Codex P2).
let _instanceSeq = 0;
</script>

<script lang="ts">
import { untrack } from "svelte";
import FieldIssues from "./FieldIssues.svelte";
import { looksLikePeriod, periodFromWire, periodToWire, rangeRepresentable } from "./period";
import PeriodListInput from "./PeriodListInput.svelte";
import PeriodRangeInput from "./PeriodRangeInput.svelte";
import type { Period } from "./project_data";
import type { ValidationIssue } from "./validation";

// Editable Source.period (see reg_schema/DESIGN.md → Two layers: models vs.
// validator). RANGE-FIRST (#308, generalizing the earlier year-range-first
// directive): the DEFAULT mode is the shared grain-aware from/to picker
// (PeriodRangeInput — year/term/quarter/month/day), so picking any range is the
// path of least resistance, not free text. The other modes cover the rest:
// "List" (the #307/#338 interrupted-series mode — segments accumulated with
// the same grain-aware controls via PeriodListInput; an explicit opt-in, never
// the default), "Token" (free text for special/mixed-grain values — e.g. the
// #306 succession clips like 1992..2009-06-30 — and the raw comma list) and
// "Default" (the "_default" snapshot sentinel).
//
// Per the Pydantic boundary (see reg_webapp/DESIGN.md → Pydantic boundary), this
// NEVER validates structurally — a malformed value is echoed via the
// backend's `invalid_period` on the period pointer (the mounted <FieldIssues>).
// Emits through `periodFromWire`, so a single year is the bare int arm, a range
// is the {from,to} OBJECT (the only schema-valid range shape), and an
// incomplete selection is the unset "" (the amber incomplete hint below).
const { period, issues, onchange } = $props<{
  period: Period;
  issues: ValidationIssue[];
  onchange: (next: Period) => void;
}>();

type Mode = "range" | "list" | "token" | "default";

// Infer the initial mode from the incoming value. A value the range UI can
// REPRESENT (bare year int, "" unset, single grammar token, uniform-grain
// range — numeric or token endpoints) opens in Range; "_default" in Default;
// everything else (mixed-grain ranges, junk, malformed non-strings) falls back
// to Token raw-text so the editor never crashes OR silently blanks a value —
// the backend flags what's actually wrong.
function inferMode(value: Period): Mode {
  if (value === "_default") {
    return "default";
  }
  if (Array.isArray(value)) {
    // The #307 interrupted-series list opens in the dedicated List mode
    // (#338) when it serializes to a wire; a malformed list (a member
    // periodToWire can't shape) falls back to Token raw-text like any other
    // junk so it stays visible.
    return periodToWire(value) !== null ? "list" : "token";
  }
  const wire = periodToWire(value);
  if (wire === null) {
    // Unset ("" / blank) → the range picker (the common path); a malformed
    // non-string value also lands here via periodToWire's null — show IT as
    // Token text instead (seedTokenText renders the JSON).
    return typeof value === "string" || typeof value === "number"
      ? "range"
      : "token";
  }
  return rangeRepresentable(wire) ? "range" : "token";
}

// A unique radio-group name for THIS editor instance (fix C).
const groupName = `period-mode-${_instanceSeq++}`;

// The token field's seed: the raw string when it's a token, the `from..to`
// wire text for a {from,to} range (a mixed-grain range opens in Token mode —
// see inferMode — and must DISPLAY, not render a blank field; the #306
// succession auto-split routinely writes such ranges), blank for a
// numeric/default value, or the JSON of a malformed (non-string) value.
function seedTokenText(value: Period): string {
  if (typeof value === "string" && value !== "_default") {
    return value;
  }
  if (Array.isArray(value)) {
    // The #307 segment list — comma-joined wire text (JSON only for a
    // malformed list periodToWire can't serialize).
    return periodToWire(value) ?? JSON.stringify(value);
  }
  if (
    value != null &&
    typeof value === "object" &&
    "from" in value &&
    "to" in value
  ) {
    return `${value.from}..${value.to}`;
  }
  if (typeof value === "string" || typeof value === "number") {
    return "";
  }
  return JSON.stringify(value);
}

// Local UI state, seeded from the period ONCE at mount. `untrack` keeps the seed a
// one-time read (no reactive dep) so switching mode then typing isn't snapped back
// when an UNRELATED edit swaps the whole draft.
//
// No re-seed `$effect` is needed (issue #200 removed the #188 workaround): the
// each-blocks now key on a store-owned STABLE id, so a middle-source remove REMOUNTS
// the surviving editors on their own ids rather than rebinding a survivor's instance
// to a shifted source. A genuinely different source therefore gets a fresh mount that
// re-runs this one-time seed correctly — and an unrelated edit no longer reuses this
// instance for another source, so there is no stale-period symptom to paper over.
const initial: Period = untrack(() => period);
let mode = $state<Mode>(inferMode(initial));
/** The wire value the range picker seeds from / last emitted. Non-null at
 * mount ONLY when the incoming period is range-REPRESENTABLE: a mode switch to
 * Range from an unrenderable value (`_default`, a mixed-grain clip, junk) must
 * emit the unset "" and show the incomplete hint — never blank controls
 * silently preserving a hidden value (Codex P2). Mode switches have always
 * been edits in this editor (switching to Default emits immediately). */
const _initialWire = periodToWire(initial);
let rangeWire = $state<string | null>(
  _initialWire !== null && rangeRepresentable(_initialWire)
    ? _initialWire
    : null,
);
/** The list mode's seed/last emit. Any wire-serializable period seeds it — a
 * stored #307 list opens populated, and a scalar segment carries over when the
 * user opts INTO List (the "add another segment to my range" upgrade path; the
 * segment is visible as a chip, so nothing is silently preserved). `_default`
 * and malformed values seed empty, exactly like the range arm. */
let listWire = $state<string | null>(
  initial !== "_default" && _initialWire !== null ? _initialWire : null,
);
let tokenText = $state(seedTokenText(initial));

const tokenHint = $derived(
  mode === "token" &&
    tokenText.trim() !== "" &&
    !looksLikePeriod(tokenText.trim()),
);

// B1 (UI audit): until the range selection is complete, surface a subtle
// inline "incomplete" hint (NOT a red error wall — an empty period is a normal
// mid-authoring state; the backend's invalid_period stays the authority once
// the user submits).
const rangeIncomplete = $derived(mode === "range" && rangeWire === null);
const listIncomplete = $derived(mode === "list" && listWire === null);

/** The range picker's emit: thread the wire through periodFromWire so the
 * draft carries the schema-valid shape (int year / token string / {from,to}
 * object); an incomplete selection is the unset "". */
function onRangeChange(wire: string | null): void {
  rangeWire = wire;
  onchange(wire === null ? "" : periodFromWire(wire));
}

/** The list input's emit — same threading as the range arm. periodFromWire
 * shapes a multi-segment wire into the #307 segment ARRAY; a single remaining
 * segment collapses to its scalar shape (semantically identical, and the
 * canonical form for an uninterrupted period). An empty list is the unset "". */
function onListChange(wire: string | null): void {
  listWire = wire;
  onchange(wire === null ? "" : periodFromWire(wire));
}

/** Token-mode emission ALSO threads through periodFromWire (#308 closes the
 * pre-existing trap where a typed "2010..2020" landed as a raw range STRING —
 * not a valid Source.period; the schema's only range shape is the {from,to}
 * object). Comma text becomes the #307 segment LIST (periodFromWire's list
 * arm — subsumes the retired periodFromTokenText); junk rides through
 * verbatim for the backend to flag. */
function emitToken(): void {
  onchange(periodFromWire(tokenText.trim()));
}

function onModeChange(next: Mode): void {
  mode = next;
  if (next === "default") {
    onchange("_default");
  } else if (next === "range") {
    onchange(rangeWire === null ? "" : periodFromWire(rangeWire));
  } else if (next === "list") {
    onchange(listWire === null ? "" : periodFromWire(listWire));
  } else {
    emitToken();
  }
}
</script>

<div class="period-editor">
  <div class="mode-row">
    <span class="mode-label">Period</span>
    <div class="modes" role="group" aria-label="Period mode">
      <label>
        <input
          type="radio"
          name={groupName}
          checked={mode === "range"}
          onchange={() => onModeChange("range")}
        />
        Range
      </label>
      <label>
        <input
          type="radio"
          name={groupName}
          checked={mode === "list"}
          onchange={() => onModeChange("list")}
        />
        List
      </label>
      <label>
        <input
          type="radio"
          name={groupName}
          checked={mode === "token"}
          onchange={() => onModeChange("token")}
        />
        Token
      </label>
      <label>
        <input
          type="radio"
          name={groupName}
          checked={mode === "default"}
          onchange={() => onModeChange("default")}
        />
        Default
      </label>
    </div>
  </div>

  {#if mode === "range"}
    <div class="range">
      <PeriodRangeInput value={rangeWire} onchange={onRangeChange} />
      {#if rangeIncomplete}
        <!-- Subtle, non-blocking: the period isn't set yet. Distinct from a
             backend invalid_period error (which renders red via <FieldIssues>). -->
        <p class="hint incomplete">Pick at least “From” to complete the period.</p>
      {/if}
    </div>
  {:else if mode === "list"}
    <div class="range">
      <p class="hint muted">
        An interrupted series: non-overlapping segments, each picked like a
        range.
      </p>
      <PeriodListInput value={listWire} onchange={onListChange} />
      {#if listIncomplete}
        <p class="hint incomplete">Add at least one segment to complete the period.</p>
      {/if}
    </div>
  {:else if mode === "token"}
    <div class="token">
      <input
        type="text"
        value={tokenText}
        placeholder="YYYYMM, HT2018, 2010..2020…"
        oninput={(e) => {
          tokenText = e.currentTarget.value;
          emitToken();
        }}
      />
      {#if tokenHint}
        <p class="hint muted">Doesn't look like a period token — the server will confirm.</p>
      {/if}
    </div>
  {:else}
    <p class="default muted">
      Uses the <code>_default</code> snapshot sentinel.
    </p>
  {/if}

  <FieldIssues {issues} />
</div>

<style>
  .period-editor {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .mode-row {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .mode-label {
    font-weight: 600;
    font-size: 0.85rem;
  }
  .modes {
    display: flex;
    gap: 0.75rem;
    font-size: 0.85rem;
  }
  .modes label {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
  }
  .range {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .token input {
    font: inherit;
    padding: 0.3rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    min-width: 16rem;
  }
  .hint {
    font-size: 0.75rem;
    margin: 0.2rem 0 0;
  }
  /* B1: the "period incomplete" cue — amber (advisory), not the red error level.
     An empty period is a normal mid-authoring state, not a validation failure. */
  .hint.incomplete {
    color: var(--level-warning);
  }
</style>
