<script lang="ts" module>
// A per-instance sequence so each PeriodEditor's radio group gets a UNIQUE name.
// Otherwise multiple editors on one page form ONE document-wide radio group, and
// selecting a mode in one source unchecks the others' radios (Codex P2).
let _instanceSeq = 0;
</script>

<script lang="ts">
import { untrack } from "svelte";
import FieldIssues from "./FieldIssues.svelte";
import { looksLikePeriod } from "./period";
import type { Period } from "./project_data";
import type { ValidationIssue } from "./validation";

// Editable Source.period (§6.2). YEAR-RANGE-FIRST (maintainer directive): the
// DEFAULT mode is two numeric year spinners (from / to) so picking a year range is
// the path of least resistance, not free text. Other modes cover the non-year
// cases: "Token" (a text field for monthly 'YYYYMM' / special tokens) and
// "Default" (the "_default" snapshot sentinel).
//
// §9.6: NEVER validates structurally — a malformed value is echoed via the
// backend's `invalid_period` on the period pointer (the mounted <FieldIssues>).
// Emits a bare int when from===to (the single-year Period int arm), else {from,to}.
const { period, issues, onchange } = $props<{
  period: Period;
  issues: ValidationIssue[];
  onchange: (next: Period) => void;
}>();

type Mode = "years" | "token" | "default";

// Infer the initial mode from the incoming value's JS type. A malformed value
// (boolean / array / unexpected object) falls back to Token raw-text so the editor
// never crashes — the backend flags it. `{from,to}` and a bare number are both
// "years"; numeric range endpoints fill the spinners, non-numeric ones (token
// ranges) fall back to Token.
function inferMode(value: Period): Mode {
  if (typeof value === "number") {
    return "years";
  }
  if (value === "_default") {
    return "default";
  }
  if (typeof value === "string") {
    // An empty/unset period (a fresh source seeds `period: ""`) defaults to the
    // YEARS picker — the year-range-first common case (maintainer directive). A
    // non-empty token string opens in Token mode.
    return value === "" ? "years" : "token";
  }
  if (
    value != null &&
    typeof value === "object" &&
    "from" in value &&
    "to" in value &&
    typeof value.from === "number" &&
    typeof value.to === "number"
  ) {
    return "years";
  }
  // Malformed (boolean / array / token-range object): show it as raw text.
  return "token";
}

// A unique radio-group name for THIS editor instance (fix C).
const groupName = `period-mode-${_instanceSeq++}`;

// The year spinners' seed: a numeric value / numeric range fills them; a
// non-numeric start leaves them blank (so the inputs render blank, not NaN).
function initialYears(value: Period): { from: string; to: string } {
  if (typeof value === "number") {
    return { from: String(value), to: String(value) };
  }
  if (
    value != null &&
    typeof value === "object" &&
    typeof value.from === "number" &&
    typeof value.to === "number"
  ) {
    return { from: String(value.from), to: String(value.to) };
  }
  return { from: "", to: "" };
}

// The token field's seed: the raw string when it's a token, blank for a
// numeric/range/default value, or the JSON of a malformed (non-string) value.
function seedTokenText(value: Period): string {
  if (typeof value === "string" && value !== "_default") {
    return value;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    (value != null && typeof value === "object")
  ) {
    return "";
  }
  return JSON.stringify(value);
}

// Local UI state, seeded from the period at mount. `untrack` keeps the seed a
// one-time read (no reactive dep) so switching mode then typing isn't snapped back
// when an UNRELATED edit swaps the whole draft.
const initial: Period = untrack(() => period);
let mode = $state<Mode>(inferMode(initial));
const seedYears = initialYears(initial);
let yearFrom = $state(seedYears.from);
let yearTo = $state(seedYears.to);
let tokenText = $state(seedTokenText(initial));

// RE-SEED when `period` arrives with a value we did NOT just emit — i.e. the
// index-keyed instance was REUSED for a DIFFERENT source (a middle-source remove)
// or the draft was replaced. `emit` sets `seeded` to the value it sends, so our own
// writes don't re-seed (no snap-back); an unrelated edit leaves THIS source's period
// unchanged → no re-seed. Without this the editor displays/overwrites a stale
// source's period after a middle remove (panel + Codex P2).
let seeded = $state(JSON.stringify(initial));
$effect(() => {
  const incoming = JSON.stringify(period);
  // Only `period` is a dependency — untrack the `seeded` read so writing it below
  // doesn't re-trigger this effect (the re-seed runs once per external change).
  if (incoming !== untrack(() => seeded)) {
    mode = inferMode(period);
    const ys = initialYears(period);
    yearFrom = ys.from;
    yearTo = ys.to;
    tokenText = seedTokenText(period);
    seeded = incoming;
  }
});

// Funnel every period write so `seeded` matches what we sent (see the re-seed
// effect above) — keeps our own emits from re-seeding.
function emit(next: Period): void {
  seeded = JSON.stringify(next);
  onchange(next);
}

const tokenHint = $derived(
  tokenText.trim() !== "" && !looksLikePeriod(tokenText.trim()),
);

// Emit the years value: a bare int when from===to (single year → the Period int
// arm), else a {from,to} range. A blank/non-integer endpoint is emitted as the raw
// string so the backend's invalid_period flags it rather than us coercing to NaN.
function emitYears(): void {
  const fromNum = Number.parseInt(yearFrom, 10);
  const toNum = Number.parseInt(yearTo, 10);
  const fromOk = yearFrom.trim() !== "" && String(fromNum) === yearFrom.trim();
  const toOk = yearTo.trim() !== "" && String(toNum) === yearTo.trim();
  const from: number | string = fromOk ? fromNum : yearFrom.trim();
  const to: number | string = toOk ? toNum : yearTo.trim();
  if (fromOk && toOk && fromNum === toNum) {
    emit(fromNum);
    return;
  }
  emit({ from, to });
}

function onModeChange(next: Mode): void {
  mode = next;
  if (next === "default") {
    emit("_default");
  } else if (next === "years") {
    emitYears();
  } else {
    emit(tokenText.trim());
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
          checked={mode === "years"}
          onchange={() => onModeChange("years")}
        />
        Years
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

  {#if mode === "years"}
    <div class="years">
      <label>
        <span>From</span>
        <input
          type="number"
          value={yearFrom}
          placeholder="2010"
          oninput={(e) => {
            yearFrom = e.currentTarget.value;
            emitYears();
          }}
        />
      </label>
      <label>
        <span>To</span>
        <input
          type="number"
          value={yearTo}
          placeholder="2020"
          oninput={(e) => {
            yearTo = e.currentTarget.value;
            emitYears();
          }}
        />
      </label>
      <p class="hint muted">A single year sets from = to.</p>
    </div>
  {:else if mode === "token"}
    <div class="token">
      <input
        type="text"
        value={tokenText}
        placeholder="YYYYMM, HT2018, 2010..2020…"
        oninput={(e) => {
          tokenText = e.currentTarget.value;
          emit(tokenText.trim());
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
  .years {
    display: flex;
    align-items: flex-end;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .years label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: 0.8rem;
  }
  .years input {
    width: 6rem;
    font: inherit;
    padding: 0.3rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
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
</style>
