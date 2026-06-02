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
    return "token";
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

// Snapshot the incoming period ONCE at init. The UI state below is seeded from this
// snapshot and is NOT re-derived on every prop change: a user who switches to Token
// then types must not get snapped back to Years by the parent re-render (every edit
// swaps the whole draft). `untrack` makes the one-time read explicit (no reactive
// dependency) — these seeds intentionally capture only the initial value.
const initial: Period = untrack(() => period);

let mode = $state<Mode>(inferMode(initial));

// The year spinners. Seeded from a numeric value / numeric range; an empty string
// for a non-numeric start so the inputs render blank rather than NaN.
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
const seedYears = initialYears(initial);
let yearFrom = $state(seedYears.from);
let yearTo = $state(seedYears.to);

// The token field text: the raw string token value when in Token mode, blank for a
// numeric/range/default seed, or the JSON of a malformed (non-string) value so the
// user can see what's there.
let tokenText = $state(
  typeof initial === "string" && initial !== "_default"
    ? initial
    : typeof initial === "string" ||
        typeof initial === "number" ||
        typeof initial === "object"
      ? ""
      : JSON.stringify(initial),
);

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
    onchange(fromNum);
    return;
  }
  onchange({ from, to });
}

function onModeChange(next: Mode): void {
  mode = next;
  if (next === "default") {
    onchange("_default");
  } else if (next === "years") {
    emitYears();
  } else {
    onchange(tokenText.trim());
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
          name="period-mode"
          checked={mode === "years"}
          onchange={() => onModeChange("years")}
        />
        Years
      </label>
      <label>
        <input
          type="radio"
          name="period-mode"
          checked={mode === "token"}
          onchange={() => onModeChange("token")}
        />
        Token
      </label>
      <label>
        <input
          type="radio"
          name="period-mode"
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
          onchange(tokenText.trim());
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
