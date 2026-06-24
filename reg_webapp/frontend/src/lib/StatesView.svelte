<script lang="ts">
import type { VariableStateModel } from "./api";
import CodeList from "./CodeList.svelte";
import {
  catalogHref,
  type DistinctValueSet,
  distinctValueSets,
  formatDataType,
  formatStateWindow,
  formatWindow,
  humanizeClassificationSlug,
  matchesFilter,
  type ValueSetTechnicalChange,
  windowTitle,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";
import { VALUE_SET_VERSION_NONE } from "./period";
import TechnicalDetails from "./TechnicalDetails.svelte";

// Presentational view of a variable's `variable_state` rows (from the full
// node's embedded `states` OR a `?period`-narrowed StatesResponse). Pure
// presentation + selection callbacks — it never fetches and never navigates;
// BindingLeafView owns the URL writes.
//
//   length === 1 → single-state DETAIL (variant, validity, type/length, column,
//                  value-set version + the (code, label) table in a
//                  height-constrained scroll container).
//   length  > 1 → a VALUE-SET-centric view (#668 / dogfooding M13/M18/M20):
//                  the states dedup at TWO levels into DISTINCT value sets
//                  (classification editions by `classification_slug`, others by
//                  `value_set_id` — kommun's 415 states → ~21 LKF editions + a few
//                  plain code lists), shown as a compact list by DEFAULT (the
//                  union). A FilterInput narrows the list and a per-row "Isolate"
//                  focuses one (both LOCAL view state); "All value sets" resets.
//                  A classification value set links out (no code dump); a plain
//                  one expands its codes. The narrowing picker (variant /
//                  value-set version) still writes `?variant`/`?value_set_version`
//                  to resolve to length-1 — distinct from the LOCAL isolation.
//   length === 0 → a clean "no state delivered for this period" message (a valid
//                  period outside every validity window — NOT an error).
// No add affordance here: the page-level "Add to project" (#306) lives in
// BindingLeafView — a state is an implementation concept, not a pick target.
let {
  states,
  narrowed,
  activeVariant = null,
  activeValueSetVersion = null,
  scopeStates = null,
  onpickVariant,
  onpickValueSetVersion,
}: {
  states: VariableStateModel[];
  /** True when these are the `?period`-narrowed subset (drives empty wording). */
  narrowed: boolean;
  activeVariant?: string | null;
  activeValueSetVersion?: string | null;
  /** The period-resolved subset when `states` intentionally carries full history. */
  scopeStates?: VariableStateModel[] | null;
  onpickVariant: (variant: string) => void;
  onpickValueSetVersion: (valueSetVersion: string) => void;
} = $props();

const resolutionStates = $derived(scopeStates ?? states);
const single = $derived(
  narrowed
    ? resolutionStates.length === 1
      ? resolutionStates[0]
      : null
    : states.length === 1
      ? states[0]
      : null,
);

// Distinct variants / value-set versions across the (multi-state) set — the two
// narrowing axes. Order-preserving de-dup so the picker is stable.
function distinct<K>(xs: K[]): K[] {
  return [...new Set(xs)];
}
const choiceStates = $derived(narrowed ? resolutionStates : states);
const variants = $derived(distinct(choiceStates.map((s) => s.variant)));
// ALL distinct versions, INCLUDING the empty default label (`value_set_version_label`
// is `TEXT NOT NULL DEFAULT ''`): the states DIFFER by version when this has >1,
// so it drives whether the version axis can narrow. The CHIPS, though, are only
// the non-empty labels — you can't narrow to "no version" via `?value_set_version=`
// (it would be omitted), so an empty-label state is narrowed by variant instead.
const versionsAll = $derived(
  distinct(choiceStates.map((s) => s.value_set_version_label)),
);
const versionChips = $derived(versionsAll.filter((v) => v !== ""));
// A state may carry the empty/default label; it gets a "(no version)" chip
// (sending the `_none` sentinel) so it's individually selectable too.
const hasEmptyVersion = $derived(versionsAll.includes(""));
// Whether either narrowing axis can actually resolve the multi-state set to one.
const canNarrow = $derived(variants.length > 1 || versionsAll.length > 1);

// #668: the dedup that powers the multi-state view — the DISTINCT value sets
// (classification editions by slug, others by `value_set_id`), each carrying
// which variants/spans use it. kommun's 415 states collapse to ~21 LKF editions
// + a few plain code lists here.
const valueSets = $derived(distinctValueSets(states));
const resolutionValueSets = $derived(distinctValueSets(choiceStates));
const scopeValueSetKeys = $derived.by(() => {
  if (scopeStates === null) {
    return null;
  }
  return new Set(distinctValueSets(scopeStates).map((vs) => vs.key));
});

// A version label shared by ≥2 NON-classification rows can't tell them apart on
// its own (kommun's "Kommun historisk" ×22) — those rows get their overall span
// appended to disambiguate. A label unique among the plain rows stays bare.
const ambiguousLabels = $derived.by(() => {
  const counts = new Map<string, number>();
  for (const vs of valueSets) {
    if (!vs.classificationSlug) {
      const label = vs.versionLabel || "(no version)";
      counts.set(label, (counts.get(label) ?? 0) + 1);
    }
  }
  return new Set(
    [...counts.entries()].filter(([, n]) => n > 1).map(([label]) => label),
  );
});

// Local filter + isolate: LOCAL view state (NOT a URL write — distinct from the
// `?variant`/`?value_set_version` narrowing picker, which BindingLeafView owns).
// Isolation is keyed by the value set's STABLE `key` (not a list index, which the
// filter's slice would invalidate); null = the union (default). Both reset when
// the state set changes underneath (navigation / narrowing) so a stale key can't
// isolate / a stale needle can't hide the wrong value set.
let isolatedKey = $state<string | null>(null);
let filter = $state("");
$effect(() => {
  void states;
  void scopeStates;
  isolatedKey = null;
  filter = "";
});
const isolated = $derived(
  isolatedKey == null
    ? null
    : (valueSets.find((vs) => vs.key === isolatedKey) ?? null),
);
// The filtered union list — matched against the row label + every variant slug,
// so hunting a variant ("doda") surfaces the value sets it uses. The isolate view
// shows the single isolated set and ignores the filter.
const shownValueSets = $derived(
  valueSets
    .filter((vs) => matchesFilter(filter, valueSetLabel(vs), ...vs.variants))
    .filter((vs) => scopeValueSetKeys === null || inScope(vs)),
);
const collapsedValueSets = $derived(
  scopeValueSetKeys === null
    ? []
    : valueSets
        .filter((vs) =>
          matchesFilter(filter, valueSetLabel(vs), ...vs.variants),
        )
        .filter((vs) => !inScope(vs)),
);

// A value set is IN SCOPE for the active resolution when no variant is pinned, or
// when one of its variants matches the pinned `?variant`, and when the
// period-resolved subset contains the value-set key. Period-out-of-scope rows are
// collapsed under a disclosure so high-cardinality variables keep their context
// without rendering every historical code list inline (#744).
//
// Greying only has a VISIBLE effect when `states` is MULTI-variant — i.e. full
// history (no `?period`), a `?variant`-without-`?period` deep link, or the
// narrowedError fallback. In the normal variant-pick flow the SERVER narrows the
// states to the active variant, so the out-of-scope value sets are simply ABSENT
// and `inScope` is a no-op (every remaining value set matches `activeVariant`).
// Keeping the test here makes the grey appear whenever multi-variant states DO
// reach the view, without the view needing to know which path produced them.
// (Follow-up: a richer "show full history + grey out-of-scope even on a
// server-narrowed variant-pick" behavior is out of scope for #668.)
function inScope(vs: DistinctValueSet): boolean {
  const inPeriod =
    scopeValueSetKeys === null ? true : scopeValueSetKeys.has(vs.key);
  const inVariant =
    activeVariant == null || vs.variants.includes(activeVariant);
  return inPeriod && inVariant;
}

// The label for a distinct value set: a classification value set reads
// "LKF ⟨vintage⟩" (humanized slug); otherwise its version label (or a
// "(no version)" fallback for the empty default), with the overall span appended
// when that label is shared by another plain row ("Kommun historisk · 1968–1970").
function valueSetLabel(vs: DistinctValueSet): string {
  if (vs.classificationSlug) {
    return humanizeClassificationSlug(vs.classificationSlug);
  }
  const label = vs.versionLabel || "(no version)";
  return ambiguousLabels.has(label)
    ? `${label} · ${formatWindow(vs.overallSpan.from, vs.overallSpan.to)}`
    : label;
}

function usageChanges(
  usage: DistinctValueSet["usages"][number],
): ValueSetTechnicalChange[] {
  return usage.spans.flatMap((span) => span.changes ?? []);
}

function changeDateLabel(at: string): string {
  return /^\d{4}-01-01$/.test(at) ? at.slice(0, 4) : at;
}

function technicalChangeLabel(change: ValueSetTechnicalChange): string {
  return `changed ${changeDateLabel(change.at)}: ${change.notes.join("; ")}`;
}
</script>

<!-- The (code, label) viewer — the SAME rendering for the detail mode and a
     list row's inline expansion (#310). The shared CodeList (#638 PR3): a
     variable value set is a code→label set, identical to a classification's
     codes, so it renders through the unified viewer (which owns the
     size-dependent filter + the height-constrained scroll). -->
{#snippet valueSetTable(valueSet: NonNullable<VariableStateModel["value_set"]>)}
  <CodeList
    codes={valueSet}
    filterLabel="Filter value set"
    filterPlaceholder="Filter value set…"
  />
{/snippet}

<!-- #668: which variants / period spans use a distinct value set — one line per
     variant, its adjacent-collapsed (M20) spans joined compactly. `formatWindow`
     renders each span's bounds the same coarsest-exact way the single-state detail
     does (the open-ended ceiling reads "since …"). -->
{#snippet usage(vs: DistinctValueSet)}
  <ul class="vs-usage">
    {#each vs.usages as u (u.variant)}
      {@const changes = usageChanges(u)}
      <li>
        <code class="vs-usage-variant">{u.variant}</code>
        <span class="muted vs-usage-spans">
          {u.spans.map((sp) => formatWindow(sp.from, sp.to)).join(", ")}
        </span>
        {#if changes.length > 0}
          <span class="vs-change-list">
            {#each changes as change (`${change.at}:${change.notes.join("|")}`)}
              <span class="vs-change">{technicalChangeLabel(change)}</span>
            {/each}
          </span>
        {/if}
      </li>
    {/each}
  </ul>
{/snippet}

<!-- #668: a classification value set links to its classification instead of
     dumping its (often 1000+) codes — the M13/kommun fix; a plain value set
     expands its codes inline through the shared CodeList (#310). -->
{#snippet valueSetBody(vs: DistinctValueSet)}
  {#if vs.classificationSlug}
    <p class="vs-classification">
      Codes from the
      <a href={catalogHref(`class/${vs.classificationSlug}`)}>
        {humanizeClassificationSlug(vs.classificationSlug)}
      </a>
      classification.
    </p>
  {:else if vs.valueSet && vs.valueSet.length > 0}
    {@render valueSetTable(vs.valueSet)}
  {:else}
    <p class="muted">No value set.</p>
  {/if}
{/snippet}

{#snippet valueSetRow(vs: DistinctValueSet)}
  <li class:out-of-scope={!inScope(vs)}>
    <div class="vs-row">
      {#if vs.classificationSlug}
        <!-- A classification value set: link out, never dump the (huge)
             code list. -->
        <a class="vs-label" href={catalogHref(`class/${vs.classificationSlug}`)}>
          = {humanizeClassificationSlug(vs.classificationSlug)}
        </a>
      {:else}
        <span class="vs-label">{valueSetLabel(vs)}</span>
        {#if vs.valueSet && vs.valueSet.length > 0}
          <span class="muted vs-count">({vs.valueSet.length})</span>
        {/if}
      {/if}
      <button
        type="button"
        class="chip vs-isolate"
        onclick={() => (isolatedKey = vs.key)}
      >
        Isolate
      </button>
    </div>
    {@render usage(vs)}
    {#if !vs.classificationSlug && vs.valueSet && vs.valueSet.length > 0}
      <!-- #310: inspect a plain value set's codes inline, without isolating. -->
      <details class="vs-codes">
        <summary>Values ({vs.valueSet.length})</summary>
        {@render valueSetTable(vs.valueSet)}
      </details>
    {/if}
  </li>
{/snippet}

{#if states.length === 0}
  <p class="muted empty">
    {#if narrowed}
      No state delivered for this period.
    {:else}
      This variable has no recorded states.
    {/if}
  </p>
{:else if single}
  {@const s = single}
  <div class="state-detail">
    <dl class="meta">
      <dt>Variant</dt>
      <dd><code>{s.variant}</code></dd>
      <dt>Valid</dt>
      <!-- #309/#321: sentinel-free, coarsest-exact window ("since 2016",
           "VT2009"); the raw ISO window stays on the tooltip. -->
      <dd title={windowTitle(s.valid_from, s.valid_to)}>{formatStateWindow(s)}</dd>
      <dt>Value-set version</dt>
      <dd>{s.value_set_version_label || "(no version)"}</dd>
    </dl>

    <!-- #638 PR4: Data type and Delivery column are STRUCTURAL backend fields
         (the physical SQL type + source column) — kept available but demoted
         behind the "Technical details" disclosure. Both stay conditionally
         rendered; the disclosure is omitted entirely when neither is present. -->
    {#if s.data_type || s.delivery_column_name}
      <TechnicalDetails>
        <dl class="meta">
          {#if s.data_type}
            <dt>Data type</dt>
            <!-- formatDataType drops a meaningless "(0)"/empty length parenthetical
                 (the "bigint(0)" artifact) while keeping real ones like "char(25)". -->
            <dd>{formatDataType(s.data_type, s.data_length)}</dd>
          {/if}
          {#if s.delivery_column_name}
            <dt>Delivery column</dt>
            <dd><code>{s.delivery_column_name}</code></dd>
          {/if}
        </dl>
      </TechnicalDetails>
    {/if}

    {#if s.value_set && s.value_set.length > 0}
      <h4 class="vs-heading">
        Value set <span class="muted">({s.value_set.length})</span>
      </h4>
      {@render valueSetTable(s.value_set)}
    {:else}
      <p class="muted">No value set.</p>
    {/if}
  </div>
{:else}
  <!-- Multiple states → the value-set-centric view (#668). The narrowing picker
       writes `?variant`/`?value_set_version`, which the server only honors WITH a
       `?period` (it 422s them otherwise), so it's shown only when a period is
       active (`narrowed`). Without a period the view is the full state history —
       set a period to narrow to one. -->
  {#if narrowed && resolutionStates.length === 0}
    <p class="muted picker-hint">
      No state delivered for this period. Historical value sets outside this period
      are collapsed below.
    </p>
  {:else if narrowed && canNarrow}
    <p class="muted picker-hint">
      {resolutionValueSets.length}
      {resolutionValueSets.length === 1 ? "value set" : "value sets"} at this period across
      {variants.length}
      {variants.length === 1 ? "variant" : "variants"}. Narrow to a single state:
    </p>
    {#if variants.length > 1}
      <fieldset class="picker">
        <legend>Variant</legend>
        <div class="chips">
          {#each variants as variant (variant)}
            <button
              type="button"
              class="chip"
              class:active={variant === activeVariant}
              aria-pressed={variant === activeVariant}
              onclick={() => onpickVariant(variant)}
            >
              {variant}
            </button>
          {/each}
        </div>
      </fieldset>
    {/if}
    {#if versionsAll.length > 1}
      <fieldset class="picker">
        <legend>Value-set version</legend>
        <div class="chips">
          {#each versionChips as version (version)}
            <button
              type="button"
              class="chip"
              class:active={version === activeValueSetVersion}
              aria-pressed={version === activeValueSetVersion}
              onclick={() => onpickValueSetVersion(version)}
            >
              {version}
            </button>
          {/each}
          {#if hasEmptyVersion}
            <!-- The empty/default-label state — selectable via the `_none`
                 sentinel (an empty `?value_set_version` can't ride in the URL). -->
            <button
              type="button"
              class="chip"
              class:active={activeValueSetVersion === VALUE_SET_VERSION_NONE}
              aria-pressed={activeValueSetVersion === VALUE_SET_VERSION_NONE}
              onclick={() => onpickValueSetVersion(VALUE_SET_VERSION_NONE)}
            >
              (no version)
            </button>
          {/if}
        </div>
      </fieldset>
    {/if}
  {:else if narrowed}
    <!-- A period that resolves to several states which share one variant AND one
         value-set version (e.g. a RANGE crossing validity windows): no narrowing
         axis can pick one, so don't promise "narrow to a single state". -->
    <p class="muted picker-hint">
      {resolutionValueSets.length} overlapping value sets at this period — narrow to a single
      point period to resolve to one.
    </p>
  {:else}
    <p class="muted picker-hint">
      {valueSets.length}
      {valueSets.length === 1 ? "value set" : "value sets"} across {states.length}
      states over time. Set a period above to resolve to one.
    </p>
  {/if}

  {#if isolated}
    <!-- ISOLATED: one value set's full detail — its label/link or code table,
         then which variants/spans use it. "All value sets" returns to the union
         (the one always-present reset — replaces the old all-chips strip). -->
    {@const vs = isolated}
    <div class="vs-detail">
      <button
        type="button"
        class="chip vs-reset"
        onclick={() => (isolatedKey = null)}
      >
        ← All value sets
      </button>
      <h4 class="vs-heading">
        {#if vs.classificationSlug}
          <a href={catalogHref(`class/${vs.classificationSlug}`)}>
            = {humanizeClassificationSlug(vs.classificationSlug)}
          </a>
        {:else}
          {valueSetLabel(vs)}
          {#if vs.valueSet && vs.valueSet.length > 0}
            <span class="muted">({vs.valueSet.length})</span>
          {/if}
        {/if}
      </h4>
      {@render valueSetBody(vs)}
      <h5 class="vs-usage-heading">Used by</h5>
      {@render usage(vs)}
    </div>
  {:else}
    <!-- UNION (default): the distinct value sets, compact. A FilterInput narrows
         the list (a row label / variant-slug substring) — the SCALABLE form of
         the old all-chips strip — and each row carries an Isolate affordance.
         A classification value set shows the "= LKF ⟨vintage⟩" link (NO code dump
         — the M13/kommun fix); a plain one expands its codes inline (#310).
         Out-of-scope value sets are greyed, not removed. -->
    {#if valueSets.length > 1}
      <FilterInput
        bind:value={filter}
        total={valueSets.length}
        shown={shownValueSets.length}
        label="Filter value sets"
        placeholder="Filter value sets…"
      />
    {/if}
    {#if shownValueSets.length === 0 && collapsedValueSets.length === 0}
      <p class="muted">No value set matches the filter.</p>
    {/if}
    <ul class="vs-list">
      {#each shownValueSets as vs (vs.key)}
        {@render valueSetRow(vs)}
      {/each}
    </ul>
    {#if collapsedValueSets.length > 0}
      <details class="out-of-period">
        <summary>
          {collapsedValueSets.length}
          {collapsedValueSets.length === 1 ? "value set" : "value sets"} outside
          this period
        </summary>
        <ul class="vs-list out-of-period-list">
          {#each collapsedValueSets as vs (vs.key)}
            {@render valueSetRow(vs)}
          {/each}
        </ul>
      </details>
    {/if}
  {/if}
{/if}

<style>
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.3rem 1rem;
    margin: 0.5rem 0 1rem;
  }
  .meta dt {
    font-weight: 600;
  }
  .vs-heading {
    margin: 0.5rem 0 0.4rem;
  }
  .picker {
    border: 1px solid var(--border);
    border-radius: 6px;
    margin: 0.5rem 0;
    padding: 0.5rem 0.75rem 0.75rem;
  }
  .picker legend {
    font-weight: 600;
    padding: 0 0.3rem;
  }
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
  }
  .chip {
    padding: 0.25rem 0.7rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    font: inherit;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .chip.active {
    background: var(--accent);
    color: #fff;
  }
  .chip:hover {
    background: var(--accent-bg);
  }
  .chip.active:hover {
    background: var(--accent);
    filter: brightness(0.95);
  }
  /* #668: the "← All value sets" reset on the isolated detail — a one-click
     return to the union (replaces the old all-chips strip). */
  .vs-reset {
    font-size: 0.75rem;
    padding: 0.1rem 0.6rem;
    margin-bottom: 0.5rem;
  }
  .vs-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .vs-list li {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    padding: 0.4rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .vs-list li.out-of-scope {
    opacity: 0.55;
  }
  .vs-row {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem;
  }
  .vs-label {
    font-weight: 600;
  }
  .vs-count {
    font-size: 0.85em;
  }
  .vs-isolate {
    margin-left: auto;
    font-size: 0.75rem;
    padding: 0.1rem 0.6rem;
  }
  /* Per-variant usage lines — compact, monospace variant + muted spans. */
  .vs-usage {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    font-size: 0.85rem;
  }
  .vs-usage li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem;
  }
  .vs-usage-variant {
    font-size: 0.9em;
  }
  .vs-change-list {
    flex-basis: 100%;
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    color: var(--muted);
  }
  .vs-change {
    font-size: 0.9em;
  }
  .vs-codes {
    margin-top: 0.1rem;
  }
  .vs-codes summary {
    cursor: pointer;
    font-size: 0.8rem;
    color: var(--accent);
  }
  .vs-detail {
    margin-top: 0.25rem;
  }
  .vs-classification {
    margin: 0.25rem 0;
  }
  .vs-usage-heading {
    margin: 0.75rem 0 0.3rem;
    font-size: 0.85rem;
  }
  .out-of-period {
    margin-top: 0.5rem;
  }
  .out-of-period summary {
    cursor: pointer;
    color: var(--accent);
    font-size: 0.85rem;
  }
  .out-of-period-list {
    margin-top: 0.4rem;
  }
</style>
