<script lang="ts">
import type { VariableStateModel } from "./api";
import CodeList from "./CodeList.svelte";
import {
  catalogHref,
  type DenseIntegerValueSetRange,
  type DistinctValueSet,
  denseIntegerValueSetRange,
  distinctValueSets,
  formatStateWindow,
  formatWindow,
  humanizeClassificationSlug,
  matchesFilter,
  type ValueSetTechnicalChange,
  valueSetKeyForColumn,
  windowTitle,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";

// The PURE value-set / coding viewer for a variable's `variable_state` rows
// (extracted from the retired StatesView — #905). Presentation ONLY: it never
// fetches, never navigates, and carries NO resolution (variant / value-set
// version) state — the picker (RepresentationPicker) owns the
// `?variant`/`?value_set_version` narrowing now, and BindingLeafView owns the URL
// writes. This view just renders the codings the leaf hands it, focused (via
// `focusColumn` + `focusVariant`) on the (variant, column) a deep-link points at.
//
// DELIBERATELY STANDALONE: kept as its own component (not folded back into the
// leaf) so the graph/picker surface can host the same coding display next to selected
// representations without duplicating value-set rendering.
//
//   length === 1 → single-state DETAIL (variant, validity, value-set version,
//                  operational definition + the (code, label) table in a
//                  height-constrained / large-list-collapsed container). Structural
//                  fields live in BindingLeafView's bottom Technical details disclosure.
//   length  > 1 → a VALUE-SET-centric view (#668 / dogfooding M13/M18/M20):
//                  the states dedup at TWO levels into DISTINCT value sets
//                  (classification editions by `classification_slug`, others by
//                  `value_set_id` — kommun's 415 states → ~21 LKF editions + a few
//                  plain code lists), shown as a compact list by DEFAULT (the
//                  union). A FilterInput narrows the list and a per-row "Isolate"
//                  focuses one (both LOCAL view state); "All value sets" resets.
//                  A `focusColumn` deep-link seeds the same isolation. A
//                  classification value set links out (no code dump); a plain one
//                  expands its codes.
//   length === 0 → a clean "no state delivered for this period" message (a valid
//                  period outside every validity window — NOT an error).
let {
  states,
  narrowed,
  scopeStates = null,
  focusColumn = null,
  focusVariant = null,
}: {
  states: VariableStateModel[];
  /** True when these are the `?period`-narrowed subset (drives empty wording). */
  narrowed: boolean;
  /** The period-resolved subset when `states` intentionally carries full history. */
  scopeStates?: VariableStateModel[] | null;
  /** A delivery column to FOCUS (#905): the picker's "codings vary" nudge deep-links
   * to `?codes=<variant>::<column>`, which BindingLeafView passes here. It seeds the
   * local isolation onto the distinct value set that (`focusVariant`, column) resolves
   * to and scrolls it into view. Null (or a column no state delivers) → no focus
   * (the default union). */
  focusColumn?: string | null;
  /** The variant scoping the focus (#905): a delivery column can be shared across
   * variants/populations with DISTINCT codings (picker rows are keyed
   * `(variant, column)`), so the focus isolates THIS variant's coding. Null → the
   * column is considered across all variants (a column unique across variants, or a
   * deep link with no variant) — `valueSetKeyForColumn`'s back-compat path. */
  focusVariant?: string | null;
} = $props();

// Single-state DETAIL — PERIOD-AWARE (#905, Codex P2). A single state reaching the view
// (`states.length === 1`) drives the detail — the full-history view with one recorded
// state, OR a `?period` that BindingLeafView resolved to exactly one state (it passes
// that single state as `states`). BUT when narrowed, the period must have actually
// DELIVERED it: a variable with one historical state viewed at a `?period` OUTSIDE that
// state falls back to the lone `node.states` (length 1) with an EMPTY `scopeStates`, and
// must NOT render that out-of-era state as the in-period detail. So a narrowed view with
// an empty period scope suppresses the single detail and falls through to the
// "No state delivered for this period" path (consistent with `hasInPeriodValueSets`).
// A multi-history view (`states.length > 1`) is never single regardless of scope, so the
// value-set list / focus-isolation render. Uses only `states` + `scopeStates` — never
// resolution-modifier coupling.
const single = $derived.by(() => {
  if (states.length !== 1) {
    return null;
  }
  if (narrowed && scopeStates !== null && scopeStates.length === 0) {
    return null;
  }
  return states[0];
});

// #668: the dedup that powers the multi-state view — the DISTINCT value sets
// (classification editions by slug, others by `value_set_id`), each carrying
// which variants/spans use it. kommun's 415 states collapse to ~21 LKF editions
// + a few plain code lists here.
const valueSets = $derived(distinctValueSets(states));
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
    if (!vs.classificationSlug && vs.versionLabel) {
      const label = vs.versionLabel;
      counts.set(label, (counts.get(label) ?? 0) + 1);
    }
  }
  return new Set(
    [...counts.entries()].filter(([, n]) => n > 1).map(([label]) => label),
  );
});

// Local filter + isolate: LOCAL view state (NOT a URL write). Isolation is keyed
// by the value set's STABLE `key` (not a list index, which the filter's slice
// would invalidate); null = the union (default). Both reset when the state set
// changes underneath (navigation / narrowing) so a stale key can't isolate / a
// stale needle can't hide the wrong value set. A `focusColumn` deep-link SEEDS the
// isolation to the value set that column resolves to (#905).
let isolatedKey = $state<string | null>(null);
let filter = $state("");
$effect(() => {
  void states;
  void scopeStates;
  void focusVariant;
  // A deep-link `?codes=<variant>::<column>` seeds the isolation to that ROW's
  // (variant-scoped, latest-era) distinct value set; absent / unmatched → the default
  // union. `focusVariant` narrows to the clicked row's coding when a column is shared
  // across variants.
  isolatedKey =
    focusColumn != null
      ? valueSetKeyForColumn(states, focusColumn, focusVariant)
      : null;
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
    .filter((vs) => inPeriod(vs)),
);
const collapsedValueSets = $derived(
  scopeValueSetKeys === null
    ? []
    : valueSets
        .filter((vs) =>
          matchesFilter(filter, valueSetLabel(vs), ...vs.variants),
        )
        .filter((vs) => !inPeriod(vs)),
);
const filteredValueSetCount = $derived(
  shownValueSets.length + collapsedValueSets.length,
);

// The UNFILTERED period scope: whether the period genuinely delivered ANY in-period
// value set, computed BEFORE the text filter (#905, Codex P3). The empty hint keys off
// this — not off `shownValueSets`/`collapsedValueSets`, which the filter already
// narrows — so a filter that hides in-period rows (matching only an out-of-period one)
// does NOT mis-report "No state delivered for this period". The filter's own
// zero-results state describes "no matches" separately. Null scope (full history) →
// always non-empty (no period to be empty for).
const hasInPeriodValueSets = $derived(
  scopeValueSetKeys === null || valueSets.some((vs) => inPeriod(vs)),
);

// Scroll the focused (isolated) detail into view once it renders — the deep-link
// from the picker's "codings vary" nudge lands the user on the right coding. Gated
// on a focusColumn so ordinary in-page isolation (a row's "Isolate" click) doesn't
// yank the scroll position.
let detailEl = $state<HTMLDivElement | null>(null);
$effect(() => {
  if (focusColumn != null && detailEl) {
    detailEl.scrollIntoView({ block: "nearest" });
  }
});

// A value set is IN PERIOD when the period-resolved subset contains its key.
// Period-out-of-scope rows are collapsed under a disclosure so high-cardinality
// variables keep their context without rendering every historical code list inline
// (#744). No scope → everything is in period (full-history view).
function inPeriod(vs: DistinctValueSet): boolean {
  return scopeValueSetKeys === null ? true : scopeValueSetKeys.has(vs.key);
}

// The label for a distinct value set: a classification value set reads
// "LKF ⟨vintage⟩" (humanized slug); otherwise its version label, with the
// overall span appended when that label is shared by another plain row
// ("Kommun historisk · 1968–1970"). Empty/default labels do not render the old
// empty-version placeholder; they fall back to the span, or a neutral label when
// the span is wholly unknown.
function valueSetLabel(vs: DistinctValueSet): string {
  if (vs.classificationSlug) {
    return humanizeClassificationSlug(vs.classificationSlug);
  }
  const span = formatWindow(vs.overallSpan.from, vs.overallSpan.to);
  if (!vs.versionLabel) {
    return span ? `Value set · ${span}` : "Value set";
  }
  return ambiguousLabels.has(vs.versionLabel) && span
    ? `${vs.versionLabel} · ${span}`
    : vs.versionLabel;
}

function usageChanges(
  usage: DistinctValueSet["usages"][number],
): ValueSetTechnicalChange[] {
  return usage.spans.flatMap((span) => span.changes ?? []);
}

function definitionStates(
  usage: DistinctValueSet["usages"][number],
): VariableStateModel[] {
  return usage.states.filter((s) => s.operational_definition);
}

function repeatedDefinitionLabels(states: VariableStateModel[]): Set<string> {
  const counts = new Map<string, number>();
  for (const state of states) {
    const label = stateDefinitionBaseLabel(state);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }
  return new Set(
    [...counts.entries()]
      .filter(([, count]) => count > 1)
      .map(([label]) => label),
  );
}

function stateDefinitionBaseLabel(s: VariableStateModel): string {
  return (
    s.delivery_column_name ?? formatStateWindow(s) ?? "Operational definition"
  );
}

function stateDefinitionLabel(
  s: VariableStateModel,
  repeated: Set<string>,
): string {
  const label = stateDefinitionBaseLabel(s);
  const window = formatStateWindow(s);
  return repeated.has(label) && window ? `${label} (${window})` : label;
}

function stateDefinitionKey(s: VariableStateModel): string {
  return [
    s.state_id,
    s.delivery_column_name ?? "",
    s.valid_from,
    s.valid_to,
    s.value_set_version_label ?? "",
  ].join("|");
}

function changeDateLabel(at: string): string {
  return /^\d{4}-01-01$/.test(at) ? at.slice(0, 4) : at;
}

function technicalChangeLabel(change: ValueSetTechnicalChange): string {
  return `changed ${changeDateLabel(change.at)}: ${change.notes.join("; ")}`;
}

function overlapPercent(overlap: number): string {
  return `${Math.round(overlap * 100)}%`;
}

function usageWindowLabels(
  spans: DistinctValueSet["usages"][number]["spans"],
): string[] {
  return spans
    .map((sp) => formatWindow(sp.from, sp.to))
    .filter((label): label is string => label !== null);
}

function usageVariantLabel(variant: string): string | null {
  return variant === "_default" ? null : variant;
}
</script>

<!-- The (code, label) viewer — the SAME rendering for the detail mode and a
     list row's inline expansion (#310). The shared CodeList (#638 PR3): a
     variable value set is a code→label set, identical to a classification's
     codes, so it renders through the unified viewer (which owns the
     size-dependent filter + large-list collapse). -->
{#snippet valueSetTable(valueSet: NonNullable<VariableStateModel["value_set"]>)}
  <CodeList
    codes={valueSet}
    filterLabel="Filter value set"
    filterPlaceholder="Filter value set…"
  />
{/snippet}

{#snippet denseIntegerRange(range: DenseIntegerValueSetRange)}
  <p class="vs-numeric-range">
    Integer values <code>{range.min}</code>-<code>{range.max}</code>
    <span class="muted">({range.count} values)</span>
  </p>
{/snippet}

{#snippet conformanceNotice(conf: NonNullable<VariableStateModel["classification_conformance"]>)}
  <div class:severed={conf.status === "severed"} class="conformance-notice">
    {#if conf.status === "severed"}
      <p>
        Declared coding
        <a href={catalogHref(`class/${conf.declared_classification_slug}`)}>
          {humanizeClassificationSlug(conf.declared_classification_slug)}
        </a>
        severed: {overlapPercent(conf.overlap)} of checked codes match this
        classification.
      </p>
    {:else if conf.nonconforming_code_count > 0}
      <p>
        Declared coding
        <a href={catalogHref(`class/${conf.declared_classification_slug}`)}>
          {humanizeClassificationSlug(conf.declared_classification_slug)}
        </a>
        kept, but {conf.nonconforming_code_count}
        {conf.nonconforming_code_count === 1 ? "code is" : "codes are"} not part
        of this classification.
      </p>
    {/if}
    {#if conf.nonconforming_codes.length > 0}
      <details>
        <summary>
          Nonconforming codes ({conf.nonconforming_codes.length})
        </summary>
        <CodeList
          codes={conf.nonconforming_codes}
          filterLabel="Filter nonconforming codes"
          filterPlaceholder="Filter nonconforming codes…"
        />
      </details>
    {/if}
  </div>
{/snippet}

<!-- #668: which variants / period spans use a distinct value set — one line per
     variant, its adjacent-collapsed (M20) spans joined compactly. `formatWindow`
     renders each span's bounds the same coarsest-exact way the single-state detail
     does (the open-ended ceiling reads "since …"). -->
{#snippet usage(vs: DistinctValueSet)}
  <ul class="vs-usage">
    {#each vs.usages as u (u.variant)}
      {@const changes = usageChanges(u)}
      {@const definedStates = definitionStates(u)}
      {@const repeatedLabels = repeatedDefinitionLabels(definedStates)}
      {@const usageSpans = usageWindowLabels(u.spans)}
      {@const variantLabel = usageVariantLabel(u.variant)}
      {#if variantLabel || usageSpans.length > 0 || changes.length > 0 || definedStates.length > 0}
      <li>
        {#if variantLabel}
          <code class="vs-usage-variant">{variantLabel}</code>
        {/if}
        {#if usageSpans.length > 0}
          <span class="muted vs-usage-spans">{usageSpans.join(", ")}</span>
        {/if}
        {#if changes.length > 0}
          <span class="vs-change-list">
            {#each changes as change (`${change.at}:${change.notes.join("|")}`)}
              <span class="vs-change">{technicalChangeLabel(change)}</span>
            {/each}
          </span>
        {/if}
        {#if definedStates.length > 0}
          <dl class="state-definitions">
            {#each definedStates as s (stateDefinitionKey(s))}
              <div>
                <dt>
                  {#if s.delivery_column_name}
                    <code>{stateDefinitionLabel(s, repeatedLabels)}</code>
                  {:else}
                    {stateDefinitionLabel(s, repeatedLabels)}
                  {/if}
                </dt>
                <dd>{s.operational_definition}</dd>
              </div>
            {/each}
          </dl>
        {/if}
      </li>
      {/if}
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
    {#if vs.classificationConformance && vs.classificationConformance.nonconforming_code_count > 0}
      {@render conformanceNotice(vs.classificationConformance)}
    {/if}
  {:else if vs.valueSet && vs.valueSet.length > 0}
    {@const range = denseIntegerValueSetRange(vs.valueSet)}
    {#if vs.classificationConformance}
      {@render conformanceNotice(vs.classificationConformance)}
    {/if}
    {#if range}
      {@render denseIntegerRange(range)}
    {:else}
      {@render valueSetTable(vs.valueSet)}
    {/if}
  {:else}
    {#if vs.classificationConformance}
      {@render conformanceNotice(vs.classificationConformance)}
    {/if}
  {/if}
{/snippet}

{#snippet valueSetRow(vs: DistinctValueSet)}
  <li>
    <div class="vs-row">
      {#if vs.classificationSlug}
        <!-- A classification value set: link out, never dump the (huge)
             code list. -->
        <a class="vs-label" href={catalogHref(`class/${vs.classificationSlug}`)}>
          = {humanizeClassificationSlug(vs.classificationSlug)}
        </a>
      {:else}
        {@const range = denseIntegerValueSetRange(vs.valueSet)}
        <span class="vs-label">{valueSetLabel(vs)}</span>
        {#if vs.valueSet && vs.valueSet.length > 0}
          <span class="muted vs-count">({vs.valueSet.length})</span>
        {/if}
        {#if range}
          <span class="muted vs-range">{range.min}-{range.max}</span>
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
    {#if vs.classificationConformance && (vs.classificationConformance.status === "severed" || vs.classificationConformance.nonconforming_code_count > 0)}
      {@render conformanceNotice(vs.classificationConformance)}
    {/if}
    {#if !vs.classificationSlug && vs.valueSet && vs.valueSet.length > 0}
      {@const range = denseIntegerValueSetRange(vs.valueSet)}
      <!-- #310: inspect a plain value set's codes inline, without isolating. -->
      {#if range}
        {@render denseIntegerRange(range)}
      {:else}
        <details class="vs-codes">
          <summary>Values ({vs.valueSet.length})</summary>
          {@render valueSetTable(vs.valueSet)}
        </details>
      {/if}
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
  {@const validWindow = formatStateWindow(s)}
  <div class="state-detail">
    <dl class="meta">
      {#if s.variant !== "_default"}
        <dt class="micro-label">Variant</dt>
        <dd><code>{s.variant}</code></dd>
      {/if}
      <!-- #309/#321: sentinel-free, coarsest-exact window ("since 2016",
           "VT2009"); the raw ISO window stays on the tooltip. -->
      {#if validWindow}
        <dt class="micro-label">Valid</dt>
        <dd title={windowTitle(s.valid_from, s.valid_to)}>{validWindow}</dd>
      {/if}
      {#if s.value_set_version_label}
        <dt class="micro-label">Value-set version</dt>
        <dd>{s.value_set_version_label}</dd>
      {/if}
      {#if s.operational_definition}
        <dt class="micro-label">Operational definition</dt>
        <dd>{s.operational_definition}</dd>
      {/if}
    </dl>

    {#if s.classification_conformance && (s.classification_conformance.status === "severed" || s.classification_conformance.nonconforming_code_count > 0)}
      {@render conformanceNotice(s.classification_conformance)}
    {/if}

    {#if s.value_set && s.value_set.length > 0}
      {@const range = denseIntegerValueSetRange(s.value_set)}
      <h4 class="vs-heading">
        Value set <span class="muted">({s.value_set.length})</span>
      </h4>
      {#if range}
        {@render denseIntegerRange(range)}
      {:else}
        {@render valueSetTable(s.value_set)}
      {/if}
    {/if}
  </div>
{:else}
  <!-- Multiple states → the value-set-centric view (#668). With NO period this is
       the full state history; a `?period` collapses out-of-period value sets under
       a disclosure (#744). Narrowing to a single state is the PICKER's job now
       (it writes `?variant`/`?value_set_version`) — this view only DISPLAYS. -->
  {#if narrowed && scopeValueSetKeys !== null && !hasInPeriodValueSets}
    <!-- Keyed off the UNFILTERED period scope (`hasInPeriodValueSets`), NOT the
         filter-narrowed lists: the period genuinely delivered zero in-period value
         sets. A text filter that hides in-period rows falls through to the union
         branch's own "No value set matches the filter" instead (#905, Codex P3). -->
    <p class="muted picker-hint">
      No state delivered for this period. Historical value sets outside this period
      are collapsed below.
    </p>
  {:else}
    <p class="muted picker-hint">
      {valueSets.length}
      {valueSets.length === 1 ? "value set" : "value sets"} across {states.length}
      states over time.
    </p>
  {/if}

  {#if isolated}
    <!-- ISOLATED: one value set's full detail — its label/link or code table,
         then which variants/spans use it. "All value sets" returns to the union
         (the one always-present reset — replaces the old all-chips strip). The
         deep-link `?codes=<column>` lands here, scrolled into view (#905). -->
    {@const vs = isolated}
    <div class="vs-detail" bind:this={detailEl}>
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
          {@const range = denseIntegerValueSetRange(vs.valueSet)}
          {valueSetLabel(vs)}
          {#if vs.valueSet && vs.valueSet.length > 0}
            <span class="muted">({vs.valueSet.length})</span>
          {/if}
          {#if range}
            <span class="muted vs-range">{range.min}-{range.max}</span>
          {/if}
        {/if}
      </h4>
      {@render valueSetBody(vs)}
      <h5 class="vs-usage-heading micro-label">Used by</h5>
      {@render usage(vs)}
    </div>
  {:else}
    <!-- UNION (default): the distinct value sets, compact. A FilterInput narrows
         the list (a row label / variant-slug substring) — the SCALABLE form of
         the old all-chips strip — and each row carries an Isolate affordance.
         A classification value set shows the "= LKF ⟨vintage⟩" link (NO code dump
         — the M13/kommun fix); a plain one expands its codes inline (#310).
         Out-of-period value sets are collapsed below, not removed (#744). -->
    {#if valueSets.length > 1}
      <FilterInput
        bind:value={filter}
        total={valueSets.length}
        shown={filteredValueSetCount}
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
    gap: 0.3rem var(--space-4);
    margin: var(--space-2) 0 var(--space-4);
  }
  .vs-heading {
    margin: var(--space-2) 0 0.4rem;
  }
  .chip {
    padding: var(--space-1) 0.7rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    background: var(--surface);
    color: var(--accent);
    font: inherit;
    font-size: var(--text-sm);
    cursor: pointer;
  }
  .chip:hover {
    background: var(--accent-bg);
    color: var(--accent-ink);
  }
  /* #668: the "← All value sets" reset on the isolated detail — a one-click
     return to the union (replaces the old all-chips strip). */
  .vs-reset {
    font-size: var(--text-micro);
    padding: 0.1rem 0.6rem;
    margin-bottom: var(--space-2);
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
    padding: 0.4rem var(--space-2);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
  }
  .vs-row {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-2);
  }
  .vs-label {
    font-weight: 600;
  }
  .vs-count {
    font-size: 0.85em;
  }
  .vs-range {
    font-family: var(--font-mono);
    font-size: 0.85em;
  }
  .vs-isolate {
    margin-left: auto;
    font-size: var(--text-micro);
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
    font-size: var(--text-sm);
  }
  .vs-usage li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-2);
  }
  /* The variant slug is a machine identifier → mono (DESIGN.md). */
  .vs-usage-variant {
    font-family: var(--font-mono);
    font-size: 0.9em;
  }
  .vs-change-list {
    flex-basis: 100%;
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    color: var(--text-muted);
  }
  .vs-change {
    font-size: 0.9em;
  }
  .state-definitions {
    flex-basis: 100%;
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 0.15rem var(--space-2);
    margin: 0;
    color: var(--text);
  }
  .state-definitions div {
    display: contents;
  }
  .state-definitions dt {
    min-width: 0;
  }
  .state-definitions dd {
    min-width: 0;
    margin: 0;
    color: var(--text-muted);
  }
  .state-definitions code,
  .state-definitions dd {
    overflow-wrap: anywhere;
  }
  @media (min-width: 42rem) {
    .state-definitions {
      grid-template-columns: minmax(8rem, 16rem) minmax(0, 1fr);
    }
  }
  .vs-codes {
    margin-top: 0.1rem;
  }
  .vs-codes summary {
    cursor: pointer;
    font-size: var(--text-sm);
    color: var(--accent);
  }
  .vs-detail {
    margin-top: var(--space-1);
  }
  .vs-classification {
    margin: var(--space-1) 0;
  }
  .vs-numeric-range {
    margin: var(--space-1) 0;
    font-size: var(--text-sm);
  }
  .vs-numeric-range code {
    font-family: var(--font-mono);
  }
  .conformance-notice {
    margin: var(--space-1) 0;
    padding: var(--space-2);
    border: 1px solid var(--border);
    border-left: 3px solid var(--warn);
    border-radius: var(--radius-sm);
    background: var(--warn-bg);
    color: var(--text);
    font-size: var(--text-sm);
  }
  .conformance-notice.severed {
    border-left-color: var(--err);
  }
  .conformance-notice p {
    margin: 0;
  }
  .conformance-notice details {
    margin-top: var(--space-1);
  }
  .conformance-notice summary {
    cursor: pointer;
    color: var(--accent);
  }
  .vs-usage-heading {
    margin: var(--space-3) 0 0.3rem;
  }
  .out-of-period {
    margin-top: var(--space-2);
  }
  .out-of-period summary {
    cursor: pointer;
    color: var(--accent);
    font-size: var(--text-sm);
  }
  .out-of-period-list {
    margin-top: 0.4rem;
  }
</style>
