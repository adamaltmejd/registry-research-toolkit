<script lang="ts">
import { getValues, type ValueRow } from "./api";
import { asyncResource } from "./async.svelte";
import CodeList from "./CodeList.svelte";
import FilterInput from "./FilterInput.svelte";
import { Button, Skeleton } from "./ui";

// The (code, label) viewer for ONE set of codes, read a page at a time (Y-46)
// through the `values` operation: one state's value set (`ref` the variable,
// `stateId` the state), that state's part against a declared classification
// (`classification` + `partition`), or a classification's own codes (`ref` the
// classification, no state).
//
// A variable's states carry each state's `value_set_id` and a
// cardinality-independent summary, NOT the members — a variable whose 290
// yearly states share a few large codings would otherwise ship those codings 290
// times over. So this panel is where the codes are actually fetched, and it is
// mounted only where a code table is shown: a CLOSED disclosure holds no panel
// and therefore issues no request.
//
// Filtering is SERVER-side and covers the COMPLETE set, not the pages already
// loaded — the server folds diacritics and case exactly as `matchesFilter` does
// in the browser, and reports how many of the whole set matched. Filter, then
// window: that composition order is what keeps "12 of 740" true.
//
// PRESENTATION only — no navigation, no resolution state. Rendering stays the
// shared CodeList so a value set, a classification's codes and a mismatch list
// all look alike; CodeList renders these pages verbatim because the filter and
// the bound both live here.

interface Props {
  /** Whose codes to read: a variable FQID (with `stateId`) or a classification
   * FQID (`class/<slug>`, no state). */
  ref: string;
  /** The variable's state whose value set is read; with `classification`, its
   * stored part against that declared classification instead. */
  stateId?: string | null;
  partition?: "canonical" | "source_extensions" | "nonstandard" | "sentinels";
  /** The declared classification's ref (`class/<slug>`). A bare slug would be
   * resolved as a name, which can be ambiguous. */
  classification?: string;
  column?: string | null;
  aliasWindowFrom?: string | null;
  /** How many codes the whole (unfiltered) set has — from the leaf's summary, so
   * the filter affordance and its count render before the first page lands. Null
   * when the caller does not know: the first unfiltered page's `total` decides. */
  codeCount: number | null;
  filterLabel?: string;
  filterPlaceholder?: string;
  /** The line for a set with no codes at all. */
  emptyText?: string;
}

let {
  ref,
  stateId = null,
  partition = "source_extensions",
  classification,
  column = null,
  aliasWindowFrom = null,
  codeCount,
  filterLabel = "Filter codes",
  filterPlaceholder = "Filter codes…",
  emptyText = "This value set has no codes.",
}: Props = $props();

// One request covers the ordinary coding; it is also the server's ceiling.
const PAGE_SIZE = 200;
// Below this many codes the filter box is hidden — per the maintainer: pointless
// for a handful of items (a small classification or short value set).
const CODE_FILTER_THRESHOLD = 5;
// This filter runs on the server, over the whole set — so a burst of keystrokes
// has to become ONE read, not one per character (SearchOmnibox's idiom).
const FILTER_DEBOUNCE_MS = 200;

let filter = $state("");
// What the server was last asked for: `filter` once the typing settles. Keeping
// the two apart is what makes the box responsive while the reads stay bounded.
let query = $state("");
// Bumped by Retry, and READ inside the fetch, so a failed page re-requests
// without anything about the request changing.
let attempt = $state(0);

$effect(() => {
  const typed = filter;
  if (typed.trim() === query.trim()) {
    return;
  }
  const timer = setTimeout(() => {
    query = typed;
  }, FILTER_DEBOUNCE_MS);
  return () => clearTimeout(timer);
});

// The identity of the SET being read, and of the filtered read of it. Everything
// accumulated is keyed on the latter, so a new set, a new state or a new query
// drops the earlier pages instead of appending to them — no reset effect, and so
// no effect-ordering hazard.
const setIdentity = $derived(
  `${ref}:${stateId ?? ""}:${partition}:${classification ?? ""}:${column ?? ""}:${aliasWindowFrom ?? ""}`,
);
const setKey = $derived(`${setIdentity}:${query.trim()}`);

// The pages BEFORE the one in flight, the matching total they were counted
// against, and the cursor that continues after them (so the progress line and
// the "load more" bound survive the next page's flight). `key` is what makes a
// stale accumulation self-invalidate.
type Carried = {
  codes: ValueRow[];
  total: number | null;
  cursor: string | null;
};
let accumulated = $state<Carried & { key: string }>({
  key: "",
  codes: [],
  total: null,
  cursor: null,
});
const carried = $derived<Carried>(
  accumulated.key === setKey
    ? accumulated
    : { codes: [], total: null, cursor: null },
);

const resource = asyncResource((signal) => {
  void attempt;
  // The leaf already counted this set: a known-empty coding has no page to ask
  // for, and the filter box (which is what could ask for a different count) is
  // not shown below the threshold. Answer it here rather than spend a round trip
  // and a skeleton on being told zero.
  if (codeCount === 0) {
    return Promise.resolve({ items: [], next_cursor: null, total: 0 });
  }
  return getValues(
    ref,
    {
      state: stateId,
      // A partition is a part of a declared classification's book; without one
      // the state's whole value set is read.
      partition: classification ? partition : undefined,
      classification,
      column: column ?? undefined,
      alias_window_from: aliasWindowFrom ?? undefined,
      q: query.trim(),
      cursor: carried.cursor,
      limit: PAGE_SIZE,
    },
    { signal },
  );
});

const codes = $derived([...carried.codes, ...(resource.data?.items ?? [])]);
// Where the next page starts; null once the server says the set is exhausted.
// While a later page is in flight it is the cursor that page was asked from, so
// the "load more" button stays put (busy) instead of vanishing under the reader.
const nextCursor = $derived(
  resource.data ? (resource.data.next_cursor ?? null) : carried.cursor,
);
// How many codes match `filter` across the WHOLE set, per the server — null
// until it has said, so the count is withheld instead of claiming the full size.
const total = $derived(resource.data?.total ?? carried.total);
// The count belongs to the query the SERVER answered, so it is withheld while the
// box is ahead of it (mid-debounce, mid-flight) rather than restating the count
// of a query the reader has already moved off.
const shownTotal = $derived(filter.trim() === query.trim() ? total : null);
const filtering = $derived(query.trim().length > 0);

// The whole set's size: the caller's count, else the `total` of the first
// unfiltered page (a filtered `total` counts matches, not the set, so it never
// stands in). Remembered per set so typing a filter can't make the box vanish.
let answeredSize = $state<{ identity: string; size: number } | null>(null);
$effect(() => {
  const data = resource.data;
  if (codeCount === null && data && !filtering) {
    answeredSize = { identity: setIdentity, size: data.total };
  }
});
const setSize = $derived(
  codeCount ??
    (answeredSize?.identity === setIdentity ? answeredSize.size : null),
);
// The filter box appears at the same set size the shared viewer has always shown
// it at — below that it is more chrome than help.
const showFilter = $derived(
  setSize !== null && setSize >= CODE_FILTER_THRESHOLD,
);

function loadMore(): void {
  // Busy, not disabled: disabling the button the reader just pressed hands focus
  // back to <body>, so a second press mid-flight has to be a no-op instead.
  if (resource.loading || nextCursor === null) {
    return;
  }
  accumulated = { key: setKey, codes, total, cursor: nextCursor };
}

function retry(): void {
  attempt += 1;
}
</script>

<div class="value-set-codes">
  {#if showFilter}
    <FilterInput
      bind:value={filter}
      total={setSize ?? 0}
      shown={shownTotal}
      label={filterLabel}
      placeholder={filterPlaceholder}
    />
  {/if}

  {#if codes.length > 0}
    <CodeList {codes} />
  {/if}

  {#if resource.loading}
    <div class="codes-status" aria-busy="true">
      <span class="visually-hidden">Loading codes…</span>
      <Skeleton count={codes.length > 0 ? 1 : 3} />
    </div>
  {:else if resource.error}
    <div class="codes-error" role="alert">
      <p class="error"><span aria-hidden="true">✕</span> Could not load codes: {resource.error}</p>
      <Button size="sm" onclick={retry}>Retry</Button>
    </div>
  {:else if codes.length === 0}
    <p class="codes-empty">
      <!-- The shared viewer's own wording for the same two cases, because this
           renders INSIDE it; `EmptyState` is for a route's empty list, not for a
           line inside a disclosure. -->
      {#if filtering}
        No codes match “{query}”.
      {:else}
        {emptyText}
      {/if}
    </p>
  {/if}

  {#if nextCursor !== null && !resource.error}
    <div class="codes-more">
      <!-- Outside the status chain, so the button the reader just pressed is NOT
           swapped for a skeleton mid-page — that drops keyboard focus to <body>
           on every page. It stays put and reports itself busy. -->
      <Button size="sm" onclick={loadMore} aria-busy={resource.loading}>
        Load more codes
      </Button>
      {#if total !== null}
        <span class="codes-progress">Showing {codes.length} of {total} codes.</span>
      {/if}
    </div>
  {/if}
</div>

<style>
  /* Stretched, not shrink-wrapped: the filter box has to span the panel like
     every other filter on the page rather than hug its placeholder. */
  .value-set-codes {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
  .codes-empty,
  .codes-progress {
    margin: 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* A status row, not a banner: the error role with a leading glyph, so the
     failure reads without hue carrying it (frontend/DESIGN.md → status) and is
     told apart from the warn-tinted conformance notice around it. */
  .codes-error,
  .codes-more {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-3);
  }
  .codes-error p {
    margin: 0;
    font-size: var(--text-sm);
  }
</style>
