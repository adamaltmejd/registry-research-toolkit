<script lang="ts">
import { getValueSetCodes, type ValueSetMemberModel } from "./api";
import { asyncResource } from "./async.svelte";
import CodeList, { CODE_FILTER_THRESHOLD } from "./CodeList.svelte";
import FilterInput from "./FilterInput.svelte";
import { Button, Skeleton } from "./ui";

// The (code, label) viewer for ONE coding, read a page at a time (Y-46).
//
// The binding leaf and its `?period` subset carry each state's `value_set_id`
// and a cardinality-independent summary, NOT the members — a variable whose 290
// yearly states share a few large codings would otherwise ship those codings 290
// times over. So this panel is where the codes are actually fetched, and it is
// mounted only where a code table is shown: a CLOSED disclosure holds no panel
// and therefore issues no request.
//
// Filtering is SERVER-side and covers the COMPLETE set, not the pages already
// loaded — the backend folds diacritics and case exactly as `matchesFilter` does
// in the browser, and reports how many of the whole set matched. Filter, then
// window: that composition order is what keeps "12 of 740" true.
//
// PRESENTATION only — no navigation, no resolution state. Rendering stays the
// shared CodeList so a value set, a classification's codes and a mismatch list
// all look alike; CodeList renders these pages verbatim (`paged`) because the
// filter and the bound both live here.

interface Props {
  /** The coding to read. */
  valueSetId: number;
  /** Set to read that state's stored classification MISMATCH list instead of the
   * value set's own membership. The state must carry `valueSetId`. */
  stateId?: number | null;
  /** How many codes the whole (unfiltered) set has — from the leaf's summary, so
   * the filter affordance and its count render before the first page lands. */
  codeCount: number;
  filterLabel?: string;
  filterPlaceholder?: string;
}

let {
  valueSetId,
  stateId = null,
  codeCount,
  filterLabel = "Filter codes",
  filterPlaceholder = "Filter codes…",
}: Props = $props();

// One request covers the ordinary coding; the server's own ceiling is higher.
const PAGE_SIZE = 200;
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

// The identity of the SET being read. Everything accumulated is keyed on it, so
// a new coding, a new state or a new query drops the earlier pages instead of
// appending to them — no reset effect, and so no effect-ordering hazard.
const setKey = $derived(`${valueSetId}:${stateId ?? ""}:${query.trim()}`);

// The pages BEFORE the one in flight, plus the matching total they were counted
// against (so the progress line and the "load more" bound survive the next
// page's flight). `key` is what makes a stale accumulation self-invalidate.
let accumulated = $state<{
  key: string;
  codes: ValueSetMemberModel[];
  total: number | null;
}>({ key: "", codes: [], total: null });
const carried = $derived(
  accumulated.key === setKey ? accumulated : { codes: [], total: null },
);

const resource = asyncResource((signal) => {
  void attempt;
  return getValueSetCodes(
    valueSetId,
    {
      state: stateId,
      q: query,
      offset: carried.codes.length,
      limit: PAGE_SIZE,
    },
    { signal },
  );
});

const codes = $derived([...carried.codes, ...(resource.data?.codes ?? [])]);
// How many codes match `filter` across the WHOLE set, per the server — null
// until it has said, so the count is withheld instead of claiming the full size.
const total = $derived(resource.data?.total ?? carried.total);
// The count belongs to the query the SERVER answered, so it is withheld while the
// box is ahead of it (mid-debounce, mid-flight) rather than restating the count
// of a query the reader has already moved off.
const shownTotal = $derived(filter.trim() === query.trim() ? total : null);
// The filter box appears at the same set size the shared viewer has always shown
// it at — below that it is more chrome than help.
const showFilter = $derived(codeCount >= CODE_FILTER_THRESHOLD);
const filtering = $derived(query.trim().length > 0);

function loadMore(): void {
  // Busy, not disabled: disabling the button the reader just pressed hands focus
  // back to <body>, so a second press mid-flight has to be a no-op instead.
  if (resource.loading) {
    return;
  }
  accumulated = { key: setKey, codes, total };
}

function retry(): void {
  attempt += 1;
}
</script>

<div class="value-set-codes">
  {#if showFilter}
    <FilterInput
      bind:value={filter}
      total={codeCount}
      shown={shownTotal}
      label={filterLabel}
      placeholder={filterPlaceholder}
    />
  {/if}

  {#if codes.length > 0}
    <CodeList {codes} paged />
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
        This value set has no codes.
      {/if}
    </p>
  {/if}

  {#if total !== null && codes.length < total && !resource.error}
    <div class="codes-more">
      <!-- Outside the status chain, so the button the reader just pressed is NOT
           swapped for a skeleton mid-page — that drops keyboard focus to <body>
           on every page. It stays put and reports itself busy. -->
      <Button size="sm" onclick={loadMore} aria-busy={resource.loading}>
        Load more codes
      </Button>
      <span class="codes-progress">Showing {codes.length} of {total} codes.</span>
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
