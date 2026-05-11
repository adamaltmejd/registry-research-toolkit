<script lang="ts">
  import {
    columnHasConcern,
    store,
    type ConcernFilter,
  } from "../store.svelte";
  import { TYPE_LABEL_SHORT } from "../types";
  import type { ColumnInfo, ColumnType, StateSnapshot } from "../types";
  import ColumnsPicker from "./ColumnsPicker.svelte";

  interface Props {
    snapshot: StateSnapshot;
  }

  let { snapshot }: Props = $props();

  const TYPES: ColumnType[] = ["id", "categorical", "numeric", "opaque", "date"];
  const CONCERNS: ConcernFilter[] = [
    "manual",
    "mismatch",
    "unmatched",
    "opaque",
  ];
  const CONCERN_LABEL: Record<ConcernFilter, string> = {
    manual: "manual",
    mismatch: "regmeta mismatch",
    unmatched: "unmatched categorical",
    opaque: "opaque",
  };

  // Counts go over every column in every source — we want the chip
  // labels to reflect total cells the filter would catch, not unique
  // partitions in by-column view. Total ≈ sum over (source, column);
  // matches the per-type counts shown in the snapshot summary.
  let allColumns = $derived.by<ColumnInfo[]>(() => {
    const out: ColumnInfo[] = [];
    for (const g of snapshot.groups) {
      for (const cols of Object.values(g.columns_by_source)) {
        for (const c of cols) out.push(c);
      }
    }
    return out;
  });

  let typeCounts = $derived.by(() => {
    const m: Record<ColumnType, number> = {
      id: 0,
      categorical: 0,
      numeric: 0,
      opaque: 0,
      date: 0,
    };
    for (const c of allColumns) m[c.current_type]++;
    return m;
  });

  let concernCounts = $derived.by(() => {
    const m: Record<ConcernFilter, number> = {
      manual: 0,
      mismatch: 0,
      unmatched: 0,
      opaque: 0,
    };
    for (const c of allColumns) {
      for (const k of CONCERNS) {
        if (columnHasConcern(c, k)) m[k]++;
      }
    }
    return m;
  });
</script>

<section class="filter-bar" aria-label="Variable filters">
  <div class="row">
    <input
      type="search"
      placeholder="Search variable name…"
      value={store.filterQuery}
      oninput={(e) =>
        store.setFilterQuery((e.target as HTMLInputElement).value)}
      aria-label="Search variable name"
    />
    {#if store.hasActiveFilters()}
      <button
        type="button"
        class="clear"
        onclick={() => store.clearFilters()}
        title="Clear all filters"
      >
        Clear filters
      </button>
    {/if}
    <ColumnsPicker />
  </div>

  <div class="row chips" aria-label="Type filter">
    <span class="chip-label">Type</span>
    {#each TYPES as t (t)}
      {@const n = typeCounts[t]}
      <button
        type="button"
        class="chip type-{t}"
        class:active={store.filterType === t}
        class:empty={n === 0}
        onclick={() => store.toggleFilterType(t)}
        disabled={n === 0 && store.filterType !== t}
        title={`Show only ${t} variables (${n})`}
      >
        {TYPE_LABEL_SHORT[t]}
        <span class="count">{n}</span>
      </button>
    {/each}
  </div>

  <div class="row chips" aria-label="Concern filter">
    <span class="chip-label">Review</span>
    {#each CONCERNS as c (c)}
      {@const n = concernCounts[c]}
      <button
        type="button"
        class="chip concern concern-{c}"
        class:active={store.filterConcern === c}
        class:empty={n === 0}
        onclick={() => store.toggleFilterConcern(c)}
        disabled={n === 0 && store.filterConcern !== c}
        title={`Show only ${CONCERN_LABEL[c]} (${n})`}
      >
        {CONCERN_LABEL[c]}
        <span class="count">{n}</span>
      </button>
    {/each}
  </div>
</section>

<style>
  /* Pinned just below the sticky app header so the search field and
     chips stay reachable while reviewing a long group's column table.
     `top` reads `--mdw-header-height` (set by App.svelte via a
     ResizeObserver) so the bar lands flush against whatever height
     the header rendered at — the meta line wraps on narrow viewports
     and a hardcoded rem would either gap or overlap. Falls back to
     a safe estimate when the variable is unset. */
  .filter-bar {
    position: sticky;
    top: var(--mdw-header-height, 5.6rem);
    z-index: 40;
    max-width: 72rem;
    margin: 0.75rem auto 0;
    padding: 0.6rem 1rem;
    background: #fff;
    border: 1px solid #e1e1e1;
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06);
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  input[type="search"] {
    flex: 1 1 16rem;
    min-width: 12rem;
    padding: 0.4rem 0.6rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
  }
  input[type="search"]:focus {
    outline: 2px solid #1656c0;
    outline-offset: 1px;
  }
  .clear {
    padding: 0.35rem 0.7rem;
    border: 1px solid #ccc;
    background: #fff;
    border-radius: 4px;
    cursor: pointer;
    font: inherit;
    color: #444;
  }
  .clear:hover {
    background: #f5f5f5;
  }
  .chip-label {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #888;
    margin-right: 0.2rem;
    flex: 0 0 auto;
  }
  .chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.18rem 0.55rem;
    border: 1px solid #d0d0d0;
    background: #fff;
    border-radius: 999px;
    cursor: pointer;
    font: inherit;
    font-size: 0.85rem;
    color: #333;
    line-height: 1.3;
  }
  .chip:hover:not(:disabled) {
    background: #f7f7f9;
  }
  .chip:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }
  .chip.empty:not(.active) {
    /* Visible but quiet: zero-count chips still say "no manual edits"
       at a glance, which is reassuring rather than missing. */
    color: #aaa;
  }
  .chip.active {
    background: #1656c0;
    color: #fff;
    border-color: #1656c0;
  }
  .chip.active .count {
    background: rgba(255, 255, 255, 0.25);
    color: #fff;
  }
  .count {
    background: rgba(0, 0, 0, 0.08);
    color: #555;
    padding: 0 0.4rem;
    border-radius: 999px;
    font-size: 0.78em;
    font-weight: 600;
  }
  /* Color-mirror the type chips to the pills so the user wires the
     two together visually. Active state still wins. */
  .chip.type-id:not(.active):not(:disabled) {
    border-color: #c4dbf3;
  }
  .chip.type-categorical:not(.active):not(:disabled) {
    border-color: #d8c8f0;
  }
  .chip.type-numeric:not(.active):not(:disabled) {
    border-color: #c5e4cf;
  }
  .chip.type-date:not(.active):not(:disabled) {
    border-color: #f0d7b6;
  }
  .chip.type-opaque:not(.active):not(:disabled) {
    border-color: #d8d0bf;
  }
  .chip.concern:not(.active):not(:disabled) {
    border-color: #ecc8b0;
    color: #884a14;
  }
</style>
