<script lang="ts">
import { getCatalogRoot } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, DATA_BROWSER_LABEL, matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";
import { type Column, DataTable, EmptyState } from "./ui";

// The catalog root: every provider plus the classification-root sentinel
// (`class`). Children are a `kind`-tagged union (`provider` | `classification-
// root`); both link via path-based URLs mirroring the API.
const root = asyncResource(() => getCatalogRoot());

// Same type-to-filter affordance as the deeper browse lists, for consistency.
// Match on display name and FQID only: the root page is navigation, not metadata.
let filter = $state("");
const children = $derived(root.data?.children ?? []);
type Child = (typeof children)[number];
type RootRow = {
  fqid: string;
  name?: string | null;
};

function rootRow(child: Child): RootRow {
  return {
    fqid: child.fqid,
    name: child.name,
  };
}

const rows = $derived(children.map(rootRow));
const filtered = $derived(
  rows.filter((r) => matchesFilter(filter, r.name, r.fqid)),
);

const columns: Column<RootRow>[] = [{ key: "name", label: "Name" }];
</script>

<article>
  <h2>{DATA_BROWSER_LABEL}</h2>
  {#if root.loading}
    <p class="muted" aria-busy="true">Loading…</p>
  {:else if root.error}
    <p class="error" role="alert">Failed to load catalog: {root.error}</p>
  {:else if root.data}
    <FilterInput
      bind:value={filter}
      total={rows.length}
      shown={filtered.length}
      placeholder="Filter catalog sections…"
      label="Filter catalog sections"
    />
    {#if filtered.length > 0}
      <DataTable framed {columns} rows={filtered} rowNavigation>
        {#snippet cell(row)}
          <a class="row-link" href={catalogHref(row.fqid)} title={row.fqid}>
            {row.name ?? row.fqid}
          </a>
        {/snippet}
      </DataTable>
    {:else}
      <EmptyState title={`No catalog sections match “${filter}”`} />
    {/if}
  {/if}
</article>

<style>
  .row-link {
    font-weight: 600;
    /* Long-name breaking now comes from DataTable's cell-level
       `overflow-wrap: anywhere`, which inherits into this link (#832). */
  }
</style>
