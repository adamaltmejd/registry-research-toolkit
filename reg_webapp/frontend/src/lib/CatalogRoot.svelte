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

// Same type-to-filter affordance as the deeper browse lists, for consistency
// (this list is short today, but the markup/behavior stays uniform). Match on
// display name and FQID.
let filter = $state("");
const children = $derived(root.data?.children ?? []);
const filtered = $derived(
  children.filter((c) => matchesFilter(filter, c.name, c.fqid)),
);

// One "Provider" column: the name renders as a catalog link via the `cell`
// escape hatch (DataTable's default would print the raw value). The FQID is
// dropped — the link's display name is the identity now.
type Child = (typeof children)[number];
const columns: Column<Child>[] = [{ key: "name", label: "Provider" }];
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
      total={children.length}
      shown={filtered.length}
      placeholder="Filter providers…"
      label="Filter providers"
    />
    {#if filtered.length > 0}
      <DataTable {columns} rows={filtered}>
        {#snippet cell(child)}
          <a class="row-link" href={catalogHref(child.fqid)} title={child.fqid}>
            {child.name ?? child.fqid}
          </a>
        {/snippet}
      </DataTable>
    {:else}
      <EmptyState title={`No providers match “${filter}”`} />
    {/if}
  {/if}
</article>

<style>
  .row-link {
    font-weight: 600;
  }
</style>
