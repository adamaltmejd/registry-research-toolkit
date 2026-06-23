<script lang="ts">
import { getCatalogRoot } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, DATA_BROWSER_LABEL, matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";

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
      <ul class="children">
        {#each filtered as child (child.fqid)}
          <li>
            <a href={catalogHref(child.fqid)}>
              <span class="label">
                {child.kind === "classification-root" ? child.name : (child.name ?? child.fqid)}
              </span>
              <code class="child-fqid">{child.fqid}</code>
            </a>
          </li>
        {/each}
      </ul>
    {:else}
      <p class="muted">No providers match “{filter}”.</p>
    {/if}
  {/if}
</article>

<style>
  .children {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .children li a {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .children .label {
    font-weight: 600;
  }
  .child-fqid {
    color: var(--muted);
    font-size: 0.85em;
  }
</style>
