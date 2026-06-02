<script lang="ts">
import { getCatalogRoot } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref } from "./catalog";

// The catalog root: every provider plus the classification-root sentinel
// (`class`). Children are a `kind`-tagged union (`provider` | `classification-
// root`); both link via path-based URLs mirroring the API.
const root = asyncResource(() => getCatalogRoot());
</script>

<article>
  <h2>Catalog</h2>
  {#if root.loading}
    <p class="muted">Loading…</p>
  {:else if root.error}
    <p class="error" role="alert">Failed to load catalog: {root.error}</p>
  {:else if root.data}
    <ul class="children">
      {#each root.data.children as child (child.fqid)}
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
