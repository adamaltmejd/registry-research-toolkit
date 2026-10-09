<script lang="ts">
import type { ClassificationShow, Derivation } from "./api";
import { catalogHref } from "./catalog";

let { node }: { node: ClassificationShow } = $props();

const hasDerivedRefs = $derived(
  (node.derived_from?.length ?? 0) > 0 || (node.derivatives?.length ?? 0) > 0,
);
</script>

<!-- A derivation names its classification by FQID; one outside the catalog (no
     FQID) reads as text, with no link to follow. -->
{#snippet derivation(ref: Derivation)}
  {#if ref.fqid}
    <a href={catalogHref(ref.fqid)}>{ref.short_name ?? ref.fqid}</a>
  {:else}
    <span class="ref-code">{ref.short_name}</span>
  {/if}
  <span class="ref-name">{ref.name}</span>
  {#if ref.note}
    <span class="ref-note">{ref.note}</span>
  {/if}
{/snippet}

{#if hasDerivedRefs}
  <section
    class="derived-links"
    aria-labelledby="derived-classifications-heading"
  >
    <h3 id="derived-classifications-heading">Related classifications</h3>
    {#if node.derived_from && node.derived_from.length > 0}
      <div class="derived-block">
        <p class="micro-label link-label">Derived from</p>
        <ul>
          {#each node.derived_from as ref, i (ref.fqid ?? i)}
            <li>{@render derivation(ref)}</li>
          {/each}
        </ul>
      </div>
    {/if}
    {#if node.derivatives && node.derivatives.length > 0}
      <div class="derived-block">
        <p class="micro-label link-label">Derived classifications</p>
        <ul>
          {#each node.derivatives as ref, i (ref.fqid ?? i)}
            <li>{@render derivation(ref)}</li>
          {/each}
        </ul>
      </div>
    {/if}
  </section>
{/if}

<style>
  .derived-links {
    margin: var(--space-4) 0;
    padding: var(--space-4);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--surface);
  }
  .derived-links h3 {
    margin: 0 0 var(--space-3);
    font-size: var(--text-h3);
    font-weight: var(--heading-weight);
    line-height: 1.25;
  }
  .derived-block + .derived-block {
    margin-top: var(--space-4);
  }
  .link-label {
    margin: 0 0 var(--space-2);
    color: var(--text-faint);
  }
  .derived-links ul {
    display: grid;
    gap: var(--space-2);
    padding: 0;
    margin: 0;
    list-style: none;
  }
  .derived-links li {
    display: grid;
    grid-template-columns: max-content minmax(0, 1fr);
    gap: var(--space-1) var(--space-3);
    align-items: baseline;
  }
  .ref-code {
    font-family: var(--font-mono);
  }
  .derived-links a {
    font-family: var(--font-mono);
    color: var(--accent-ink);
    text-decoration: none;
  }
  .derived-links a:hover {
    text-decoration: underline;
  }
  .derived-links a:focus-visible {
    border-radius: var(--radius-sm);
    box-shadow: var(--focus-ring);
    outline: none;
  }
  .ref-name {
    min-width: 0;
    color: var(--text);
  }
  .ref-note {
    grid-column: 2;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  @media (max-width: 48rem) {
    .derived-links li {
      grid-template-columns: minmax(0, 1fr);
    }
    .ref-note {
      grid-column: auto;
    }
  }
</style>
