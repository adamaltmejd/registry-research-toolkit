<script lang="ts">
import { type ClassificationNodeData, getBindingGraph } from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import ClassificationEditionGraph from "./ClassificationEditionGraph.svelte";
import { catalogHref, nodeLabel } from "./catalog";
import SubjectView from "./SubjectView.svelte";

// The classification LEAF — a standard ("Utbildningsnivå") rendered through the
// unified SubjectView shell (#638 PR1). The node EMBEDS its codes, so the codes
// panel renders synchronously. The picker surface owns the compact classification
// edition DAG (#906) over the relationship-graph contract (#761/#792); it is
// navigational/read-only for now, not an add-to-project picker. No period picker,
// docs surface, or LineageDetails here — classifications carry no study-window or
// lineage/warnings.
let { node }: { node: ClassificationNodeData } = $props();

// The relationship graph for this classification edition (#678). Its OWN failure
// domain: an error / empty (`nodes: []`) / unresolved fetch omits the graph and never
// blanks the leaf (the codes + meta render synchronously regardless).
const graphResource = asyncResource(() => getBindingGraph(node.fqid));
const graph = $derived(graphResource.data);
const graphReady = $derived(
  !graphResource.loading && !graphResource.error && graph != null,
);
const hasDerivedRefs = $derived(
  (node.derived_from?.length ?? 0) > 0 || (node.derivatives?.length ?? 0) > 0,
);

function classRefHref(ref: { fqid: string | null; slug: string }): string {
  return catalogHref(ref.fqid ?? `class/${ref.slug}`);
}
</script>

{#snippet description()}
  <dl class="meta">
    <dt>Short name</dt>
    <dd>{node.short_name}</dd>
  </dl>
{/snippet}

<!-- #609: the embedded value-set code viewer (the resolved edition's codes,
     in-memory filterable). Omits itself when empty. -->
{#snippet valueSet()}
  <ClassificationCodesPanel {node} />
{/snippet}

<!-- #906: a compact non-timeline edition DAG in the picker slot. Omits itself on an
     empty graph or while the fetch is unresolved/errored (its own failure domain). -->
{#snippet picker()}
  {#if graphReady && graph}
    <ClassificationEditionGraph {graph} />
  {/if}
{/snippet}

<!-- Non-temporal classification derivation links live in the relationships slot;
     temporal succession stays in the compact picker graph above. -->
{#snippet relationships()}
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
            {#each node.derived_from as ref (ref.slug)}
              <li>
                <a href={classRefHref(ref)}>{ref.short_name}</a>
                <span class="ref-name">{ref.name}</span>
                {#if ref.note}
                  <span class="ref-note">{ref.note}</span>
                {/if}
              </li>
            {/each}
          </ul>
        </div>
      {/if}
      {#if node.derivatives && node.derivatives.length > 0}
        <div class="derived-block">
          <p class="micro-label link-label">Derived classifications</p>
          <ul>
            {#each node.derivatives as ref (ref.slug)}
              <li>
                <a href={classRefHref(ref)}>{ref.short_name}</a>
                <span class="ref-name">{ref.name}</span>
                {#if ref.note}
                  <span class="ref-note">{ref.note}</span>
                {/if}
              </li>
            {/each}
          </ul>
        </div>
      {/if}
    </section>
  {/if}
{/snippet}

<SubjectView
  title={nodeLabel(node)}
  fqid={node.fqid}
  {description}
  {picker}
  {valueSet}
  {relationships}
/>

<style>
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.35rem 1rem;
    margin: 1rem 0;
  }
  .meta dt {
    font-weight: 600;
  }
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
