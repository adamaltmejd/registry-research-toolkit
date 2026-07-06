<script lang="ts">
import { type ClassificationNodeData, getBindingGraph } from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import ClassificationEditionGraph from "./ClassificationEditionGraph.svelte";
import ClassificationRelatedLinks from "./ClassificationRelatedLinks.svelte";
import { nodeLabel } from "./catalog";
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
  <ClassificationRelatedLinks {node} />
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
</style>
