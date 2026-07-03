<script lang="ts">
import { type ClassificationNodeData, getBindingGraph } from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import { nodeLabel } from "./catalog";
import HistoryGraph from "./HistoryGraph.svelte";
import SubjectView from "./SubjectView.svelte";

// The classification LEAF — a standard ("Utbildningsnivå") rendered through the
// unified SubjectView shell (#638 PR1). The node EMBEDS its codes, so the codes
// panel renders synchronously. The relationships surface is the #678 unified
// history graph over the relationship-graph contract (#761/#792): the route serves
// classification leaves now (`getBindingGraph(node.fqid)` dispatches on FQID kind),
// and the renderer draws editions as version-ordered points with succession edges.
// No period picker yet (an edition picker is a later PR), and no docs surface
// (classifications carry no doc mentions), so those two SubjectView sections are
// omitted. No LineageDetails here either — classifications carry no lineage/warnings.
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

<!-- #678: the unified history graph — editions as version-ordered points
     (succession edges, Fork B group clusters). Omits itself on an empty graph or while
     the fetch is unresolved/errored (its own failure domain). -->
{#snippet relationships()}
  {#if graphReady && graph}
    <HistoryGraph {graph} />
  {/if}
{/snippet}

<SubjectView
  title={nodeLabel(node)}
  fqid={node.fqid}
  {description}
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
