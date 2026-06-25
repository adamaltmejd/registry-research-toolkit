<script lang="ts">
import {
  type ClassificationNodeData,
  getCatalogNode,
  isCatalogNode,
} from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import ClassificationDimensionsPanel from "./ClassificationDimensionsPanel.svelte";
import ClassificationLineagePanels from "./ClassificationLineagePanels.svelte";
import { nodeLabel } from "./catalog";
import HistoryGraphPrototype from "./HistoryGraphPrototype.svelte";
import {
  historyGraphFromClassification,
  historyGraphFromClassificationGroup,
} from "./history_graph";
import SubjectView from "./SubjectView.svelte";

// The classification LEAF — a standard ("Utbildningsnivå") rendered through the
// unified SubjectView shell (#638 PR1). Extracted verbatim from CatalogNodeView's
// `kind === "classification"` arm so all three leaf kinds share one shell. The
// node EMBEDS everything (codes / dimensions / edition_chain), so the panels render
// synchronously — this view owns no fetch. No period picker yet (an edition picker
// is a later PR), and no docs surface (classifications carry no doc mentions), so
// those two SubjectView sections are omitted.
let {
  node,
  vintageYear,
}: { node: ClassificationNodeData; vintageYear?: number } = $props();
const graphGroup = $derived(
  (node.dimensions ?? []).find((group) =>
    group.members.some((member) => member.fqid === node.fqid),
  ) ?? null,
);
const graphMembers = asyncResource<ClassificationNodeData[]>(async (signal) => {
  if (!graphGroup) {
    return [];
  }
  return Promise.all(
    graphGroup.members.map(async (member) => {
      const resolved = await getCatalogNode(member.fqid, undefined, { signal });
      if (isCatalogNode(resolved) && resolved.kind === "classification") {
        return resolved;
      }
      throw new Error(`Expected classification node for ${member.fqid}`);
    }),
  );
});
const historyGraph = $derived(
  graphGroup && graphMembers.data
    ? historyGraphFromClassificationGroup(
        graphGroup,
        graphMembers.data,
        node.fqid,
      )
    : historyGraphFromClassification(node),
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

<!-- The niva ↔ aggregate granularity cross-reference (#609) + the embedded edition
     succession chain (#571, oldest → current). Each omits itself when empty / for a
     standalone classification with no succession. -->
{#snippet relationships()}
  {#if graphGroup && graphMembers.loading}
    <p class="muted" aria-busy="true">Loading group graph…</p>
  {:else if graphGroup && graphMembers.error}
    <p class="error" role="alert">{graphMembers.error}</p>
  {:else}
    <HistoryGraphPrototype graph={historyGraph} {vintageYear} />
  {/if}
  <ClassificationDimensionsPanel {node} />
  <ClassificationLineagePanels {node} />
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
