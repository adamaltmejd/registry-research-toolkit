<script lang="ts">
import {
  type ClassificationNodeData,
  type ConceptGroup,
  getCatalogNode,
  isCatalogNode,
} from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import ClassificationDimensionsPanel from "./ClassificationDimensionsPanel.svelte";
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

function groupContaining(
  groupNode: ClassificationNodeData,
): ConceptGroup | null {
  return (
    (groupNode.dimensions ?? []).find((group) =>
      group.members.some((member) => member.fqid === groupNode.fqid),
    ) ?? null
  );
}

function editionGroupAnchors(groupNode: ClassificationNodeData): string[] {
  const seen = new Set([groupNode.fqid]);
  const ordered = [
    ...(groupNode.edition_chain ?? []).filter((edition) => edition.is_current),
    ...(groupNode.edition_chain ?? []).filter((edition) => !edition.is_current),
  ];
  const anchors: string[] = [];
  for (const edition of ordered) {
    if (!edition.fqid || seen.has(edition.fqid)) {
      continue;
    }
    seen.add(edition.fqid);
    anchors.push(edition.fqid);
  }
  return anchors;
}

async function fetchClassificationNode(
  fqid: string,
  signal: AbortSignal,
): Promise<ClassificationNodeData> {
  const resolved = await getCatalogNode(fqid, undefined, { signal });
  if (isCatalogNode(resolved) && resolved.kind === "classification") {
    return resolved;
  }
  throw new Error(`Expected classification node for ${fqid}`);
}

const directGraphGroup = $derived(groupContaining(node));
const graphData = asyncResource<{
  group: ConceptGroup | null;
  members: ClassificationNodeData[];
}>(async (signal) => {
  let group = directGraphGroup;
  for (const fqid of editionGroupAnchors(node)) {
    if (group) {
      break;
    }
    const resolved = await fetchClassificationNode(fqid, signal);
    group = groupContaining(resolved);
  }
  if (!group) {
    return { group: null, members: [] };
  }
  const members = await Promise.all(
    group.members.map((member) => fetchClassificationNode(member.fqid, signal)),
  );
  return { group, members };
});
const graphGroup = $derived(graphData.data?.group ?? null);
const historyGraph = $derived(
  graphGroup && graphData.data
    ? historyGraphFromClassificationGroup(
        graphGroup,
        graphData.data.members,
        node.fqid,
      )
    : historyGraphFromClassification(node),
);
const graphLoading = $derived(graphData.loading);
const graphError = $derived(graphData.error);
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
  {#if graphLoading}
    <p class="muted" aria-busy="true">Loading group graph…</p>
  {:else if graphError}
    <p class="error" role="alert">{graphError}</p>
  {:else}
    <HistoryGraphPrototype graph={historyGraph} {vintageYear} />
  {/if}
  <ClassificationDimensionsPanel {node} />
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
