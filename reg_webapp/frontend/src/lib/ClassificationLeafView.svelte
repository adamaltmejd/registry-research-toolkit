<script lang="ts">
import type { ClassificationNodeData } from "./api";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import { nodeLabel } from "./catalog";
import SubjectView from "./SubjectView.svelte";

// The classification LEAF — a standard ("Utbildningsnivå") rendered through the
// unified SubjectView shell (#638 PR1). The node EMBEDS its codes, so the codes
// panel renders synchronously. The old year-axis HistoryGraph is retired by #904;
// the compact classification succession DAG belongs to the picker surface in #906.
// No period picker yet (an edition picker is a later PR), no relationships surface
// until #906, and no docs surface
// (classifications carry no doc mentions), so those two SubjectView sections are
// omitted. No LineageDetails here either — classifications carry no
// lineage/warnings.
let { node }: { node: ClassificationNodeData } = $props();
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

<SubjectView
  title={nodeLabel(node)}
  fqid={node.fqid}
  {description}
  {valueSet}
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
