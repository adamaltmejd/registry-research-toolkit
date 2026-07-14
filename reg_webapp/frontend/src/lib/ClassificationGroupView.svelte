<script lang="ts">
import { Tabs } from "bits-ui";
import {
  type ClassificationFamilyNodeData,
  type ClassificationGraphNode,
  type ClassificationGroupNodeData,
  type ClassificationNodeData,
  getCatalogNode,
  getClassificationGroup,
  getClassificationGroupGraph,
  type RelationshipGraph,
} from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";
import ClassificationEditionGraph from "./ClassificationEditionGraph.svelte";
import ClassificationRelatedLinks from "./ClassificationRelatedLinks.svelte";
import { catalogHref, leafSlug, narrowCatalogNode } from "./catalog";
import { classificationGraphHasRenderableSuccession } from "./picker_graph";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import TechnicalDetails from "./TechnicalDetails.svelte";

// The classification-umbrella SUBJECT page (#756): fetches a classification
// umbrella by key and renders its members + facets. The classification sibling of
// ConceptGroupView, but MUCH thinner — a classification umbrella is catalog-global
// (no provider/register), its members carry no coverage, and classifications have
// no year-grain study window, so there is NO period picker, NO availability lens,
// NO coverage greying. It gives the umbrella group (e.g. the SUN umbrella, key
// `sun`) a first-class subject page like the register groups have, instead of the
// inline <details> fold the classification-root browse used before (#673 → #756).
//
// Renders through the unified SubjectView shell (#638 PR1), same as the register
// group + the leaves: a `description` (a Technical details disclosure holding
// key/axes/source) and a `picker` (the member selector — member chips, each
// labelled by the member's own curated short facet label; classification umbrellas
// are axis-less, so there is no shared group facet axis). The loading / error arms
// stay OUTSIDE the shell (the shell is the success body). Identity is carried by the
// #803 topbar trail + the page <h2> (SubjectView title), not an in-page breadcrumb.
interface Props {
  key: string;
  activeFqid?: string | null;
  initialActiveNode?: ClassificationNodeData | null;
}

let { key, activeFqid = null, initialActiveNode = null }: Props = $props();

const resource = asyncResource(() => getClassificationGroup(key));
const node = $derived(resource.data);
const EMPTY_GRAPH: RelationshipGraph = { nodes: [], edges: [], focus_id: null };
// Classification umbrella pages consume the same relationship-graph contract as
// classification leaves (#757/#761), but read-only: editions navigate to their leaf
// pages; there is no add-to-project picker for classifications.
const graphResource = asyncResource(() => {
  const activeKey = key;
  return node?.kind === "classification-group" ||
    node?.kind === "classification-family"
    ? getClassificationGroupGraph(activeKey)
    : Promise.resolve(EMPTY_GRAPH);
});
const graph = $derived(graphResource.data);
const graphReady = $derived(
  !graphResource.loading && !graphResource.error && graph != null,
);

interface EditionTab {
  value: string;
  fqid: string | null;
  label: string;
  name: string;
  meta: string;
  versionYear: number | null;
  isCurrent: boolean;
}

/** A member's display label: its own curated short facet label (umbrellas are
 * axis-less — each member carries its own picker label, with no shared group
 * axis), falling back to the member name, then its leaf slug. */
function memberLabel(
  member: ClassificationGroupNodeData["members"][number],
): string {
  return member.facets[0]?.label ?? member.name ?? leafSlug(member.fqid);
}

function editionLabel(
  edition: ClassificationFamilyNodeData["editions"][number],
): string {
  return edition.name ?? edition.slug;
}

function graphNodeForFqid(fqid: string | null): ClassificationGraphNode | null {
  if (fqid == null || graph == null) {
    return null;
  }
  return (
    graph.nodes.find(
      (item): item is ClassificationGraphNode =>
        item.kind === "classification" && item.fqid === fqid,
    ) ?? null
  );
}

function memberTab(
  member: ClassificationGroupNodeData["members"][number],
): EditionTab {
  const point = graphNodeForFqid(member.fqid);
  return {
    value: member.fqid,
    fqid: member.fqid,
    label: memberLabel(member),
    name: member.name ?? member.fqid,
    meta: leafSlug(member.fqid),
    versionYear: point?.version_year ?? null,
    isCurrent: point?.is_current ?? false,
  };
}

function familyTab(
  edition: ClassificationFamilyNodeData["editions"][number],
): EditionTab {
  return {
    value: edition.fqid ?? `missing:${edition.slug}`,
    fqid: edition.fqid,
    label: editionLabel(edition),
    name: edition.name ?? edition.slug,
    meta: `${edition.slug}${edition.is_current ? " - current" : ""}`,
    versionYear: edition.version_year,
    isCurrent: edition.is_current,
  };
}

const tabs = $derived.by((): EditionTab[] => {
  if (node?.kind === "classification-family") {
    return node.editions.map(familyTab);
  }
  if (node?.kind === "classification-group") {
    return node.members.map(memberTab);
  }
  return [];
});
const expectsEditionGraph = $derived(
  tabs.length > 1 || (initialActiveNode?.edition_chain?.length ?? 0) > 1,
);

function latestTabFqid(tabList: EditionTab[]): string | null {
  let best: EditionTab | null = null;
  for (const tab of tabList) {
    if (tab.fqid == null) {
      continue;
    }
    if (best == null) {
      best = tab;
      continue;
    }
    if (tab.isCurrent !== best.isCurrent) {
      if (tab.isCurrent) {
        best = tab;
      }
      continue;
    }
    const tabYear = tab.versionYear ?? Number.NEGATIVE_INFINITY;
    const bestYear = best.versionYear ?? Number.NEGATIVE_INFINITY;
    if (tabYear > bestYear) {
      best = tab;
    }
  }
  return best?.fqid ?? null;
}

let selectedFqid = $state<string | null>(null);
const tabSelectionReady = $derived(
  node?.kind !== "classification-group" || !graphResource.loading,
);

$effect(() => {
  const requested =
    activeFqid != null && tabs.some((tab) => tab.fqid === activeFqid)
      ? activeFqid
      : null;
  if (requested != null) {
    selectedFqid = requested;
    return;
  }
  if (!tabSelectionReady) {
    return;
  }
  if (selectedFqid == null || !tabs.some((tab) => tab.fqid === selectedFqid)) {
    selectedFqid = latestTabFqid(tabs);
  }
});

const activeNodeResource = asyncResource(async () => {
  const fqid = selectedFqid;
  if (fqid == null) {
    return null;
  }
  if (initialActiveNode?.fqid === fqid) {
    return initialActiveNode;
  }
  const resolved = narrowCatalogNode(await getCatalogNode(fqid));
  if (resolved?.kind !== "classification") {
    throw new Error(`${fqid} did not resolve to a classification`);
  }
  return resolved;
});
const activeNode = $derived(activeNodeResource.data);
const activeNodeHasCodes = $derived((activeNode?.codes ?? []).length > 0);
const focusedGraph = $derived.by((): RelationshipGraph | null => {
  if (graph == null || selectedFqid == null) {
    return graph;
  }
  const focus = graphNodeForFqid(selectedFqid);
  return focus == null ? graph : { ...graph, focus_id: focus.id };
});
const graphRenderable = $derived(
  graphReady &&
    focusedGraph != null &&
    classificationGraphHasRenderableSuccession(focusedGraph),
);
const reserveEditionGraph = $derived(
  expectsEditionGraph && (graphResource.loading || graphRenderable),
);

function selectEdition(value: string): void {
  const tab = tabs.find((item) => item.value === value);
  if (tab?.fqid == null) {
    return;
  }
  selectedFqid = tab.fqid;
  router.navigate(catalogHref(tab.fqid));
}
</script>

{#snippet valueSet()}
  {#if tabs.length > 0 && selectedFqid != null}
    <section class="edition-tabs-section" aria-labelledby="edition-tabs-heading">
      <h3 id="edition-tabs-heading">Value set</h3>
      <div class="edition-tabs">
        <Tabs.Root
          value={selectedFqid}
          onValueChange={selectEdition}
          activationMode="manual"
          loop
        >
          <Tabs.List class="edition-tab-list" aria-label="Classification editions">
            {#each tabs as tab (tab.value)}
              <Tabs.Trigger
                value={tab.value}
                disabled={tab.fqid == null}
                class="edition-tab"
                title={tab.name}
              >
                <span class="tab-label">{tab.label}</span>
                <span class="tab-meta">{tab.meta}</span>
              </Tabs.Trigger>
            {/each}
          </Tabs.List>

          <Tabs.Content value={selectedFqid} class="edition-tab-panel">
            {#if activeNodeResource.loading}
              <p class="muted" aria-busy="true">Loading value set…</p>
            {:else if activeNodeResource.error}
              <p class="error" role="alert">{activeNodeResource.error}</p>
            {:else if activeNode && activeNodeHasCodes}
              <ClassificationCodesPanel node={activeNode} />
            {:else}
              <p class="muted">No codes are available for this edition.</p>
            {/if}
          </Tabs.Content>
        </Tabs.Root>
      </div>
    </section>
  {/if}
{/snippet}

{#snippet relationships()}
  {#if activeNode}
    <ClassificationRelatedLinks node={activeNode} />
  {/if}
{/snippet}

{#snippet picker()}
  {#if graphRenderable && focusedGraph}
    <ClassificationEditionGraph graph={focusedGraph} />
  {/if}
{/snippet}

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: classification group or family <code>{key}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
{:else if node?.kind === "classification-family"}
  {#snippet description()}
    <TechnicalDetails>
      <dl class="meta">
        <dt>Family</dt>
        <dd><code>{node.key}</code></dd>
        <dt>Editions</dt>
        <dd>{node.editions.length}</dd>
      </dl>
    </TechnicalDetails>
  {/snippet}

  <div class:reserve-edition-graph={reserveEditionGraph}>
    <SubjectView
      title={node.label}
      {description}
      {picker}
      valueSet={valueSet}
      {relationships}
    />
  </div>
{:else if node}
  {#snippet description()}
    <!-- Key, axes, and source are build-derivation metadata, not researcher-facing
         — demoted together behind the Technical details disclosure (mirrors the
         register-group page, minus its coverage). The page then leads with the
         title + member selector. -->
    <TechnicalDetails>
      <dl class="meta">
        <dt>Group</dt>
        <dd><code>{node.key}</code></dd>
        {#if node.axes.length > 0}
          <dt>Facets</dt>
          <dd>{node.axes.map((a) => a.label).join(", ")}</dd>
        {/if}
        <dt>Source</dt>
        <dd>{node.source}</dd>
      </dl>
    </TechnicalDetails>
  {/snippet}

  <div class:reserve-edition-graph={reserveEditionGraph}>
    <SubjectView
      title={`Classification group: ${node.label}`}
      {description}
      {picker}
      valueSet={valueSet}
      {relationships}
    />
  </div>
{/if}

<style>
  /* Multi-edition classification subjects render their value-set tabs before
     the optional graph request finishes. Hold one compact DAG row while that
     request is pending, retain it only for a renderable succession graph, and
     collapse it after an empty/error response. A branched graph scrolls inside
     this bounded slot instead of growing the row after it resolves. */
  .reserve-edition-graph :global(article) {
    --edition-graph-slot: 13rem;
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto auto var(--edition-graph-slot);
    min-width: 0;
  }
  .reserve-edition-graph :global(article > .subject-header) {
    grid-row: 1;
  }
  .reserve-edition-graph :global(article > .tech-details) {
    grid-row: 2;
  }
  .reserve-edition-graph :global(article > .classification-editions) {
    grid-row: 3;
    align-self: start;
    box-sizing: border-box;
    max-block-size: calc(var(--edition-graph-slot) - var(--space-4));
    margin-block: var(--space-4) 0;
    overflow: auto;
  }
  .reserve-edition-graph :global(article > .edition-tabs-section) {
    grid-row: 4;
  }
  .reserve-edition-graph :global(article > .derived-links) {
    grid-row: 5;
  }
  /* #638 PR4: row spacing standardized across the subject kinds. */
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: var(--space-1) var(--space-4);
    margin: var(--space-4) 0;
  }
  .meta dt {
    font-weight: 600;
  }
  .edition-tabs-section {
    margin: var(--space-4) 0;
  }
  .edition-tabs-section h3 {
    margin: 0 0 var(--space-2);
    padding-bottom: var(--space-1);
    border-bottom: 1px solid var(--border);
    font-size: var(--text-h3);
  }
  .edition-tabs :global(.edition-tab-list) {
    display: flex;
    align-items: flex-end;
    gap: 0;
    max-width: 100%;
    overflow-x: auto;
    border-bottom: 1px solid var(--border);
  }
  .edition-tabs :global(.edition-tab) {
    box-sizing: border-box;
    display: inline-flex;
    min-width: 8.5rem;
    max-width: 16rem;
    flex: 0 0 auto;
    flex-direction: column;
    align-items: flex-start;
    gap: 1px;
    margin: 0 0 -1px;
    padding: var(--space-2) var(--space-3);
    border: 1px solid transparent;
    border-bottom-color: var(--border);
    border-radius: var(--radius-sm) var(--radius-sm) 0 0;
    background: transparent;
    color: var(--text);
    font: inherit;
    text-align: left;
    cursor: pointer;
  }
  .edition-tabs :global(.edition-tab:hover:not(:disabled)) {
    background: var(--surface-hover);
  }
  .edition-tabs :global(.edition-tab[data-state="active"]) {
    border-color: var(--border);
    border-bottom-color: var(--surface);
    background: var(--surface);
  }
  .edition-tabs :global(.edition-tab:disabled) {
    color: var(--text-faint);
    cursor: not-allowed;
  }
  .edition-tabs :global(.edition-tab:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .tab-label {
    max-width: 100%;
    overflow: hidden;
    font-weight: 600;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .tab-meta {
    max-width: 100%;
    overflow: hidden;
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .edition-tabs :global(.edition-tab-panel) {
    padding-top: var(--space-3);
  }
  @media (max-width: 48rem) {
    .edition-tabs :global(.edition-tab) {
      min-width: 9rem;
      max-width: 13rem;
    }
  }
</style>
