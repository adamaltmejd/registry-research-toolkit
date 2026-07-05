<script lang="ts">
import {
  type ClassificationFamilyNodeData,
  type ClassificationGroupNodeData,
  getClassificationGroup,
  getClassificationGroupGraph,
  type RelationshipGraph,
} from "./api";
import { asyncResource } from "./async.svelte";
import ClassificationEditionGraph from "./ClassificationEditionGraph.svelte";
import { catalogHref, leafSlug, memberKey } from "./catalog";
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
let { key }: { key: string } = $props();

const resource = asyncResource(() => getClassificationGroup(key));
const node = $derived(resource.data);
const EMPTY_GRAPH: RelationshipGraph = { nodes: [], edges: [], focus_id: null };
// Classification umbrella pages consume the same relationship-graph contract as
// classification leaves (#757/#761), but read-only: editions navigate to their leaf
// pages; there is no add-to-project picker for classifications.
const graphResource = asyncResource(() => {
  const activeKey = key;
  return node?.kind === "classification-group"
    ? getClassificationGroupGraph(activeKey)
    : Promise.resolve(EMPTY_GRAPH);
});
const graph = $derived(graphResource.data);
const graphReady = $derived(
  !graphResource.loading && !graphResource.error && graph != null,
);

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
</script>

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

  {#snippet picker()}
    <section class="member-selector" aria-labelledby="editions-heading">
      <h3 id="editions-heading">Editions</h3>
      <ul class="facet-chips">
        {#each node.editions as edition (edition.slug)}
          <li>
            {#if edition.fqid}
              <a class="chip edition-chip" href={catalogHref(edition.fqid)} title={edition.slug}>
                <span>{editionLabel(edition)}</span>
                <span class="edition-meta">
                  {edition.slug}{edition.is_current ? " - current" : ""}
                </span>
              </a>
            {:else}
              <span class="chip edition-chip">
                <span>{editionLabel(edition)}</span>
                <span class="edition-meta">
                  {edition.slug}{edition.is_current ? " - current" : ""}
                </span>
              </span>
            {/if}
          </li>
        {/each}
      </ul>
    </section>
  {/snippet}

  <SubjectView title={node.label} {description} {picker} />
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

  {#snippet picker()}
    <!-- The member selector: classification umbrellas are axis-less, so each
         member renders as a chip labelled by its own curated short facet label, a
         link to its classification leaf FQID. No coverage / availability lens —
         classifications have no year-grain study window. -->
    <section class="member-selector" aria-labelledby="members-heading">
      <h3 id="members-heading">Members</h3>
      <ul class="facet-chips">
        {#each node.members as member (memberKey(member))}
          <li>
            <a class="chip" href={catalogHref(member.fqid)} title={member.fqid}>
              {memberLabel(member)}
            </a>
          </li>
        {/each}
      </ul>
    </section>
    {#if graphReady && graph}
      <ClassificationEditionGraph {graph} />
    {/if}
  {/snippet}

  <SubjectView title={`Classification group: ${node.label}`} {description} {picker} />
{/if}

<style>
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
  /* The member selector mirrors ConceptGroupView's chips shape (copied, not
     imported — scoped styles don't cross components), minus the coverage /
     availability decoration classifications don't have. */
  .facet-chips {
    list-style: none;
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    padding: 0;
    margin: var(--space-2) 0;
  }
  /* The chip pill borrows ConceptGroupRow's `.chip` geometry (--border, em-based
     padding); it keeps the rounded 1rem radius the chips already had. */
  .chip {
    display: inline-block;
    border: 1px solid var(--border);
    border-radius: 1rem;
    padding: 0.1em 0.5em;
    text-decoration: none;
  }
  .edition-chip {
    display: inline-flex;
    flex-direction: column;
    gap: 0.05rem;
    max-width: 24rem;
  }
  .edition-meta {
    color: var(--text-muted);
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    overflow-wrap: anywhere;
  }
  /* Keyboard focus on a member chip link: the shared --focus-ring (#808/#828). */
  .chip:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
</style>
