<script lang="ts">
import {
  type ClassificationGroupNodeData,
  getClassificationGroup,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  catalogHref,
  DATA_BROWSER_LABEL,
  leafSlug,
  memberKey,
} from "./catalog";
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
// are axis-less, so there is no shared group facet axis). The breadcrumbs
// + loading / error arms stay OUTSIDE the shell (the shell is the success body).
let { key }: { key: string } = $props();

const resource = asyncResource(() => getClassificationGroup(key));
const node = $derived(resource.data);

/** A member's display label: its own curated short facet label (umbrellas are
 * axis-less — each member carries its own picker label, with no shared group
 * axis), falling back to the member name, then its leaf slug. */
function memberLabel(
  member: ClassificationGroupNodeData["members"][number],
): string {
  return member.facets[0]?.label ?? member.name ?? leafSlug(member.fqid);
}
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">{DATA_BROWSER_LABEL}</a>
  <span class="sep" aria-hidden="true">/</span>
  <a href={catalogHref("class")}>class</a>
  <span class="sep" aria-hidden="true">/</span>
  <span class="current">group/{key}</span>
</nav>

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: classification group <code>{key}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
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
  {/snippet}

  <SubjectView title={node.label} {description} {picker} />
{/if}

<style>
  .breadcrumbs {
    font-size: 0.9rem;
    margin-bottom: 1rem;
  }
  .breadcrumbs .sep {
    color: var(--text-muted);
    margin: 0 0.25rem;
  }
  .breadcrumbs .current {
    color: var(--text-muted);
  }
  /* #638 PR4: row spacing standardized to 0.3rem across the subject kinds. */
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.3rem 1rem;
    margin: 1rem 0;
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
    gap: 0.5rem;
    padding: 0;
    margin: 0.5rem 0;
  }
  .chip {
    display: inline-block;
    border: 1px solid var(--text-muted);
    border-radius: 1rem;
    padding: 0.1rem 0.6rem;
    text-decoration: none;
  }
</style>
