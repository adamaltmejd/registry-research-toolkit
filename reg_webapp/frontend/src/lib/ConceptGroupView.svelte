<script lang="ts">
import { type ConceptGroupNodeMember, getConceptGroup } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, formatWindow, OPEN_ENDED_VALID_TO } from "./catalog";
import { router } from "./router.svelte";

// The concept-group SUBJECT page (#617): fetches a group by (provider, register,
// key) and renders its members + facets + per-member coverage. A group's default
// selection is "all members", which a member FQID can't express — so the group
// has its own address (`/catalog/group/<p>/<r>/<key>`), distinct from the FQID
// browse path that CatalogNodeView serves.
//
// MINIMAL render by design: the full unified SubjectView (5 sections) and
// browse-row routing into it are OUT of scope here (deferred to the SubjectView
// work, epic #611). This page just makes the group browsable — label + members +
// facets + coverage — reusing the member-link shape from ConceptGroupRow rather
// than the folded <details> (the user navigated TO the group, so members show
// expanded).
let {
  provider,
  register,
  key,
}: { provider: string; register: string; key: string } = $props();

// The `?member=` focus hint lives in the query (like `?period`), so refining it
// doesn't remount this view. Read it reactively and pass it to the fetch (the
// backend echoes it on the node only when it names a real member).
const memberHint = $derived(router.getQueryParam("member"));
const resource = asyncResource(() =>
  getConceptGroup(provider, register, key, memberHint ?? undefined),
);
const node = $derived(resource.data);

// The register the group lives under — the breadcrumb target and a member's
// shared ancestor (a group is always register-scoped).
const registerFqid = $derived(`${provider}/${register}`);

function leafSlug(fqid: string): string {
  return fqid.split("/").at(-1) ?? fqid;
}

/** A member's facet labels along the group's axes (e.g. "januari"), joined for a
 * one-line display. Empty for an edge-group member (no facets). */
function facetText(member: ConceptGroupNodeMember): string {
  return member.facets.map((f) => f.label).join(" · ");
}

/** A minimal study-window line for a member's coverage (#351), or "" when the
 * member carries none (a stateless member). Delegates to `formatWindow` so the
 * display matches the rest of the catalog (year-collapsed bounds; the open-ended
 * sentinel renders "since <year>", never the raw 9999). */
function coverageText(member: ConceptGroupNodeMember): string {
  const cov = member.coverage;
  if (!cov || cov.coverage_from == null) {
    return "";
  }
  // `_coverage_bounds` contract: a finite window carries a non-null `coverage_to`;
  // open-ended maps to the sentinel. The null guard is defensive (shouldn't fire).
  const to = cov.open_ended ? OPEN_ENDED_VALID_TO : cov.coverage_to;
  if (to == null) {
    return "";
  }
  return formatWindow(cov.coverage_from, to);
}
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">catalog</a>
  <span class="sep" aria-hidden="true">/</span>
  <a href={catalogHref(provider)}>{provider}</a>
  <span class="sep" aria-hidden="true">/</span>
  <a href={catalogHref(registerFqid)}>{register}</a>
  <span class="sep" aria-hidden="true">/</span>
  <span class="current">group/{key}</span>
</nav>

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: concept group <code>{key}</code> in <code>{registerFqid}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
{:else if node}
  <article>
    <h2>{node.label}</h2>
    <dl class="meta">
      <dt>Group</dt>
      <dd><code>{node.key}</code> · {node.source}</dd>
      {#if node.axes.length > 0}
        <dt>Facets</dt>
        <dd>{node.axes.join(", ")}</dd>
      {/if}
    </dl>
    <h3>Members</h3>
    <ul class="members">
      {#each node.members as member (member.fqid)}
        {@const focused = node.member === leafSlug(member.fqid)}
        <li class:focused>
          <a href={catalogHref(member.fqid)}>
            <span class="label">{member.name ?? leafSlug(member.fqid)}</span>
            <code class="member-fqid">{member.fqid}</code>
          </a>
          {#if facetText(member)}
            <span class="facet muted">{facetText(member)}</span>
          {/if}
          {#if coverageText(member)}
            <span class="coverage muted">{coverageText(member)}</span>
          {/if}
        </li>
      {/each}
    </ul>
  </article>
{/if}

<style>
  .breadcrumbs {
    font-size: 0.9rem;
    margin-bottom: 1rem;
  }
  .breadcrumbs .sep {
    color: var(--muted);
    margin: 0 0.25rem;
  }
  .breadcrumbs .current {
    color: var(--muted);
  }
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.35rem 1rem;
    margin: 1rem 0;
  }
  .meta dt {
    font-weight: 600;
  }
  .members {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .members li {
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 0.25rem 0.75rem;
  }
  /* The `?member=` focus hint: a faint left accent rule so a deep-linked member
     reads as highlighted without a heavy box. */
  .members li.focused {
    border-left: 3px solid var(--accent);
    padding-left: 0.5rem;
    margin-left: -0.75rem;
  }
  .members .label {
    font-weight: 600;
  }
  .member-fqid {
    color: var(--muted);
    font-size: 0.85em;
  }
  .facet,
  .coverage {
    font-size: 0.85em;
  }
</style>
