<script lang="ts">
import type {
  ClassificationSearchResult,
  CodeSearchResult,
  ConceptGroupSearchResult,
  SearchResponse,
  VariableSearchResult,
} from "./api";
import { search } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref } from "./catalog";
import { router } from "./router.svelte";

// The routed search-results panel (#379). Reads `?q=` off the router and renders
// the four ORDERED, typed groups GET /api/search returns (registers / variables /
// classifications / codes). Every leaf navigates via a plain internal <a> the
// shell's `use:link` intercepts — never `router.navigate` from here.

const q = $derived((router.getQueryParam("q") ?? "").trim());

// Don't fire /api/search below this — a single-char query is the most expensive
// server-side AND the least useful. The "keep typing" hint covers 1 char; the
// empty-query hint covers 0 (see the template's state branches).
const MIN_QUERY_LENGTH = 2;

// asyncResource registers an $effect, so it can't be created conditionally; the
// fetch fn short-circuits a too-short `q` to an EMPTY response WITHOUT a network
// call (and reads `q` so it refetches when the query changes). It also threads the
// teardown `signal` into `search` so a superseded query aborts the in-flight HTTP
// request (and the ~12s timeout `search` layers on can abort it too).
const results = asyncResource<SearchResponse>((signal) =>
  q.length >= MIN_QUERY_LENGTH
    ? search(q, { signal })
    : Promise.resolve({ kind: "search", query: q, groups: [] }),
);

// Distinguish a TIMEOUT abort from every other failure. A supersede/unmount abort
// never reaches here (asyncResource's `cancelled` guard swallows it); a timeout
// abort fires while NOT cancelled, surfacing as an error. asyncResource exposes
// only the stringified error, and `String(timeoutError)` is name-prefixed
// ("TimeoutError: signal timed out" — AbortSignal.timeout's reason is a
// `DOMException` named "TimeoutError"), so match that prefix. Only this maps to
// the friendly copy — other errors keep the generic "Search failed" banner.
const timedOut = $derived(results.error?.startsWith("TimeoutError") ?? false);

const groups = $derived(results.data?.groups ?? []);
// A searched query (≥ min length) with zero results across every group (distinct
// from the empty / keep-typing hints and from loading). Gate on the min length so
// a 1-char query shows the keep-typing hint, not a spurious "no matches".
const noMatches = $derived(
  q.length >= MIN_QUERY_LENGTH &&
    !results.loading &&
    !results.error &&
    groups.every((g) => g.results.length === 0),
);

const GROUP_HEADINGS = {
  registers: "Registers",
  variables: "Variables",
  classifications: "Classifications",
  codes: "Codes / values",
} as const;

/** The "showing N of M" caption when the displayed slice is smaller than the full
 * match count (N = rendered rows, M = total before the server's per-request
 * limit), else null (don't caption a complete group). */
function showingOf(shown: number, total: number): string | null {
  return shown < total ? `showing ${shown} of ${total}` : null;
}

// Discriminate a variable/classification group's mixed results on `type`.
function isConceptGroup(r: { type: string }): r is ConceptGroupSearchResult {
  return r.type === "group";
}
</script>

<article class="search-view">
  <h2>Search</h2>

  {#if q === ""}
    <p class="muted">
      Start typing to search registers, variables, codes, classifications.
    </p>
  {:else if q.length < MIN_QUERY_LENGTH}
    <p class="muted">Keep typing to search…</p>
  {:else if results.loading}
    <p class="muted" aria-busy="true">Searching…</p>
  {:else if timedOut}
    <p class="error" role="alert">
      Search timed out — try a more specific term.
    </p>
  {:else if results.error}
    <p class="error" role="alert">Search failed: {results.error}</p>
  {:else if noMatches}
    <p class="muted">No matches for “{q}”.</p>
  {:else}
    {#each groups as group (group.group)}
      {#if group.results.length > 0}
        {@const caption = showingOf(group.results.length, group.total_count)}
        <section class="group">
          <h3>
            {GROUP_HEADINGS[group.group]}
            {#if caption}<span class="count">{caption}</span>{/if}
          </h3>

          {#if group.group === "registers"}
            <ul class="results">
              {#each group.results as result (result.fqid ?? result.name)}
                <li>
                  {@render fqidLeaf(result.fqid, result.name)}
                  {#if result.purpose}
                    <span class="hit-detail muted">{result.purpose}</span>
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "variables"}
            <ul class="results">
              {#each group.results as result (isConceptGroup(result) ? result.group_key : (result.fqid ?? result.name))}
                <li>
                  {#if isConceptGroup(result)}
                    {@render conceptGroup(result)}
                  {:else}
                    {@render variableLeaf(result as VariableSearchResult)}
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "classifications"}
            <ul class="results">
              {#each group.results as result (isConceptGroup(result) ? result.group_key : (result.fqid ?? result.short_name))}
                <li>
                  {#if isConceptGroup(result)}
                    {@render conceptGroup(result)}
                  {:else}
                    {@render classificationLeaf(result as ClassificationSearchResult)}
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "codes"}
            <ul class="results">
              {#each group.results as result (`${result.code}|${result.label}`)}
                <li>{@render codeHit(result)}</li>
              {/each}
            </ul>
          {/if}
        </section>
      {/if}
    {/each}
  {/if}
</article>

<!-- The shared leaf shape across register / variable / classification / member
     hits: a `label + <code>fqid</code>` link when FQID-addressable, else plain
     text (the hit has no catalog node). `label` is the per-type display name
     (the FQID is the fallback when linking, "—" when not). -->
{#snippet fqidLeaf(fqid: string | null | undefined, label: string | null | undefined)}
  {#if fqid}
    <a href={catalogHref(fqid)}>
      <span class="label">{label ?? fqid}</span>
      <code class="hit-fqid">{fqid}</code>
    </a>
  {:else}
    <span class="label">{label ?? "—"}</span>
  {/if}
{/snippet}

{#snippet variableLeaf(result: VariableSearchResult)}
  {@render fqidLeaf(result.fqid, result.name)}
  {#if result.register}
    <span class="hit-context muted">{result.register}</span>
  {/if}
  {#if result.definition}
    <span class="hit-detail muted">{result.definition}</span>
  {/if}
{/snippet}

{#snippet classificationLeaf(result: ClassificationSearchResult)}
  {@render fqidLeaf(result.fqid, result.short_name ?? result.name)}
  {#if result.name && result.name !== result.short_name}
    <span class="hit-detail muted">{result.name}</span>
  {/if}
{/snippet}

<!-- A folded concept-group family (#322): the group itself is NOT FQID-addressable,
     so it expands to its member leaves' links. The family hint reads "matched M of
     N" (matched_count of member_count). -->
{#snippet conceptGroup(result: ConceptGroupSearchResult)}
  <details class="concept-group">
    <summary>
      <span class="label">{result.group_label}</span>
      <span class="count">
        matched {result.matched_count} of {result.member_count}
      </span>
      {#if result.register}
        <span class="hit-context muted">{result.register}</span>
      {/if}
    </summary>
    <ul class="members">
      {#each result.members as member (member.fqid)}
        <li>
          <a href={catalogHref(member.fqid)}>
            <span class="label">{member.name ?? member.fqid}</span>
            <code class="hit-fqid">{member.fqid}</code>
          </a>
        </li>
      {/each}
    </ul>
  </details>
{/snippet}

<!-- A code/value hit (#352): the actionable target is the OWNING variable /
     classification, NOT the bare (code, label) pair (which is just the hit header).
     The bounded `variables`/`classifications` slices each link their owner; a
     muted, non-interactive "+N more" surfaces the slice cap from the counts. -->
{#snippet codeHit(result: CodeSearchResult)}
  <div class="code-hit">
    <div class="code-header">
      <code class="code">{result.code}</code>
      <span class="label">{result.label}</span>
    </div>
    {#if result.variables.length > 0 || result.variable_count > 0}
      <ul class="owners">
        {#each result.variables as owner (owner.fqid ?? owner.name)}
          <li>
            {#if owner.fqid}
              <a href={catalogHref(owner.fqid)}>
                <span class="label">{owner.name ?? owner.fqid}</span>
              </a>
            {:else}
              <span class="label">{owner.name ?? "—"}</span>
            {/if}
            {#if owner.register}
              <span class="hit-context muted">{owner.register}</span>
            {/if}
          </li>
        {/each}
        {#if result.variable_count > result.variables.length}
          <li class="more muted">
            +{result.variable_count - result.variables.length} more
          </li>
        {/if}
      </ul>
    {/if}
    {#if result.classifications.length > 0 || result.classification_count > 0}
      <ul class="owners">
        {#each result.classifications as owner (owner.fqid ?? owner.short_name)}
          <li>
            {#if owner.fqid}
              <a href={catalogHref(owner.fqid)}>
                <span class="label">
                  {owner.short_name ?? owner.name ?? owner.fqid}
                </span>
              </a>
            {:else}
              <span class="label">{owner.short_name ?? owner.name ?? "—"}</span>
            {/if}
          </li>
        {/each}
        {#if result.classification_count > result.classifications.length}
          <li class="more muted">
            +{result.classification_count - result.classifications.length} more
          </li>
        {/if}
      </ul>
    {/if}
  </div>
{/snippet}

<style>
  .search-view h2 {
    margin-bottom: 1rem;
  }
  .group {
    margin-bottom: 1.5rem;
  }
  .group h3 {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    margin: 0 0 0.5rem;
    font-size: 1rem;
  }
  .count {
    color: var(--muted);
    font-size: 0.85em;
    font-weight: 400;
    white-space: nowrap;
  }
  .results {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .results > li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem 0.75rem;
  }
  .label {
    font-weight: 600;
  }
  .hit-fqid {
    color: var(--muted);
    font-size: 0.85em;
  }
  .hit-context {
    font-size: 0.85em;
  }
  .hit-detail {
    flex-basis: 100%;
    font-size: 0.9em;
  }
  .concept-group summary {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    cursor: pointer;
  }
  .members {
    list-style: none;
    padding: 0;
    margin: 0.5rem 0 0.5rem 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .members a {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .code-hit {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .code-header {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }
  .code {
    font-size: 0.85em;
    color: var(--muted);
  }
  .owners {
    list-style: none;
    padding: 0;
    margin: 0 0 0 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }
  .owners li {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }
  .more {
    font-size: 0.85em;
  }
</style>
