<script lang="ts">
import type {
  ClassificationSearchResult,
  ClassificationSuccessionSearchResult,
  CodeSearchResult,
  ConceptGroupSearchResult,
  DocResult,
  DocSearchResponse,
  SearchResponse,
  SearchType,
  VariableSearchResult,
} from "./api";
import { docSearch, SEARCH_MIN_QUERY_LENGTH, search } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, showingOf } from "./catalog";
import { parseInlineMarkdown } from "./inline_markdown";
import { router } from "./router.svelte";
import { Tag } from "./ui";

// The routed search-results panel (#379). Reads `?q=` off the router and renders
// the four ORDERED, typed groups GET /api/search returns (registers / variables /
// classifications / codes). Every leaf navigates via a plain internal <a> the
// shell's `use:link` intercepts — never `router.navigate` from here.

const q = $derived((router.getQueryParam("q") ?? "").trim());

// The scoped-search toggle (#393 item 1). `?type=` lives in the URL (deep-linkable
// / shareable / back-forward-correct, like `?q=`/`?period`). An unknown value
// degrades to "all" (don't 422 the SPA over a hand-edited URL — the toggle just
// renders nothing active). Read inside the `results` fetcher below so the resource
// refetches when `?type=` changes.
const SEARCH_TYPES: readonly SearchType[] = [
  "all",
  "register",
  "variable",
  "classification",
  "value",
];
const searchType = $derived.by<SearchType>(() => {
  const raw = router.getQueryParam("type");
  return SEARCH_TYPES.includes(raw as SearchType) ? (raw as SearchType) : "all";
});

// The toggle's button set: label + the `?type=` value it routes to.
const TYPE_TOGGLE: ReadonlyArray<{ value: SearchType; label: string }> = [
  { value: "all", label: "All" },
  { value: "register", label: "Registers" },
  { value: "variable", label: "Variables" },
  { value: "classification", label: "Classifications" },
  { value: "value", label: "Codes" },
];

/** Route to the current query scoped to `type` (in-place replace — scope is a
 * refinement, not a new history entry). OMIT `?type=` for `all` (the server
 * default) so the canonical/shareable URL stays clean and the ETag is stable. */
function selectType(type: SearchType): void {
  const base = `/search?q=${encodeURIComponent(q)}`;
  router.replace(type === "all" ? base : `${base}&type=${type}`);
}

// asyncResource registers an $effect, so it can't be created conditionally; the
// fetch fn short-circuits a too-short `q` to an EMPTY response WITHOUT a network
// call (and reads `q` so it refetches when the query changes). It also threads the
// teardown `signal` into `search` so a superseded query aborts the in-flight HTTP
// request (and the ~12s timeout `search` layers on can abort it too).
const results = asyncResource<SearchResponse>((signal) =>
  // Read `searchType` HERE so the resource refetches when `?type=` changes (the
  // scoped-search toggle, #393 item 1).
  q.length >= SEARCH_MIN_QUERY_LENGTH
    ? search(q, { signal, type: searchType })
    : Promise.resolve({ kind: "search", query: q, groups: [] }),
);

// A SEPARATE resource for the additive "Documentation" group (#394), keyed on the
// same `q`. Kept independent from `results` for FAILURE ISOLATION: a docs failure
// (error/timeout) or an absent docs index (`ingested:false`) must NEVER blank or
// error the four main groups — there is intentionally no docs loading indicator
// and no docs error banner; silent omission IS the isolation. Short-circuits a
// too-short query to an empty response without a network call (mirrors `results`).
// Docs is shown ONLY in the unscoped (`all`) view: the #393 toggle has no Docs
// option, and a scoped search means "show only that one group", so any non-`all`
// scope short-circuits to the empty `ingested:false` response (no fetch, and
// `docsHasHits` stays false → the section is hidden). Read `searchType` HERE so the
// resource refetches when the scope returns to `all`.
const docs = asyncResource<DocSearchResponse>((signal) =>
  q.length >= SEARCH_MIN_QUERY_LENGTH && searchType === "all"
    ? docSearch(q, { signal })
    : Promise.resolve({
        kind: "doc-search",
        query: q,
        ingested: false,
        total_count: 0,
        results: [],
      }),
);

// The docs group renders ONLY when the index is present AND there are hits.
const docsHasHits = $derived(
  !!docs.data?.ingested && (docs.data?.results.length ?? 0) > 0,
);

// Distinguish a TIMEOUT abort from every other failure. A supersede/unmount abort
// never reaches here (asyncResource's `cancelled` guard swallows it); a timeout
// abort fires while NOT cancelled, surfacing as an error. asyncResource exposes
// only the stringified error, and `String(e)` on a DOMException is name-prefixed
// (`<name>: <message>`). AbortSignal.timeout's reason is a DOMException named
// "TimeoutError", so match only the spec-stable NAME prefix — the message tail
// after ": " is engine-specific (varies by browser) and must NOT be matched. Only
// this maps to the friendly copy — other errors keep the generic "Search failed".
const timedOut = $derived(results.error?.startsWith("TimeoutError") ?? false);

const groups = $derived(results.data?.groups ?? []);
// A searched query (≥ min length) with zero results across every group (distinct
// from the empty / keep-typing hints and from loading). Gate on the min length so
// a 1-char query shows the keep-typing hint, not a spurious "no matches".
// The docs group participates in "any results at all" so we never show "No
// matches" above a rendered docs group; gated on `!docs.loading && !docsHasHits`
// so a docs failure/empty/absent-index still lets the main "No matches" show.
const noMatches = $derived(
  q.length >= SEARCH_MIN_QUERY_LENGTH &&
    !results.loading &&
    !results.error &&
    groups.every((g) => g.results.length === 0) &&
    !docs.loading &&
    !docsHasHits,
);

const GROUP_HEADINGS = {
  registers: "Registers",
  variables: "Variables",
  classifications: "Classifications",
  codes: "Codes / values",
} as const;

// Discriminate a variable/classification group's mixed results on `type`.
function isConceptGroup(r: { type: string }): r is ConceptGroupSearchResult {
  return r.type === "group";
}

// A folded classification-succession row (#571) in the classifications group — a
// query hit ≥2 editions of one chain, collapsed onto the terminal edition.
function isClassificationSuccession(r: {
  type: string;
}): r is ClassificationSuccessionSearchResult {
  return r.type === "classification_succession";
}

// The codes group, bucketed by code system (#393 item 3). STABLE group-by on
// `code_system`, preserving FIRST-APPEARANCE order — so the item-2 ranking (which
// already floats classification-backed/curated codes to the front) decides which
// systems lead. `null`/empty → a trailing "Register-local" bucket. `label` is the
// subsection heading; `key` is a stable each-key (the raw code_system, or JS
// `null` for the register-local bucket — a Map treats `null` as a distinct key
// that can never collide with any real code_system string). Map iteration is
// insertion order, so its values come back in first-appearance order directly.
const REGISTER_LOCAL_LABEL = "Register-local";
type CodeSystemBucket = {
  key: string | null;
  label: string;
  codes: CodeSearchResult[];
};
function groupCodesBySystem(results: CodeSearchResult[]): CodeSystemBucket[] {
  const buckets = new Map<string | null, CodeSystemBucket>();
  for (const code of results) {
    const key = code.code_system || null;
    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = { key, label: key ?? REGISTER_LOCAL_LABEL, codes: [] };
      buckets.set(key, bucket);
    }
    bucket.codes.push(code);
  }
  return [...buckets.values()];
}
</script>

<article class="search-view">
  <h2>Search</h2>

  <!-- Scoped-search toggle (#393 item 1): visible whenever there's a query (incl.
       loading / no-match / results) so the user can switch scope from any state.
       Each button routes `?type=` (omitting it for `all`); `aria-pressed` marks the
       active scope. A `role="group"` segmented control. -->
  {#if q !== ""}
    <div class="type-toggle" role="group" aria-label="Search scope">
      {#each TYPE_TOGGLE as option (option.value)}
        <button
          type="button"
          class="type-button"
          aria-pressed={searchType === option.value}
          onclick={() => selectType(option.value)}
        >
          {option.label}
        </button>
      {/each}
    </div>
  {/if}

  {#if q === ""}
    <p class="muted">
      Start typing to search registers, variables, codes, classifications.
    </p>
  {:else if q.length < SEARCH_MIN_QUERY_LENGTH}
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
              <!-- Key by array INDEX: result lists are replaced wholesale per
                   query (never incrementally mutated), so the index is stable
                   within a render and guaranteed unique. Natural keys collide —
                   `fqid` can be null and a folded group_key is only register-scoped
                   unique (#322) — and a SINGLE duplicate key crashes the whole
                   keyed each (Svelte each_key_duplicate). See #379 omnibox-dup-key. -->
              {#each group.results as result, i (i)}
                <li>
                  {@render typeBadge("reg", "REG")}
                  {@render fqidLeaf(result.fqid, result.name)}
                  {#if result.purpose}
                    <span class="hit-detail muted">{result.purpose}</span>
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "variables"}
            <ul class="results">
              {#each group.results as result, i (i)}
                <li>
                  {#if isConceptGroup(result)}
                    {@render typeBadge("group", "GROUP")}
                    {@render conceptGroup(result)}
                  {:else}
                    {@render typeBadge("var", "VAR")}
                    {@render variableLeaf(result as VariableSearchResult)}
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "classifications"}
            <ul class="results">
              {#each group.results as result, i (i)}
                <li>
                  {#if isConceptGroup(result)}
                    {@render typeBadge("group", "GROUP")}
                    {@render conceptGroup(result)}
                  {:else if isClassificationSuccession(result)}
                    {@render typeBadge("class", "CLASS")}
                    {@render classificationSuccession(result)}
                  {:else}
                    {@render typeBadge("class", "CLASS")}
                    {@render classificationLeaf(result as ClassificationSearchResult)}
                  {/if}
                </li>
              {/each}
            </ul>
          {:else if group.group === "codes"}
            <!-- Per-code-system subsections (#393 item 3). The codes are already
                 item-2-ordered (classification-backed first), and groupCodesBySystem
                 preserves first-appearance order, so curated systems lead;
                 null/empty code_system folds into a trailing "Register-local"
                 subsection. Each row keeps the codeHit snippet; the per-subsection
                 `{#each}` keys stay INDEX-based (the #379/#391 each_key_duplicate
                 lesson — natural keys can collide and crash the keyed each). -->
            {#each groupCodesBySystem(group.results) as system (system.key)}
              <div class="code-system">
                <h4 class="code-system-heading">{system.label}</h4>
                <ul class="results">
                  {#each system.codes as result, i (i)}
                    <li>{@render codeHit(result)}</li>
                  {/each}
                </ul>
              </div>
            {/each}
          {/if}
        </section>
      {/if}
    {/each}
  {/if}

  <!-- Additive "Documentation" group (#394), driven by the SEPARATE `docs`
       resource and rendered as a SIBLING of the main-search `{#if}` block — so it
       shows whenever it has hits REGARDLESS of the main groups' loading / error /
       timeout / no-match state (full failure isolation: e.g. the main /api/search
       codes sub-query can time out while the separate docs index resolves fine).
       Its own <section> with a literal heading (NOT folded into GROUP_HEADINGS,
       which is keyed on the four main group literals). A docs failure / empty /
       absent index is silently omitted; the empty + too-short query states
       short-circuit `docs` to `ingested:false`, so `docsHasHits` is false there. -->
  {#if docsHasHits && docs.data}
    {@const caption = showingOf(
      docs.data.results.length,
      docs.data.total_count,
    )}
    <section class="group">
      <h3>
        Documentation
        {#if caption}<span class="count">{caption}</span>{/if}
      </h3>
      <ul class="results">
        <!-- Key by array INDEX: like the other groups, this list is replaced
             wholesale per query, so the index is stable + unique within a render
             (a natural key like `filename` could collide and crash the keyed
             each — see the #379/#391 each_key_duplicate lesson above). -->
        {#each docs.data.results as result, i (i)}
          <li>{@render docHit(result)}</li>
        {/each}
      </ul>
    </section>
  {/if}
</article>

<!-- A leading categorical TYPE badge (#808): a scannable type marker preceding
     each row's label, using the #804 `Tag` primitive on the categorical palette
     (reg/var/code/class/group → --cat-*). The label is the short uppercase type
     code; `Tag` carries the AA-cleared tint + ink. The Documentation group is
     additive (not one of the categorical node types) and is intentionally left
     un-badged. -->
{#snippet typeBadge(tone: "reg" | "var" | "code" | "class" | "group", label: string)}
  <span class="type-badge"><Tag {tone}>{label}</Tag></span>
{/snippet}

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
  <!-- A lone non-current edition hit (#571): link to the current/terminal edition
       so the user can jump forward. Only present when this is an old edition the
       query matched alone (absent for current/non-edition classifications). -->
  {#if result.terminal_fqid}
    <a class="terminal-link" href={catalogHref(result.terminal_fqid)}>
      → current edition
    </a>
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
      {#each result.members as member, i (i)}
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

<!-- A folded classification-succession row (#571): unlike a concept group, the
     terminal (current) edition IS navigable — so its `fqid` link is the always-
     visible header. A <details> discloses the full edition chain (terminal-first,
     descending year); each edition with a live `fqid` is itself a link. The
     family hint reads "matched M of N editions". -->
{#snippet classificationSuccession(result: ClassificationSuccessionSearchResult)}
  {@const editions = result.editions ?? []}
  <details class="concept-group">
    <summary>
      {#if result.fqid}
        <a href={catalogHref(result.fqid)}>
          <span class="label">{result.short_name ?? result.name ?? result.fqid}</span>
          <code class="hit-fqid">{result.fqid}</code>
        </a>
      {:else}
        <span class="label">{result.short_name ?? result.name ?? "—"}</span>
      {/if}
      <span class="count">
        matched {result.matched_count} of {editions.length} editions
      </span>
    </summary>
    <ul class="members">
      {#each editions as edition, i (i)}
        <li>
          {#if edition.fqid}
            <a href={catalogHref(edition.fqid)}>
              <span class="label">{edition.name ?? edition.slug}</span>
              <code class="hit-fqid">{edition.fqid}</code>
            </a>
          {:else}
            <span class="label">{edition.name ?? edition.slug}</span>
          {/if}
          {#if edition.effective_year != null}
            <span class="hit-context muted">{edition.effective_year}</span>
          {/if}
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
      {@render typeBadge("code", "CODE")}
      <code class="code">{result.code}</code>
      <span class="label">{result.label}</span>
    </div>
    {#if result.variables.length > 0 || result.variable_count > 0}
      <ul class="owners">
        {#each result.variables as owner, i (i)}
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
        {#each result.classifications as owner, i (i)}
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

<!-- A documentation hit (#394): links to the minimal /doc viewer (the shell's
     use:link intercepts it). `snippet` is an FTS EXCERPT rendered through the
     same SAFE inline-emphasis subset as DocMentionsPanel: parse into DATA
     segments, then interpolate each `{seg.text}` so Svelte auto-escapes it.
     NEVER {@html}, as the full body is never fetched/rendered (it lives at the
     SCB source). -->
{#snippet docHit(result: DocResult)}
  <a href={`/doc/${encodeURIComponent(result.filename)}`}>
    <span class="label">{result.display_name ?? result.filename}</span>
  </a>
  {#if result.register}
    <span class="hit-context muted">{result.register}</span>
  {/if}
  {#if result.snippet}
    <span class="hit-detail muted"
      >{#each parseInlineMarkdown(result.snippet) as seg, si (si)}{#if seg.emphasis === "strong"}<mark
          >{seg.text}</mark
        >{:else if seg.emphasis === "em"}<em>{seg.text}</em
        >{:else}{seg.text}{/if}{/each}</span
    >
  {/if}
{/snippet}

<style>
  .search-view h2 {
    margin-bottom: 1rem;
  }
  .type-toggle {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    margin-bottom: 1.25rem;
  }
  .type-button {
    padding: 0.3rem 0.7rem;
    font: inherit;
    font-size: var(--text-sm);
    color: var(--text);
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    cursor: pointer;
  }
  .type-button:hover {
    border-color: var(--accent);
  }
  .type-button:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .type-button[aria-pressed="true"] {
    background: var(--accent);
    border-color: var(--accent);
    color: var(--accent-fg);
    font-weight: 600;
  }
  .group {
    margin-bottom: 1.5rem;
  }
  .code-system {
    margin-bottom: 1rem;
  }
  .code-system-heading {
    margin: 0 0 0.4rem;
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-muted);
  }
  .group h3 {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
    margin: 0 0 0.5rem;
    font-size: 1rem;
  }
  .count {
    color: var(--text-muted);
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
    padding: 0.25rem 0.4rem;
    margin: 0 -0.4rem;
    border-radius: var(--radius-sm);
  }
  /* A subtle dashboard-scan hover affordance on each result row (the row, not the
     link) — keeps the link's own focus/hover intact. */
  .results > li:hover {
    background: var(--surface-hover);
  }
  /* The leading categorical type badge: a small inline marker that doesn't push
     the baseline of the row's text. The Tag itself carries tint + ink. */
  .type-badge {
    align-self: center;
    line-height: 1;
  }
  .label {
    font-weight: 600;
  }
  .hit-fqid {
    color: var(--text-muted);
    font-size: 0.85em;
  }
  .hit-context {
    font-size: 0.85em;
  }
  .terminal-link {
    font-size: 0.85em;
  }
  .hit-detail {
    flex-basis: 100%;
    font-size: 0.9em;
  }
  .hit-detail mark {
    background: var(--accent-bg);
    color: var(--accent-ink);
    border-radius: var(--radius);
    padding: 0 0.1em;
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
    color: var(--text-muted);
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
