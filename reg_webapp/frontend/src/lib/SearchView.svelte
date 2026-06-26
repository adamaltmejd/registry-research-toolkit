<script lang="ts">
import type {
  ClassificationSearchResult,
  ClassificationSuccessionSearchResult,
  CodeSearchResult,
  ConceptGroupSearchResult,
  DocResult,
  DocSearchResponse,
  RegisterSearchResult,
  SearchResponse,
  SearchType,
  VariableSearchResult,
} from "./api";
import { docSearch, SEARCH_MIN_QUERY_LENGTH, search } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, leafSlug, showingOf } from "./catalog";
import { parseInlineMarkdown } from "./inline_markdown";
import { router } from "./router.svelte";
import { type Column, DataTable, Tag } from "./ui";

// The routed search-results panel (#379). Reads `?q=` off the router and renders
// the four ORDERED, typed groups GET /api/search returns (registers / variables /
// classifications / codes). Every leaf navigates via a plain internal <a> the
// shell's `use:link` intercepts — never `router.navigate` from here.
//
// #808 (round 2): the leaf groups render as compact DataTables (the #806 pattern,
// matching CatalogNodeView's provider arm) — categorical type identity moves to
// the GROUP HEADING (a single Tag), the raw FQID is hidden everywhere (the leaf
// SLUG is the only identifier shown), and the whole row is click/keyboard-
// navigable to the hit via DataTable selection-as-navigation. Folded families
// (concept groups, classification succession) stay expandable <details> — they
// have no flat one-row-one-target model — and codes stay a compact list (a code
// fans out to multiple owners).

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

// Per-group heading + its categorical type tone (#808): the tone marks the group
// identity ONCE in the heading (a single Tag), replacing the old per-row badges.
// The codes heading carries no single tone (a code fans out to mixed owners), so
// it has none.
const GROUP_HEADINGS = {
  registers: { label: "Registers", tone: "reg" },
  variables: { label: "Variables", tone: "var" },
  classifications: { label: "Classifications", tone: "class" },
  codes: { label: "Codes / values", tone: null },
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

// ── Leaf-table row shapes (#808) ─────────────────────────────────────────────
// Each leaf table is a DataTable over a thin row type that carries a STABLE
// synthetic `rowId` (the array index) AND the per-column display fields. The
// `rowId` is DataTable's `getRowId` key — crash-proof even when several rows
// share a null `fqid` and the same name (the #379/#391 each_key_duplicate lesson
// — a natural `fqid` key collides). The cell snippet keys off `column.key`; the
// row's `result` drives the navigation target. DataTable keys its column-each on
// `col.key`, so every column needs a DISTINCT key — hence the dedicated display
// fields (the slug column gets its own `slug` key, not a second `result`).
type RegisterRow = {
  rowId: string;
  name: string | null | undefined;
  fqid: string | null;
  purpose: string | null | undefined;
};
type VariableRow = {
  rowId: string;
  name: string | null | undefined;
  fqid: string | null;
  register: string | null | undefined;
  slug: string;
  definition: string | null | undefined;
};
type ClassificationRow = {
  rowId: string;
  short: string | null | undefined;
  name: string | null | undefined;
  fqid: string | null;
  terminalFqid: string | null | undefined;
};

const registerColumns: Column<RegisterRow>[] = [
  { key: "name", label: "Register" },
  { key: "purpose", label: "Description" },
];
const variableColumns: Column<VariableRow>[] = [
  { key: "name", label: "Variable" },
  { key: "register", label: "Register" },
  { key: "slug", label: "Column", mono: true },
];
const classificationColumns: Column<ClassificationRow>[] = [
  { key: "short", label: "Classification" },
  { key: "name", label: "Name" },
];

/** Navigate to a leaf hit on row-select — a null `fqid` row can't navigate, so
 * bail (its name renders as plain text, the click no-ops). The shell's router
 * owns history; `catalogHref` mirrors the API path. */
function navigateTo(fqid: string | null | undefined): void {
  if (!fqid) return;
  router.navigate(catalogHref(fqid));
}

function registerRows(results: RegisterSearchResult[]): RegisterRow[] {
  return results.map((r, i) => ({
    rowId: String(i),
    name: r.name,
    fqid: r.fqid,
    purpose: r.purpose,
  }));
}
function variableRows(results: VariableSearchResult[]): VariableRow[] {
  return results.map((r, i) => ({
    rowId: String(i),
    name: r.name,
    fqid: r.fqid,
    register: r.register,
    slug: r.fqid ? leafSlug(r.fqid) : "",
    definition: r.definition,
  }));
}
function classificationRows(
  results: ClassificationSearchResult[],
): ClassificationRow[] {
  return results.map((r, i) => ({
    rowId: String(i),
    short: r.short_name ?? r.name,
    name: r.name,
    fqid: r.fqid,
    terminalFqid: r.terminal_fqid,
  }));
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

// Split a variable/classification group's mixed results into leaf hits (flat
// table rows) and folded families (<details>) — the two render as distinct
// sub-blocks: the leaf DataTable first, the folds after under a muted sub-label.
function splitVariableResults(results: VariableSearchResult[] | unknown[]): {
  leaves: VariableSearchResult[];
  folds: ConceptGroupSearchResult[];
} {
  const leaves: VariableSearchResult[] = [];
  const folds: ConceptGroupSearchResult[] = [];
  for (const r of results as { type: string }[]) {
    if (isConceptGroup(r)) folds.push(r);
    else leaves.push(r as VariableSearchResult);
  }
  return { leaves, folds };
}
function splitClassificationResults(results: unknown[]): {
  leaves: ClassificationSearchResult[];
  folds: (ConceptGroupSearchResult | ClassificationSuccessionSearchResult)[];
} {
  const leaves: ClassificationSearchResult[] = [];
  const folds: (
    | ConceptGroupSearchResult
    | ClassificationSuccessionSearchResult
  )[] = [];
  for (const r of results as { type: string }[]) {
    if (isConceptGroup(r) || isClassificationSuccession(r)) folds.push(r);
    else leaves.push(r as ClassificationSearchResult);
  }
  return { leaves, folds };
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
        {@const heading = GROUP_HEADINGS[group.group]}
        {@const caption = showingOf(group.results.length, group.total_count)}
        <section class="group">
          <h3>
            {#if heading.tone}<span class="heading-tag"
                ><Tag tone={heading.tone}>{heading.label}</Tag></span
              >{:else}{heading.label}{/if}
            {#if caption}<span class="count">{caption}</span>{/if}
          </h3>

          {#if group.group === "registers"}
            <!-- Registers → a 2-column DataTable: Register (name → catalog link)
                 + Description (purpose, 2-line clamp). The whole row navigates to
                 the hit (selection-as-navigation); the name stays a real <a> so
                 middle-click / open-in-new-tab / screen readers keep a link. -->
            <DataTable
              columns={registerColumns}
              rows={registerRows(group.results as RegisterSearchResult[])}
              getRowId={(r) => r.rowId}
              onselect={(r) => navigateTo(r.fqid)}
            >
              {#snippet cell(row, column)}
                {#if column.key === "name"}
                  {@render leafName(row.fqid, row.name)}
                {:else if row.purpose}
                  <span class="clamp-2">{row.purpose}</span>
                {/if}
              {/snippet}
            </DataTable>
          {:else if group.group === "variables"}
            {@const split = splitVariableResults(group.results)}
            {#if split.leaves.length > 0}
              <DataTable
                columns={variableColumns}
                rows={variableRows(split.leaves)}
                getRowId={(r) => r.rowId}
                onselect={(r) => navigateTo(r.fqid)}
              >
                {#snippet cell(row, column)}
                  {#if column.key === "name"}
                    {@render leafName(row.fqid, row.name)}
                    {#if row.definition}
                      <span class="sub muted">{row.definition}</span>
                    {/if}
                  {:else if column.key === "register"}
                    <!-- Register is PROMINENT (#808 round 2): a display-name
                         string (no fqid), its own normal-weight column. -->
                    {#if row.register}
                      <span class="register">{row.register}</span>
                    {/if}
                  {:else}
                    <!-- The catalog's canonical column/binding identifier: the
                         leaf slug (NOT the full FQID path). -->
                    {row.slug}
                  {/if}
                {/snippet}
              </DataTable>
            {/if}
            {@render foldedFamilies(split.folds)}
          {:else if group.group === "classifications"}
            {@const split = splitClassificationResults(group.results)}
            {#if split.leaves.length > 0}
              <DataTable
                columns={classificationColumns}
                rows={classificationRows(split.leaves)}
                getRowId={(r) => r.rowId}
                onselect={(r) => navigateTo(r.fqid)}
              >
                {#snippet cell(row, column)}
                  {#if column.key === "short"}
                    {@render leafName(row.fqid, row.short)}
                    <!-- A lone non-current edition hit (#571): link to the
                         current/terminal edition so the user can jump forward.
                         Rendered inside the row so it still navigates to
                         `terminal_fqid` (a nested link DataTable's
                         fromInteractiveChild guard leaves alone). -->
                    {#if row.terminalFqid}
                      <a
                        class="terminal-link"
                        href={catalogHref(row.terminalFqid)}
                      >
                        → current edition
                      </a>
                    {/if}
                  {:else if row.name && row.name !== row.short}
                    <span class="sub muted">{row.name}</span>
                  {/if}
                {/snippet}
              </DataTable>
            {/if}
            {@render foldedFamilies(split.folds)}
          {:else if group.group === "codes"}
            <!-- Per-code-system subsections (#393 item 3). A code fans out to
                 multiple owner targets, so it is NOT one-row-one-target — it
                 stays a compact list, not a DataTable. The codes are already
                 item-2-ordered (classification-backed first), and
                 groupCodesBySystem preserves first-appearance order, so curated
                 systems lead; null/empty code_system folds into a trailing
                 "Register-local" subsection. -->
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

<!-- A leaf hit's NAME cell: a catalog link when FQID-addressable, else plain text
     (the hit has no catalog node). The raw FQID is NEVER shown (#808 round 2) —
     the per-type slug/register columns carry the compact identifier. The link is
     kept REAL (a real <a href>) so middle-click / open-in-new-tab / screen-reader
     users get a link even though the whole row also navigates via DataTable
     selection (the nested link + row-nav don't double-fire — DataTable's
     fromInteractiveChild guards it). -->
{#snippet leafName(fqid: string | null | undefined, label: string | null | undefined)}
  {#if fqid}
    <a class="row-link" href={catalogHref(fqid)}>{label ?? leafSlug(fqid)}</a>
  {:else}
    <span class="row-link plain">{label ?? "—"}</span>
  {/if}
{/snippet}

<!-- The folded-family sub-block for the variables / classifications groups (#322
     concept groups + #571 classification succession). They don't fit a flat table
     row (no colspan model — same limitation #806 documented for grouped lists), so
     they stay expandable <details> in their OWN sub-block after the leaf table,
     under a muted sub-label so they read as a distinct treatment. -->
{#snippet foldedFamilies(
  folds: (ConceptGroupSearchResult | ClassificationSuccessionSearchResult)[],
)}
  {#if folds.length > 0}
    <div class="folds">
      <p class="folds-label muted">Grouped families</p>
      {#each folds as fold, i (i)}
        {#if isConceptGroup(fold)}
          {@render conceptGroup(fold)}
        {:else}
          {@render classificationSuccession(fold)}
        {/if}
      {/each}
    </div>
  {/if}
{/snippet}

<!-- A folded concept-group family (#322): the group itself is NOT FQID-addressable,
     so it expands to its member leaves' links. The family hint reads "matched M of
     N" (matched_count of member_count). Member rows show the leaf SLUG (not the
     full FQID) as the compact identifier (#808 round 2). -->
{#snippet conceptGroup(result: ConceptGroupSearchResult)}
  <details class="concept-group">
    <summary>
      <span class="label">{result.group_label}</span>
      <span class="count">
        matched {result.matched_count} of {result.member_count}
      </span>
      {#if result.register}
        <span class="register muted">{result.register}</span>
      {/if}
    </summary>
    <ul class="members">
      {#each result.members as member, i (i)}
        <li>
          <a href={catalogHref(member.fqid)}>
            <span class="label">{member.name ?? leafSlug(member.fqid)}</span>
            <code class="member-slug">{leafSlug(member.fqid)}</code>
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
     family hint reads "matched M of N editions". Member rows show the leaf SLUG
     (not the full FQID), #808 round 2. -->
{#snippet classificationSuccession(result: ClassificationSuccessionSearchResult)}
  {@const editions = result.editions ?? []}
  <details class="concept-group">
    <summary>
      {#if result.fqid}
        <a href={catalogHref(result.fqid)}>
          <span class="label"
            >{result.short_name ?? result.name ?? leafSlug(result.fqid)}</span
          >
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
              <code class="member-slug">{edition.slug}</code>
            </a>
          {:else}
            <span class="label">{edition.name ?? edition.slug}</span>
          {/if}
          {#if edition.effective_year != null}
            <span class="register muted">{edition.effective_year}</span>
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
              <span class="register muted">{owner.register}</span>
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
    <span class="register muted">{result.register}</span>
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
  /* The categorical group-type Tag sits on the heading baseline (replacing the
     old per-row badges, #808 round 2). */
  .heading-tag {
    align-self: center;
    line-height: 1;
  }
  .count {
    color: var(--text-muted);
    font-size: 0.85em;
    font-weight: 400;
    white-space: nowrap;
  }
  /* A leaf-table NAME link / plain-text fallback — the NAME is primary. Long
     Swedish compound words otherwise force a min-content width past the 375px
     mobile canvas (#806); break them only when they can't fit. */
  .row-link {
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  .row-link.plain {
    color: var(--text);
  }
  /* The prominent Register column on a variable hit (#808 round 2): normal text
     weight, NOT muted trailing text — register must be far more visible. */
  .register {
    overflow-wrap: anywhere;
  }
  .register.muted {
    font-size: 0.85em;
  }
  /* A muted secondary line under a leaf name (a variable definition, a
     classification's full name) — sits below the name in the same cell. */
  .sub {
    display: block;
    font-size: 0.9em;
  }
  /* Clamp a register's description to ~2 lines in the DataTable cell; the full
     text lives on the register's own subject page (the #806 treatment). */
  .clamp-2 {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .terminal-link {
    margin-left: 0.5rem;
    font-size: 0.85em;
    font-weight: 400;
  }
  .hit-detail {
    display: block;
    font-size: 0.9em;
  }
  .hit-detail mark {
    background: var(--accent-bg);
    color: var(--accent-ink);
    border-radius: var(--radius);
    padding: 0 0.1em;
  }
  /* The folded-families sub-block under a leaf table. */
  .folds {
    margin-top: 0.75rem;
  }
  .folds-label {
    margin: 0 0 0.4rem;
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-muted);
  }
  .concept-group {
    margin-bottom: 0.35rem;
  }
  .concept-group summary {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem 0.75rem;
    cursor: pointer;
  }
  .concept-group .label {
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  .member-slug {
    color: var(--text-muted);
    font-size: 0.85em;
    font-family: var(--font-mono);
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
  .results {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
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
