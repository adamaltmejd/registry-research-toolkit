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
// #808 (round 3): the variables / classifications groups render as a SINGLE
// CSS-grid "table" (CatalogNodeView's `.children.table` pattern) iterating the
// group's results in their ORIGINAL rank order — a leaf is a whole-row link
// (`display: contents` on an `<a>`, so the row is ONE real link, no role=grid),
// and a fold (concept group / classification succession) is a column-spanning
// row rendering its existing expandable <details> INLINE at its rank position (no
// pulled-out "Grouped families" block). Registers stay a DataTable (the #806
// provider-arm shape). Codes render a compact, code-FIRST grid table per
// code-system bucket (the bucket heading names the classification / value-set);
// each row's owner VARIABLES are the navigable targets (a code has no own page).
// Categorical type identity lives on the GROUP HEADING (a single Tag); the raw
// FQID is hidden everywhere (the leaf SLUG is the only identifier shown).

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
// A group the render loop SKIPS (an unknown/future `group` value with no
// GROUP_HEADINGS entry — see the render guard below) must NOT count as a match:
// its non-empty `results` render nothing, so without this a response carrying ONLY
// an unknown group would blank the body (neither a group NOR "No matches"). Every
// group the loop actually renders (registers/variables/classifications/codes) is a
// GROUP_HEADINGS key, and successions ride INSIDE the classifications group's
// results (a `classification_succession` row, not a top-level group), so this
// exclusion can never suppress "No matches" above rendered content.
const noMatches = $derived(
  q.length >= SEARCH_MIN_QUERY_LENGTH &&
    !results.loading &&
    !results.error &&
    groups.every(
      (g) => g.results.length === 0 || !(g.group in GROUP_HEADINGS),
    ) &&
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

// The keyed-each key for a variables / classifications grid row. Folds the row's
// CONTENT identity (a concept group's `group_key`, else the leaf/succession `fqid`)
// into the key, NOT the bare index — because a fold row renders a native <details>
// disclosure, and a bare-index key makes Svelte REUSE the existing <details> for
// whatever NEW result lands at position `i` on a query refine, carrying the prior
// row's `open` state over (a freshly-fetched fold would render expanded though the
// user never opened it). Identity changes → new key → fresh CLOSED <details>; an
// unchanged row keeps its state. The index stays in the key for UNIQUENESS: a
// concept_group's `group_key` is only register-scoped-unique (the same key recurs
// across registers, #322) and a null/duplicate `fqid` recurs too, so identity alone
// could collide and crash the render (the #379/#391 each_key_duplicate lesson).
function resultKey(
  r:
    | VariableSearchResult
    | ClassificationSearchResult
    | ClassificationSuccessionSearchResult
    | ConceptGroupSearchResult,
  i: number,
): string {
  const identity = isConceptGroup(r) ? r.group_key : r.fqid;
  return `${identity}|${i}`;
}

// ── Registers leaf-table row shape (#808) ────────────────────────────────────
// The registers group keeps its DataTable (the #806 provider-arm shape). Its rows
// carry a STABLE synthetic `rowId` (the array index) so the keyed each is crash-
// proof even when several rows share a null `fqid` and the same name (the
// #379/#391 each_key_duplicate lesson — a natural `fqid` key collides). The
// variables / classifications groups are NOT DataTables — they render the
// `.children.table` CSS grid directly over their raw results (see template), so
// they need no row-shape mapping.
type RegisterRow = {
  rowId: string;
  name: string | null | undefined;
  fqid: string | null;
  purpose: string | null | undefined;
};

const registerColumns: Column<RegisterRow>[] = [
  { key: "name", label: "Register" },
  { key: "purpose", label: "Description" },
];

/** Navigate to a register hit on row-select — a null `fqid` row can't navigate,
 * so bail (its name renders as plain text, the click no-ops). The shell's router
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

// The collapsed code row's MUTED owner-count summary (#808 round 5): the full
// owner totals (`variable_count` / `classification_count`, before the slice cap),
// pluralized, joined with " · ", omitting a zero side — e.g. "11 variables",
// "2 classifications", "11 variables · 2 classifications". An all-zero code shows
// no summary (it's not a disclosure at all — see codeRow).
function usageSummary(result: CodeSearchResult): string {
  const parts: string[] = [];
  if (result.variable_count > 0) {
    parts.push(
      `${result.variable_count} variable${result.variable_count === 1 ? "" : "s"}`,
    );
  }
  if (result.classification_count > 0) {
    parts.push(
      `${result.classification_count} classification${
        result.classification_count === 1 ? "" : "s"
      }`,
    );
  }
  return parts.join(" · ");
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
      <!-- Look the heading up ABOVE the render guard so an unknown / future `group`
           value (the backend documents `group` as an extension point) is SKIPPED,
           not crashed: `heading` is undefined for it, the `&& heading` guard fails,
           and we render nothing for that group instead of dereferencing
           `heading.tone` on undefined and crashing the whole search page. -->
      {@const heading = GROUP_HEADINGS[group.group]}
      {#if group.results.length > 0 && heading}
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
            <!-- #808 round 3: ONE CSS-grid table over the group's results IN RANK
                 ORDER (CatalogNodeView's `.children.table`). A leaf variable is a
                 whole-row link (`display: contents` on the <a>); a concept-group
                 fold is a column-spanning row rendering its <details> INLINE at
                 its rank position. Columns: Variable (name + muted definition) ·
                 Register (PROMINENT, own column) · Column (leaf slug, mono). -->
            <div class="children table cols-3" role="presentation">
              <div class="head-row" aria-hidden="true">
                <span class="col-head">Variable</span>
                <span class="col-head">Register</span>
                <span class="col-head">Column</span>
              </div>
              {#each group.results as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  <div class="span-row">{@render conceptGroup(result)}</div>
                {:else}
                  {@const v = result as VariableSearchResult}
                  {@render variableLeafRow(v)}
                {/if}
              {/each}
            </div>
          {:else if group.group === "classifications"}
            <!-- #808 round 3: ONE CSS-grid table over the group's results IN RANK
                 ORDER. A leaf classification is a whole-row link; a concept-group
                 or classification-succession fold is a column-spanning row with
                 its <details> INLINE. Columns: Classification (short_name ?? name,
                 + the "→ current edition" terminal link) · Name (full name when it
                 differs from the short name). -->
            <div class="children table cols-2" role="presentation">
              <div class="head-row" aria-hidden="true">
                <span class="col-head">Classification</span>
                <span class="col-head">Name</span>
              </div>
              {#each group.results as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  <div class="span-row">{@render conceptGroup(result)}</div>
                {:else if isClassificationSuccession(result)}
                  <div class="span-row">
                    {@render classificationSuccession(result)}
                  </div>
                {:else}
                  {@const c = result as ClassificationSearchResult}
                  {@render classificationLeafRow(c)}
                {/if}
              {/each}
            </div>
          {:else if group.group === "codes"}
            <!-- Per-code-system buckets (#393 item 3, #808 round 5). The bucket
                 heading NAMES the classification / value-set the codes come from
                 (the null bucket → "Register-local"), so each code row need NOT
                 repeat its owner classification. The codes are already
                 item-2-ordered (classification-backed first), and
                 groupCodesBySystem preserves first-appearance order, so curated
                 systems lead; null/empty code_system folds into the trailing
                 "Register-local" bucket. Each bucket is a compact, code-FIRST grid
                 table — the CODE is the highlighted primary column, the Label the
                 second, and a MUTED owner-count summary the third. A code WITH
                 owners is a native <details> DISCLOSURE (the <summary> is the
                 aligned collapsed row, keyboard- + `aria-expanded`-correct for
                 free) that expands an indented owner sub-table; an OWNERLESS code
                 (the common value-set code) is a plain, non-expandable Code · Label
                 row with no count. -->
            {#each groupCodesBySystem(group.results) as system (system.key)}
              <div class="code-system">
                <h4 class="code-system-heading">{system.label}</h4>
                <div class="children table codes" role="presentation">
                  <div class="head-row code-cells" aria-hidden="true">
                    <span class="col-head">Code</span>
                    <span class="col-head">Label</span>
                    <span class="col-head">Used in</span>
                  </div>
                  <!-- Key by `code|index`, NOT the bare index: each code is a
                       native <details> disclosure, and a bare-index key makes
                       Svelte REUSE the existing <details> element for whatever NEW
                       code lands at position `i` on a query refine, carrying the
                       prior code's `open` state over (a freshly-fetched code would
                       render expanded though the user never opened it). Folding the
                       `code` into the key means a different code at `i` → new key →
                       fresh CLOSED <details>; an unchanged code keeps its state. The
                       index stays in the key for uniqueness — duplicate `code`
                       values DO recur within one bucket (the each_key_duplicate
                       lesson), so `code` alone could collide and crash the render. -->
                  {#each system.codes as result, i (`${result.code}|${i}`)}
                    {@render codeRow(result)}
                  {/each}
                </div>
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

<!-- The registers DataTable's NAME cell: a real catalog link when FQID-addressable,
     else plain text. The raw FQID is never shown — the name is the only label, and
     the whole row navigates via DataTable selection (the nested link + row-nav
     don't double-fire — DataTable's fromInteractiveChild guards it). -->
{#snippet leafName(fqid: string | null | undefined, label: string | null | undefined)}
  {#if fqid}
    <a class="row-link" href={catalogHref(fqid)}>{label ?? leafSlug(fqid)}</a>
  {:else}
    <span class="row-link plain">{label ?? "—"}</span>
  {/if}
{/snippet}

<!-- A LEAF variable row (#808 round 3): a whole-row link via `display: contents`
     on the <a> — its child <span>s become the grid cells, so the WHOLE row is one
     real link (middle-click / open-in-new-tab / screen-reader friendly), no
     role=grid, no nested interactive elements. A null-fqid leaf can't navigate, so
     it renders as a non-link row. The raw FQID is never shown — the Column cell
     carries the leaf SLUG only. -->
{#snippet variableLeafRow(v: VariableSearchResult)}
  {#if v.fqid}
    <a class="leaf-row" href={catalogHref(v.fqid)}>
      <span class="name-cell">
        <span class="row-link">{v.name ?? leafSlug(v.fqid)}</span>
        {#if v.definition}<span class="sub muted">{v.definition}</span>{/if}
      </span>
      <!-- Register is PROMINENT: a display-name string, its own normal-weight
           column (NOT a muted trailing label). -->
      <span class="register">{v.register ?? ""}</span>
      <!-- The catalog's canonical column/binding identifier: the leaf slug. -->
      <code class="slug-cell mono muted">{leafSlug(v.fqid)}</code>
    </a>
  {:else}
    <div class="leaf-row plain">
      <span class="name-cell"><span class="row-link plain">{v.name ?? "—"}</span>
        {#if v.definition}<span class="sub muted">{v.definition}</span>{/if}
      </span>
      <span class="register">{v.register ?? ""}</span>
      <span class="slug-cell"></span>
    </div>
  {/if}
{/snippet}

<!-- A LEAF classification row (#808 round 3): whole-row link (display:contents),
     short_name ?? name as the primary cell + the "→ current edition" terminal link
     when set; the full name fills the second column when it differs. The terminal
     link is a SECOND interactive target, so a leaf carrying one can't be a single
     whole-row link — that case renders as a non-link row whose name is its own
     <a> when its own fqid resolves, else plain text (one link per nav target, no
     nesting). The terminal link renders INDEPENDENTLY of own-fqid resolvability: a
     malformed vintage (fqid: null) that still carries a terminal_fqid must keep its
     "→ current edition" target — that's the only navigable hit for the row. -->
{#snippet classificationLeafRow(c: ClassificationSearchResult)}
  {@const short = c.short_name ?? c.name}
  {@const showName = c.name && c.name !== short}
  {#if c.terminal_fqid}
    <div class="leaf-row{c.fqid ? '' : ' plain'}">
      <span class="name-cell">
        {#if c.fqid}
          <a class="row-link" href={catalogHref(c.fqid)}>{short ?? leafSlug(c.fqid)}</a>
        {:else}
          <span class="row-link plain">{short ?? "—"}</span>
        {/if}
        <a class="terminal-link" href={catalogHref(c.terminal_fqid)}>
          → current edition
        </a>
      </span>
      <span class="name-full muted">{showName ? c.name : ""}</span>
    </div>
  {:else if c.fqid}
    <a class="leaf-row" href={catalogHref(c.fqid)}>
      <span class="name-cell"
        ><span class="row-link">{short ?? leafSlug(c.fqid)}</span></span
      >
      <span class="name-full muted">{showName ? c.name : ""}</span>
    </a>
  {:else}
    <div class="leaf-row plain">
      <span class="name-cell"><span class="row-link plain">{short ?? "—"}</span></span>
      <span class="name-full muted">{showName ? c.name : ""}</span>
    </div>
  {/if}
{/snippet}

<!-- A compact, code-FIRST code row (#808 round 5) — a master-detail disclosure.
     THREE collapsed columns: the highlighted primary CODE (mono + strong + a
     code-tint ink), its Label, and a MUTED owner-COUNT summary ("11 variables" /
     "2 classifications" / "11 variables · 2 classifications", omitting a zero
     side). The owner classification is NOT named per row (the bucket heading
     already names the value-set). A code's owners are no longer exploded inline:
     a code WITH owners (variable_count or classification_count > 0) is a native
     <details>, its <summary> the collapsed grid row (keyboard- + aria-expanded-
     correct for free), expanding an indented owner SUB-TABLE — one row per owner
     MATCH (variable owners first with their muted register, then classification
     owners as a `class`-tone Tag), each a whole-row link to the owner's catalog
     node (the row IS a flex `<a>`; a null-fqid owner → a non-link row), capped per
     side with a muted "+N more" from the count vs the returned slice length. A
     code with ZERO owners (the common classification value-set code) is a plain,
     NON-expandable Code · Label row with no count and no disclosure. -->
{#snippet ownerSubRows(result: CodeSearchResult)}
  {#each result.variables as owner, i (i)}
    {#if owner.fqid}
      <a class="owner-row" href={catalogHref(owner.fqid)}>
        <span class="row-link">{owner.name ?? leafSlug(owner.fqid)}</span>
        {#if owner.register}<span class="register muted">{owner.register}</span>{/if}
      </a>
    {:else}
      <div class="owner-row plain">
        <span class="row-link plain">{owner.name ?? "—"}</span>
        {#if owner.register}<span class="register muted">{owner.register}</span>{/if}
      </div>
    {/if}
  {/each}
  {#if result.variable_count > result.variables.length}
    <div class="owner-row more-row">
      <span class="more muted">
        +{result.variable_count - result.variables.length} more
      </span>
    </div>
  {/if}
  {#each result.classifications as owner, i (i)}
    {#if owner.fqid}
      <a class="owner-row" href={catalogHref(owner.fqid)}>
        <span class="row-link">{owner.short_name ?? owner.name ?? leafSlug(owner.fqid)}</span>
        <span class="owner-kind"><Tag tone="class">classification</Tag></span>
      </a>
    {:else}
      <div class="owner-row plain">
        <span class="row-link plain">{owner.short_name ?? owner.name ?? "—"}</span>
        <span class="owner-kind"><Tag tone="class">classification</Tag></span>
      </div>
    {/if}
  {/each}
  {#if result.classification_count > result.classifications.length}
    <div class="owner-row more-row">
      <span class="more muted">
        +{result.classification_count - result.classifications.length} more
      </span>
    </div>
  {/if}
{/snippet}
{#snippet codeCells(result: CodeSearchResult)}
  <span class="code-cells">
    <code class="code-cell mono">{result.code}</code>
    <span class="code-label">{result.label}</span>
    <span class="usage-count muted">{usageSummary(result)}</span>
  </span>
{/snippet}
{#snippet codeRow(result: CodeSearchResult)}
  {#if result.variable_count > 0 || result.classification_count > 0}
    <details class="code-row code-disclosure">
      <summary>{@render codeCells(result)}</summary>
      <div class="owner-table">{@render ownerSubRows(result)}</div>
    </details>
  {:else}
    <div class="code-row">{@render codeCells(result)}</div>
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

  /* ── CSS-grid result table (#808 round 3) ─────────────────────────────────
     Mirrors CatalogNodeView's `.children.table`: one grid on the container
     aligns columns ACROSS rows; a LEAF row's <a> is `display: contents` so the
     <a>'s children become the grid cells — the WHOLE row is one real link, no
     nesting / no role=grid. A FOLD or HEADER row spans all columns. Columns are
     `minmax(0, …)` so long Swedish compounds shrink instead of overflowing the
     375px canvas (#806). */
  .children.table {
    display: grid;
    column-gap: var(--space-3);
    /* STRETCH (not baseline): a leaf row's cells differ in height (a variable's
       definition sub-line / a classification's full-name cell make the name cell
       taller than its siblings). With baseline, each cell's own bottom border lands
       at a different vertical position, so the row separator splits into staggered
       hairline segments. Stretch sizes every cell to the row's full height so their
       bottom borders align into ONE continuous rule; cell CONTENT is top-aligned
       (below) so multi-line cells grow downward and still read top-down. */
    align-items: stretch;
  }
  .children.table.cols-3 {
    /* Variable · Register · Column(slug) */
    grid-template-columns: minmax(0, 2fr) minmax(0, 1fr) minmax(0, max-content);
  }
  .children.table.cols-2 {
    /* Classification · Name */
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  }
  /* The codes bucket is a master-detail disclosure list (#808 round 5), NOT a flat
     grid: a `<details>` row must STACK its collapsed <summary> over its expanded
     owner sub-table, which a `display:contents` grid leaf can't do. So the bucket
     is a FLEX COLUMN of rows; column alignment lives on an inner `.code-cells`
     grid that the head row AND every collapsed row (a <summary> or the ownerless
     <div>) share with the SAME template. A leading fixed-width MARKER column holds
     the disclosure triangle (a custom rotating glyph — the native list marker is
     suppressed so the cells don't wrap below it); the head row + ownerless rows
     reserve the same marker column (empty) so every collapsed row lines up as one
     aligned table regardless of which rows are disclosures. Overrides the shared
     `.children.table` grid. */
  .children.table.codes {
    display: flex;
    flex-direction: column;
  }
  .children.table.codes > .head-row {
    display: grid;
  }
  .code-cells {
    display: grid;
    /* marker · Code(highlighted) · Label · usage-count. */
    grid-template-columns:
      1rem minmax(0, max-content) minmax(0, 1fr) minmax(0, max-content);
    column-gap: var(--space-3);
    align-items: baseline;
  }
  .code-cells > * {
    min-width: 0;
  }
  /* Every `.code-cells` reserves the leading marker column with an empty `::before`
     so the head row, ownerless rows, and disclosure summaries all align. Only a
     disclosure summary's marker carries the triangle glyph (the native list marker
     is suppressed below so the cells don't wrap onto a second line); it rotates
     down when the <details> is open. */
  .code-cells::before {
    content: "";
  }
  .code-row > summary > .code-cells::before {
    content: "▸";
    color: var(--text-muted);
    font-size: 0.8em;
    transition: transform 0.12s ease;
  }
  .code-row[open] > summary > .code-cells::before {
    transform: rotate(90deg);
  }
  /* A code DISCLOSURE row: <details>; its <summary> carries the collapsed cells. A
     non-disclosure (ownerless) code row carries its `.code-cells` directly. Both
     get the same row padding + hairline so all collapsed rows align as one table.
     The native disclosure marker is suppressed (the custom ::before is the glyph). */
  .code-row > summary {
    cursor: pointer;
    list-style: none;
  }
  .code-row > summary::-webkit-details-marker {
    display: none;
  }
  .code-row > summary,
  .code-row:not(.code-disclosure) {
    padding: var(--space-1) 0;
    border-bottom: 1px solid var(--border);
  }
  .code-row > summary:hover,
  .code-row:not(.code-disclosure):hover {
    background: var(--surface-hover);
  }
  /* The owner SUB-TABLE under an expanded disclosure: indented, one row per owner
     match, each a flex whole-row `<a>` link or a non-link row. */
  .owner-table {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    margin: 0.4rem 0 0.6rem 1.25rem;
  }
  .owner-row {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    color: inherit;
    text-decoration: none;
    overflow-wrap: anywhere;
    font-size: 0.9em;
  }
  .owner-row:hover .row-link {
    text-decoration: underline;
  }
  /* Keyboard focus on a whole-row owner link. Unlike `.leaf-row`, an owner row IS a
     flex `<a>` with its own box, so a normal box-shadow focus ring draws fine (the
     shared `--focus-ring` token, matching DataTable's selectable rows). */
  .owner-row:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .owner-kind {
    line-height: 1;
  }
  /* The uppercase micro-label header row (matches DataTable's <th> treatment). */
  .head-row {
    display: contents;
  }
  .col-head {
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    color: var(--text-muted);
    padding-bottom: var(--space-1);
    border-bottom: 1px solid var(--border);
  }
  /* A LEAF row dissolves into the grid so its cells land in the columns; a
     hairline separator + a hover affordance read the row as a unit. The row is a
     real <a> (whole-row link) OR a <div> (null-fqid / second-link cases). */
  .leaf-row {
    display: contents;
    color: inherit;
    text-decoration: none;
  }
  .leaf-row > * {
    min-width: 0;
    padding: var(--space-1) 0;
    border-bottom: 1px solid var(--border);
  }
  /* Hover the whole row (every cell tints) — the grid has no row element, so key
     off the contents-link's hover via :hover on the <a>. */
  .leaf-row:hover > * {
    background: var(--surface-hover);
  }
  /* NOTE (#808 a11y fork): a `display: contents` <a> is NOT keyboard-focusable in
     Chromium — it's dropped from sequential tab navigation entirely (verified: Tab
     skips straight past it), so a `.leaf-row:focus-visible` rule could never fire.
     The real defect is that the whole-row leaf link is unreachable by keyboard at
     all; a CSS focus indicator can't fix that. The same pattern (and gap) lives in
     CatalogNodeView's `.children.table` leaf rows. Surfaced to the lead — fixing it
     needs a focusability change (tabindex / not using display:contents for the
     link), which is a larger re-architecture than #808's scope. */
  /* A FOLD row (concept group / succession <details>) spans all columns and owns
     its own internal layout, sitting inline at its rank position. */
  .span-row {
    grid-column: 1 / -1;
    padding: var(--space-1) 0;
    border-bottom: 1px solid var(--border);
  }
  /* The name cell stacks the primary name over an optional muted sub-line. */
  .name-cell {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .name-full {
    font-size: 0.9em;
    overflow-wrap: anywhere;
  }
  .slug-cell {
    font-size: 0.85em;
    text-align: right;
    /* A long unbroken mono slug must break to fit the narrow (375px) canvas
       instead of spilling past its minmax(0, max-content) track (#806). */
    overflow-wrap: anywhere;
  }
  /* The CODE is the highlighted primary column: mono + strong + a code-tint ink
     so it reads as the main thing in the row (#808 round 3). */
  .code-cell {
    font-family: var(--font-mono);
    font-weight: 700;
    color: var(--cat-code-ink);
    overflow-wrap: anywhere;
  }
  .code-label {
    overflow-wrap: anywhere;
  }
  /* The MUTED owner-count summary in the collapsed code row's third column —
     right-aligned to read as a trailing tally, mirroring the variable slug cell. */
  .usage-count {
    font-size: 0.85em;
    text-align: right;
    white-space: nowrap;
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
  /* The docs group (#394) stays a simple vertical list. */
  .results {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .more {
    font-size: 0.85em;
  }
</style>
