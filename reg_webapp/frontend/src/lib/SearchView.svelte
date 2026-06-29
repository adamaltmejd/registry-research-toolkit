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
import {
  catalogHref,
  classGroupHref,
  fqidSegments,
  groupHref,
  leafSlug,
  showingOf,
} from "./catalog";
import { parseInlineMarkdown } from "./inline_markdown";
import { router } from "./router.svelte";
import { Panel } from "./ui";

// The routed search-results panel (#379). Reads `?q=` off the router and renders
// the four ORDERED, typed groups GET /api/search returns (registers / variables /
// classifications / codes). Every leaf navigates via a plain internal <a> the
// shell's `use:link` intercepts — never `router.navigate` from here.
//
// #808 (round 3): the registers / variables / classifications groups render as a
// SINGLE CSS-grid "table" (CatalogNodeView's `.children.table` pattern) iterating
// the group's results in their ORIGINAL rank order — a leaf is a whole-row link
// (an `<a>` that is `display: grid` + `grid-template-columns: subgrid` spanning
// `1 / -1`, so the row is ONE real, KEYBOARD-FOCUSABLE link whose cells align to
// the parent grid's tracks — NOT `display: contents`, which drops the anchor from
// Chromium's sequential tab order entirely, #808 a11y fork). Concept-group hits
// are also flat links: to the group page when addressable, otherwise to member
// leaves. Classification succession stays a disclosure because it represents an
// edition chain under one terminal classification. Codes render a compact,
// code-FIRST grid table per code-system bucket (the bucket heading names the
// classification / value-set);
// each row's owner VARIABLES are the navigable targets (a code has no own page).
// Group headings stay plain text; the raw FQID is hidden everywhere. Variable row
// headings surface only delivery-column metadata as compact chips, while the
// owning register sits in the muted detail line with the definition.
//
// The registers group previously used a DataTable with selection-as-navigation,
// but a null-fqid register made the row focusable/clickable while `navigateTo`
// no-opped — an interactive-looking dead row (L319). DataTable has no per-row
// opt-out of selection (selectable is table-wide), so registers now share the
// SAME subgrid whole-row-link pattern: a null-fqid register renders as a plain,
// non-interactive `<div>` row, never a dead tab stop.

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
      (g) => displayResults(g).length === 0 || !(g.group in GROUP_HEADINGS),
    ) &&
    !docs.loading &&
    !docsHasHits,
);

// Per-group heading labels. Keep these as normal text headings; result type and
// context live inside rows, not in heading badges.
const GROUP_HEADINGS = {
  registers: { label: "Registers" },
  variables: { label: "Variables" },
  classifications: { label: "Classifications" },
  codes: { label: "Codes / values" },
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
// into the key. The index stays in the key for UNIQUENESS: a concept_group's
// `group_key` is only register-scoped-unique (the same key recurs across
// registers, #322) and a null/duplicate `fqid` recurs too, so identity alone could
// collide and crash the render (the #379/#391 each_key_duplicate lesson).
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

function memberRegisterFqid(
  member: ConceptGroupSearchResult["members"][number],
): string | null {
  const [provider, register] = fqidSegments(member.fqid);
  return provider && register ? `${provider}/${register}` : null;
}

function sharedMemberRegisterFqid(
  result: ConceptGroupSearchResult,
): string | null {
  const scopes = new Set(
    result.members
      .map((member) => memberRegisterFqid(member))
      .filter((scope): scope is string => scope != null),
  );
  return scopes.size === 1 ? [...scopes][0] : null;
}

function normalizedVariableGroupKey(
  result: ConceptGroupSearchResult,
  registerFqid: string,
): string {
  const [provider, register, key] = fqidSegments(result.group_key);
  return key && `${provider}/${register}` === registerFqid
    ? key
    : result.group_key;
}

function normalizedClassGroupKey(result: ConceptGroupSearchResult): string {
  const [head, key] = fqidSegments(result.group_key);
  return head === "class" && key ? key : result.group_key;
}

function conceptGroupHref(result: ConceptGroupSearchResult): string | null {
  if (result.kind === "classification") {
    return classGroupHref(normalizedClassGroupKey(result));
  }
  const registerFqid = sharedMemberRegisterFqid(result);
  return registerFqid
    ? groupHref(registerFqid, normalizedVariableGroupKey(result, registerFqid))
    : null;
}

type SearchGroup = SearchResponse["groups"][number];
type SearchResult = SearchGroup["results"][number];
type VariableDisplayResult = VariableSearchResult | ConceptGroupSearchResult;
type ClassificationDisplayResult =
  | ClassificationSearchResult
  | ClassificationSuccessionSearchResult
  | ConceptGroupSearchResult;
type CodeOwnerClassification = CodeSearchResult["classifications"][number];
type CodeOwnerVariable = CodeSearchResult["variables"][number];

function isVariableResult(r: { type: string }): r is VariableSearchResult {
  return r.type === "variable";
}

function groupMemberFqids(results: readonly SearchResult[]): Set<string> {
  const fqids = new Set<string>();
  for (const result of results) {
    if (isConceptGroup(result) && result.kind === "variable") {
      for (const member of result.members) {
        fqids.add(member.fqid);
      }
    }
  }
  return fqids;
}

function displayResults(group: SearchGroup): SearchResult[] {
  if (group.group !== "variables") {
    return [...group.results];
  }
  const groupedMembers = groupMemberFqids(group.results);
  if (groupedMembers.size === 0) {
    return [...group.results];
  }
  return group.results.filter(
    (result) =>
      !(
        isVariableResult(result) &&
        result.fqid != null &&
        groupedMembers.has(result.fqid)
      ),
  );
}

function registerDisplayResults(group: SearchGroup): RegisterSearchResult[] {
  return group.group === "registers"
    ? (displayResults(group) as RegisterSearchResult[])
    : [];
}

function variableDisplayResults(group: SearchGroup): VariableDisplayResult[] {
  return group.group === "variables"
    ? (displayResults(group) as VariableDisplayResult[])
    : [];
}

function classificationDisplayResults(
  group: SearchGroup,
): ClassificationDisplayResult[] {
  return group.group === "classifications"
    ? (displayResults(group) as ClassificationDisplayResult[])
    : [];
}

function codeDisplayResults(group: SearchGroup): CodeSearchResult[] {
  return group.group === "codes"
    ? (displayResults(group) as CodeSearchResult[])
    : [];
}

function variableDetailParts(v: VariableSearchResult): string[] {
  return [
    v.register,
    v.definition,
    v.operational_definition
      ? `Operational: ${v.operational_definition}`
      : null,
  ].filter((part): part is string => part != null && part !== "");
}

function groupDetailParts(result: ConceptGroupSearchResult): string[] {
  if (result.kind === "variable") {
    return [result.register].filter(
      (part): part is string => part != null && part !== "",
    );
  }
  return [];
}

// The registers / variables / classifications groups render the `.children.table`
// CSS grid directly over their raw results (see template), so they need no
// row-shape mapping. The keyed each folds the array INDEX into the key (alongside
// `fqid` / `group_key`) so a null/duplicate `fqid` can't collide and crash the
// render (the #379/#391 each_key_duplicate lesson).

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
  href: string | null;
  codes: CodeSearchResult[];
};

function codeSystemHref(result: CodeSearchResult): string | null {
  const owner =
    result.classifications.find(
      (classification) =>
        classification.fqid != null &&
        (classification.short_name ?? classification.name) ===
          result.code_system,
    ) ??
    result.classifications.find(
      (classification) => classification.fqid != null,
    );
  return owner?.fqid ? catalogHref(owner.fqid) : null;
}

function groupCodesBySystem(results: CodeSearchResult[]): CodeSystemBucket[] {
  const buckets = new Map<string | null, CodeSystemBucket>();
  for (const code of results) {
    const key = code.code_system || null;
    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = {
        key,
        label: key ?? REGISTER_LOCAL_LABEL,
        href: codeSystemHref(code),
        codes: [],
      };
      buckets.set(key, bucket);
    } else if (bucket.href == null) {
      bucket.href = codeSystemHref(code);
    }
    bucket.codes.push(code);
  }
  return [...buckets.values()];
}

// The collapsed code row's MUTED owner summary. The common single-classification
// case is represented by the bucket heading/link; only reused codes with multiple
// classification owners repeat that count at row level.
function usageSummary(result: CodeSearchResult): string {
  const parts: string[] = [];
  if (result.variable_count > 1) {
    parts.push(`${result.variable_count} variables`);
  }
  if (result.classification_count > 1) {
    parts.push(`${result.classification_count} classifications`);
  }
  return parts.join(" | ");
}

function singleVariableOwner(
  result: CodeSearchResult,
): CodeOwnerVariable | null {
  return result.variable_count === 1 ? (result.variables[0] ?? null) : null;
}

function secondaryClassificationOwners(
  result: CodeSearchResult,
): CodeOwnerClassification[] {
  if (result.classification_count <= 1) {
    return [];
  }
  const headingHref = codeSystemHref(result);
  return result.classifications.filter((owner) => {
    if (owner.fqid == null) {
      return true;
    }
    return catalogHref(owner.fqid) !== headingHref;
  });
}

function hasExpandableOwners(result: CodeSearchResult): boolean {
  return (
    result.variable_count > 1 ||
    secondaryClassificationOwners(result).length > 0
  );
}

function omittedVariableOwnerCount(result: CodeSearchResult): number {
  return Math.max(0, result.variable_count - result.variables.length);
}

const DELIVERY_COLUMN_LIMIT = 3;

function deliveryColumnNames(result: VariableSearchResult): string[] {
  return result.delivery_column_names ?? [];
}

function deliveryColumnQueryTerms(): string[] {
  return q
    .toLocaleLowerCase()
    .split(/[^\p{Letter}\p{Number}]+/u)
    .filter((term) => term !== "");
}

function deliveryColumnMatchesQuery(column: string): boolean {
  const haystack = column.toLocaleLowerCase();
  return deliveryColumnQueryTerms().some((term) => haystack.includes(term));
}

function visibleDeliveryColumns(result: VariableSearchResult): string[] {
  return [...deliveryColumnNames(result)]
    .sort((a, b) => {
      const aMatched = deliveryColumnMatchesQuery(a);
      const bMatched = deliveryColumnMatchesQuery(b);
      if (aMatched !== bMatched) {
        return aMatched ? -1 : 1;
      }
      return 0;
    })
    .slice(0, DELIVERY_COLUMN_LIMIT);
}

function hiddenDeliveryColumnCount(result: VariableSearchResult): number {
  return Math.max(
    0,
    deliveryColumnNames(result).length - DELIVERY_COLUMN_LIMIT,
  );
}

function closeSearch(): void {
  router.replace(router.searchReturnUrl);
}
</script>

<article class="search-view">
  <div class="search-heading">
    <h2>Search</h2>
    <button type="button" class="close-search" aria-label="Close search" onclick={closeSearch}>
      Close
    </button>
  </div>

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
      {@const renderedResults = displayResults(group)}
      {#if renderedResults.length > 0 && heading}
        {@const caption = showingOf(renderedResults.length, group.total_count)}
        <div class="group">
          <Panel title={heading.label} flush>
            {#snippet meta()}
              {#if caption}<span class="count">{caption}</span>{/if}
            {/snippet}

          {#if group.group === "registers"}
            <!-- Registers share the variables' one-column result shape: primary
                 line is the register name, secondary line is the muted
                 description. No split name/description columns. -->
            <div class="children table cols-1" role="presentation">
              {#each registerDisplayResults(group) as result, i (`${result.fqid}|${i}`)}
                {@render registerLeafRow(result)}
              {/each}
            </div>
          {:else if group.group === "variables"}
            <!-- #808 round 3: ONE CSS-grid table over the group's results IN RANK
                 ORDER (CatalogNodeView's `.children.table`). Variable leaves and
                 concept groups are whole-row subgrid links; no inline grouped
                 disclosures. Delivery-column chips ride in the result heading;
                 register context stays in the muted detail line. -->
            <div class="children table cols-1" role="presentation">
              {#each variableDisplayResults(group) as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  {@render conceptGroup(result)}
                {:else}
                  {@render variableLeafRow(result)}
                {/if}
              {/each}
            </div>
          {:else if group.group === "classifications"}
            <!-- #808 round 3: ONE CSS-grid table over the group's results IN RANK
                 ORDER. A leaf classification or concept group is a whole-row link;
                 classification-succession families emit direct linked rows. -->
            <div class="children table cols-1" role="presentation">
              {#each classificationDisplayResults(group) as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  {@render conceptGroup(result)}
                {:else if isClassificationSuccession(result)}
                  {@render classificationSuccession(result)}
                {:else}
                  {@render classificationLeafRow(result)}
                {/if}
              {/each}
            </div>
          {:else if group.group === "codes"}
            <!-- Per-code-system buckets (#393 item 3, #808 round 5). The bucket
                 heading NAMES the classification / value-set the codes come from
                 (the null bucket → "Register-local"), so each code row need NOT
                 repeat its owner classification. Classification-backed headings
                 link to their classification page. The codes are already
                 item-2-ordered (classification-backed first), and
                 groupCodesBySystem preserves first-appearance order, so curated
                 systems lead; null/empty code_system folds into the trailing
                 "Register-local" bucket. Each bucket is a compact, code-FIRST list
                 — the CODE is highlighted first, followed by the label and, for
                 multi-variable hits, a MUTED variable-count summary. A code with
                 multiple variable matches is a native <details> disclosure; a
                 single variable match is shown as a muted detail line; a code with
                 no variable matches is a plain Code · Label row. -->
            {#each groupCodesBySystem(codeDisplayResults(group)) as system (system.key)}
              <div class="code-system">
                <h4 class="code-system-heading">
                  {#if system.href}
                    <a href={system.href}>{system.label}</a>
                  {:else}
                    {system.label}
                  {/if}
                </h4>
                <div class="children table codes" role="presentation">
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
          </Panel>
        </div>
      {/if}
    {/each}
  {/if}

  <!-- Additive "Documentation" group (#394), driven by the SEPARATE `docs`
       resource and rendered as a SIBLING of the main-search `{#if}` block — so it
       shows whenever it has hits REGARDLESS of the main groups' loading / error /
       timeout / no-match state (full failure isolation: e.g. the main /api/search
       codes sub-query can time out while the separate docs index resolves fine).
       Its own panel with a literal heading (NOT folded into GROUP_HEADINGS,
       which is keyed on the four main group literals). A docs failure / empty /
       absent index is silently omitted; the empty + too-short query states
       short-circuit `docs` to `ingested:false`, so `docsHasHits` is false there. -->
  {#if docsHasHits && docs.data}
    {@const caption = showingOf(
      docs.data.results.length,
      docs.data.total_count,
    )}
    <div class="group">
      <Panel title="Documentation" flush>
        {#snippet meta()}
          {#if caption}<span class="count">{caption}</span>{/if}
        {/snippet}
        <ul class="results">
          <!-- Key by array INDEX: like the other groups, this list is replaced
               wholesale per query, so the index is stable + unique within a render
               (a natural key like `filename` could collide and crash the keyed
               each — see the #379/#391 each_key_duplicate lesson above). -->
          {#each docs.data.results as result, i (i)}
            <li class="integrated-list-row">{@render docHit(result)}</li>
          {/each}
        </ul>
      </Panel>
    </div>
  {/if}
</article>

<!-- A LEAF register row: same one-column visual shape as variable rows. -->
{#snippet registerLeafRow(r: RegisterSearchResult)}
  {#if r.fqid}
    <a class="leaf-row integrated-list-row" href={catalogHref(r.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{r.name ?? leafSlug(r.fqid)}</span>
        </span>
        {#if r.purpose}<span class="result-detail muted clamp-2">{r.purpose}</span>{/if}
      </span>
    </a>
  {:else}
    <div class="leaf-row integrated-list-row plain">
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link plain">{r.name ?? "—"}</span>
        </span>
        {#if r.purpose}<span class="result-detail muted clamp-2">{r.purpose}</span>{/if}
      </span>
    </div>
  {/if}
{/snippet}

<!-- A LEAF variable row (#808 round 3 / a11y): a whole-row, KEYBOARD-FOCUSABLE
     link — the <a> is a subgrid grid (`display:grid` spanning `1 / -1` with
     `grid-template-columns: subgrid`) so its child <span>s land in the parent
     grid's tracks while the anchor stays a real focusable box (middle-click /
     open-in-new-tab / screen-reader / Tab friendly), no role=grid, no nested
     interactive elements. A null-fqid leaf can't navigate, so it renders as a
     non-link <div> row (no focus ring). The raw FQID is never shown; delivery
     columns are compact chips in the heading, while register/definition context
     stays in the muted detail line. -->
{#snippet variableMetaPills(v: VariableSearchResult)}
  {@const columns = visibleDeliveryColumns(v)}
  {@const hidden = hiddenDeliveryColumnCount(v)}
  {#if columns.length > 0 || hidden > 0}
    <span class="result-pills">
      {#each columns as column (column)}
        <code class="col-chip">{column}</code>
      {/each}
      {#if hidden > 0}<span class="more muted column-more">+{hidden}</span>{/if}
    </span>
  {/if}
{/snippet}

{#snippet variableLeafRow(v: VariableSearchResult)}
  {@const detailParts = variableDetailParts(v)}
  {#if v.fqid}
    <a class="leaf-row integrated-list-row" href={catalogHref(v.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{v.name ?? leafSlug(v.fqid)}</span>
          {@render variableMetaPills(v)}
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(detailParts)}
        {/if}
      </span>
    </a>
  {:else}
    <div class="leaf-row integrated-list-row plain">
      <span class="name-cell"><span class="result-title">
          <span class="row-link plain">{v.name ?? "—"}</span>
          {@render variableMetaPills(v)}
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(detailParts)}
        {/if}
      </span>
    </div>
  {/if}
{/snippet}

{#snippet detailLine(parts: string[])}
  <span class="result-detail muted">
    {#each parts as part, i (i)}
      <span>{part}</span>
    {/each}
  </span>
{/snippet}

<!-- A LEAF classification row (#808 round 3 / a11y): whole-row, keyboard-focusable
     subgrid link, short_name ?? name as the primary cell + the "→ current edition"
     terminal link when set; the full name fills the second column when it differs.
     The terminal link is a SECOND interactive target, so a leaf carrying one can't
     be a single whole-row link — that case renders as a non-link <div> row whose
     name is its own <a> when its own fqid resolves, else plain text (one link per
     nav target, no nesting; the nested name + terminal <a>s stay normal focusable
     inline links). The terminal link renders INDEPENDENTLY of own-fqid
     resolvability: a malformed vintage (fqid: null) that still carries a
     terminal_fqid must keep its "→ current edition" target — the only navigable
     hit for the row. -->
{#snippet classificationLeafRow(c: ClassificationSearchResult)}
  {@const short = c.short_name ?? c.name}
  {@const showName = c.name && c.name !== short}
  {#if c.terminal_fqid}
    <div class="leaf-row integrated-list-row{c.fqid ? '' : ' plain'}">
      <span class="name-cell">
        <span class="result-title">
          {#if c.fqid}
            <a class="row-link" href={catalogHref(c.fqid)}
              >{short ?? leafSlug(c.fqid)}</a
            >
          {:else}
            <span class="row-link plain">{short ?? "—"}</span>
          {/if}
        </span>
        {#if showName || c.terminal_fqid}
          <span class="result-detail muted">
            {#if showName}<span>{c.name}</span>{/if}
            <a class="detail-link" href={catalogHref(c.terminal_fqid)}>
              current edition
            </a>
          </span>
        {/if}
      </span>
    </div>
  {:else if c.fqid}
    <a class="leaf-row integrated-list-row" href={catalogHref(c.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{short ?? leafSlug(c.fqid)}</span>
        </span>
        {#if showName}<span class="result-detail muted">{c.name}</span>{/if}
      </span>
    </a>
  {:else}
    <div class="leaf-row integrated-list-row plain">
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link plain">{short ?? "—"}</span>
        </span>
        {#if showName}<span class="result-detail muted">{c.name}</span>{/if}
      </span>
    </div>
  {/if}
{/snippet}

<!-- A compact, code-FIRST code row. The bucket heading/link represents the normal
     single-classification owner; reused codes with multiple classifications repeat
     the secondary classification owners in the expansion. Multiple owners use a
     native <details> disclosure; one variable owner is a non-expandable row with
     the matched variable in a muted detail line; zero owners render as a plain
     Code · Label row. -->
{#snippet ownerSubRows(result: CodeSearchResult)}
  {@const classificationOwners = secondaryClassificationOwners(result)}
  {@const omittedVariables = omittedVariableOwnerCount(result)}
  {#each result.variables as owner, i (i)}
    {#if owner.fqid}
      <a class="owner-row integrated-list-row" href={catalogHref(owner.fqid)}>
        <span class="row-link">{owner.name ?? leafSlug(owner.fqid)}</span>
        {#if owner.register}<span class="register muted">{owner.register}</span>{/if}
      </a>
    {:else}
      <div class="owner-row integrated-list-row plain">
        <span class="row-link plain">{owner.name ?? "—"}</span>
        {#if owner.register}<span class="register muted">{owner.register}</span>{/if}
      </div>
    {/if}
  {/each}
  {#if omittedVariables > 0}
    <div class="owner-row integrated-list-row owner-note plain">
      <span class="muted">
        Showing first {result.variables.length} of {result.variable_count} variables
      </span>
    </div>
  {/if}
  {#each classificationOwners as owner, i (`${owner.fqid ?? owner.short_name ?? owner.name}|${i}`)}
    {#if owner.fqid}
      <a class="owner-row integrated-list-row" href={catalogHref(owner.fqid)}>
        <span class="row-link">
          {owner.short_name ?? owner.name ?? leafSlug(owner.fqid)}
        </span>
        {#if owner.name}<span class="register muted">{owner.name}</span>{/if}
      </a>
    {:else}
      <div class="owner-row integrated-list-row plain">
        <span class="row-link plain">{owner.short_name ?? owner.name ?? "—"}</span>
        {#if owner.name}<span class="register muted">{owner.name}</span>{/if}
      </div>
    {/if}
  {/each}
{/snippet}

{#snippet singleOwnerLine(owner: CodeOwnerVariable)}
  <span class="result-detail muted code-owner-single">
    {#if owner.fqid}
      <a class="detail-link" href={catalogHref(owner.fqid)}
        >{owner.name ?? leafSlug(owner.fqid)}</a
      >
    {:else}
      <span>{owner.name ?? "—"}</span>
    {/if}
    {#if owner.register}<span>{owner.register}</span>{/if}
  </span>
{/snippet}

{#snippet codeCells(result: CodeSearchResult)}
  <span class="code-cells">
    <code class="code-cell mono">{result.code}</code>
    <span class="code-label">{result.label}</span>
    <span class="usage-count muted">{usageSummary(result)}</span>
  </span>
{/snippet}
{#snippet codeRow(result: CodeSearchResult)}
  {@const singleOwner = singleVariableOwner(result)}
  {#if hasExpandableOwners(result)}
    <details class="code-row code-disclosure">
      <summary class="integrated-list-row code-summary">
        <span class="disclosure-icon" aria-hidden="true"></span>
        {@render codeCells(result)}
      </summary>
      <div class="owner-table">{@render ownerSubRows(result)}</div>
    </details>
  {:else}
    <div class="code-row integrated-list-row single-code-row">
      {@render codeCells(result)}
      {#if singleOwner}{@render singleOwnerLine(singleOwner)}{/if}
    </div>
  {/if}
{/snippet}

<!-- A concept-group family (#322): search stays flat. Prefer the first-class group
     subject page when its route is derivable; if not, emit the member leaf links
     directly rather than putting a disclosure inside the results list. -->
{#snippet conceptGroup(result: ConceptGroupSearchResult)}
  {@const href = conceptGroupHref(result)}
  {@const detailParts = groupDetailParts(result)}
  {#if href}
    <a class="leaf-row integrated-list-row group-result-row" href={href}>
      <span class="name-cell">
        <span class="result-title">
          <span class="group-prefix">Group:</span>
          <span class="row-link">{result.group_label}</span>
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(detailParts)}
        {/if}
      </span>
    </a>
  {:else}
    {#each result.members as member, i (`${member.fqid}|${i}`)}
      <a
        class="leaf-row integrated-list-row group-member-row"
        href={catalogHref(member.fqid)}
      >
        <span class="name-cell">
          <span class="result-title">
            <span class="row-link">{member.name ?? leafSlug(member.fqid)}</span>
          </span>
          {@render detailLine([`Group: ${result.group_label}`])}
        </span>
      </a>
    {/each}
  {/if}
{/snippet}

<!-- A classification-succession family (#571): keep search flat like variable
     concept groups. The current edition is the primary linked row; older
     editions are normal linked rows underneath, not an in-list disclosure. -->
{#snippet classificationSuccession(result: ClassificationSuccessionSearchResult)}
  {@const editions = result.editions ?? []}
  {@const primaryLabel = result.short_name ?? result.name ?? "—"}
  {#if result.fqid}
    <a class="leaf-row integrated-list-row group-result-row" href={catalogHref(result.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{primaryLabel}</span>
        </span>
        {@render detailLine([
          `matched ${result.matched_count} of ${editions.length} editions`,
        ])}
      </span>
    </a>
  {:else}
    <div class="leaf-row integrated-list-row group-result-row plain">
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link plain">{primaryLabel}</span>
        </span>
        {@render detailLine([
          `matched ${result.matched_count} of ${editions.length} editions`,
        ])}
      </span>
    </div>
  {/if}
  {#each editions as edition, i (`${edition.fqid ?? edition.slug}|${i}`)}
    {#if edition.fqid && edition.fqid !== result.fqid}
      <a
        class="leaf-row integrated-list-row group-member-row"
        href={catalogHref(edition.fqid)}
      >
        <span class="name-cell">
          <span class="result-title">
            <span class="row-link">{edition.name ?? edition.slug}</span>
          </span>
          {@render detailLine([
            edition.effective_year == null
              ? "Edition"
              : `superseded ${edition.effective_year}`,
          ])}
        </span>
      </a>
    {:else if !edition.fqid}
      <div class="leaf-row integrated-list-row group-member-row plain">
        <span class="name-cell">
          <span class="result-title">
            <span class="row-link plain">{edition.name ?? edition.slug}</span>
          </span>
          {@render detailLine([
            edition.effective_year == null
              ? "Edition"
              : `superseded ${edition.effective_year}`,
          ])}
        </span>
      </div>
    {/if}
  {/each}
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
  .search-heading {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: 1rem;
  }
  .search-heading h2 {
    margin: 0;
  }
  .close-search {
    padding: 0.25rem 0.55rem;
    font: inherit;
    font-size: var(--text-sm);
    color: var(--text-muted);
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    cursor: pointer;
  }
  .close-search:hover {
    color: var(--text);
    border-color: var(--accent);
  }
  .close-search:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
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
    margin: 0;
  }
  .code-system + .code-system {
    border-top: 1px solid var(--border);
  }
  .code-system-heading {
    margin: 0;
    padding: 0.55rem 0.75rem 0.35rem;
    border-bottom: 1px solid var(--border);
    background: var(--surface);
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-muted);
  }
  .code-system-heading a {
    color: inherit;
    text-decoration: none;
  }
  .code-system-heading a:hover {
    color: var(--text);
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
  /* Register text outside variable result pills, e.g. code owners and group folds. */
  .register {
    overflow-wrap: anywhere;
  }
  .register.muted {
    font-size: 0.85em;
  }
  .result-detail {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.15rem 0.75rem;
    font-size: 0.9em;
    overflow-wrap: anywhere;
  }
  .detail-link {
    color: var(--text-muted);
    font-weight: 500;
    text-decoration: none;
  }
  .detail-link:hover {
    color: var(--accent-ink);
  }
  /* Clamp a register's description to ~2 lines in the result row; the full
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

  /* ── Integrated result list (#808 round 3 / a11y) ─────────────────────────
     Mirrors CatalogNodeView's `.children.table`: one grid on the container
     aligns columns ACROSS rows; a LEAF row's <a> is a SUBGRID box (`display:grid`
     spanning `1 / -1` with `grid-template-columns: subgrid`) so the <a>'s children
     land in the PARENT grid's tracks while the anchor itself stays a real,
     keyboard-FOCUSABLE element (a `display:contents` <a> is dropped from Chromium's
     sequential tab order — the #808 a11y defect this round fixes). Inside the search
     Panel it follows the shared integrated-list treatment used by
     RepresentationPicker: rows span the full panel surface, separators run full
     width, and hover uses the accent tint rather than a local grey table hover. */
  .children.table {
    display: grid;
    column-gap: 0;
    /* STRETCH (not baseline): a leaf row's cells differ in height (a variable's
       definition sub-line / a classification's full-name cell make the name cell
       taller than its siblings). With baseline, each cell's own bottom border lands
       at a different vertical position, so the row separator splits into staggered
       hairline segments. Stretch sizes every cell to the row's full height so their
       bottom borders align into ONE continuous rule; cell CONTENT is top-aligned
       (below) so multi-line cells grow downward and still read top-down. */
    align-items: stretch;
  }
  .children.table.cols-1 {
    /* Full-width one-column result rows (registers, variables, classifications, groups). */
    grid-template-columns: minmax(0, 1fr);
  }
  /* The codes bucket is a master-detail disclosure list. A `<details>` row stacks
     its collapsed summary over the expanded owner rows, so this bucket is a flex
     column rather than a subgrid. */
  .children.table.codes {
    display: flex;
    flex-direction: column;
  }
  .code-cells {
    display: grid;
    /* Code(highlighted) · Label · usage-count. */
    grid-template-columns:
      minmax(0, max-content) minmax(0, 1fr) minmax(0, max-content);
    column-gap: var(--space-3);
    align-items: baseline;
  }
  .code-cells > * {
    min-width: 0;
  }
  .code-summary {
    display: grid;
    grid-template-columns: 0.85rem minmax(0, 1fr);
    column-gap: 0.45rem;
    align-items: baseline;
  }
  .disclosure-icon {
    display: inline-grid;
    place-items: center;
    width: 0.85rem;
    height: 0.85rem;
    color: var(--text-muted);
    transform-origin: 50% 50%;
    transition: transform var(--motion-fast) ease;
  }
  .disclosure-icon::before {
    content: "";
    width: 0.5rem;
    height: 0.5rem;
    border-right: 1.5px solid currentColor;
    border-bottom: 1.5px solid currentColor;
    transform: rotate(-45deg);
  }
  .code-row[open] > summary .disclosure-icon {
    transform: rotate(90deg);
  }
  /* A code DISCLOSURE row: <details>; its <summary> carries the collapsed cells.
     Non-disclosure rows carry `.code-cells` directly plus an optional muted owner
     detail line. The native marker is suppressed in favor of the centered chevron. */
  .code-row > summary {
    cursor: pointer;
    list-style: none;
  }
  .code-row > summary::-webkit-details-marker {
    display: none;
  }
  .code-row > summary,
  .single-code-row {
    padding: 0.45rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  .single-code-row {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .code-row:last-child:not([open]) > summary,
  .code-row:last-child.single-code-row {
    border-bottom: none;
  }
  /* Expanded owner rows use the same integrated-list surface, not an inset mini
     table: row highlights and separators span the full panel width. */
  .owner-table {
    display: flex;
    flex-direction: column;
    margin: 0;
  }
  .owner-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.45fr);
    align-items: baseline;
    column-gap: 1rem;
    row-gap: 0.1rem;
    color: inherit;
    text-decoration: none;
    overflow-wrap: anywhere;
    font-size: 0.9em;
    padding: 0.35rem 0.75rem 0.35rem 1.8rem;
    border-bottom: 1px solid var(--border);
  }
  .owner-row > * {
    min-width: 0;
  }
  .owner-note > * {
    grid-column: 1 / -1;
  }
  .owner-row:hover .row-link {
    color: var(--accent-ink);
  }
  .code-row:last-child .owner-row:last-child {
    border-bottom: none;
  }
  /* Keyboard focus on a whole-row owner link. Unlike `.leaf-row`, an owner row IS a
     flex `<a>` with its own box, so a normal box-shadow focus ring draws fine (the
     shared `--focus-ring` token, matching DataTable's selectable rows). */
  .owner-row:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  /* A LEAF row is a real, keyboard-focusable box (an <a> whole-row link OR a <div>
     for the null-fqid / second-link cases) that spans every column and aligns its
     own cells to the PARENT grid's tracks via `subgrid` — so the whole row is one
     interactive element AND a focus ring can draw on its box. A hairline separator
     + a hover affordance read the row as a unit. */
  .leaf-row {
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: subgrid;
    column-gap: 0;
    align-items: stretch;
    color: inherit;
    text-decoration: none;
  }
  .leaf-row > * {
    min-width: 0;
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  .leaf-row:last-child > * {
    border-bottom: none;
  }
  /* Hover the whole row (every cell tints). */
  .leaf-row:hover > * {
    background: inherit;
  }
  /* #808 a11y: now the leaf link is a real focusable box (subgrid, NOT
     display:contents), a visible keyboard focus ring draws on it — the shared
     `--focus-ring` token, matching DataTable's selectable rows + the codes
     `.owner-row`. Only the link (<a>) gets the ring; a non-link `.plain` <div> row
     is not focusable and gets none. */
  a.leaf-row:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .group-result-row > .name-cell,
  .group-member-row > .name-cell {
    grid-column: 1 / -1;
  }
  /* The name cell stacks the primary name over an optional muted sub-line. */
  .name-cell {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .result-title {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.25rem 0.45rem;
    min-width: 0;
  }
  .result-pills {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.2rem 0.35rem;
    min-width: 0;
  }
  @media (max-width: 640px) {
    .owner-row {
      grid-template-columns: minmax(0, 1fr);
    }
  }
  .col-chip {
    display: inline-flex;
    align-items: baseline;
    line-height: 1.3;
    padding: 0.05rem 0.4rem;
    border-radius: var(--radius-sm);
    max-width: 100%;
    overflow-wrap: anywhere;
  }
  /* Mirrors RepresentationPicker's delivery-column chip: mono, purple/indigo
     variable hue, and no link affordance on the search-result metadata. */
  .col-chip {
    font-family: var(--font-mono);
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--cat-var-ink);
    border: 1px solid color-mix(in srgb, var(--cat-var) 35%, transparent);
    background: color-mix(in srgb, var(--cat-var) 10%, var(--surface));
  }
  .column-more {
    font-size: 0.85em;
  }
  .group-prefix {
    color: var(--cat-group-ink);
    font-weight: 600;
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
  /* The MUTED variable-count summary in the collapsed code row's third column. */
  .usage-count {
    font-size: 0.85em;
    text-align: right;
    white-space: nowrap;
  }
  /* The docs group (#394) stays a simple vertical list. */
  .results {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0;
  }
  .results li {
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  .results li:last-child {
    border-bottom: none;
  }
  .more {
    font-size: 0.85em;
  }
</style>
