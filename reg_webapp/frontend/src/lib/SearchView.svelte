<script lang="ts">
import type {
  ClassificationHit,
  ClassificationSuccessionHit,
  CodeHit,
  ConceptGroupHit,
  RegisterHit,
  SearchHit,
  SearchPage,
  SearchType,
  VariableHit,
} from "./api";
import { SEARCH_MIN_QUERY_LENGTH, search } from "./api";
import { asyncResource } from "./async.svelte";
import {
  catalogHref,
  classGroupHref,
  fqidSegments,
  groupHref,
  leafSlug,
} from "./catalog";
import { router } from "./router.svelte";
import { Button, Panel } from "./ui";

// The routed search-results panel (#379). Reads `?q=` off the router and renders
// the Rust server's `search` (decision 17: one ranked list per call): one untyped
// page for the top-results strip, then one page per arm, in the fixed order
// registers / variables / classifications / classification codes / register-local
// value sets, each continued by its own cursor. Every leaf navigates via a plain
// internal <a> the shell's `use:link` intercepts — never `router.navigate` here.
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
// degrades to "all" (don't fail the SPA over a hand-edited URL — the toggle just
// renders nothing active). Read inside the `results` fetcher below so the resource
// refetches when `?type=` changes. `value` is the SPA's own name for the two code
// arms together; the server's `type` names one arm.
type Scope = "all" | "register" | "variable" | "classification" | "value";
const SCOPES: readonly Scope[] = [
  "all",
  "register",
  "variable",
  "classification",
  "value",
];
const searchType = $derived.by<Scope>(() => {
  const raw = router.getQueryParam("type");
  return SCOPES.includes(raw as Scope) ? (raw as Scope) : "all";
});

// The toggle's button set: label + the `?type=` value it routes to.
const TYPE_TOGGLE: ReadonlyArray<{ value: Scope; label: string }> = [
  { value: "all", label: "All" },
  { value: "register", label: "Registers" },
  { value: "variable", label: "Variables" },
  { value: "classification", label: "Classifications" },
  { value: "value", label: "Codes / values" },
];

/** Route to the current query scoped to `type` (in-place replace — scope is a
 * refinement, not a new history entry). OMIT `?type=` for `all` so the
 * canonical/shareable URL stays clean. */
function selectType(type: Scope): void {
  const base = `/search?q=${encodeURIComponent(q)}`;
  router.replace(type === "all" ? base : `${base}&type=${type}`);
}

// One result section per call: `type` is the arm, `null` the untyped top results.
// A section's key is its continuation slot.
type Section = { type: SearchType | null; page: SearchPage };
const TOP_LIMIT = 5;
const ARM_LIMIT = 3;
const ARMS: readonly SearchType[] = [
  "register",
  "variable",
  "classification",
  "classification_code",
  "register_value",
];

function armsFor(scope: Scope): readonly SearchType[] {
  if (scope === "all") return ARMS;
  if (scope === "value") return ["classification_code", "register_value"];
  return [scope];
}

function sectionKey(type: SearchType | null): string {
  return type ?? "top";
}

function pageLimit(type: SearchType | null): number {
  return type == null ? TOP_LIMIT : ARM_LIMIT;
}

// asyncResource registers an $effect, so it can't be created conditionally; the
// fetch fn short-circuits a too-short `q` to NO sections WITHOUT a network call
// (and reads `q` so it refetches when the query changes). It also threads the
// teardown `signal` into every `search` so a superseded query aborts its
// in-flight HTTP requests (and the ~12s timeout `search` layers on can abort
// them too). Any failed call fails the search as a whole.
const results = asyncResource<Section[]>((signal) => {
  if (q.length < SEARCH_MIN_QUERY_LENGTH) {
    return Promise.resolve([]);
  }
  // Read `searchType` HERE so the resource refetches when `?type=` changes.
  const types: (SearchType | null)[] = [
    ...(searchType === "all" ? [null] : []),
    ...armsFor(searchType),
  ];
  return Promise.all(
    types.map(async (type) => ({
      type,
      page: await search(q, {
        signal,
        type: type ?? undefined,
        limit: pageLimit(type),
      }),
    })),
  );
});

function syncDetailSeparators(node: HTMLElement): {
  update: () => void;
  destroy: () => void;
} {
  let frame: number | null = null;

  const measure = () => {
    frame = null;
    const separators = [
      ...node.querySelectorAll<HTMLElement>(".detail-separator"),
    ];
    for (const separator of separators) {
      separator.hidden = false;
    }
    for (const separator of separators) {
      const previous = separator.previousElementSibling as HTMLElement | null;
      const next = separator.nextElementSibling as HTMLElement | null;
      if (previous == null || next == null) {
        separator.hidden = true;
        continue;
      }
      const previousRect = previous.getBoundingClientRect();
      const nextRect = next.getBoundingClientRect();
      const previousCenter = previousRect.top + previousRect.height / 2;
      const nextCenter = nextRect.top + nextRect.height / 2;
      separator.hidden = Math.abs(previousCenter - nextCenter) > 4;
    }
  };

  const schedule = () => {
    if (frame != null) return;
    frame = requestAnimationFrame(measure);
  };

  const observer =
    typeof ResizeObserver === "undefined" ? null : new ResizeObserver(schedule);
  observer?.observe(node);
  window.addEventListener("resize", schedule);
  schedule();

  return {
    update: schedule,
    destroy() {
      if (frame != null) {
        cancelAnimationFrame(frame);
      }
      observer?.disconnect();
      window.removeEventListener("resize", schedule);
    },
  };
}

// Distinguish a TIMEOUT abort from every other failure. A supersede/unmount abort
// never reaches here (asyncResource's `cancelled` guard swallows it); a timeout
// abort fires while NOT cancelled, surfacing as an error. asyncResource exposes
// only the stringified error, and `String(e)` on a DOMException is name-prefixed
// (`<name>: <message>`). AbortSignal.timeout's reason is a DOMException named
// "TimeoutError", so match only the spec-stable NAME prefix — the message tail
// after ": " is engine-specific (varies by browser) and must NOT be matched. Only
// this maps to the friendly copy — other errors keep the generic "Search failed".
const timedOut = $derived(results.error?.startsWith("TimeoutError") ?? false);

let continuedPages = $state<Record<string, SearchPage>>({});
let continuationLoading = $state<Record<string, boolean>>({});
let continuationErrors = $state<Record<string, string>>({});
let continuationContext = "";
$effect(() => {
  const context = `${q}\u0000${searchType}`;
  if (context !== continuationContext) {
    continuationContext = context;
    continuedPages = {};
    continuationLoading = {};
    continuationErrors = {};
  }
});
const sections = $derived(
  (results.data ?? []).map((section) => ({
    ...section,
    page: continuedPages[sectionKey(section.type)] ?? section.page,
  })),
);
// The top-results strip only earns its place when it ranks more than one hit:
// a lone hit is already its arm's only row below.
function shown(section: Section): boolean {
  return section.page.items.length > (section.type == null ? 1 : 0);
}
// A searched query (≥ min length) with zero results in every section (distinct
// from the empty / keep-typing hints and from loading). Gate on the min length so
// a 1-char query shows the keep-typing hint, not a spurious "no matches".
const noMatches = $derived(
  q.length >= SEARCH_MIN_QUERY_LENGTH &&
    !results.loading &&
    !results.error &&
    sections.every((section) => section.page.items.length === 0),
);

// Per-section heading labels. Keep these as normal text headings; result type and
// context live inside rows, not in heading badges.
const HEADINGS: Record<string, string> = {
  top: "Top results",
  register: "Registers",
  variable: "Variables",
  classification: "Classifications",
  classification_code: "Classification codes",
  register_value: "Register-local value sets",
};

// Discriminate a variable/classification group's mixed results on `type`.
function isConceptGroup(r: { type: string }): r is ConceptGroupHit {
  return r.type === "group";
}

// A folded classification-succession row (#571) in the classifications group — a
// query hit ≥2 editions of one chain, collapsed onto the terminal edition.
function isClassificationSuccession(r: {
  type: string;
}): r is ClassificationSuccessionHit {
  return r.type === "classification_succession";
}

// The keyed-each key for a variables / classifications grid row. Folds the row's
// CONTENT identity (a concept group's `key`, else the leaf/succession `fqid`)
// into the key. The index stays in the key for UNIQUENESS: a concept group's
// `key` is only register-scoped-unique (the same key recurs across
// registers, #322) and a null/duplicate `fqid` recurs too, so identity alone could
// collide and crash the render (the #379/#391 each_key_duplicate lesson).
function resultKey(
  r:
    | VariableHit
    | ClassificationHit
    | ClassificationSuccessionHit
    | ConceptGroupHit,
  i: number,
): string {
  const identity = isConceptGroup(r) ? r.key : r.fqid;
  return `${identity}|${i}`;
}

function memberRegisterFqid(
  member: ConceptGroupHit["members"][number],
): string | null {
  const [provider, register] = fqidSegments(member.fqid);
  return provider && register ? `${provider}/${register}` : null;
}

function sharedMemberRegisterFqid(result: ConceptGroupHit): string | null {
  const scopes = new Set(
    result.members
      .map((member) => memberRegisterFqid(member))
      .filter((scope): scope is string => scope != null),
  );
  return scopes.size === 1 ? [...scopes][0] : null;
}

function normalizedVariableGroupKey(
  result: ConceptGroupHit,
  registerFqid: string,
): string {
  const [provider, register, key] = fqidSegments(result.key);
  return key && `${provider}/${register}` === registerFqid ? key : result.key;
}

function normalizedClassGroupKey(result: ConceptGroupHit): string {
  const [head, key] = fqidSegments(result.key);
  return head === "class" && key ? key : result.key;
}

function conceptGroupHref(result: ConceptGroupHit): string | null {
  if (result.kind === "classification") {
    return classGroupHref(normalizedClassGroupKey(result));
  }
  const registerFqid = sharedMemberRegisterFqid(result);
  return registerFqid
    ? groupHref(registerFqid, normalizedVariableGroupKey(result, registerFqid))
    : null;
}

type VariableDisplayResult = VariableHit | ConceptGroupHit;
type ClassificationDisplayResult =
  | ClassificationHit
  | ClassificationSuccessionHit
  | ConceptGroupHit;
type CodeOwnerClassification = CodeHit["classifications"][number];
type CodeOwnerVariable = CodeHit["variables"][number];

async function loadMore(section: Section): Promise<void> {
  const key = sectionKey(section.type);
  const cursor = section.page.next_cursor;
  if (cursor == null || continuationLoading[key]) {
    return;
  }
  const requestQuery = q;
  const requestType = searchType;
  continuationLoading[key] = true;
  continuationErrors[key] = "";
  try {
    const next = await search(requestQuery, {
      type: section.type ?? undefined,
      limit: pageLimit(section.type),
      cursor,
    });
    if (q !== requestQuery || searchType !== requestType) return;
    continuedPages[key] = {
      items: [...section.page.items, ...next.items],
      next_cursor: next.next_cursor,
    };
  } catch (error) {
    if (q !== requestQuery || searchType !== requestType) return;
    continuationErrors[key] = String(error);
  } finally {
    if (q === requestQuery && searchType === requestType) {
      continuationLoading[key] = false;
    }
  }
}

function registerDisplayResults(results: SearchHit[]): RegisterHit[] {
  return results as RegisterHit[];
}

function variableDisplayResults(results: SearchHit[]): VariableDisplayResult[] {
  return results as VariableDisplayResult[];
}

function classificationDisplayResults(
  results: SearchHit[],
): ClassificationDisplayResult[] {
  return results as ClassificationDisplayResult[];
}

function codeDisplayResults(results: SearchHit[]): CodeHit[] {
  return results as CodeHit[];
}

function topResultKey(result: SearchHit, i: number): string {
  if (isConceptGroup(result)) {
    return `group|${result.kind}|${result.key}|${i}`;
  }
  if (result.type === "code") {
    return `code|${result.code}|${result.label}|${result.code_system ?? ""}|${i}`;
  }
  const identity = "fqid" in result ? result.fqid : "";
  return `${result.type}|${identity}|${i}`;
}

function normalizedDisplayText(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ").toLocaleLowerCase("sv-SE");
}

function isRepeatedDefinition(
  name: string | null | undefined,
  definition: string | null | undefined,
): boolean {
  return normalizedDisplayText(name) === normalizedDisplayText(definition);
}

const PROVIDER_LABELS: Record<string, string> = {
  fohm: "FoHM",
  fk: "FK",
  lakemedelsverket: "Lakemedelsverket",
  pliktverket: "Pliktverket",
  riksarkivet: "RA",
  scb: "SCB",
  sos: "SoS",
  umu: "UMU",
};

function providerLabelFromFqid(fqid: string | null | undefined): string | null {
  if (!fqid) {
    return null;
  }
  const [provider] = fqidSegments(fqid);
  if (!provider) {
    return null;
  }
  return PROVIDER_LABELS[provider] ?? provider.toLocaleUpperCase("sv-SE");
}

function providerRegisterContext(
  fqid: string | null | undefined,
  register: string | null | undefined,
): string | null {
  const provider = providerLabelFromFqid(fqid);
  if (!register) {
    return provider;
  }
  return provider ? `${provider}: ${register}` : register;
}

type LinkedPill = {
  label: string;
  href: string | null;
};

function registerHrefFromFqid(fqid: string | null | undefined): string | null {
  if (!fqid) {
    return null;
  }
  const [provider, register] = fqidSegments(fqid);
  return provider && register ? catalogHref(`${provider}/${register}`) : null;
}

function providerRegisterPill(
  fqid: string | null | undefined,
  register: string | null | undefined,
): LinkedPill | null {
  const label = providerRegisterContext(fqid, register);
  if (!label) {
    return null;
  }
  return { label, href: registerHrefFromFqid(fqid) };
}

function variableRegisterPill(v: VariableHit): LinkedPill | null {
  return providerRegisterPill(v.fqid, v.register_name);
}

function groupRegisterPill(result: ConceptGroupHit): LinkedPill | null {
  if (result.kind !== "variable") {
    return null;
  }
  return providerRegisterPill(
    sharedMemberRegisterFqid(result),
    result.register_name,
  );
}

function ownerRegisterContext(owner: CodeOwnerVariable): string | null {
  return providerRegisterContext(owner.fqid, owner.register_name);
}

function variableDetailParts(v: VariableHit): string[] {
  const definition = isRepeatedDefinition(v.name, v.definition)
    ? null
    : v.definition;
  return [definition, v.operational_definition].filter(
    (part): part is string => part != null && part !== "",
  );
}

// The registers / variables / classifications groups render the `.children.table`
// CSS grid directly over their raw results (see template), so they need no
// row-shape mapping. The keyed each folds the array INDEX into the key (alongside
// `fqid` / `key`) so a null/duplicate `fqid` can't collide and crash the
// render (the #379/#391 each_key_duplicate lesson).

// Classification codes are bucketed by code system (#393 item 3). STABLE group-by
// on `code_system`, preserving FIRST-APPEARANCE order. Register-local value sets
// render in their own top-level group, so they do not need a nested
// "Register-local" subsection heading.
const REGISTER_LOCAL_LABEL = "Register-local";
type CodeSystemBucket = {
  key: string | null;
  label: string;
  href: string | null;
  codes: CodeHit[];
};

function codeSystemHref(result: CodeHit): string | null {
  const owner = codeSystemOwner(result);
  return owner?.fqid ? catalogHref(owner.fqid) : null;
}

function codeSystemOwner(result: CodeHit): CodeOwnerClassification | null {
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
  return owner ?? null;
}

function groupCodesBySystem(results: CodeHit[]): CodeSystemBucket[] {
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
function usageSummary(result: CodeHit): string {
  const parts: string[] = [];
  if (result.variable_count > 1) {
    parts.push(`${result.variable_count} variables`);
  }
  if (result.classification_count > 1) {
    parts.push(`${result.classification_count} classifications`);
  }
  return parts.join(" | ");
}

function singleVariableOwner(result: CodeHit): CodeOwnerVariable | null {
  return result.variable_count === 1 ? (result.variables[0] ?? null) : null;
}

function secondaryClassificationOwners(
  result: CodeHit,
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

function topClassificationOwners(result: CodeHit): CodeOwnerClassification[] {
  const systemOwner = codeSystemOwner(result);
  if (systemOwner == null) {
    return secondaryClassificationOwners(result);
  }
  return [
    systemOwner,
    ...secondaryClassificationOwners(result).filter(
      (owner) => owner !== systemOwner,
    ),
  ];
}

function hasExpandableOwners(result: CodeHit): boolean {
  return (
    result.variable_count > 1 ||
    secondaryClassificationOwners(result).length > 0
  );
}

const DELIVERY_COLUMN_LIMIT = 3;

function deliveryColumnNames(result: VariableHit): string[] {
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

function visibleDeliveryColumns(result: VariableHit): string[] {
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

function hiddenDeliveryColumnCount(result: VariableHit): number {
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
    {#each sections as section (sectionKey(section.type))}
      {@const key = sectionKey(section.type)}
      {@const items = section.page.items}
      {#if shown(section)}
        {@const caption =
          section.type == null
            ? null
            : section.page.next_cursor != null
              ? `${items.length}+ results`
              : `${items.length} ${items.length === 1 ? "result" : "results"}`}
        <div class={section.type == null ? "group top-results-group" : "group"}>
          <Panel title={HEADINGS[key]} flush>
            {#snippet meta()}
              {#if caption}<span class="count">{caption}</span>{/if}
            {/snippet}

          {#if section.type == null}
            <!-- Cross-arm best bets (#393 items 6/7, decision 17): the untyped
                 ranked list. Rows reuse the same typed snippets as the arms below,
                 so this is a ranking layer, not a second row model. -->
            <div class="children table top-results" role="presentation">
              {#each items as result, i (topResultKey(result, i))}
                {@render topResult(result)}
              {/each}
            </div>
          {:else if section.type === "register"}
            <!-- Registers share the variables' one-column result shape: primary
                 line is the register name, secondary line is the muted
                 description. No split name/description columns. -->
            <div class="children table cols-1" role="presentation">
              {#each registerDisplayResults(items) as result, i (`${result.fqid}|${i}`)}
                {@render registerLeafRow(result)}
              {/each}
            </div>
          {:else if section.type === "variable"}
            <!-- #808 round 3: ONE CSS-grid table over the arm's hits IN RANK
                 ORDER (CatalogNodeView's `.children.table`). Variable leaves and
                 concept groups are whole-row subgrid links; no inline grouped
                 disclosures. Delivery-column chips ride in the result heading;
                 register context stays in the muted detail line. -->
            <div class="children table cols-1" role="presentation">
              {#each variableDisplayResults(items) as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  {@render conceptGroup(result)}
                {:else}
                  {@render variableLeafRow(result)}
                {/if}
              {/each}
            </div>
          {:else if section.type === "classification"}
            <!-- #808 round 3: ONE CSS-grid table over the arm's hits IN RANK
                 ORDER. A leaf classification or concept group is a whole-row link;
                 classification-succession families emit direct linked rows. -->
            <div class="children table cols-1" role="presentation">
              {#each classificationDisplayResults(items) as result, i (resultKey(result, i))}
                {#if isConceptGroup(result)}
                  {@render conceptGroup(result)}
                {:else if isClassificationSuccession(result)}
                  {@render classificationSuccession(result)}
                {:else}
                  {@render classificationLeafRow(result)}
                {/if}
              {/each}
            </div>
          {:else if section.type === "classification_code"}
            <!-- Per-code-system buckets (#393 item 3, #808 round 5). The bucket
                 heading NAMES the classification / value-set the codes come from
                 so each code row need NOT repeat its owner classification.
                 Classification-backed headings link to their classification page. -->
            {#each groupCodesBySystem(codeDisplayResults(items)) as system (system.key)}
              <div class="code-system">
                <h4 class="code-system-heading">
                  {@render codeSystemPill(system.label, system.href)}
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
          {:else if section.type === "register_value"}
            <!-- Register-local value-set codes have no owning classification, so
                 the section label is enough. Rows keep the same compact
                 code-FIRST rendering and disclosure behavior as classification
                 code buckets. -->
            <div class="children table codes" role="presentation">
              {#each codeDisplayResults(items) as result, i (`${result.code}|${i}`)}
                {@render codeRow(result)}
              {/each}
            </div>
          {/if}
          {#if section.page.next_cursor != null}
            <div class="continuation">
              <Button
                variant="default"
                size="sm"
                disabled={continuationLoading[key]}
                aria-busy={continuationLoading[key]}
                onclick={() => void loadMore(section)}
              >
                {continuationLoading[key] ? "Loading…" : "Load more"}
              </Button>
            </div>
          {/if}
          {#if continuationErrors[key]}
            <p class="continuation-error error" role="alert">
              Could not load more results. Try again.
            </p>
          {/if}
          </Panel>
        </div>
      {/if}
    {/each}
  {/if}

</article>

<!-- A LEAF register row: same one-column visual shape as variable rows. -->
{#snippet registerLeafRow(r: RegisterHit)}
  {@const detailParts = r.purpose ? [r.purpose] : []}
  {@const context = providerLabelFromFqid(r.fqid)}
  {#if r.fqid}
    <a class="leaf-row integrated-list-row" href={catalogHref(r.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{r.name ?? leafSlug(r.fqid)}</span>
          {#if context}{@render registerContextPill(context)}{/if}
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(null, detailParts, true)}
        {/if}
      </span>
    </a>
  {:else}
    <div class="leaf-row integrated-list-row plain">
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link plain">{r.name ?? "—"}</span>
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(null, detailParts, true)}
        {/if}
      </span>
    </div>
  {/if}
{/snippet}

{#snippet topResult(result: SearchHit)}
  {#if result.type === "register"}
    {@render registerLeafRow(result)}
  {:else if result.type === "variable"}
    {@render variableLeafRow(result)}
  {:else if result.type === "classification"}
    {@render classificationLeafRow(result)}
  {:else if result.type === "classification_succession"}
    {@render classificationSuccession(result)}
  {:else if result.type === "group"}
    {@render conceptGroup(result)}
  {:else if result.type === "code"}
    {@render topCodeRow(result)}
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
{#snippet variableMetaPills(v: VariableHit)}
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

{#snippet variableLeafRow(v: VariableHit)}
  {@const detailParts = variableDetailParts(v)}
  {@const context = variableRegisterPill(v)}
  {#if v.fqid}
    <div class="leaf-row integrated-list-row">
      <span class="name-cell">
        <span class="result-title">
          <a class="row-link" href={catalogHref(v.fqid)}
            >{v.name ?? leafSlug(v.fqid)}</a
          >
          {@render variableMetaPills(v)}
          {#if context}{@render registerContextPill(context.label, context.href)}{/if}
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(null, detailParts)}
        {/if}
      </span>
    </div>
  {:else}
    <div class="leaf-row integrated-list-row plain">
      <span class="name-cell"><span class="result-title">
          <span class="row-link plain">{v.name ?? "—"}</span>
          {@render variableMetaPills(v)}
          {#if context}{@render registerContextPill(context.label, context.href)}{/if}
        </span>
        {#if detailParts.length > 0}
          {@render detailLine(null, detailParts)}
        {/if}
      </span>
    </div>
  {/if}
{/snippet}

{#snippet registerContextPill(context: string, href: string | null = null)}
  {#if href}
    <a class="register-context-chip pill-link" {href}
      >{context}<span class="chip-arrow" aria-hidden="true">↗</span></a
    >
  {:else}
    <span class="register-context-chip">{context}</span>
  {/if}
{/snippet}

{#snippet detailLine(
  context: string | null,
  parts: string[],
  clampParts: boolean = false,
)}
  <span class="result-detail muted" use:syncDetailSeparators>
    {#if context}
      <span class="register-context-chip">{context}</span>
    {/if}
    {#each parts as part, i (i)}
      {#if context || i > 0}
        <span class="detail-separator" aria-hidden="true">·</span>
      {/if}
      <span class:clamp-2={clampParts}>{part}</span>
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
{#snippet classificationLeafRow(c: ClassificationHit)}
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
     native <details> disclosure; one variable owner is a whole-row link with
     matched variable context inline; zero owners render as a plain
     Code · Label row. -->
{#snippet ownerSubRows(
  result: CodeHit,
  includeCodeSystemOwner: boolean = false,
)}
  {@const classificationOwners = includeCodeSystemOwner
    ? topClassificationOwners(result)
    : secondaryClassificationOwners(result)}
  {#each result.variables as owner, i (i)}
    {@const context = ownerRegisterContext(owner)}
    {#if owner.fqid}
      <a class="owner-row integrated-list-row" href={catalogHref(owner.fqid)}>
        {@render ownerInline(owner.name ?? leafSlug(owner.fqid), context)}
      </a>
    {:else}
      <div class="owner-row integrated-list-row plain">
        {@render ownerInline(owner.name ?? "—", context)}
      </div>
    {/if}
  {/each}
  {#each classificationOwners as owner, i (`${owner.fqid ?? owner.short_name ?? owner.name}|${i}`)}
    {@const label =
      owner.short_name ?? owner.name ?? (owner.fqid ? leafSlug(owner.fqid) : "—")}
    {@const detail = owner.name && owner.name !== label ? owner.name : null}
    {#if owner.fqid}
      <a class="owner-row integrated-list-row" href={catalogHref(owner.fqid)}>
        {@render ownerInline(label, detail)}
      </a>
    {:else}
      <div class="owner-row integrated-list-row plain">
        {@render ownerInline(label, detail)}
      </div>
    {/if}
  {/each}
{/snippet}

{#snippet ownerInline(label: string, detail: string | null | undefined)}
  <span class="owner-inline">
    <span class="owner-name">{label}</span>
    {#if detail}<span class="owner-context muted">{detail}</span>{/if}
  </span>
{/snippet}

{#snippet singleOwnerLine(
  owner: CodeOwnerVariable,
  href: string | null = null,
)}
  {@const context = ownerRegisterContext(owner)}
  {#if href}
    <a class="owner-inline muted code-owner-single single-owner-link" {href}>
      <span class="owner-name">
        {owner.name ?? (owner.fqid ? leafSlug(owner.fqid) : "—")}
      </span>
      {#if context}<span class="owner-context">{context}</span>{/if}
    </a>
  {:else}
    <span class="owner-inline muted code-owner-single">
      <span class="owner-name">
        {owner.name ?? (owner.fqid ? leafSlug(owner.fqid) : "—")}
      </span>
      {#if context}<span class="owner-context">{context}</span>{/if}
    </span>
  {/if}
{/snippet}

{#snippet codeSystemPill(label: string, href: string | null = null)}
  {#if href}
    <a class="code-system-chip pill-link" {href}
      >{label}<span class="chip-arrow" aria-hidden="true">↗</span></a
    >
  {:else}
    <span class="code-system-chip">{label}</span>
  {/if}
{/snippet}

{#snippet codeCells(
  result: CodeHit,
  showCodeSystemPill: boolean = false,
)}
  {@const usage = usageSummary(result)}
  <span class="code-cells">
    <span class="code-expression">
      <code class="code-cell mono">{result.code}</code>
      <span class="code-equals">=</span>
      <span class="code-label">{result.label}</span>
      {#if showCodeSystemPill}
        {@render codeSystemPill(
          result.code_system ?? REGISTER_LOCAL_LABEL,
          codeSystemHref(result),
        )}
      {/if}
    </span>
    {#if usage}<span class="usage-count muted">{usage}</span>{/if}
  </span>
{/snippet}
{#snippet codeRow(result: CodeHit)}
  {@const singleOwner = singleVariableOwner(result)}
  {#if hasExpandableOwners(result)}
    <details class="code-row code-disclosure">
      <summary class="integrated-list-row code-summary">
        <span class="disclosure-icon" aria-hidden="true"></span>
        {@render codeCells(result)}
      </summary>
      <div class="owner-table">
        {@render ownerSubRows(result)}
      </div>
    </details>
  {:else if singleOwner?.fqid}
    <a
      class="code-row integrated-list-row single-code-row"
      href={catalogHref(singleOwner.fqid)}
    >
      {@render codeCells(result)}
      {@render singleOwnerLine(singleOwner)}
    </a>
  {:else}
    <div class="code-row integrated-list-row single-code-row">
      {@render codeCells(result)}
      {#if singleOwner}{@render singleOwnerLine(singleOwner)}{/if}
    </div>
  {/if}
{/snippet}

{#snippet topCodeRow(result: CodeHit)}
  {@const singleOwner = singleVariableOwner(result)}
  {@const systemHref = codeSystemHref(result)}
  {#if hasExpandableOwners(result)}
    <details class="code-row code-disclosure top-code-row">
      <summary class="integrated-list-row code-summary top-code-summary">
        <span class="disclosure-icon" aria-hidden="true"></span>
        <span class="top-code-summary-body">
          {@render codeCells(result, true)}
        </span>
      </summary>
      <div class="owner-table">{@render ownerSubRows(result, true)}</div>
    </details>
  {:else if singleOwner?.fqid}
    {#if systemHref}
      <div class="code-row integrated-list-row single-code-row top-code-row">
        {@render codeCells(result, true)}
        {@render singleOwnerLine(singleOwner, catalogHref(singleOwner.fqid))}
      </div>
    {:else}
      <a
        class="code-row integrated-list-row single-code-row top-code-row"
        href={catalogHref(singleOwner.fqid)}
      >
        {@render codeCells(result, true)}
        {@render singleOwnerLine(singleOwner)}
      </a>
    {/if}
  {:else if systemHref}
    <div class="code-row integrated-list-row single-code-row top-code-row">
      {@render codeCells(result, true)}
    </div>
  {:else}
    <div class="code-row integrated-list-row single-code-row top-code-row">
      {@render codeCells(result, true)}
      {#if singleOwner}{@render singleOwnerLine(singleOwner)}{/if}
    </div>
  {/if}
{/snippet}

<!-- A concept-group family (#322): search stays flat. Prefer the first-class group
     subject page when its route is derivable; if not, emit the member leaf links
     directly rather than putting a disclosure inside the results list. -->
{#snippet conceptGroup(result: ConceptGroupHit)}
  {@const href = conceptGroupHref(result)}
  {@const context = groupRegisterPill(result)}
  {#if href}
    <div class="leaf-row integrated-list-row group-result-row">
      <span class="name-cell">
        <span class="result-title">
          <a class="row-link" {href}>{result.label}</a>
          <span class="group-chip">Group</span>
          {#if context}{@render registerContextPill(context.label, context.href)}{/if}
        </span>
      </span>
    </div>
  {:else}
    {#each result.members as member, i (`${member.fqid}|${i}`)}
      {@const memberContext = providerRegisterPill(
        member.fqid,
        result.register_name,
      )}
      <div
        class="leaf-row integrated-list-row group-member-row"
      >
        <span class="name-cell">
          <span class="result-title">
            <a class="row-link" href={catalogHref(member.fqid)}
              >{member.name ?? leafSlug(member.fqid)}</a
            >
            <span class="group-chip">Group</span>
            {#if memberContext}
              {@render registerContextPill(
                memberContext.label,
                memberContext.href,
              )}
            {/if}
          </span>
          {@render detailLine(null, [result.label])}
        </span>
      </div>
    {/each}
  {/if}
{/snippet}

<!-- A classification-succession family (#571): keep search flat like variable
     concept groups. The current edition is the primary linked row; older
     editions are normal linked rows underneath, not an in-list disclosure. -->
{#snippet classificationSuccession(result: ClassificationSuccessionHit)}
  {@const editions = result.editions ?? []}
  {@const primaryLabel = result.short_name ?? result.name ?? "—"}
  {#if result.fqid}
    <a class="leaf-row integrated-list-row group-result-row" href={catalogHref(result.fqid)}>
      <span class="name-cell">
        <span class="result-title">
          <span class="row-link">{primaryLabel}</span>
        </span>
        {@render detailLine(null, [
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
        {@render detailLine(null, [
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
          {@render detailLine(null, [
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
          {@render detailLine(null, [
            edition.effective_year == null
              ? "Edition"
              : `superseded ${edition.effective_year}`,
          ])}
        </span>
      </div>
    {/if}
  {/each}
{/snippet}

<style>
  .search-view {
    --search-row-inline: calc(var(--space-4) + var(--space-1));
    --search-subrow-gutter: 3px;
  }
  .search-heading {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: 1rem;
  }
  .search-heading h2 {
    margin: 0;
    font-weight: var(--heading-weight);
  }
  .close-search {
    padding: var(--space-1) var(--space-2);
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
    padding: var(--space-1) var(--space-3);
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
  .top-results-group {
    margin-bottom: 2rem;
    padding-bottom: 1.25rem;
    border-bottom: 1px solid var(--border-strong);
  }
  .code-system {
    margin: 0;
  }
  .code-system + .code-system {
    border-top: 1px solid var(--border);
  }
  .code-system-heading {
    box-sizing: border-box;
    margin: 0;
    padding: var(--space-2) var(--search-row-inline) var(--space-1);
    border-left: var(--search-subrow-gutter) solid transparent;
    border-bottom: 1px solid var(--border);
    background: var(--surface);
    font-size: var(--text-sm);
    font-weight: var(--heading-weight);
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
    font-size: var(--text-sm);
    font-weight: 400;
    white-space: nowrap;
  }
  /* A leaf-table NAME link / plain-text fallback — the NAME is primary. Long
     Swedish compound words otherwise force a min-content width past the 375px
     mobile canvas (#806); break them only when they can't fit. */
  .row-link {
    font-weight: 600;
    color: var(--text);
    text-decoration: none;
    overflow-wrap: anywhere;
  }
  a.row-link:hover {
    color: var(--accent-ink);
  }
  .row-link.plain {
    color: var(--text);
  }
  .result-detail {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.15rem 0.35rem;
    font-size: var(--text-sm);
    overflow-wrap: anywhere;
  }
  .detail-separator {
    color: var(--text-muted);
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
    --search-row-block: calc(var(--space-1) * 1.5);
    font-size: var(--text-sm);
    /* STRETCH (not baseline): a leaf row's cells differ in height (a variable's
       definition sub-line / a classification's full-name cell make the name cell
       taller than its siblings). With baseline, each cell's own bottom border lands
       at a different vertical position, so the row separator splits into staggered
       hairline segments. Stretch sizes every cell to the row's full height so their
       bottom borders align into ONE continuous rule; cell CONTENT is top-aligned
       (below) so multi-line cells grow downward and still read top-down. */
    align-items: stretch;
  }
  .children.table.cols-1,
  .children.table.top-results {
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
  .children.table.codes,
  .children.table.top-results {
    --code-row-inline: var(--search-row-inline);
    --code-disclosure-size: 0.65rem;
    --code-disclosure-gap: 0.5rem;
    --code-disclosure-left: calc(
      var(--code-row-inline) - var(--code-disclosure-size) -
        var(--code-disclosure-gap)
    );
  }
  .code-cells {
    display: grid;
    grid-template-columns: minmax(0, 1fr) max-content;
    column-gap: var(--space-3);
    row-gap: 0.1rem;
    align-items: baseline;
  }
  .code-cells > * {
    min-width: 0;
  }
  .code-expression {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.05rem 0.35rem;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .code-summary {
    position: relative;
    display: block;
  }
  .top-code-summary-body {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: 0;
  }
  .disclosure-icon {
    position: absolute;
    top: calc(var(--search-row-block) + 0.75em);
    left: var(--code-disclosure-left);
    display: inline-grid;
    place-items: center;
    width: var(--code-disclosure-size);
    height: var(--code-disclosure-size);
    color: var(--text-muted);
    pointer-events: none;
    transform-origin: 50% 50%;
    transform: translateY(-50%);
    transition: transform var(--motion-fast) ease;
  }
  .disclosure-icon::before {
    content: "";
    width: 0.42rem;
    height: 0.42rem;
    border-right: 1.5px solid currentColor;
    border-bottom: 1.5px solid currentColor;
    transform: rotate(-45deg);
  }
  .code-row[open] > summary .disclosure-icon {
    transform: translateY(-50%) rotate(90deg);
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
    padding: var(--search-row-block) var(--code-row-inline);
    border-bottom: 1px solid var(--border);
  }
  .single-code-row {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    padding-left: var(--code-row-inline);
    color: inherit;
    text-decoration: none;
  }
  .code-row:last-child:not([open]) > summary,
  .code-row:last-child.single-code-row {
    border-bottom: none;
  }
  a.single-code-row:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  /* Expanded owner rows use the same integrated-list surface, not an inset mini
     table: row highlights and separators span the full panel width. */
  .owner-table {
    display: flex;
    flex-direction: column;
    margin: 0;
    background: color-mix(in srgb, var(--surface-sunken) 60%, var(--surface));
  }
  .owner-row {
    display: flex;
    align-items: baseline;
    box-sizing: border-box;
    color: inherit;
    text-decoration: none;
    overflow-wrap: anywhere;
    padding: var(--search-row-block) var(--code-row-inline);
    border-left: var(--search-subrow-gutter) solid var(--border-strong);
    border-bottom: 1px solid var(--border);
  }
  .owner-row > * {
    min-width: 0;
  }
  .owner-inline {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.1rem 0.45rem;
    min-width: 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .owner-name {
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  .owner-context {
    overflow-wrap: anywhere;
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
    padding: var(--search-row-block) var(--search-row-inline);
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
  .col-chip,
  .group-chip,
  .register-context-chip,
  .code-system-chip {
    display: inline-flex;
    align-items: baseline;
    gap: 0.2rem;
    line-height: 1.3;
    padding: 0 var(--space-1);
    border-radius: var(--radius-sm);
    max-width: 100%;
    overflow-wrap: anywhere;
    text-decoration: none;
  }
  /* Mirrors RepresentationPicker's delivery-column chip: mono, purple/indigo
     variable hue, and no link affordance on the search-result metadata. */
  .col-chip {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    font-weight: 500;
    color: var(--cat-var-ink);
    border: 1px solid color-mix(in srgb, var(--cat-var) 35%, transparent);
    background: color-mix(in srgb, var(--cat-var) 10%, var(--surface));
  }
  .group-chip {
    font-size: var(--text-sm);
    font-weight: 500;
    color: var(--cat-group-ink);
    border: 1px solid color-mix(in srgb, var(--cat-group) 35%, transparent);
    background: color-mix(in srgb, var(--cat-group) 10%, var(--surface));
  }
  .register-context-chip,
  .code-system-chip {
    font-size: var(--text-sm);
    font-weight: 500;
    color: var(--text-muted);
    border: 1px solid color-mix(in srgb, var(--border) 72%, transparent);
    background: color-mix(in srgb, var(--surface-sunken) 42%, var(--surface));
  }
  .pill-link {
    cursor: pointer;
  }
  .pill-link .chip-arrow {
    font-size: 1.1em;
    line-height: 1;
    opacity: 0.85;
  }
  .pill-link:hover,
  .pill-link:focus-visible {
    color: var(--text);
    border-color: color-mix(in srgb, var(--border-strong) 75%, transparent);
    background: color-mix(in srgb, var(--surface-sunken) 72%, var(--surface));
  }
  .pill-link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .column-more {
    font-size: var(--text-sm);
  }
  /* Code and label form one expression ("code = label") so the value reads as a
     paired token rather than two unrelated columns. */
  .code-cell {
    font-family: var(--font-mono);
    font-weight: 600;
    color: var(--text);
    overflow-wrap: anywhere;
  }
  .code-label {
    font-weight: 600;
    color: var(--text);
    overflow-wrap: anywhere;
  }
  .code-equals {
    color: var(--text-muted);
    font-weight: 600;
  }
  /* The MUTED variable-count summary in the collapsed code row's third column. */
  .usage-count {
    font-size: var(--text-sm);
    text-align: right;
    white-space: nowrap;
  }
  .single-owner-link {
    text-decoration: none;
  }
  .single-owner-link:hover {
    color: var(--accent-ink);
  }
  .more {
    font-size: var(--text-sm);
  }
  .continuation {
    display: flex;
    justify-content: center;
    padding: var(--space-3);
    border-top: 1px solid var(--border);
  }
  .continuation-error {
    margin: 0;
    padding: 0 var(--space-3) var(--space-3);
    font-size: var(--text-sm);
  }
</style>
