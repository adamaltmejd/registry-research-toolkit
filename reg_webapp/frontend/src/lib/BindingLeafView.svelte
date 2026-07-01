<script lang="ts">
import {
  type BindingNodeData,
  type CatalogNode,
  getBindingGraph,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
  type VariableGraphNode,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  addWindowBounds,
  coverageFromStates,
  fqidSegments,
  grainsFromStates,
  groupLinkFromFocus,
  narrowStatesByModifier,
  parseCodesParam,
  pickerRepresentations,
  pickerWindowYears,
  qualifierFromFocus,
  registerPrefixOf,
  rowAddPeriod,
} from "./catalog";
import DocMentionsPanel from "./DocMentionsPanel.svelte";
import HistoryGraph from "./HistoryGraph.svelte";
import LineageDetails from "./LineageDetails.svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import { nextResolutionQuery, VALUE_SET_VERSION_NONE } from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import RepresentationPicker, {
  type PickerSelection,
} from "./RepresentationPicker.svelte";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { Tag } from "./ui";
import ValueSetView from "./ValueSetView.svelte";
import { windowStore } from "./window.svelte";

// The binding LEAF — the addressable variable, its resolution controls, the
// states view, and the lineage panels. The FULL record (metadata + embedded
// edges + the default states) is resolved ONCE by the parent (CatalogNodeView,
// no query) and passed in as `node`, so it's ALWAYS present — a cold deep-link
// to `…/kon?period=2020` renders the metadata + lineage immediately while the
// states narrow.
//
// Resolution state (`?period`/`?variant`/`?value_set_version`) lives in the URL
// query (the single source of truth — see reg_webapp/DESIGN.md → Catalog router
// structure), read off the router's reactive
// `search` so changing it re-fetches WITHOUT a remount. WITH a `?period` we fetch
// the resolve_at subset (a `StatesResponse`) to narrow the visible states; the
// metadata + lineage (from `node`) are unaffected.
let {
  fqidPath,
  node,
  regMetaVersion,
  steward,
  vintageYear,
}: {
  fqidPath: string;
  node: BindingNodeData;
  // C1: the deployment seed for an IMPLICIT "New project" when the store is
  // pristine (App → CatalogNodeView → here). Empty until /api/context resolves — an
  // implicit project created with an empty seed is NEVER re-seeded (it carries a
  // blank reg_meta_version into project_data.json), so the Add action is DISABLED
  // until both seed fields are present (sub-second; see `seedReady`).
  regMetaVersion: string;
  steward: string;
  // #631: the catalog VINTAGE year (App → CatalogNodeView → here), threaded into
  // the period picker so the local slider's open-ended ceiling caps at the catalog
  // vintage — matching the header window slider — instead of wall-clock time.
  vintageYear: number;
} = $props();

// Read the resolution modifiers off the reactive query so the fetch re-runs when
// they change.
const params = $derived({
  period: router.getQueryParam("period") ?? undefined,
  variant: router.getQueryParam("variant") ?? undefined,
  value_set_version: router.getQueryParam("value_set_version") ?? undefined,
});
const hasResolutionModifier = $derived(
  params.variant !== undefined || params.value_set_version !== undefined,
);

// #905: a `?codes=<variant>::<column>` deep link (the picker's "codings vary" nudge)
// focuses the value-set viewer on that ROW's `(variant, column)` coding. The variant
// is carried because a column can be shared across variants/populations with distinct
// codings, so the focus must target the clicked row's coding — not just the column.
// This is a DEDICATED `?codes=` encoding, separate from the `?variant` RESOLUTION
// modifier (which narrows + re-fetches), so the focus never perturbs the resolution.
// Pure VIEW state — it does NOT re-fetch, so it's read separately from `params` and
// passed straight to ValueSetView. A bare `<column>` (no `::`) parses variant=null.
const codesFocus = $derived(parseCodesParam(router.getQueryParam("codes")));
const focusColumn = $derived(codesFocus?.column ?? null);
const focusVariant = $derived(codesFocus?.variant ?? null);

// Fetch the narrowed states ONLY when a `?period` is active — otherwise the full
// node's embedded states are shown (no redundant request; CatalogNodeView already
// fetched the full node). SYNC `fn()` so the effect tracks the reactive `params`.
const periodResource = asyncResource<CatalogNode | StatesResponse | null>(() =>
  params.period ? getCatalogNode(fqidPath, params) : Promise.resolve(null),
);

// When `?variant` / `?value_set_version` narrows the resolution, the value-set
// history still needs a PERIOD-ONLY scope for #744's outside-period disclosure.
// Otherwise same-period rows for other variants/versions would be mislabeled as
// outside the period instead of staying inline and greyed.
const periodScopeResource = asyncResource<CatalogNode | StatesResponse | null>(
  () =>
    params.period && hasResolutionModifier
      ? getCatalogNode(fqidPath, { period: params.period })
      : Promise.resolve(null),
);

const narrowedStates = $derived.by(() => {
  const data = periodResource.data;
  // A `?period` resolve returns a StatesResponse (the only non-node arm here);
  // `!isCatalogNode` narrows `CatalogNode | StatesResponse` to it.
  return data !== null && !isCatalogNode(data) ? data.states : null;
});
const periodScopeStates = $derived.by(() => {
  if (!hasResolutionModifier) {
    return null;
  }
  const data = periodScopeResource.data;
  return data !== null && !isCatalogNode(data) ? data.states : null;
});

// ANY error on the primary period resolve (a 422 bad-modifier, but also a 5xx / 502 /
// network drop where `status` stays null) is surfaced inline — the metadata +
// the picker stay usable and the states fall back to the full history. Without
// this (filtering to only 422/400), a server/network error would leave
// `narrowedStates` null and wedge the states section on a permanent
// "Loading states…" with no feedback.
const narrowedError = $derived(
  params.period && periodResource.error ? periodResource.error : null,
);
const scopeError = $derived(
  params.period && !periodResource.error && hasResolutionModifier
    ? periodScopeResource.error
    : null,
);

// States that drive resolution-sensitive behavior: Add planning and the
// single-state detail stay on the `?period` subset. The value-set list may render
// full history separately so out-of-period value sets can be collapsed instead of
// removed (#744).
const resolvedStates = $derived.by(() =>
  params.period && !narrowedError ? narrowedStates : node.states,
);

// States to show: loading while a period resolve is in flight; a single resolved
// state renders the detail view; multi/empty period resolves render the full
// embedded history with `scopeStates` marking what is in-period (#744).
const states = $derived.by(() => {
  if (!params.period || narrowedError) {
    return node.states;
  }
  if (
    narrowedStates === null ||
    (hasResolutionModifier &&
      periodScopeStates === null &&
      periodScopeResource.loading)
  ) {
    return null;
  }
  return narrowedStates.length === 1 ? narrowedStates : node.states;
});

// Whether the visible states are period-narrowed (drives the "narrowed to X" note
// + ValueSetView's empty-message wording).
const isNarrowed = $derived(!!params.period && !narrowedError);
const stateScope = $derived(
  isNarrowed
    ? hasResolutionModifier
      ? (periodScopeStates ?? resolvedStates)
      : resolvedStates
    : null,
);

// #905 (Codex P2): when a `?variant` / `?value_set_version` modifier is active, the
// page shows a "Narrowed by" chip and the picker is scoped via
// `narrowStatesByModifier`. The value-set view must reflect the SAME narrowing —
// otherwise it would list OTHER variants'/versions' same-period codings as in-scope
// rows, contradicting the rest of the page. Apply the modifier-narrowing (the SAME
// helper the picker uses) ON TOP of the loading/single/multi `states` derivation and
// the `stateScope` it pairs with — never instead of it, so the loading (`null`) and
// single-state paths are preserved. With NO modifier active these pass through
// unchanged (full history). ValueSetView stays PURE: the leaf decides which states it
// gets; the view carries no resolution state.
//
// CRUCIALLY gated on `!narrowedError` (mirroring `resolvedStates`/`states`): when the
// `?period` resolve FAILS, `states` deliberately falls back to `node.states` (full
// history). A stale/typo `?variant` would then narrow that fallback to empty, defeating
// the full-history fallback — so on a resolve error we skip the modifier narrowing and
// let `valueSetStates`/`valueSetScope` equal the same full-history fallback the rest of
// the page shows.
const valueSetStates = $derived.by(() => {
  if (states === null || narrowedError || !hasResolutionModifier) {
    return states;
  }
  return [
    ...narrowStatesByModifier(
      states,
      params.variant ?? null,
      params.value_set_version ?? null,
    ),
  ];
});
const valueSetScope = $derived.by(() => {
  if (stateScope === null || narrowedError || !hasResolutionModifier) {
    return stateScope;
  }
  return [
    ...narrowStatesByModifier(
      stateScope,
      params.variant ?? null,
      params.value_set_version ?? null,
    ),
  ];
});

// ── The relationship-graph fetch (#678) ─────────────────────────────────────
// The leaf owns ONE `/graph` fetch (#761/#792): it feeds the HistoryGraph
// renderer AND the #670 header identity (qualifier + group link), derived from
// the graph FOCUS node — no separate `/dimensions` request. Read `fqidPath`
// synchronously inside `fn` so the resource refetches when the leaf changes (same
// pattern as `periodResource`).
//
// FAILURE-DOMAIN isolation: this resource is independent of `node`, so a graph
// error / empty / timeout NEVER blanks the leaf — the HistoryGraph section omits
// the graph, and the header just omits the qualifier/link (both gate on a
// RESOLVED, non-empty graph; additive).
const graphResource = asyncResource(() => getBindingGraph(fqidPath));
const graph = $derived(graphResource.data);
// The graph has RESOLVED (a settled fetch with a payload) — the gate the header
// identity derivations wait on, mirroring the old `dimReady`. While in flight
// (or on error) the header shows just `node.name`, then the qualifier appears
// once — no flicker, never blanked.
const graphReady = $derived(
  !graphResource.loading && !graphResource.error && graph != null,
);

// The FOCUS variable node — the node whose `id === focus_id` (the requested
// binding, post-same_as). The #670 header qualifier + group link read its
// `facets` / `group_label`. A binding `/graph` always carries a variable focus;
// the `kind` guard keeps TS sound (and tolerates a focus-less group payload).
// `graphReady` (above) is the one render gate; this trusts the qualifier/link
// helpers' null-tolerance (they accept a null focus), so it just reads from
// `graph?.…` — a loading/errored/empty graph yields a null focus, never blanks.
const focusNode = $derived.by((): VariableGraphNode | null => {
  const f = graph?.nodes.find((n) => n.id === graph.focus_id);
  return f?.kind === "variable" ? f : null;
});

// The member-distinguishing qualifier (e.g. "AGI · 2007 SNI edition") — the
// focus node's facet labels (`kind: "facets"`); for a GROUPED member with no
// facets (an edge group's split siblings) the leaf slug fallback
// (`kind: "slug"`); `null` for an ungrouped variable (its `node.name` suffices),
// or until the graph resolves. `node.fqid` is the leaf's own fqid (the
// slug-fallback source — the focus node's fqid can be null).
const qualifier = $derived(qualifierFromFocus(focusNode, node.fqid));

// The "member of ⟨group label⟩" context link to the group subject page (#670) —
// the focus node's `group_label` + the leaf `node.group` ref's href. Null when
// ungrouped, or until the graph resolves (additive; never blanks).
const groupLink = $derived(groupLinkFromFocus(focusNode, node.group));

/** Write the resolution params to the URL (preserving the pathname), which the
 * reactive query picks up → refetch. An empty `period` clears the narrowing
 * (full history). `variant`/`value_set_version` are only meaningful WITH a
 * period (the server 422s them otherwise), so they're dropped when period is
 * cleared (the merge rule in `nextResolutionQuery`). */
function setResolution(next: {
  period?: string | null;
  variant?: string | null;
  value_set_version?: string | null;
}): void {
  const query = nextResolutionQuery(params, next);
  router.navigate(window.location.pathname + (query ? `?${query}` : ""));
}

// ── C1: add to project ────────────────────────────────────────────────────────
// The variable's provider/register prefix (first 2 segs of its 3-seg fqid). A
// source's register_variant is `<prefix>/<variant>`, the variant taken from the
// chosen STATE (each state carries its variant slug) — so the add is fully explicit:
// this variable, at this variant, and (when several columns co-exist) this delivery
// column.
const registerPrefix = $derived(registerPrefixOf(node.fqid));

// The deployment seed is ready once /api/context has populated BOTH fields (MINOR 3).
// An implicit project created with an empty seed is never re-seeded, so the Add
// action stays disabled until the seed is present (sub-second).
const seedReady = $derived(regMetaVersion !== "" && steward !== "");

// #308: the grains this variable's FULL state history exhibits (year always;
// finer from the #321 tokens) — pre-narrows the period picker's grain select.
const grains = $derived(grainsFromStates(node.states));

// #615: the subject's data-availability span (year-grain), derived from the
// EMBEDDED full state history — the picker's slider draws it as the live track
// and greys the not-delivered span. Computed client-side from already-present
// data (no backend coverage field on the leaf node).
const coverage = $derived(coverageFromStates(node.states));

// ── #678 redesign: direct representation picker ─────────────────────────────
// The add-to-project surface lists the variable's representations (one row per
// distinct variant + delivery column) over the FULL state history and commits
// the user's multi-selection directly — one `addFromCatalog` per picked row.
// The picker (RepresentationPicker) owns the row layout + selection; THIS host
// owns the data (enumeration) and the store wiring (commit + confirmation).
//
// The rows enumerate over `node.states` (the full history) — NOT the
// period-narrowed subset: the period window only DIMS out-of-window rows
// (`pickerWindow`), every row stays selectable. `pickerRepresentations` is pure +
// unit-tested.
//
// A `?variant` / `?value_set_version` MODIFIER (the "Narrowed by" chip) is the
// exception: it scopes the resolution to one axis, so the picker must offer ONLY
// the rows consistent with that narrowing (`narrowStatesByModifier`) — otherwise
// "select all" would add rows for variants / versions OUTSIDE the active
// narrowing. With NO modifier active the states pass through unchanged (full
// history, behavior unchanged).

/** The selectable representation rows — one per distinct (variant, delivery
 * column) over the active state history (the full history, or the
 * variant/version-narrowed subset when a modifier is active). */
const pickerRows = $derived(
  pickerRepresentations(
    narrowStatesByModifier(
      node.states,
      params.variant ?? null,
      params.value_set_version ?? null,
    ),
  ),
);

/** The active period window to DIM against, as an inclusive year pair (a
 * `?period` wire wins, else the global study window), or null (no dimming). */
const pickerWindow = $derived(
  pickerWindowYears(params.period ?? null, windowStore.value),
);

/** The committed outcome (drives the inline confirmation): the sources bindings
 * landed in (created or found) and how many were already present. */
let addOutcome = $state<{
  added: { name: string; period: string | null }[];
  already: number;
} | null>(null);

// A fresh resolution / leaf change clears the stale confirmation. Tracking
// `fqidPath` (the navigation key) re-invalidates on a leaf change independent of
// the parent's `{#key route.fqidPath}` remount, so a stale confirmation can never
// survive onto a different variable.
$effect(() => {
  void params.period;
  void params.variant;
  void params.value_set_version;
  void fqidPath;
  addOutcome = null;
});

/** Commit each selected representation through the store (synchronous appends;
 * the guarded derive lands per binding afterwards) and record the aggregate
 * outcome for the inline confirmation. The leaf passes a SINGLE band, so every
 * selection's `band.key` is this variable's fqid. The committed period is the row's
 * span INTERSECTED with the active period window (`rowAddPeriod`) — so a `?period`
 * the user narrowed to is honored (not widened to the row's full history) and an
 * open-ended row lands a finite, resolvable period rather than period-unset (#678).
 * The period also keys `addFromCatalog`'s find-or-create, so two rows of the SAME
 * register variant with DIFFERENT spans each land in their OWN correctly-periodized
 * source (#678). */
function commitSelected(selected: PickerSelection[]): void {
  if (selected.length === 0) {
    return;
  }
  const added: { name: string; period: string | null }[] = [];
  let already = 0;
  // The active window as EXACT ISO bounds — a sub-annual `?period` (`2020-Q1`) is
  // honored at its real grain on Add, not collapsed to the outer year (#678 finding).
  const addWindow = addWindowBounds(params.period ?? null, pickerWindow);
  for (const { row } of selected) {
    const addPeriod = rowAddPeriod(row, addWindow);
    const result = projectStore.addFromCatalog(
      {
        registerVariant: `${registerPrefix}/${row.variant}`,
        variable: node.fqid,
        // `row.representation` (NOT `row.column`): a folded sequential rename commits
        // null so resolution picks the right column per year; an ordinary / parallel
        // column commits its own column (#902).
        representation: row.representation,
        resolvedPeriod: addPeriod,
      },
      { reg_meta_version: regMetaReleaseTag(regMetaVersion), steward },
    );
    if (result.status === "added") {
      added.push({ name: result.sourceName, period: addPeriod });
    } else {
      already += 1;
    }
  }
  addOutcome = { added, already };
}
</script>

<!-- #638 PR1: the binding leaf renders through the unified SubjectView shell. The
     `<script>` logic is unchanged — only the markup moves into the shell's section
     snippets (description / picker / value set / relationships / docs), passed in
     the canonical order the shell renders. -->
{#snippet description()}
  <!-- #670: the member-distinguishing qualifier + "member of ⟨group⟩" context
       link, rendered directly under the header (the shell renders `description`
       first). Both are ADDITIVE and gated on a RESOLVED /dimensions fetch — an
       ungrouped variable, or a loading/errored fetch, renders neither, leaving
       the page exactly as it was. Gating on resolved (not just !error) avoids a
       transient slug flicker before the facets load. -->
  {#if qualifier || groupLink}
    <p class="member-identity">
      {#if qualifier}
        {#if qualifier.kind === "slug"}
          <!-- #670: a grouped member with no facets (edge-group split siblings):
               the slug is the only differentiator, rendered as a technical
               identifier (mono) rather than a human label. -->
          <code class="qualifier slug">{qualifier.text}</code>
        {:else}
          <span class="qualifier">{qualifier.text}</span>
        {/if}
      {/if}
      {#if groupLink}
        <span class="group-context">
          member of <a href={groupLink.href}>{groupLink.label}</a>
        </span>
      {/if}
    </p>
  {/if}

  {#if node.via_same_as && node.via_same_as.length > 0}
    <p class="muted via">
      Resolved via <code>same_as</code>: {node.via_same_as.join(" → ")}
    </p>
  {/if}

  {#if node.tags && node.tags.length > 0}
    <div class="tag-strip" aria-label="Thematic tags">
      {#each node.tags as tag (tag.slug)}
        <span class="tag-item">
          <Tag tone="neutral">{tag.label}</Tag>
          {#if tag.starred && tag.note}
            <span class="tag-note">Recommended: {tag.note}</span>
          {/if}
        </span>
      {/each}
    </div>
  {/if}

  <dl class="meta">
    {#if node.definition}
      <dt>Definition</dt>
      <dd>{node.definition}</dd>
    {/if}
    {#if node.description}
      <dt>Description</dt>
      <dd>{node.description}</dd>
    {/if}
    {#if node.operational_definition}
      <dt>Operational definition</dt>
      <dd>{node.operational_definition}</dd>
    {/if}
    {#if node.measurement_unit}
      <dt>Unit</dt>
      <dd>{node.measurement_unit}</dd>
    {/if}
  </dl>

  <!-- #638 PR4: Sensitive / Identifier are STRUCTURAL backend flags — useful but
       not what a user reads first — so they live behind the "Technical details"
       disclosure. Both are always present (booleans), so the disclosure is never
       empty here. -->
  <TechnicalDetails>
    <dl class="meta">
      <dt>Sensitive</dt>
      <dd>{node.is_sensitive ? "yes" : "no"}</dd>
      <dt>Identifier</dt>
      <dd>{node.is_identifier ? "yes" : "no"}</dd>
    </dl>
  </TechnicalDetails>
{/snippet}

{#snippet picker()}
  <!-- #615: the period picker seeds from the global project window (windowStore)
       and shows the subject's coverage track. PRECEDENCE `?period` > window >
       full history is resolved inside the picker; submit/clear flow through the
       same `?period` URL path (a local change writes `?period` only, never the
       global window). -->
  <PeriodPicker
    period={params.period ?? null}
    {grains}
    window={windowStore.value}
    {coverage}
    {vintageYear}
    onsubmit={(period) => setResolution({ period })}
    onclear={() => setResolution({ period: null })}
  />

  <!-- #678 redesign: the direct representation picker. The variable's
       representations (one row per distinct variant + delivery column over the
       FULL state history) are listed as selectable rows; the user picks several
       and commits with one "Add to project". Replaces the auto-planning
       population selector + post-click rep chooser. The period window only DIMS
       out-of-window rows (`pickerWindow`), never filters them — selection works
       on any row. `seedReady` still gates the commit. -->
  {#if pickerRows.length > 0}
    <RepresentationPicker
      bands={[
        {
          key: node.fqid,
          name: node.name ?? node.fqid,
          registerPrefix,
          isSensitive: node.is_sensitive,
          isIdentifier: node.is_identifier,
          rows: pickerRows,
        },
      ]}
      window={pickerWindow}
      canAdd={seedReady}
      onadd={commitSelected}
    />
  {/if}

  {#if addOutcome}
    <p class="page-add">
      {#if addOutcome.added.length === 0}
        <span class="add-confirm already" role="status">Already in project</span>
      {:else}
        <span class="add-confirm" role="status">
          {#if addOutcome.added.length === 1}
            Added to project ({addOutcome.added[0].name})
          {:else}
            Added {addOutcome.added.length} columns:
            {addOutcome.added
              .map((a) => (a.period ? `${a.name} (${a.period})` : a.name))
              .join(", ")}
          {/if}
          {#if addOutcome.already > 0}
            · {addOutcome.already} already in project
          {/if}
          — <a href="/project">view</a>
        </span>
      {/if}
    </p>
  {/if}

  {#if params.period && (params.variant || params.value_set_version)}
    <!-- Active narrowing modifiers, each clearable — so narrowing to one state
         isn't a one-way trap (clearing a modifier refetches the wider set and the
         picker reappears, letting the user switch variant/version). Gated on a
         period: `?variant`/`?value_set_version` only narrow a period resolve (the
         server 422s them alone), so without a period they're inert — don't claim
         "narrowed by" for a modifier-only deep-link (the full history shows). -->
    <div class="active-modifiers">
      <span class="muted">Narrowed by:</span>
      {#if params.variant}
        <button
          type="button"
          class="modifier-chip"
          aria-label="Clear variant filter"
          onclick={() => setResolution({ variant: null })}
        >
          variant: {params.variant} <span aria-hidden="true">✕</span>
        </button>
      {/if}
      {#if params.value_set_version}
        <button
          type="button"
          class="modifier-chip"
          aria-label="Clear value-set version filter"
          onclick={() => setResolution({ value_set_version: null })}
        >
          version: {params.value_set_version === VALUE_SET_VERSION_NONE
            ? "(no version)"
            : params.value_set_version} <span aria-hidden="true">✕</span>
        </button>
      {/if}
    </div>
  {/if}

  {#if narrowedError}
    <!-- The full node is still shown; a bad ?period/?variant modifier 422'd.
         Surface it inline (the picker stays usable) rather than blanking. -->
    <p class="error inline-error" role="alert">{narrowedError}</p>
  {/if}
  {#if scopeError}
    <p class="error inline-error" role="alert">
      Could not load full period value-set context: {scopeError}
    </p>
  {/if}
{/snippet}

{#snippet valueSet()}
  <section aria-labelledby="states-heading">
    <h3 id="states-heading">
      <!-- A #307 comma list reads as segments joined with "+" (the union the
           backend resolves since #340) — `2005..2010 + 2015..2020`. -->
      States{#if isNarrowed && params.period}<span class="muted narrowed-note">
          · narrowed to {params.period.split(",").join(" + ")}</span
        >{/if}
    </h3>
    {#if valueSetStates}
      <!-- The pure value-set / coding viewer (#905). Narrowing to one state is the
           PICKER's job now (it writes `?variant`/`?value_set_version`); this view
           only DISPLAYS the codings. The "codings vary" nudge deep-links here via
           `?codes=<variant>::<column>` → `focusColumn`/`focusVariant`, focusing the
           right (variant, column) coding. -->
      <ValueSetView
        states={valueSetStates}
        narrowed={isNarrowed}
        scopeStates={valueSetScope}
        {focusColumn}
        {focusVariant}
      />
    {:else}
      <p class="muted" aria-busy="true">Loading states…</p>
    {/if}
  </section>
{/snippet}

{#snippet relationships()}
  <!-- #678: the unified history-graph view over the relationship-graph contract
       (#761/#792) — succession / groups (Fork B) / same_as / focus
       highlight, drawn as SVG. Omits itself on an empty graph (`nodes: []`) or
       while the fetch is unresolved/errored (its own failure domain — never
       blanks the leaf). It REPLACES the retired Dimensions + Lineage panels. -->
  {#if graphReady && graph}
    <!-- vintageYear is the open-ended ceiling for the shared time axis: an
         open-ended cell ("still delivered") extends to the catalog vintage rather
         than ballooning the scale (#678 rework). -->
    <HistoryGraph {graph} {vintageYear} />
  {/if}

  <!-- The two NON-graph affordances the #761 payload doesn't carry — provenance
       (lineage edges + source register) and the fetched lineage warnings — live
       here on the binding leaf (succession is a graph edge now). -->
  <LineageDetails {fqidPath} {node} />
{/snippet}

{#snippet docs()}
  <!-- #402/#967: "Parsed documentation" — a SIBLING of the lineage panels,
       deliberately a separate component over a separate optional DB (its own
       failure domain; a docs error/timeout/absent-index never blanks the leaf). -->
  <DocMentionsPanel {node} />
{/snippet}

<SubjectView
  title={node.name ?? node.fqid}
  fqid={node.fqid}
  showFqid={false}
  {description}
  {picker}
  {valueSet}
  {relationships}
  {docs}
/>

<style>
  .via code {
    font-size: 0.9em;
  }
  /* #670: the member-distinguishing qualifier + group context link, sitting just
     under the shared concept header. Two inline pieces separated by a thin
     divider so the qualifier (what locates this member) reads as primary and the
     "member of …" link as secondary context. */
  .member-identity {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem 0.75rem;
    margin: -0.25rem 0 0.75rem;
    font-size: 0.9rem;
  }
  .member-identity .qualifier {
    font-weight: 600;
  }
  /* #670: the slug-fallback qualifier reads as a technical identifier, not a
     human label — mono, lighter weight, in a subtle code chip. */
  .member-identity .qualifier.slug {
    font-family: var(--font-mono);
    font-weight: 500;
    font-size: 0.85em;
  }
  .member-identity .group-context {
    color: var(--text-muted);
  }
  /* #638 PR4: row spacing standardized to 0.3rem across the three subject kinds. */
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.3rem 1rem;
    margin: 1rem 0;
  }
  .meta dt {
    font-weight: 600;
  }
  .meta dd {
    min-width: 0;
    margin: 0;
  }
  .tag-strip {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    margin: 0.75rem 0 1rem;
  }
  .tag-item {
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: var(--space-2);
    min-width: 0;
  }
  .tag-note {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .narrowed-note {
    font-weight: 400;
    font-size: 0.85rem;
    margin-left: 0.4rem;
  }
  .inline-error {
    margin: 0.5rem 0;
  }
  .active-modifiers {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin: 0.5rem 0;
    font-size: 0.85rem;
  }
  .modifier-chip {
    display: inline-flex;
    align-items: baseline;
    gap: 0.4rem;
    padding: 0.2rem 0.6rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    background: var(--accent-bg);
    color: var(--accent-ink);
    font: inherit;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .modifier-chip:hover {
    background: var(--surface);
  }
  /* #678: the add-confirmation line (the picker owns the Add button now). */
  .page-add {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    margin: 0.75rem 0;
  }
  .add-confirm {
    font-size: 0.85rem;
    color: var(--accent);
  }
  .add-confirm.already {
    color: var(--text-muted);
  }
  @media (max-width: 48rem) {
    .meta {
      grid-template-columns: 1fr;
      gap: 0.15rem;
    }
    .meta dt:not(:first-child) {
      margin-top: var(--space-2);
    }
  }
</style>
