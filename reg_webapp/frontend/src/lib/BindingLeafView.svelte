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
  type BindingResolution,
  bindingFieldsFromResolution,
  coverageFromStates,
  formatDataType,
  fqidSegments,
  groupLinkFromFocus,
  narrowStatesByModifier,
  parseCodesParam,
  pickerRepresentations,
  pickerWindowYears,
  qualifierFromFocus,
  registerPrefixOf,
  resolveBindingAt,
  rowAddPeriod,
  windowsAddPeriod,
  windowsOverlapWindow,
} from "./catalog";
import DocMentionsPanel from "./DocMentionsPanel.svelte";
import LineageDetails from "./LineageDetails.svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import {
  clampYearPeriodWire,
  clampYearWindow,
  isStructurallyValidPeriodWire,
  nextResolutionQuery,
  periodFromWire,
  periodToWire,
  VALUE_SET_VERSION_NONE,
} from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import RepresentationPicker, {
  type PickerApplyPayload,
  type PickerSelection,
} from "./RepresentationPicker.svelte";
import { router } from "./router.svelte";
import SubjectView from "./SubjectView.svelte";
import {
  committedPickerRows,
  finalSourcePeriodsForStagedAdds,
  periodChangesWithStagedAdds,
  rowRegisterVariantForVariant,
  sourcePeriodsFromDraft,
  stagedRemoveForCommitted,
} from "./staged_picker";
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
  windowMinYear,
  vintageYear,
  windowMaxYear = vintageYear,
  enforcePeriodBounds = false,
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
  // #1037: steward-aware slider floor (App → CatalogNodeView → here).
  windowMinYear: number;
  // #631: the true catalog vintage year, used by open-ended graph timelines.
  vintageYear: number;
  // #1037: steward-aware period-control ceiling. Defaults to `vintageYear` for
  // direct component callers that are outside App's steward context.
  windowMaxYear?: number;
  // #1037: true only for real steward-derived app bounds. The global 1960 floor
  // is a fallback and must not erase genuinely earlier coverage.
  enforcePeriodBounds?: boolean;
} = $props();

// Read the resolution modifiers off the reactive query so the fetch re-runs when
// they change.
const params = $derived({
  period: router.getQueryParam("period") ?? undefined,
  variant: router.getQueryParam("variant") ?? undefined,
  value_set_version: router.getQueryParam("value_set_version") ?? undefined,
});
const boundedPickerPeriod = $derived(
  enforcePeriodBounds
    ? clampYearPeriodWire(params.period, windowMinYear, windowMaxYear)
    : (params.period ?? null),
);
const effectiveParams = $derived({
  ...params,
  period: boundedPickerPeriod ?? undefined,
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
  effectiveParams.period
    ? getCatalogNode(fqidPath, effectiveParams)
    : Promise.resolve(null),
);

// When `?variant` / `?value_set_version` narrows the resolution, the value-set
// history still needs a PERIOD-ONLY scope for #744's outside-period disclosure.
// Otherwise same-period rows for other variants/versions would be mislabeled as
// outside the period instead of staying inline and greyed.
const periodScopeResource = asyncResource<CatalogNode | StatesResponse | null>(
  () =>
    effectiveParams.period && hasResolutionModifier
      ? getCatalogNode(fqidPath, { period: effectiveParams.period })
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
  effectiveParams.period && periodResource.error ? periodResource.error : null,
);
const scopeError = $derived(
  effectiveParams.period && !periodResource.error && hasResolutionModifier
    ? periodScopeResource.error
    : null,
);

// States that drive resolution-sensitive behavior: Add planning and the
// single-state detail stay on the `?period` subset. The value-set list may render
// full history separately so out-of-period value sets can be collapsed instead of
// removed (#744).
const resolvedStates = $derived.by(() =>
  effectiveParams.period && !narrowedError ? narrowedStates : node.states,
);

// States to show: loading while a period resolve is in flight; a single resolved
// state renders the detail view; multi/empty period resolves render the full
// embedded history with `scopeStates` marking what is in-period (#744).
const states = $derived.by(() => {
  if (!effectiveParams.period || narrowedError) {
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
const isNarrowed = $derived(!!effectiveParams.period && !narrowedError);
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

const technicalState = $derived.by(() => {
  if (valueSetStates === null || valueSetStates.length !== 1) {
    return null;
  }
  if (isNarrowed && valueSetScope !== null && valueSetScope.length === 0) {
    return null;
  }
  return valueSetStates[0];
});

// ── The relationship-graph fetch (#678/#904) ────────────────────────────────
// The leaf owns ONE `/graph` fetch (#761/#792): it feeds the picker graph mode AND the
// #670 header identity (qualifier + group link), derived from the graph FOCUS node — no
// separate `/dimensions` request. Read `fqidPath`
// synchronously inside `fn` so the resource refetches when the leaf changes (same
// pattern as `periodResource`).
//
// FAILURE-DOMAIN isolation: this resource is independent of `node`, so a graph
// error / empty / timeout NEVER blanks the leaf — the picker falls back to the list, and
// the header just omits the qualifier/link (both gate on a RESOLVED, non-empty graph;
// additive).
const graphResource = asyncResource(() => getBindingGraph(fqidPath));
const graph = $derived(graphResource.data);
// The graph has RESOLVED (a settled fetch with a payload) — the gate the header
// identity derivations wait on, mirroring the old `dimReady`. While in flight
// (or on error) the header shows just `node.name`, then the qualifier appears
// once — no flicker, never blanked.
const graphReady = $derived(
  !graphResource.loading && !graphResource.error && graph != null,
);
const graphHasDrawableContext = $derived(
  graphReady &&
    graph != null &&
    (graph.edges.length > 0 ||
      graph.nodes.some(
        (n) =>
          n.kind === "variable" &&
          (n.same_as.length > 0 || n.states.length > 0),
      )),
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

// #615: the subject's data-availability span (year-grain), derived from the
// EMBEDDED full state history — the picker's slider draws it as the live track
// and greys the not-delivered span. Computed client-side from already-present
// data (no backend coverage field on the leaf node).
const coverage = $derived(coverageFromStates(node.states));

// ── #678/#995 redesign: direct representation picker ────────────────────────
// The add-to-project surface lists the variable's representations (one row per
// distinct variant + delivery column) over the FULL state history and stages
// the user's multi-selection until the footer applies one store diff.
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
const pickerBands = $derived([
  {
    key: node.fqid,
    name: node.name ?? node.fqid,
    registerPrefix,
    isSensitive: node.is_sensitive,
    isIdentifier: node.is_identifier,
    rows: pickerRows,
  },
]);
const committedRows = $derived(
  committedPickerRows(projectStore.draft, pickerBands),
);

const narrowedPeriodLabel = $derived(
  (boundedPickerPeriod ?? params.period ?? "").split(",").join(" + "),
);
const boundedProjectWindow = $derived(
  windowStore.value === null || !enforcePeriodBounds
    ? windowStore.value
    : clampYearWindow(windowStore.value, windowMinYear, windowMaxYear),
);
const activePickerPeriod = $derived(
  boundedPickerPeriod &&
    !narrowedError &&
    isStructurallyValidPeriodWire(boundedPickerPeriod)
    ? boundedPickerPeriod
    : null,
);
/** The active period window to DIM against, as an inclusive year pair (a
 * structurally valid `?period` wire wins, else the global study window), or null
 * (no dimming). */
const pickerWindow = $derived(
  pickerWindowYears(activePickerPeriod, boundedProjectWindow),
);

/** The applied outcome (drives the inline confirmation). */
let applyOutcome = $state<{
  added: number;
  removed: number;
  periodChanged: number;
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
  applyOutcome = null;
});

function stagedAddCandidates(selection: PickerSelection) {
  const { band, row } = selection;
  const addWindow = addWindowBounds(activePickerPeriod, pickerWindow);
  const segments =
    row.variantSegments && row.variantSegments.length > 0
      ? row.variantSegments
      : [{ variant: row.variant, windows: row.windows }];
  const overlappingSegments =
    segments.length === 1
      ? segments
      : segments.filter((segment) =>
          windowsOverlapWindow(segment.windows, addWindow),
        );
  const selectedSegments =
    overlappingSegments.length > 0 ? overlappingSegments : segments;
  return selectedSegments.map((segment) => {
    const addPeriod =
      segments.length === 1
        ? rowAddPeriod(row, addWindow)
        : windowsAddPeriod(
            segment.windows,
            overlappingSegments.length > 0 ? addWindow : null,
            false,
          );
    return {
      selection,
      variant: segment.variant,
      registerVariant: rowRegisterVariantForVariant(band, segment.variant),
      periodWire: addPeriod,
      period: periodFromWire(addPeriod),
    };
  });
}

type StagedAddCandidate = ReturnType<typeof stagedAddCandidates>[number];

async function stagedAdd(
  candidate: StagedAddCandidate,
  resolvePeriodWire: string | null,
) {
  const { band, row } = candidate.selection;
  let resolution: BindingResolution;
  try {
    resolution = await resolveBindingAt(
      band.key,
      resolvePeriodWire,
      candidate.variant,
    );
  } catch {
    resolution = { kind: "unresolved" as const, reason: "no-states" as const };
  }
  return {
    registerVariant: candidate.registerVariant,
    period: candidate.period,
    binding: bindingFieldsFromResolution(
      band.key,
      resolution,
      row.representation,
    ),
  };
}

/** Apply the staged diff through ONE synchronous store mutation. Adds carry final
 * binding fields from the picker row, so the project is unchanged until Apply. */
async function applyStaged(payload: PickerApplyPayload): Promise<boolean> {
  if (
    payload.adds.length === 0 &&
    payload.removes.length === 0 &&
    payload.periodChanges.length === 0
  ) {
    return true;
  }
  if (projectStore.draft === null && payload.adds.length > 0) {
    projectStore.newProject({
      reg_meta_version: regMetaReleaseTag(regMetaVersion),
      steward,
    });
  }
  const target = projectStore.draft;
  const candidates = payload.adds.flatMap(stagedAddCandidates);
  const finalPeriods = finalSourcePeriodsForStagedAdds(
    sourcePeriodsFromDraft(target),
    payload.periodChanges,
    candidates,
  );
  const adds = await Promise.all(
    candidates.map((candidate) =>
      stagedAdd(
        candidate,
        periodToWire(
          finalPeriods.get(candidate.registerVariant) ?? candidate.period,
        ),
      ),
    ),
  );
  if (projectStore.draft !== target) {
    return false;
  }
  const removes = payload.removes.flatMap((r) =>
    stagedRemoveForCommitted(r.committed),
  );
  projectStore.applyStagedDiff({
    adds,
    removes,
    periodChange: periodChangesWithStagedAdds(
      payload.periodChanges,
      candidates,
    ),
  });
  applyOutcome = {
    added: payload.adds.length,
    removed: removes.length,
    periodChanged: payload.periodChanges.length,
  };
  return true;
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

{/snippet}

{#snippet picker()}
  <!-- #615: the period picker seeds from the global project window (windowStore)
       and shows the subject's coverage track. PRECEDENCE `?period` > window >
       full history is resolved inside the picker; submit/clear flow through the
       same `?period` URL path (a local change writes `?period` only, never the
       global window). -->
  <PeriodPicker
    period={boundedPickerPeriod}
    window={boundedProjectWindow}
    {coverage}
    {windowMinYear}
    vintageYear={windowMaxYear}
    {enforcePeriodBounds}
    onsubmit={(period) => setResolution({ period })}
    onclear={() => setResolution({ period: null })}
  />

  <!-- #678 redesign: the direct representation picker. The variable's
       representations (one row per distinct variant + delivery column over the
       FULL state history) are listed as selectable rows; the user picks several
       and stages them until the footer Apply. Replaces the auto-planning
       variant selector + post-click rep chooser. The period window only DIMS
       out-of-window rows (`pickerWindow`), never filters them — selection works
       on any row. `seedReady` still gates the commit. -->
  {#if pickerRows.length > 0 || graphHasDrawableContext}
    <RepresentationPicker
      bands={pickerBands}
      window={pickerWindow}
      canAdd={seedReady}
      {committedRows}
      activePeriod={activePickerPeriod}
      graph={graphReady ? graph : null}
      {vintageYear}
      onapply={applyStaged}
      onstagechange={(hasDiff) => {
        if (hasDiff) {
          applyOutcome = null;
        }
      }}
    />
  {/if}

  {#if applyOutcome}
    <p class="page-add">
      <span class="add-confirm" role="status">
        Applied
        {[
          applyOutcome.added > 0
            ? `+${applyOutcome.added} ${applyOutcome.added === 1 ? "column" : "columns"}`
            : null,
          applyOutcome.removed > 0
            ? `-${applyOutcome.removed} ${applyOutcome.removed === 1 ? "column" : "columns"}`
            : null,
          applyOutcome.periodChanged > 0
            ? `${applyOutcome.periodChanged} ${
                applyOutcome.periodChanged === 1
                  ? "period change"
                  : "period changes"
              }`
            : null,
        ]
          .filter(Boolean)
          .join(" · ")}
        — <a href="/project">view</a>
      </span>
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
            ? "unlabeled"
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
      States{#if isNarrowed && narrowedPeriodLabel}<span class="muted narrowed-note">
          · narrowed to {narrowedPeriodLabel}</span
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
  <!-- The two NON-graph affordances the #761 payload doesn't carry — provenance
       (lineage edges + source register) and the fetched lineage warnings — live
       here on the binding leaf. -->
  <LineageDetails {fqidPath} {node} />
{/snippet}

{#snippet docs()}
  <!-- #402/#967: "Parsed documentation" — a SIBLING of the lineage panels,
       deliberately a separate component over a separate optional DB (its own
       failure domain; a docs error/timeout/absent-index never blanks the leaf). -->
  <DocMentionsPanel {node} />
{/snippet}

{#snippet technical()}
  <!-- #638 PR4 / #1038: all backend/structural rows are demoted to one bottom
       disclosure. The variable flags are always known; state type/column join the
       same disclosure only when the page is showing one resolved state. -->
  <TechnicalDetails>
    <dl class="meta">
      <dt class="micro-label">Sensitive</dt>
      <dd>{node.is_sensitive ? "yes" : "no"}</dd>
      <dt class="micro-label">Identifier</dt>
      <dd>{node.is_identifier ? "yes" : "no"}</dd>
      {#if technicalState?.data_type}
        <dt class="micro-label">Data type</dt>
        <dd>
          {formatDataType(technicalState.data_type, technicalState.data_length)}
        </dd>
      {/if}
      {#if technicalState?.delivery_column_name}
        <dt class="micro-label">Delivery column</dt>
        <dd><code>{technicalState.delivery_column_name}</code></dd>
      {/if}
    </dl>
  </TechnicalDetails>
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
  {technical}
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
