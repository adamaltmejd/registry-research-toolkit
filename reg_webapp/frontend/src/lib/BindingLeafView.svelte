<script lang="ts">
import {
  type BindingNodeData,
  type CatalogNode,
  getBindingDimensions,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  type AddSegment,
  buildAddPlan,
  coverageFromStates,
  formatWindow,
  grainsFromStates,
  memberGroupLink,
  memberQualifier,
  registerPrefixOf,
  type VariantWindow,
} from "./catalog";
import DimensionsPanel from "./DimensionsPanel.svelte";
import DocMentionsPanel from "./DocMentionsPanel.svelte";
import LineagePanels from "./LineagePanels.svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import { nextResolutionQuery, VALUE_SET_VERSION_NONE } from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import StatesView from "./StatesView.svelte";
import SubjectView from "./SubjectView.svelte";
import TechnicalDetails from "./TechnicalDetails.svelte";
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

// Fetch the narrowed states ONLY when a `?period` is active — otherwise the full
// node's embedded states are shown (no redundant request; CatalogNodeView already
// fetched the full node). SYNC `fn()` so the effect tracks the reactive `params`.
const periodResource = asyncResource<CatalogNode | StatesResponse | null>(() =>
  params.period ? getCatalogNode(fqidPath, params) : Promise.resolve(null),
);

const narrowedStates = $derived.by(() => {
  const data = periodResource.data;
  // A `?period` resolve returns a StatesResponse (the only non-node arm here);
  // `!isCatalogNode` narrows `CatalogNode | StatesResponse` to it.
  return data !== null && !isCatalogNode(data) ? data.states : null;
});

// ANY error on the period resolve (a 422 bad-modifier, but also a 5xx / 502 /
// network drop where `status` stays null) is surfaced inline — the metadata +
// the picker stay usable and the states fall back to the full history. Without
// this (filtering to only 422/400), a server/network error would leave
// `narrowedStates` null and wedge the states section on a permanent
// "Loading states…" with no feedback.
const narrowedError = $derived(
  params.period && periodResource.error ? periodResource.error : null,
);

// States to show: the narrowed subset when a valid `?period` is active (null
// while it loads → "Loading states…"); else the full node's embedded states
// (full history, or the fallback when a bad modifier 422'd).
const states = $derived.by(() =>
  params.period && !narrowedError ? narrowedStates : node.states,
);
// Whether the visible states are period-narrowed (drives the "narrowed to X" note
// + StatesView's empty-message wording).
const isNarrowed = $derived(!!params.period && !narrowedError);

// ── #670: member identity from the concept-group dimensions ─────────────────
// LIFTED from DimensionsPanel: the leaf now owns the ONE `/dimensions` fetch and
// derives the header qualifier + group context link from it, then passes the
// resolved groups (+ loading/error) DOWN to DimensionsPanel as props — so the
// page makes a single `/dimensions` request, shared. Read `fqidPath`
// synchronously inside `fn` so the resource refetches when the leaf changes (same
// pattern as `periodResource`).
//
// FAILURE-DOMAIN isolation is preserved: this resource is independent of `node`,
// so a dimensions error/timeout never blanks the leaf — the header just omits the
// qualifier/link (both gate on a RESOLVED fetch, additive), and DimensionsPanel
// renders its own inline loading/error from the same resource.
const dimResource = asyncResource(() => getBindingDimensions(fqidPath));
const dimGroups = $derived(dimResource.data?.dimensions ?? []);
const dimLoading = $derived(dimResource.loading);
const dimError = $derived(dimResource.error);

// The identity row (qualifier + group link) renders ONCE the /dimensions fetch
// has RESOLVED — gated on `!dimLoading && !dimError`. During the sub-second load
// `dimGroups` is still [], so deriving the qualifier then would hit the slug
// fallback (a grouped member has no facets yet) and the row would flash the slug,
// then flip to the facet label once loaded — a visible content flicker. Gating on
// resolved instead means the header shows just `node.name` while loading, then
// the correct qualifier appears once — the leaf is never blanked (failure domain
// preserved; an error keeps it omitted, DimensionsPanel surfaces the error).
const dimReady = $derived(!dimLoading && !dimError);

// The fqid to match a member by in the /dimensions groups. When the leaf is
// opened through a `same_as` ALIAS, the backend resolves to the target variable
// but keeps `node.fqid` as the REQUESTED alias, while the /dimensions members
// are keyed on the RESOLVED target — so matching on `node.fqid` would never find
// the faceted member and wrongly fall back to the alias slug (#670 Codex P2). The
// last `via_same_as` hop is the resolved target binding; with no alias it's
// empty/absent and `lookupFqid === node.fqid` (unchanged behavior).
const lookupFqid = $derived(node.via_same_as?.at(-1) ?? node.fqid);

// The member-distinguishing qualifier (e.g. "AGI · 2007 SNI edition") — this
// member's facet labels across its dimension groups (`kind: "facets"`), the
// canonical `node.group` group leading. For a GROUPED member with no facets (an
// edge group's split siblings) it falls back to the member's slug
// (`kind: "slug"`) so those siblings never share an identical header (#670).
// `null` for an ungrouped variable (its `node.name` suffices), or until resolved.
const qualifier = $derived(
  dimReady ? memberQualifier(dimGroups, lookupFqid, node.group?.key) : null,
);

// The "member of ⟨group label⟩" context link to the group subject page (#670) —
// null when ungrouped, or until the fetch resolves (additive; never blanks).
// `node.group` stays the link's provider/register/key source (already the
// resolved group ref); only the member-matching fqid uses the resolved target.
const groupLink = $derived(
  dimReady ? memberGroupLink(dimGroups, node.group, lookupFqid) : null,
);

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

// ── #306/#638 PR2b one-click add: plan → (genuine choices) → commit ─────────
// The page-level "Add to project" runs `buildAddPlan` over the VISIBLE states
// (period-narrowed when a `?period` is active; further narrowed by any
// `?variant`/`?value_set_version` chips). Succession auto-splits into one
// source per variant segment (informing afterward); coding-identical parallel
// columns auto-pick the #266 primary. Two genuine choices remain, but they live
// at DIFFERENT points in the flow:
//  - the VARIANT (population) choice — when ≥2 register variants co-exist for the
//    period — is surfaced PROACTIVELY in the picker (the `choose-variant`
//    selector below) and GATES the Add button (#638 PR2b): the user resolves the
//    population BEFORE clicking, not in a post-click modal;
//  - the REPRESENTATION (delivery-column) choice stays a POST-click chooser
//    (`addPrompt.stage === "rep"`) — it's per-segment and a succession split can
//    carry several, so it's an informed-after queue, not a pre-commit gate.
// Commits go one-segment-at-a-time through `addFromCatalog` — the store's guarded
// path (stable ids + gen counter) owns every async write.

/** The pending POST-click representation prompt: pick the delivery column for
 * `segments[queue[current]]` (a queue — a multi-segment succession split can
 * carry several ambiguous segments). The variant choice is no longer a prompt —
 * it's the proactive `choose-variant` selector in the picker (gates Add). */
let addPrompt = $state<{
  stage: "rep";
  segments: AddSegment[];
  queue: number[];
  current: number;
} | null>(null);

/** The committed outcome (drives the inline confirmation): the sources bindings
 * landed in (created or found) and how many were already present. */
let addOutcome = $state<{
  added: { name: string; period: string | null }[];
  already: number;
} | null>(null);

// The reactive add plan over the VISIBLE (period-narrowed) states at the page's
// wire period — recomputed whenever the states or period change. Drives BOTH the
// proactive `choose-variant` selector (rendered only when ≥2 variants co-exist
// for the period) and the Add gate. `buildAddPlan` is pure + unit-tested.
const addPlan = $derived(buildAddPlan(states ?? [], params.period ?? null));

// #638 PR2b: the picker's chosen population, when the plan is `choose-variant`.
// INVISIBLE unless ≥2 variants co-exist for the period; defaults to NONE (so a
// fresh plan starts unresolved) and GATES the Add button. The gate is
// MEMBERSHIP-based (correct-by-construction): Add is enabled only when the pick
// is one of the CURRENT plan's options (`addPlan.options`). So a null pick OR a
// stale pick naming a now-absent variant (e.g. after the plan changes
// underneath) can never gate-pass — the gate doesn't lean on the reset timing or
// the remount. The `addVariant = null` reset in the param-change effect still
// cleanly clears the VISUAL pick when the period changes.
let addVariant = $state<string | null>(null);

// A fresh resolution clears the in-flight prompt + stale confirmation AND the
// proactive variant pick (the visible states changed underneath the plan, so a
// prior population choice may name a now-absent variant). Resetting `addVariant`
// here cleanly clears the VISUAL pick when the period changes; gate CORRECTNESS
// no longer depends on it (the gate is membership-based — see `addVariant`), but
// the reset keeps the selector from showing a now-irrelevant highlight.
//
// Also tracks the LEAF IDENTITY (`fqidPath`, the navigation key) so a leaf change
// re-invalidates the add state independent of the parent's `{#key route.fqidPath}`
// remount. Today the remount supplies fresh state, but leaning on that is a
// fragile cross-component coupling: if a future leaf were reused (no remount) with
// a different `node` sharing a variant slug, a stale `addVariant` could satisfy the
// membership gate and open an ambiguous leaf with Add enabled on the old
// population. Invalidating on leaf identity here keeps the gate robust on its own.
$effect(() => {
  void params.period;
  void params.variant;
  void params.value_set_version;
  void fqidPath;
  addPrompt = null;
  addOutcome = null;
  addVariant = null;
});

/** Pick the population in the proactive selector. Changing it must invalidate any
 * in-flight rep prompt — that prompt holds the PRIOR variant's segments, so
 * committing it after switching population would write the wrong variant's data
 * (the UI shows the new pick) — and any stale add confirmation. The rep flow
 * restarts cleanly on the next Add. */
function selectPopulation(variant: string): void {
  addVariant = variant;
  addPrompt = null;
  addOutcome = null;
}

function startAdd(): void {
  addOutcome = null;
  if (addPlan.kind === "choose-variant") {
    // The gate — Add disabled unless `addVariant` is one of the current plan's
    // options — guarantees a valid pick here. Re-plan against ONLY that
    // variant's states — a single variant can't
    // be `choose-variant` again, so the result is always `segments` (at most a
    // rep prompt remains); the defensive `kind` guard keeps TS sound.
    const subset = (states ?? []).filter((s) => s.variant === addVariant);
    const plan = buildAddPlan(subset, params.period ?? null);
    if (plan.kind === "segments") {
      continueWithSegments(plan.segments);
    }
    return;
  }
  continueWithSegments(addPlan.segments);
}

function continueWithSegments(segments: AddSegment[]): void {
  const queue = segments.flatMap((s, i) => (s.needsRepChoice ? [i] : []));
  if (queue.length > 0) {
    addPrompt = { stage: "rep", segments, queue, current: 0 };
    return;
  }
  commit(segments);
}

function chooseRep(column: string): void {
  if (addPrompt?.stage !== "rep") {
    return;
  }
  // Capture the narrowed prompt — TS can't keep the union narrowed inside the
  // map callback over the reactive `addPrompt`.
  const prompt = addPrompt;
  const target = prompt.queue[prompt.current];
  const segments = prompt.segments.map((s, i) =>
    i === target ? { ...s, representation: column } : s,
  );
  if (prompt.current + 1 < prompt.queue.length) {
    addPrompt = { ...prompt, segments, current: prompt.current + 1 };
  } else {
    addPrompt = null;
    commit(segments);
  }
}

/** Commit every segment through the store (synchronous appends; the guarded
 * derive lands per binding afterwards) and record the aggregate outcome. */
function commit(segments: AddSegment[]): void {
  if (segments.length === 0) {
    // Defensive: an empty plan must not render a confirmation ("Already in
    // project" would be a lie). Unreachable via the UI (the button is disabled
    // without states; prompts re-plan over the same states).
    return;
  }
  const added: { name: string; period: string | null }[] = [];
  let already = 0;
  for (const seg of segments) {
    const result = projectStore.addFromCatalog(
      {
        registerVariant: `${registerPrefix}/${seg.variant}`,
        variable: node.fqid,
        representation: seg.representation,
        resolvedPeriod: seg.period,
      },
      { reg_meta_version: regMetaReleaseTag(regMetaVersion), steward },
    );
    if (result.status === "added") {
      added.push({ name: result.sourceName, period: seg.period });
    } else {
      already += 1;
    }
  }
  addOutcome = { added, already };
}

/** Human form of a variant's validity window for the population selector — the
 * shared #309 window display. */
function windowLabel(w: VariantWindow): string {
  return formatWindow(w.from, w.to);
}

/** The rep prompt's subject segment (null outside the rep stage). */
const repSegment = $derived(
  addPrompt?.stage === "rep"
    ? addPrompt.segments[addPrompt.queue[addPrompt.current]]
    : null,
);
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

  <dl class="meta">
    {#if node.definition}
      <dt>Definition</dt>
      <dd>{node.definition}</dd>
    {/if}
    {#if node.description}
      <dt>Description</dt>
      <dd>{node.description}</dd>
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

  <!-- #638 PR2b: the proactive population selector — rendered ONLY when ≥2
       register variants co-exist for the period (`buildAddPlan` →
       `choose-variant`). The variant is INVISIBLE for an unambiguous variable
       (1 variant, or a pure succession that auto-splits); when it shows it GATES
       the Add button (see `disabled` below) so the user resolves the population
       BEFORE committing, not in a post-click modal. Same `.add-chooser` /
       `.pick-list` / `.pick` vocabulary as the rep chooser for visual
       consistency. -->
  {#if addPlan.kind === "choose-variant"}
    <div class="add-chooser" role="group" aria-label="Pick a register variant">
      <p class="chooser-title">
        This variable has several populations for this period — pick one to add:
      </p>
      <ul class="pick-list">
        {#each addPlan.options as w (w.variant)}
          <li>
            <button
              type="button"
              class="pick"
              aria-pressed={addVariant === w.variant}
              class:selected={addVariant === w.variant}
              onclick={() => selectPopulation(w.variant)}
            >
              <span class="slug">{w.variant}</span>
              <span class="name">{windowLabel(w)}</span>
            </button>
          </li>
        {/each}
      </ul>
    </div>
  {/if}

  <!-- #306: ONE page-level add for the variable at the chosen period. Disabled
       until the deployment seed is ready and the visible states are loaded
       (nothing to add when none cover the period), and — when ≥2 variants
       co-exist (`choose-variant`) — until the user has picked the population
       above (the gate; #638 PR2b). -->
  <div class="page-add">
    <button
      type="button"
      class="add-to-project"
      disabled={!seedReady ||
        !states ||
        states.length === 0 ||
        (addPlan.kind === "choose-variant" &&
          !addPlan.options.some((o) => o.variant === addVariant))}
      onclick={startAdd}
    >
      Add to project
    </button>
    {#if addOutcome}
      {#if addOutcome.added.length === 0}
        <span class="add-confirm already" role="status">Already in project</span>
      {:else}
        <span class="add-confirm" role="status">
          {#if addOutcome.added.length === 1}
            Added to project ({addOutcome.added[0].name})
          {:else}
            <!-- The succession split's "inform afterward" (#306). -->
            Added as {addOutcome.added.length} sources:
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
    {/if}
  </div>

  {#if addPrompt?.stage === "rep" && repSegment}
    <div class="add-chooser" role="group" aria-label="Pick a representation">
      <!-- Genuinely distinct codings: an explicit pick, ranked latest-era first
           so the primary leads (#266) — same semantics as the picker chooser. -->
      <p class="chooser-title">
        <code>{node.fqid}</code> has several representations at this period
        {#if addPrompt.queue.length > 1}
          (variant <code>{repSegment.variant}</code>)
        {/if}
        — pick one:
      </p>
      <ul class="pick-list">
        {#each repSegment.reps as rep, i (rep.column)}
          <li>
            <button type="button" class="pick" onclick={() => chooseRep(rep.column)}>
              <span class="slug">{rep.column}</span>
              {#if i === 0}<span class="name">(primary)</span>{/if}
              {#if rep.label}<span class="name">{rep.label}</span>{/if}
              {#if rep.codeCount != null}<span class="name">({rep.codeCount} codes)</span>{/if}
              {#if rep.classificationSlug}<code class="classification">{rep.classificationSlug}</code>{/if}
            </button>
          </li>
        {/each}
      </ul>
      <button type="button" class="cancel" onclick={() => (addPrompt = null)}>
        Cancel
      </button>
    </div>
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
    {#if states}
      <StatesView
        {states}
        narrowed={isNarrowed}
        activeVariant={params.variant ?? null}
        activeValueSetVersion={params.value_set_version ?? null}
        onpickVariant={(variant) => setResolution({ variant })}
        onpickValueSetVersion={(value_set_version) =>
          setResolution({ value_set_version })}
      />
    {:else}
      <p class="muted" aria-busy="true">Loading states…</p>
    {/if}
  </section>
{/snippet}

{#snippet relationships()}
  <!-- #489/#670: the concept-group dimensions this variable belongs to (the "pick
       your variant" facet groups). PRESENTATIONAL since #670 — the `/dimensions`
       fetch is owned by THIS view (it also feeds the header qualifier + group
       link), so the panel receives the resolved groups + loading/error as props
       (one shared fetch). Its failure domain is unchanged: the dimensions resource
       is independent of `node`, so an error renders the panel's inline alert
       without blanking the leaf. Omits itself entirely when in no group. -->
  <DimensionsPanel groups={dimGroups} loading={dimLoading} error={dimError} />

  <LineagePanels {fqidPath} {node} />
{/snippet}

{#snippet docs()}
  <!-- #402: "Mentioned in documentation" — a SIBLING of the lineage panels,
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
    font-family: var(--mono, monospace);
    font-weight: 500;
    font-size: 0.85em;
  }
  .member-identity .group-context {
    color: var(--muted);
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
    color: var(--accent);
    font: inherit;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .modifier-chip:hover {
    background: var(--surface);
  }
  /* #306: the page-level add + its confirmation line. */
  .page-add {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    margin: 0.75rem 0;
  }
  .add-to-project {
    font: inherit;
    font-size: 0.9rem;
    padding: 0.35rem 0.9rem;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--accent-bg);
    color: var(--accent);
    cursor: pointer;
  }
  .add-to-project:hover:enabled {
    background: var(--surface);
  }
  .add-to-project:disabled {
    opacity: 0.5;
    cursor: default;
  }
  .add-confirm {
    font-size: 0.85rem;
    color: var(--accent);
  }
  .add-confirm.already {
    color: var(--muted);
  }
  /* The genuine-choice prompts (variant / representation) — same visual
     vocabulary as the CatalogPicker chooser. */
  .add-chooser {
    border: 1px dashed var(--accent);
    border-radius: 6px;
    padding: 0.6rem 0.75rem;
    margin: 0.5rem 0;
  }
  .chooser-title {
    font-size: 0.85rem;
    margin: 0 0 0.4rem;
  }
  .pick-list {
    list-style: none;
    padding: 0;
    margin: 0 0 0.4rem;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .pick {
    display: inline-flex;
    align-items: baseline;
    gap: 0.6rem;
    font: inherit;
    font-size: 0.85rem;
    padding: 0.3rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
    cursor: pointer;
    text-align: left;
  }
  .pick:hover {
    border-color: var(--accent);
  }
  /* #638 PR2b: the currently-picked population in the proactive selector. */
  .pick.selected {
    border-color: var(--accent);
    background: var(--accent-bg);
    color: var(--accent);
  }
  .pick .slug {
    font-family: var(--mono, monospace);
    font-weight: 600;
  }
  .pick .name {
    color: var(--muted);
  }
  .pick .classification {
    font-size: 0.85em;
  }
  .cancel {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.2rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
    cursor: pointer;
  }
</style>
