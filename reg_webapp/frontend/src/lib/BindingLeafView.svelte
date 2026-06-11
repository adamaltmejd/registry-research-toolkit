<script lang="ts">
import {
  type BindingNodeData,
  type CatalogNode,
  getCatalogNode,
  isCatalogNode,
  type StatesResponse,
  type VariableStateModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import { registerPrefixOf, representationsFromStates } from "./catalog";
import LineagePanels from "./LineagePanels.svelte";
import PeriodPicker from "./PeriodPicker.svelte";
import { nextResolutionQuery, VALUE_SET_VERSION_NONE } from "./period";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import StatesView from "./StatesView.svelte";

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

// Whether the CURRENTLY-VISIBLE states carry MORE THAN ONE co-existing representation
// (delivery column) — the same `representationsFromStates` the picker chooser uses.
// Only then is the per-state delivery column a genuine REPRESENTATION choice to pin on
// the binding; a single representation must NOT set `binding.representation` (it would
// desync from the store's single-rep derive, which clears it to null — and break the
// duplicate guard). `states` may be null while a narrow loads.
const multiRepresentation = $derived(
  states ? representationsFromStates(states).length > 1 : false,
);

// Per-state add feedback keyed by state_id (drives StatesView's inline confirmation).
let addStatus = $state<Record<number, "added" | "already-present">>({});

// A fresh resolution clears stale confirmations (the visible states changed).
$effect(() => {
  void params.period;
  void params.variant;
  void params.value_set_version;
  addStatus = {};
});

/** Add ONE variant-state to the project (C1). Builds the register_variant from the
 * variable's prefix + the state's variant, hands the variable + (only when several
 * columns co-exist) the state's delivery column as the representation + the page's
 * resolved period to the store, and records the outcome for the inline confirmation.
 * The store creates the project + source as needed and derives the binding at the
 * source's period — no editor import here. */
function onAdd(state: VariableStateModel): void {
  const registerVariant = `${registerPrefix}/${state.variant}`;
  const result = projectStore.addFromCatalog(
    {
      registerVariant,
      variable: node.fqid,
      // A representation is pinned ONLY when the concept genuinely has >1 co-existing
      // column at this period (the explicit per-column choice); a single-rep variable
      // adds representation-less, matching the store's single-rep derive.
      representation: multiRepresentation
        ? (state.delivery_column_name ?? null)
        : null,
      resolvedPeriod: params.period ?? null,
    },
    { reg_meta_version: regMetaReleaseTag(regMetaVersion), steward },
  );
  addStatus = { ...addStatus, [state.state_id]: result.status };
}
</script>

<article>
  <h2>{node.name ?? node.fqid}</h2>
  <p class="fqid"><code>{node.fqid}</code></p>

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
    <dt>Sensitive</dt>
    <dd>{node.is_sensitive ? "yes" : "no"}</dd>
    <dt>Identifier</dt>
    <dd>{node.is_identifier ? "yes" : "no"}</dd>
  </dl>

  <PeriodPicker
    period={params.period ?? null}
    onsubmit={(period) => setResolution({ period })}
    onclear={() => setResolution({ period: null })}
  />

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

  <section aria-labelledby="states-heading">
    <h3 id="states-heading">
      States{#if isNarrowed}<span class="muted narrowed-note">
          · narrowed to {params.period}</span
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
        onadd={seedReady ? onAdd : null}
        {addStatus}
      />
    {:else}
      <p class="muted" aria-busy="true">Loading states…</p>
    {/if}
  </section>

  <LineagePanels {fqidPath} {node} />
</article>

<style>
  .fqid {
    margin-top: -0.25rem;
    color: var(--muted);
  }
  .via code {
    font-size: 0.9em;
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
</style>
