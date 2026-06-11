<script lang="ts">
import {
  type CatalogNode,
  getCatalogNode,
  getRegisterVariants,
  type StatesResponse,
  type VariableStateModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import {
  bindingChildren,
  deriveType,
  narrowCatalogNode,
  type PickedVariable,
  type Representation,
  rankFilter,
  representationsCollapse,
  representationsFromStates,
  resolveBindingAt,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";

// INLINE-EXPAND embedded pick-mode catalog browser (maintainer decision): NO
// router import, NO overlay/modal. It reuses the catalog DATA LAYER only
// (asyncResource + the api.ts catalog GETs) and replaces the browse components'
// <a href> navigation with internal pick callbacks.
//
// Two modes (the picker mounts once per open with a FIXED mode):
//  - "variant": lists a register's variants → onpickVariant(slug); the caller
//    builds the 3-seg register_variant.
//  - "variable": SCOPED to a source's register (registerPrefix = first 2 segs of
//    register_variant) — drills ONLY within that register's binding (variable)
//    list. On selecting a leaf it DERIVES-ON-PICK by resolving the variable at the
//    source's (period, variant) → StatesResponse, then prefills type +
//    display_name from the resolved state. There is no value-set-version pin: a
//    (variable, variant, period) resolves to exactly one value set (enforced at
//    reg_meta build time), so the FQID is a bare 3-segment slug.
//
// Source-scoping the variable picker to the register prefix is the UX that prevents
// fqid_register_variant_mismatch for picked values — but it is UX only; the backend
// remains canonical (see reg_webapp/DESIGN.md → Pydantic boundary).
interface VariantProps {
  mode: "variant";
  register: string; // 2-seg provider/register FQID
  onpickVariant: (slug: string) => void;
  oncancel: () => void;
}
interface VariableProps {
  mode: "variable";
  registerPrefix: string; // 2-seg provider/register FQID
  period: string | null; // the source's period as a wire string (null → can't resolve)
  variant: string; // 3rd seg of register_variant
  onpickVariable: (picked: PickedVariable) => void;
  oncancel: () => void;
}
const props: VariantProps | VariableProps = $props();

// One top-level resource (registers its $effect at component init). The fetch
// branches on the FIXED mode: variants for a variant-pick, the register node (whose
// binding children are the variable list) for a variable-pick.
const resource = asyncResource<
  Awaited<ReturnType<typeof getRegisterVariants>> | CatalogNode | StatesResponse
>(() =>
  props.mode === "variant"
    ? getRegisterVariants(props.register)
    : getCatalogNode(props.registerPrefix),
);

// The variable-pick register node (the browse fetch is no-query → a `kind`-tagged
// node). Its `binding` children are the pickable variables.
const registerNode = $derived(
  props.mode === "variable"
    ? narrowCatalogNode(resource.data as CatalogNode | StatesResponse | null)
    : null,
);
const variableChildren = $derived(
  registerNode ? bindingChildren(registerNode) : [],
);

// The variant-pick list.
const variantList = $derived(
  props.mode === "variant" && resource.data && "variants" in resource.data
    ? resource.data.variants
    : [],
);

// In-memory type-to-filter over whichever list this picker shows (740 register
// variables open on a wall of near-identical `agi*` rows otherwise). Match on
// BOTH slug/FQID and display name (rankFilter folds diacritics + case). Unlike
// the browse pages, the picker RANKS the survivors (exact → prefix → other) so a
// target-hunt ("kon" → Kön) surfaces the wanted row first; alphabetical order is
// kept within each tier.
let filter = $state("");
const filteredVariants = $derived(
  rankFilter(variantList, filter, (v) => [v.slug, v.name]),
);
const filteredVariables = $derived(
  rankFilter(variableChildren, filter, (c) => [c.fqid, c.name]),
);

// The derive-on-pick resolve state + the REPRESENTATION chooser. A concept can
// carry several co-existing delivery columns (parallel representations: SSYK
// 3/4/5-digit, age brackets) at one period; when it does, the author must pick
// which column the binding extracts (`binding.representation`) — the job the
// retired `@version` pin once did, keyed on the delivery column.
let resolving = $state(false);
let resolveError = $state<string | null>(null);
let pending = $state<{ fqid: string; states: VariableStateModel[] } | null>(
  null,
);
const pendingReps = $derived<Representation[]>(
  pending ? representationsFromStates(pending.states) : [],
);
// CODING-IDENTICAL coexisting columns (UT0290/UT0280) collapse to the primary
// (`pendingReps[0]`, latest-era) + a reveal of the alternates, rather than a
// forced flat choice (issue #266). `showAlternates` toggles that reveal.
const collapse = $derived(representationsCollapse(pendingReps));
let showAlternates = $state(false);

async function pickVariable(fqid: string): Promise<void> {
  if (props.mode !== "variable") {
    return;
  }
  const p = props;
  pending = null;
  // The derive-on-pick resolve is the SAME shared path the store's re-derive uses
  // (catalog.resolveBindingAt) — one source of truth so a picked binding and a
  // re-derived one never disagree. Period-unset / no-states resolve to the bare
  // FQID with the opaque fallback (the backend Validate flags it; the binding row
  // shows the "unresolved" marker via the store's re-derive tracking).
  resolving = true;
  resolveError = null;
  try {
    const result = await resolveBindingAt(fqid, p.period, p.variant);
    if (result.kind === "ambiguous") {
      // >1 distinct delivery column → defer to the representation chooser below.
      showAlternates = false;
      pending = { fqid: result.fqid, states: result.states };
      return;
    }
    if (result.kind === "unresolved") {
      // Period-unset / no covering state → bare FQID, opaque fallback, no prefill.
      // Emit the resolution kind so the consumer marks the row honestly without
      // re-inferring status from value tells.
      p.onpickVariable({
        variable: fqid,
        type: "opaque",
        displayNameDefault: null,
        resolution: "unresolved",
        unresolvedReason: result.reason,
      });
      return;
    }
    p.onpickVariable({
      variable: fqid,
      type: result.type,
      displayNameDefault: result.displayNameDefault,
      representation: result.representation,
      resolution: "derived",
    });
  } catch (e) {
    resolveError = e instanceof Error ? e.message : String(e);
  } finally {
    resolving = false;
  }
}

function chooseRepresentation(rep: Representation): void {
  if (props.mode !== "variable" || !pending) {
    return;
  }
  const state = pending.states.find(
    (s) => s.delivery_column_name === rep.column,
  );
  // A chooser pick always yields a concrete representation → a genuine derive.
  props.onpickVariable({
    variable: pending.fqid,
    type: deriveType(state),
    displayNameDefault: rep.column,
    representation: rep.column,
    resolution: "derived",
  });
  pending = null;
}
</script>

<div class="picker">
  <div class="picker-head">
    {#if props.mode === "variant"}
      <span class="picker-title">Pick a variant of <code>{props.register}</code></span>
    {:else}
      <span class="picker-title">
        Pick a variable from <code>{props.registerPrefix}</code>
      </span>
    {/if}
    <button type="button" class="cancel" onclick={props.oncancel}>Cancel</button>
  </div>

  {#if props.mode === "variable" && !props.period}
    <p class="hint muted">Set the source period to auto-fill type / display name.</p>
  {/if}

  {#if resource.loading}
    <p class="muted" aria-busy="true">Loading…</p>
  {:else if resource.error}
    <p class="error" role="alert">Failed to load: {resource.error}</p>
  {:else if props.mode === "variant"}
    {#if variantList.length > 0}
      <FilterInput
        bind:value={filter}
        total={variantList.length}
        shown={filteredVariants.length}
        placeholder="Filter variants…"
        label="Filter variants"
        autofocus
      />
      {#if filteredVariants.length > 0}
        <ul class="pick-list">
          {#each filteredVariants as variant (variant.slug)}
            <li>
              <button
                type="button"
                class="pick"
                onclick={() => props.mode === "variant" && props.onpickVariant(variant.slug)}
              >
                <span class="slug">{variant.slug}</span>
                {#if variant.name}<span class="name">{variant.name}</span>{/if}
              </button>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No variants match “{filter}”.</p>
      {/if}
    {:else}
      <p class="muted">No variants for this register.</p>
    {/if}
  {:else if registerNode && registerNode.kind === "register"}
    {#if variableChildren.length > 0}
      <FilterInput
        bind:value={filter}
        total={variableChildren.length}
        shown={filteredVariables.length}
        placeholder="Filter variables…"
        label="Filter variables"
        autofocus
      />
      {#if filteredVariables.length > 0}
        <ul class="pick-list">
          {#each filteredVariables as child (child.fqid)}
            <li>
              <button
                type="button"
                class="pick"
                disabled={resolving}
                onclick={() => void pickVariable(child.fqid)}
              >
                <span class="slug">{child.name ?? child.fqid}</span>
                <code class="leaf-fqid">{child.fqid}</code>
              </button>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No variables match “{filter}”.</p>
      {/if}
    {:else}
      <p class="muted">No variables in this register.</p>
    {/if}
  {:else}
    <p class="error" role="alert">
      <code>{props.mode === "variable" ? props.registerPrefix : ""}</code> isn't a browsable register.
    </p>
  {/if}

  {#if resolving}
    <p class="muted resolve-state">Resolving variable at the source period…</p>
  {/if}
  {#if resolveError}
    <p class="error resolve-state" role="alert">Resolve failed: {resolveError}</p>
  {/if}

  {#if pending && pendingReps.length > 1}
    <div class="chooser" role="group" aria-label="Pick a representation">
      {#if collapse}
        <!-- Coding-identical parallel deliveries: lead with the primary (latest-era);
             the alternates are the SAME coding under a different delivery column, so
             they reveal-on-demand rather than forcing a co-equal choice (#266). -->
        <p class="chooser-title">
          <code>{pending.fqid}</code> is delivered as
          <code class="slug">{pendingReps[0].column}</code> (same coding under
          {pendingReps.length - 1} other column{pendingReps.length > 2 ? "s" : ""}):
        </p>
        <ul class="pick-list">
          <li>
            <button type="button" class="pick primary" onclick={() => chooseRepresentation(pendingReps[0])}>
              <span class="slug">{pendingReps[0].column}</span>
              {#if pendingReps[0].label}<span class="name">{pendingReps[0].label}</span>{/if}
              {#if pendingReps[0].codeCount != null}<span class="name">({pendingReps[0].codeCount} codes)</span>{/if}
              {#if pendingReps[0].classificationSlug}<code class="classification">{pendingReps[0].classificationSlug}</code>{/if}
            </button>
          </li>
        </ul>
        <button
          type="button"
          class="reveal"
          aria-expanded={showAlternates}
          onclick={() => (showAlternates = !showAlternates)}
        >
          {showAlternates ? "Hide" : "Also delivered as"}
          {#if !showAlternates}
            {pendingReps
              .slice(1)
              .map((r) => r.column)
              .join(", ")}
          {/if}
        </button>
        {#if showAlternates}
          <ul class="pick-list alternates">
            {#each pendingReps.slice(1) as rep (rep.column)}
              <li>
                <button type="button" class="pick" onclick={() => chooseRepresentation(rep)}>
                  <span class="slug">{rep.column}</span>
                  {#if rep.label}<span class="name">{rep.label}</span>{/if}
                  {#if rep.codeCount != null}<span class="name">({rep.codeCount} codes)</span>{/if}
                  {#if rep.classificationSlug}<code class="classification">{rep.classificationSlug}</code>{/if}
                </button>
              </li>
            {/each}
          </ul>
        {/if}
      {:else}
        <!-- Genuinely distinct codings (SSYK 3/4/5-digit, age brackets): an explicit
             pick, but ranked latest-era first so the primary leads (#266). -->
        <p class="chooser-title">
          <code>{pending.fqid}</code> has several representations at this period — pick one:
        </p>
        <ul class="pick-list">
          {#each pendingReps as rep (rep.column)}
            <li>
              <button type="button" class="pick" onclick={() => chooseRepresentation(rep)}>
                <span class="slug">{rep.column}</span>
                {#if rep.label}<span class="name">{rep.label}</span>{/if}
                {#if rep.codeCount != null}<span class="name">({rep.codeCount} codes)</span>{/if}
                {#if rep.classificationSlug}<code class="classification">{rep.classificationSlug}</code>{/if}
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  {/if}
</div>

<style>
  .picker {
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.6rem 0.75rem;
    margin-top: 0.4rem;
    background: var(--surface);
  }
  .picker-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
  }
  .picker-title {
    font-size: 0.85rem;
    font-weight: 600;
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
  .pick-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    max-height: 18rem;
    overflow-y: auto;
  }
  .pick {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    width: 100%;
    text-align: left;
    font: inherit;
    padding: 0.35rem 0.5rem;
    border: 1px solid transparent;
    border-radius: 4px;
    background: transparent;
    cursor: pointer;
  }
  .pick:hover:not(:disabled) {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .pick:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  .slug {
    font-weight: 600;
  }
  .leaf-fqid,
  .name,
  .classification {
    color: var(--muted);
    font-size: 0.85em;
  }
  .hint {
    font-size: 0.8rem;
    margin: 0 0 0.4rem;
  }
  .resolve-state {
    font-size: 0.85rem;
    margin: 0.4rem 0 0;
  }
  .chooser {
    margin-top: 0.5rem;
    padding-top: 0.5rem;
    border-top: 1px solid var(--border);
  }
  .chooser-title {
    font-size: 0.85rem;
    margin: 0 0 0.4rem;
  }
  .pick.primary {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .reveal {
    font: inherit;
    font-size: 0.8rem;
    margin-top: 0.4rem;
    padding: 0.2rem 0;
    border: none;
    background: transparent;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
  }
  .pick-list.alternates {
    margin-top: 0.25rem;
  }
</style>
