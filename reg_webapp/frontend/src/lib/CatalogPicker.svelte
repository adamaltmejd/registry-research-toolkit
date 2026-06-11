<script lang="ts">
import {
  type CatalogNode,
  getCatalogNode,
  getCatalogRoot,
  getRegisterVariants,
  type RootResponse,
  type StatesResponse,
  type VariableStateModel,
} from "./api";
import { asyncResource } from "./async.svelte";
import ConceptGroupRow from "./ConceptGroupRow.svelte";
import {
  bindingChildren,
  countFoldedMembers,
  deriveType,
  foldGroupedRows,
  groupFilterKeys,
  narrowCatalogNode,
  type PickedVariable,
  type Representation,
  rankFilter,
  registerPrefixOf,
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
//  - "variant": pick a 3-seg register_variant, emitted WHOLE via
//    onpickVariant(registerVariant). When `register` (a valid 2-seg prefix) is
//    KNOWN (the user hand-typed it), it jumps straight to that register's variant
//    list. When `register` is EMPTY / not a valid prefix (C2 — catalog→project
//    handoff), it opens in REGISTER-BROWSE mode: provider list → register list →
//    variant list, reusing the catalog DATA LAYER (root + provider node fetches) and
//    the same FilterInput/rankFilter. Either way the emitted value is the full
//    `provider/register/variant`.
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
  // The KNOWN 2-seg provider/register FQID (hand-typed prefix) → jump straight to
  // variants. Empty / not a valid 2-seg prefix → register-browse mode (C2).
  register: string;
  // Emits the WHOLE 3-seg register_variant (the picker owns the register, browsed or
  // hand-typed, so the caller just sets the field).
  onpickVariant: (registerVariant: string) => void;
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

// ── C2: register-browse navigation (variant mode only) ───────────────────────
// In variant mode, the EFFECTIVE register is either the hand-typed prefix (props
// `register`, when it's a valid 2-seg FQID) or a register the user browsed to
// (provider list → register list). `browsedRegister` holds the latter. A valid
// prop prefix short-circuits the browse (jump straight to variants).
const propPrefix = $derived(
  props.mode === "variant" ? registerPrefixOf(props.register) : "",
);
// Has the prop given us a usable register prefix? (A 2-seg FQID — registerPrefixOf
// returns "" for fewer segments.)
const havePropRegister = $derived(propPrefix !== "");
let browsedRegister = $state<string | null>(null);
// The register whose variants we list: the prop prefix wins; else the browsed one.
const effectiveRegister = $derived(
  havePropRegister ? propPrefix : browsedRegister,
);
// The browse step the picker shows when there's no prop register and no browsed one
// yet: "provider" (pick a provider) → "register" (pick a register under it) →
// (effectiveRegister set) → the variant list. `browsedProvider` is the chosen
// provider FQID at the "register" step.
let browsedProvider = $state<string | null>(null);

// One top-level resource (registers its $effect at component init). The fetch
// branches on the mode AND, in variant browse mode, on the current browse step:
//  - variable mode → the register node (its binding children are the variables);
//  - variant mode with an effective register → that register's variants;
//  - variant browse, provider step (no provider chosen) → the catalog root
//    (provider list);
//  - variant browse, register step (provider chosen) → the provider node (its
//    register children).
// The fn READS the reactive browse state so the resource re-fetches as the user
// drills in (asyncResource tracks the sync reads).
const resource = asyncResource<
  | Awaited<ReturnType<typeof getRegisterVariants>>
  | CatalogNode
  | StatesResponse
  | RootResponse
>(() => {
  if (props.mode === "variable") {
    return getCatalogNode(props.registerPrefix);
  }
  const reg = effectiveRegister;
  if (reg) {
    return getRegisterVariants(reg);
  }
  // Browse: provider step (no provider chosen) → root; register step → provider node.
  return browsedProvider ? getCatalogNode(browsedProvider) : getCatalogRoot();
});

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
// #322: fold the pickable variables under their concept groups, same as the
// browse (a register's 740 variables open on a wall of near-identical `agi*`
// rows otherwise). `foldGroupedRows` tolerates a stale pre-`groups` edge-cached
// payload (#317) by degrading to the flat list.
const variableRows = $derived(
  registerNode && registerNode.kind === "register"
    ? foldGroupedRows(variableChildren, registerNode.groups)
    : [],
);

// The variant-pick list (only when an effective register is set).
const variantList = $derived(
  props.mode === "variant" &&
    effectiveRegister &&
    resource.data &&
    "variants" in resource.data
    ? resource.data.variants
    : [],
);

// ── C2 browse lists (provider step + register step) ──────────────────────────
// Active ONLY when variant mode has no effective register yet. The root's children
// are a provider | classification-root union — keep providers only (variants live
// under providers). The provider node's children are register nodes.
const browseProviders = $derived(
  props.mode === "variant" &&
    !effectiveRegister &&
    !browsedProvider &&
    resource.data &&
    "kind" in resource.data &&
    resource.data.kind === "root"
    ? resource.data.children.filter((c) => c.kind === "provider")
    : [],
);
const browseRegisters = $derived(
  props.mode === "variant" &&
    !effectiveRegister &&
    browsedProvider &&
    resource.data &&
    "kind" in resource.data &&
    resource.data.kind === "provider"
    ? resource.data.children
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
// Variable rows are group-folded (#322): a group row matches/ranks on its
// label/key + every member's name/FQID (`groupFilterKeys` — the same match set
// as the browse's `groupMatchesFilter`), so target-hunting a member ("maj")
// still surfaces the family that folded it.
const filteredVariables = $derived(
  rankFilter(variableRows, filter, (row) =>
    row.kind === "group"
      ? groupFilterKeys(row.group)
      : [row.item.fqid, row.item.name],
  ),
);
// C2: the browse-step lists, same rankFilter (target-hunt a provider/register).
const filteredProviders = $derived(
  rankFilter(browseProviders, filter, (p) => [p.fqid, p.name]),
);
const filteredRegisters = $derived(
  rankFilter(browseRegisters, filter, (r) => [r.fqid, r.name]),
);

// C2: reset the filter when the browse STEP changes (provider → register →
// variant), so a needle that narrowed one list doesn't hide the next. The reads
// register the dependency; `untrack` would defeat the reset. A fresh list at each
// step opens unfiltered, matching the browse pages.
$effect(() => {
  void browsedProvider;
  void browsedRegister;
  filter = "";
});

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

// ── C2 browse handlers (variant mode) ────────────────────────────────────────

/** Step into a provider (the "register" browse step). */
function browseProvider(fqid: string): void {
  browsedProvider = fqid;
}
/** Step into a register (sets the effective register → the variant list). */
function browseRegister(fqid: string): void {
  browsedRegister = fqid;
}
/** Back out one browse step: register list → provider list, or variant list →
 * register list (only the BROWSED register backs out; a hand-typed prop prefix has
 * no back step — there's nothing to browse above it). */
function browseBack(): void {
  if (browsedRegister) {
    browsedRegister = null;
  } else if (browsedProvider) {
    browsedProvider = null;
  }
}

/** Emit the chosen variant as the WHOLE register_variant (register + slug). The
 * register is the effective one (hand-typed prefix or browsed). */
function emitVariant(slug: string): void {
  if (props.mode !== "variant" || !effectiveRegister) {
    return;
  }
  props.onpickVariant(`${effectiveRegister}/${slug}`);
}
</script>

<div class="picker">
  <div class="picker-head">
    {#if props.mode === "variant"}
      {#if effectiveRegister}
        <span class="picker-title">Pick a variant of <code>{effectiveRegister}</code></span>
      {:else if browsedProvider}
        <span class="picker-title">Pick a register in <code>{browsedProvider}</code></span>
      {:else}
        <span class="picker-title">Pick a register — choose a provider</span>
      {/if}
    {:else}
      <span class="picker-title">
        Pick a variable from <code>{props.registerPrefix}</code>
      </span>
    {/if}
    <button type="button" class="cancel" onclick={props.oncancel}>Cancel</button>
  </div>

  {#if props.mode === "variant" && !havePropRegister && (browsedProvider || browsedRegister)}
    <!-- C2: a back step for the register-browse path (no back from a hand-typed
         prefix — there's nothing above it to browse). -->
    <button type="button" class="back" onclick={browseBack}>← Back</button>
  {/if}

  {#if props.mode === "variable" && !props.period}
    <p class="hint muted">Set the source period to auto-fill type / display name.</p>
  {/if}

  {#if resource.loading}
    <p class="muted" aria-busy="true">Loading…</p>
  {:else if resource.error}
    <p class="error" role="alert">Failed to load: {resource.error}</p>
  {:else if props.mode === "variant" && effectiveRegister}
    <!-- Variant list (hand-typed prefix OR a browsed register). -->
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
              <button type="button" class="pick" onclick={() => emitVariant(variant.slug)}>
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
  {:else if props.mode === "variant" && browsedProvider}
    <!-- C2: register-browse step 2 — the chosen provider's registers. -->
    {#if browseRegisters.length > 0}
      <FilterInput
        bind:value={filter}
        total={browseRegisters.length}
        shown={filteredRegisters.length}
        placeholder="Filter registers…"
        label="Filter registers"
        autofocus
      />
      {#if filteredRegisters.length > 0}
        <ul class="pick-list">
          {#each filteredRegisters as register (register.fqid)}
            <li>
              <button type="button" class="pick" onclick={() => browseRegister(register.fqid)}>
                <span class="slug">{register.name ?? register.fqid}</span>
                <code class="leaf-fqid">{register.fqid}</code>
              </button>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No registers match “{filter}”.</p>
      {/if}
    {:else}
      <p class="muted">No registers for this provider.</p>
    {/if}
  {:else if props.mode === "variant"}
    <!-- C2: register-browse step 1 — the provider list (catalog root). -->
    {#if browseProviders.length > 0}
      <FilterInput
        bind:value={filter}
        total={browseProviders.length}
        shown={filteredProviders.length}
        placeholder="Filter providers…"
        label="Filter providers"
        autofocus
      />
      {#if filteredProviders.length > 0}
        <ul class="pick-list">
          {#each filteredProviders as provider (provider.fqid)}
            <li>
              <button type="button" class="pick" onclick={() => browseProvider(provider.fqid)}>
                <span class="slug">{provider.name ?? provider.fqid}</span>
                <code class="leaf-fqid">{provider.fqid}</code>
              </button>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No providers match “{filter}”.</p>
      {/if}
    {:else}
      <p class="muted">No providers.</p>
    {/if}
  {:else if registerNode && registerNode.kind === "register"}
    {#if variableChildren.length > 0}
      <!-- Counts stay in VARIABLE units after folding (#322):
           countFoldedMembers expands group rows to their member counts. -->
      <FilterInput
        bind:value={filter}
        total={variableChildren.length}
        shown={countFoldedMembers(filteredVariables)}
        placeholder="Filter variables…"
        label="Filter variables"
        autofocus
      />
      {#if filteredVariables.length > 0}
        <ul class="pick-list">
          {#each filteredVariables as row (row.kind === "group" ? row.group.key : row.item.fqid)}
            <li>
              {#if row.kind === "group"}
                <!-- Folded family (#322): expand to the facet picker; members
                     emit through the same derive-on-pick path as leaf rows. -->
                <ConceptGroupRow
                  group={row.group}
                  disabled={resolving}
                  onpick={(fqid) => void pickVariable(fqid)}
                />
              {:else}
                <button
                  type="button"
                  class="pick"
                  disabled={resolving}
                  onclick={() => void pickVariable(row.item.fqid)}
                >
                  <span class="slug">{row.item.name ?? row.item.fqid}</span>
                  <code class="leaf-fqid">{row.item.fqid}</code>
                </button>
              {/if}
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
  .back {
    font: inherit;
    font-size: 0.8rem;
    padding: 0.1rem 0;
    margin-bottom: 0.4rem;
    border: none;
    background: transparent;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
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
