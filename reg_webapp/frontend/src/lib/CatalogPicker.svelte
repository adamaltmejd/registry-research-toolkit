<script lang="ts">
import { Command } from "bits-ui";
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
  leafSlug,
  narrowCatalogNode,
  type PickedVariable,
  type Representation,
  rankFilter,
  registerPrefixOf,
  representationsCollapse,
  representationsFromStates,
  resolveBindingAt,
} from "./catalog";

// INLINE-EXPAND embedded pick-mode catalog browser (maintainer decision): NO
// router import, NO overlay/modal. It reuses the catalog DATA LAYER only
// (asyncResource + the api.ts catalog GETs) and replaces the browse components'
// <a href> navigation with internal pick callbacks.
//
// The filter+list interaction is built on Bits UI's headless `Command` primitive
// (UI-foundation spike Arm A, #689): a SINGLE tab-stop on the Command input,
// Up/Down arrow nav over the filtered rows, Enter to select the active row, and
// role="listbox"/role="option" + aria-activedescendant ARIA — replacing the
// hand-rolled FilterInput + <ul><button class=pick> lists that had N tab-stops and
// no keyboard nav. Command's OWN scoring is turned OFF (`shouldFilter={false}`):
// the picker feeds it the pre-ranked `filtered*` arrays so `rankFilter` (exact →
// prefix → other, alphabetical within tier — the target-hunt UX) stays the single
// source of truth. The "N of M" count is a sibling aria-live element, not a
// Command feature.
//
// Two modes (the picker mounts once per open with a FIXED mode):
//  - "variant": pick a 3-seg register_variant, emitted WHOLE via
//    onpickVariant(registerVariant). When `register` (a valid 2-seg prefix) is
//    KNOWN (the user hand-typed it), it jumps straight to that register's variant
//    list. When `register` is EMPTY / not a valid prefix (C2 — catalog→project
//    handoff), it opens in REGISTER-BROWSE mode: provider list → register list →
//    variant list, reusing the catalog DATA LAYER (root + provider node fetches) and
//    the same rankFilter. Either way the emitted value is the full
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
// kept within each tier. Command's own scoring is disabled — these pre-ranked
// arrays ARE the listbox content (see `shouldFilter={false}` below).
let filter = $state("");
const filteredVariants = $derived(
  rankFilter(variantList, filter, (v) => [v.slug, v.name]),
);
// Variable rows are group-folded (#322): a group row matches/ranks on its
// label/key + every member's name/FQID (`groupFilterKeys` — the same match set
// the browse uses), so target-hunting a member ("maj") still surfaces the
// family that folded it.
const filteredVariables = $derived(
  rankFilter(variableRows, filter, (row) =>
    row.kind === "group"
      ? groupFilterKeys(row.group)
      : [row.item.fqid, row.item.name],
  ),
);
// C2: the browse-step lists, same rankFilter (target-hunt a provider/register).
const filteredProviders = $derived(
  rankFilter(browseProviders, filter, (p) => [
    leafSlug(p.fqid),
    p.fqid,
    p.name,
  ]),
);
const filteredRegisters = $derived(
  rankFilter(browseRegisters, filter, (r) => [
    leafSlug(r.fqid),
    r.fqid,
    r.name,
  ]),
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

<!--
  The Command list snippet — the shared flat-list shell for the variant / provider
  / register lists (and, below, the variable leaf rows). `shouldFilter={false}`:
  the picker pre-ranks with rankFilter and feeds the survivors as the items, so the
  listbox content IS the ranked array (Command would otherwise re-score + re-order
  by its own algorithm, dropping the tiered target-hunt). `label` gives the input
  its accessible name (Bits UI renders a visually-hidden <label> the combobox
  points to via aria-labelledby). `total`/`shown` drive the aria-live count, shown
  only while filtering (matching the old FilterInput).
-->
{#snippet commandList(
  label: string,
  placeholder: string,
  total: number,
  shown: number,
  empty: string,
  body: import("svelte").Snippet,
  explicitEmptyCount?: number,
)}
  <Command.Root shouldFilter={false} {label} class="cmd">
    <div class="cmd-head">
      <!-- `filter` binds to the INPUT's search value (NOT Command.Root's `value`,
           which is the SELECTED item) — it's the needle rankFilter scores against. -->
      <Command.Input
        autofocus
        bind:value={filter}
        {placeholder}
        autocomplete="off"
        class="cmd-input"
      />
      {#if filter.trim().length > 0}
        <span class="cmd-count" aria-live="polite">{shown} of {total}</span>
      {/if}
    </div>
    <Command.List class="cmd-list">
      <Command.Viewport>
        {@render body()}
        <!-- Empty-state: Command.Empty counts only registered Command.Items, which
             is WRONG for the variable list — its folded ConceptGroupRow rows are
             role="presentation", NOT Command.Items, so a filter that survives only
             group rows (or a register with zero ungrouped leaves) leaves the item
             count at 0 and Command.Empty would show "No variables match" beside a
             VISIBLE group. So the variable list passes `explicitEmptyCount` (the
             TRUE filtered count) and we gate the message on that. The homogeneous
             variant/provider/register lists have no group rows → their item count
             IS the filtered count, so Command.Empty is correct (and cheaper). -->
        {#if explicitEmptyCount !== undefined}
          {#if explicitEmptyCount === 0}
            <p class="cmd-empty">{empty}</p>
          {/if}
        {:else}
          <Command.Empty class="cmd-empty">{empty}</Command.Empty>
        {/if}
      </Command.Viewport>
    </Command.List>
  </Command.Root>
{/snippet}

<!-- One flat pick row (variant / provider / register). The Command.Item value is
     the row's stable key; selecting it (click or Enter on the active row) fires
     `onSelect`. `keywords` would feed Command's scorer, but scoring is off — the
     pre-ranked array already decides membership, so no keywords needed. -->
{#snippet pickRow(value: string, onSelect: () => void, primary: string, secondary: string | undefined, secondaryClass: string)}
  <Command.Item {value} {onSelect} class="pick">
    <span class="slug">{primary}</span>
    {#if secondary}<span class={secondaryClass}>{secondary}</span>{/if}
  </Command.Item>
{/snippet}

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
      {@render commandList(
        "Filter variants",
        "Filter variants…",
        variantList.length,
        filteredVariants.length,
        `No variants match “${filter}”.`,
        variantRows,
      )}
    {:else}
      <p class="muted">No variants for this register.</p>
    {/if}
  {:else if props.mode === "variant" && browsedProvider}
    <!-- C2: register-browse step 2 — the chosen provider's registers. -->
    {#if browseRegisters.length > 0}
      {@render commandList(
        "Filter registers",
        "Filter registers…",
        browseRegisters.length,
        filteredRegisters.length,
        `No registers match “${filter}”.`,
        registerRows,
      )}
    {:else}
      <p class="muted">No registers for this provider.</p>
    {/if}
  {:else if props.mode === "variant"}
    <!-- C2: register-browse step 1 — the provider list (catalog root). -->
    {#if browseProviders.length > 0}
      {@render commandList(
        "Filter providers",
        "Filter providers…",
        browseProviders.length,
        filteredProviders.length,
        `No providers match “${filter}”.`,
        providerRows,
      )}
    {:else}
      <p class="muted">No providers.</p>
    {/if}
  {:else if registerNode && registerNode.kind === "register"}
    {#if variableChildren.length > 0}
      <!-- Counts stay in VARIABLE units after folding (#322):
           countFoldedMembers expands group rows to their member counts. -->
      {@render commandList(
        "Filter variables",
        "Filter variables…",
        variableChildren.length,
        countFoldedMembers(filteredVariables),
        `No variables match “${filter}”.`,
        variableRowsBody,
        // The TRUE filtered count (leaves + folded group rows) — NOT Command's
        // item count, which excludes the role="presentation" group rows (#2).
        filteredVariables.length,
      )}
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
            <button type="button" class="pick chooser-pick primary" onclick={() => chooseRepresentation(pendingReps[0])}>
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
                <button type="button" class="pick chooser-pick" onclick={() => chooseRepresentation(rep)}>
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
              <button type="button" class="pick chooser-pick" onclick={() => chooseRepresentation(rep)}>
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

<!-- ── The per-list option bodies (rendered inside commandList's Viewport) ──────
     Each is a {#each} over the pre-ranked filtered array of Command.Item rows. -->
{#snippet variantRows()}
  {#each filteredVariants as variant (variant.slug)}
    {@render pickRow(variant.slug, () => emitVariant(variant.slug), variant.slug, variant.name ?? undefined, "name")}
  {/each}
{/snippet}

{#snippet registerRows()}
  {#each filteredRegisters as register (register.fqid)}
    {@render pickRow(register.fqid, () => browseRegister(register.fqid), register.name ?? register.fqid, register.fqid, "leaf-fqid")}
  {/each}
{/snippet}

{#snippet providerRows()}
  {#each filteredProviders as provider (provider.fqid)}
    {@render pickRow(provider.fqid, () => browseProvider(provider.fqid), provider.name ?? provider.fqid, provider.fqid, "leaf-fqid")}
  {/each}
{/snippet}

<!--
  THE FRICTION (#689 headline evidence). The variable list mixes flat leaf rows
  with FOLDED group rows (#322). Leaf rows fit Command.Item cleanly (role="option",
  arrow-navigable, Enter selects). A folded group is a NESTED expandable widget
  (ConceptGroupRow: a <details> → facet matrix / chips / member list, whose members
  are themselves pick buttons) and does NOT fit Command's flat-option model:
    • Command.Item is a single selectable option; an option that instead toggles a
      disclosure, and whose expansion contains its OWN interactive controls, is
      invalid ARIA (interactive descendants inside role="option") and fights
      Command's Enter handler, which `.click()`s the active option.
    • Command's getValidItems()/arrow-nav indexes every registered item, so a group
      "option" would be a focus stop that does nothing selectable.
  Escape hatch: render the group row as a PLAIN element directly in the Viewport
  (NOT a Command.Item), wrapped in role="presentation" so it sits in the listbox
  DOM without claiming an option role. The leaf rows remain the only navigable
  options; the group keeps its own <details>/button keyboard semantics, just
  outside Command's option model. Counts/ranking still fold it (it's in
  `filteredVariables`), so the visual list is unchanged — only the keyboard model
  splits: arrow-nav covers leaves, the group is reached by Tab into its summary.
-->
{#snippet variableRowsBody()}
  {#each filteredVariables as row (row.kind === "group" ? row.group.key : row.item.fqid)}
    {#if row.kind === "group"}
      <div class="group-row-wrap" role="presentation">
        <ConceptGroupRow
          group={row.group}
          disabled={resolving}
          showGroupKey={true}
          onpick={(fqid) => void pickVariable(fqid)}
        />
      </div>
    {:else}
      <Command.Item
        value={row.item.fqid}
        disabled={resolving}
        onSelect={() => void pickVariable(row.item.fqid)}
        class="pick"
      >
        <span class="slug">{row.item.name ?? row.item.fqid}</span>
        <code class="leaf-fqid">{row.item.fqid}</code>
      </Command.Item>
    {/if}
  {/each}
{/snippet}

<style>
  .picker {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-3) var(--space-3);
    margin-top: var(--space-2);
    background: var(--surface);
  }
  .picker-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: var(--space-2);
  }
  .picker-title {
    font-size: var(--text-sm);
    font-weight: 600;
  }
  .cancel {
    font: inherit;
    font-size: var(--text-sm);
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--surface);
    cursor: pointer;
  }
  .back {
    font: inherit;
    font-size: var(--text-sm);
    padding: var(--space-1) 0;
    margin-bottom: var(--space-2);
    border: none;
    background: transparent;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
  }

  /* ── Bits UI Command surfaces ────────────────────────────────────────────
     The Command primitive renders its own DOM (an application-role wrapper, the
     combobox input, a listbox). We pass token-styled classes through; the
     `:global` rules below the scoped ones target the option role-state attrs
     (data-selected / data-disabled) Bits UI sets, which scoped CSS can't reach
     on primitive-rendered nodes. */
  .cmd-head {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    margin-bottom: var(--space-2);
  }
  :global(.cmd-input) {
    flex: 1;
    font: inherit;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--surface);
  }
  :global(.cmd-input:focus) {
    outline: none;
    border-color: var(--accent);
    box-shadow: var(--focus-ring);
  }
  .cmd-count {
    color: var(--text-muted);
    font-size: var(--text-sm);
    white-space: nowrap;
  }
  :global(.cmd-list) {
    max-height: 18rem;
    overflow-y: auto;
  }
  :global(.cmd-empty) {
    color: var(--text-muted);
    padding: var(--space-2);
  }

  /* The flat pick rows (Command.Item, role="option"). The keyboard-active row is
     marked data-selected by Bits UI; style it like the old :hover so arrow-nav and
     pointer read the same. */
  /* CONFINED to this component's subtree: `.pick` is a class Bits UI forwards onto
     a Command.Item primitive element Svelte's scoping can't hash, so the rule must
     be `:global`. But an UNSCOPED `:global(.pick)` leaks into the OTHER components
     that define their own scoped `.pick` buttons (BindingLeafView's rep-chooser),
     a build-order-dependent specificity battle on hover/selected. Prefixing with
     `.picker` (which IS in this component's markup → Svelte hashes it) keeps `.pick`
     global but matches only inside THIS picker's container (#689 review #3). */
  .picker :global(.pick) {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    width: 100%;
    text-align: left;
    font: inherit;
    padding: var(--space-1) var(--space-2);
    border: 1px solid transparent;
    border-radius: var(--radius);
    background: transparent;
    cursor: pointer;
  }
  .picker :global(.pick:hover:not([data-disabled])),
  .picker :global(.pick[data-selected]:not([data-disabled])) {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .picker :global(.pick[data-disabled]) {
    opacity: 0.5;
    cursor: not-allowed;
  }
  /* The folded-group wrapper is a non-option row — strip the option padding so the
     nested ConceptGroupRow controls its own layout. */
  :global(.group-row-wrap) {
    padding: var(--space-1) 0;
  }

  /* The slug / name / fqid / classification spans are authored in THIS component
     (the pickRow + leaf-variable snippets + the chooser), so they carry the scope
     hash even when nested inside a Command.Item's primitive-rendered div — one
     scoped rule covers both the option rows and the chooser. */
  .slug {
    font-weight: 600;
  }
  .leaf-fqid,
  .name,
  .classification {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .hint {
    font-size: var(--text-sm);
    margin: 0 0 var(--space-2);
  }
  .resolve-state {
    font-size: var(--text-sm);
    margin: var(--space-2) 0 0;
  }
  .chooser {
    margin-top: var(--space-2);
    padding-top: var(--space-2);
    border-top: 1px solid var(--border);
  }
  .chooser-title {
    font-size: var(--text-sm);
    margin: 0 0 var(--space-2);
  }
  /* The representation chooser keeps plain <button> rows (a small, static, mutually
     exclusive set — not a filterable list, so no Command primitive). */
  .pick-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  .chooser-pick {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    width: 100%;
    text-align: left;
    font: inherit;
    padding: var(--space-1) var(--space-2);
    border: 1px solid transparent;
    border-radius: var(--radius);
    background: transparent;
    cursor: pointer;
  }
  .chooser-pick:hover {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .chooser-pick.primary {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .reveal {
    font: inherit;
    font-size: var(--text-sm);
    margin-top: var(--space-2);
    padding: var(--space-1) 0;
    border: none;
    background: transparent;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
  }
  .pick-list.alternates {
    margin-top: var(--space-1);
  }
</style>
