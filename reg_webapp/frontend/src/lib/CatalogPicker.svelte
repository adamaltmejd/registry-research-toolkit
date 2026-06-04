<script lang="ts" module>
import { type CatalogNode, isCatalogNode, type StatesResponse } from "./api";

// Narrow a catch-all response to a browsable CatalogNode (or null).
function asNode(data: CatalogNode | StatesResponse | null): CatalogNode | null {
  return data !== null && isCatalogNode(data) ? data : null;
}
</script>

<script lang="ts">
import { getCatalogNode, getRegisterVariants, type VariableStateModel } from "./api";
import { asyncResource } from "./async.svelte";
import {
  deriveType,
  type Representation,
  representationsFromStates,
} from "./catalog";

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
// remains canonical (§9.6).
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
  onpickVariable: (picked: {
    variable: string;
    type: string;
    displayNameDefault: string | null;
    // The chosen REPRESENTATION (delivery column) when the concept has >1 at the
    // period; null/undefined when there is a single representation.
    representation?: string | null;
  }) => void;
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
  props.mode === "variable" ? asNode(resource.data as CatalogNode | StatesResponse | null) : null,
);
const variableChildren = $derived(
  registerNode && registerNode.kind === "register"
    ? registerNode.children.filter((c) => c.kind === "binding")
    : [],
);

// The variant-pick list.
const variantList = $derived(
  props.mode === "variant" && resource.data && "variants" in resource.data
    ? resource.data.variants
    : [],
);

// The derive-on-pick resolve state + the REPRESENTATION chooser. A concept can
// carry several co-existing delivery columns (parallel representations: SSYK
// 3/4/5-digit, age brackets) at one period; when it does, the author must pick
// which column the binding extracts (`binding.representation`) — the job the
// retired `@version` pin once did, keyed on the delivery column.
let resolving = $state(false);
let resolveError = $state<string | null>(null);
let pending = $state<{ fqid: string; states: VariableStateModel[] } | null>(null);
const pendingReps = $derived<Representation[]>(
  pending ? representationsFromStates(pending.states) : [],
);

async function pickVariable(fqid: string): Promise<void> {
  if (props.mode !== "variable") {
    return;
  }
  const p = props;
  pending = null;
  // No period → can't resolve. Pick the bare FQID; no prefill (the backend Validate
  // fills / flags type + display_name).
  if (!p.period) {
    p.onpickVariable({ variable: fqid, type: "opaque", displayNameDefault: null });
    return;
  }
  resolving = true;
  resolveError = null;
  try {
    const resolved = await getCatalogNode(fqid, {
      period: p.period,
      variant: p.variant || undefined,
    });
    // Not a StatesResponse (shouldn't happen with `?period`), or no state covers
    // the period → pick the bare FQID with no prefill; the backend flags it.
    if (isCatalogNode(resolved) || resolved.states.length === 0) {
      p.onpickVariable({ variable: fqid, type: "opaque", displayNameDefault: null });
      return;
    }
    // >1 distinct delivery column → multi-representation; defer to the chooser.
    if (representationsFromStates(resolved.states).length > 1) {
      pending = { fqid, states: resolved.states };
      return;
    }
    const first = resolved.states[0];
    p.onpickVariable({
      variable: fqid,
      type: deriveType(first),
      displayNameDefault: first.delivery_column_name ?? null,
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
  const state = pending.states.find((s) => s.delivery_column_name === rep.column);
  props.onpickVariable({
    variable: pending.fqid,
    type: deriveType(state),
    displayNameDefault: rep.column,
    representation: rep.column,
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
    <p class="muted">Loading…</p>
  {:else if resource.error}
    <p class="error" role="alert">Failed to load: {resource.error}</p>
  {:else if props.mode === "variant"}
    {#if variantList.length > 0}
      <ul class="pick-list">
        {#each variantList as variant (variant.slug)}
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
      <p class="muted">No variants for this register.</p>
    {/if}
  {:else if registerNode && registerNode.kind === "register"}
    {#if variableChildren.length > 0}
      <ul class="pick-list">
        {#each variableChildren as child (child.kind === "binding" ? child.fqid : "ref")}
          {#if child.kind === "binding"}
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
          {/if}
        {/each}
      </ul>
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
            </button>
          </li>
        {/each}
      </ul>
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
  .name {
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
</style>
