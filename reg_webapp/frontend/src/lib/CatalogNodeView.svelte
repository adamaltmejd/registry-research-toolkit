<script lang="ts">
import { getCatalogNode, isCatalogNode } from "./api";
import { asyncResource } from "./async.svelte";
import BindingLeafView from "./BindingLeafView.svelte";
import ClassificationLeafView from "./ClassificationLeafView.svelte";
import ConceptGroupRow from "./ConceptGroupRow.svelte";
import {
  axisNoun,
  bindingChildren,
  breadcrumbs,
  catalogHref,
  countFoldedMembers,
  foldGroupedRows,
  groupMatchesFilter,
  matchesFilter,
  narrowCatalogNode,
  nodeLabel,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";
import VariantBrowser from "./VariantBrowser.svelte";

// Fetches and renders one catalog node by FQID path, switching on the `kind`
// discriminator. The provider/register/classification browse fetch is a plain
// (no-query) resolve; a binding leaf delegates to `BindingLeafView`, which owns
// the period/variant resolution + states + lineage (A5.3b). The browse fetch
// here never passes `?period`, so this catch-all response is always a `kind`-
// tagged node (the `StatesResponse` arm — a no-`kind` resolve_at subset — is
// only reachable WITH a query, so it's filtered to `null` and never rendered).
let {
  fqidPath,
  regMetaVersion,
  steward,
  vintageYear,
}: {
  fqidPath: string;
  // C1: the deployment seed, threaded to BindingLeafView's "Add to project" so a
  // pristine store can implicitly create the project (App → here → BindingLeafView).
  regMetaVersion: string;
  steward: string;
  // #631: the catalog VINTAGE year (App derives it from context.reg_meta.import_date,
  // same value the header window slider caps at). Threaded to BindingLeafView's
  // period picker so the local slider's open-ended ceiling matches the header — not
  // wall-clock. Same App→here→BindingLeafView prop-drill as the deployment seed.
  vintageYear: number;
} = $props();

const resource = asyncResource(() => getCatalogNode(fqidPath));
// A browsable path resolves to a `kind`-tagged CatalogNode. A SUB-ENDPOINT path
// (e.g. a deep-link to `.../states` or `.../variants`) hits that endpoint and
// returns a no-`kind` StatesResponse/VariantsResponse — narrow it OUT of `node`
// (so the kind-switch type-checks) and flag it as `notBrowsable` so we render a
// clear message instead of a blank page.
const node = $derived(narrowCatalogNode(resource.data));
const notBrowsable = $derived(
  resource.data !== null && !isCatalogNode(resource.data),
);
const crumbs = $derived(breadcrumbs(fqidPath));

// In-memory type-to-filter over the current node's child list (a provider's 238
// registers / a register's 740 bindings render flat otherwise). Reset on
// navigation so a new node opens unfiltered. Match on BOTH display name and FQID
// (registers also match their purpose blurb); matchesFilter folds diacritics.
let filter = $state("");
$effect(() => {
  // `fqidPath` is the navigation key — touching it here clears the filter when
  // the route changes (the component is reused across catalog paths).
  void fqidPath;
  filter = "";
});
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">catalog</a>
  {#each crumbs as crumb (crumb.fqidPath)}
    <span class="sep" aria-hidden="true">/</span>
    <a href={catalogHref(crumb.fqidPath)}>{crumb.label}</a>
  {/each}
</nav>

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: <code>{fqidPath}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
{:else if node}
  <article>
    {#if node.kind === "provider"}
      {@const registers = node.children.filter((r) =>
        matchesFilter(filter, r.name, r.fqid, r.purpose),
      )}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      <h3>Registers</h3>
      {#if node.children.length > 0}
        <FilterInput
          bind:value={filter}
          total={node.children.length}
          shown={registers.length}
          placeholder="Filter registers…"
          label="Filter registers"
        />
        {#if registers.length > 0}
          <ul class="children">
            {#each registers as register (register.fqid)}
              <li>
                <a href={catalogHref(register.fqid)}>
                  <span class="label">{register.name ?? register.fqid}</span>
                  <code class="child-fqid">{register.fqid}</code>
                </a>
                {#if register.purpose}<p class="muted">{register.purpose}</p>{/if}
              </li>
            {/each}
          </ul>
        {:else}
          <p class="muted">No registers match “{filter}”.</p>
        {/if}
      {:else}
        <p class="muted">No registers.</p>
      {/if}
    {:else if node.kind === "register"}
      <!-- #303 concept-group folding: grouped bindings render as one expandable
           group row (ConceptGroupRow); ungrouped bindings stay leaf rows. The
           flat `children` list is complete — `foldGroupedRows` hides members. -->
      {@const rows = foldGroupedRows(bindingChildren(node), node.groups)}
      {@const filteredRows = rows.filter((row) =>
        row.kind === "group"
          ? groupMatchesFilter(filter, row.group)
          : matchesFilter(filter, row.item.name, row.item.fqid),
      )}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      {#if node.purpose}<p>{node.purpose}</p>{/if}
      <!-- "Variables" is the researcher-facing label for this list; the code/API
           term is "binding" (the addressable variable leaf) — display copy only. -->
      <h3>Variables</h3>
      {#if rows.length > 0}
        <!-- Counts stay in VARIABLE units after folding (a group row counts its
             members), so the "x of y" readout still reflects register size. -->
        <FilterInput
          bind:value={filter}
          total={countFoldedMembers(rows)}
          shown={countFoldedMembers(filteredRows)}
          placeholder="Filter variables…"
          label="Filter variables"
        />
        {#if filteredRows.length > 0}
          <ul class="children">
            {#each filteredRows as row (row.kind === "group" ? row.group.key : row.item.fqid)}
              <li>
                {#if row.kind === "group"}
                  <ConceptGroupRow group={row.group} noun="variables" />
                {:else}
                  <a href={catalogHref(row.item.fqid)}>
                    <span class="label">{row.item.name ?? row.item.fqid}</span>
                    <code class="child-fqid">{row.item.fqid}</code>
                  </a>
                {/if}
              </li>
            {/each}
          </ul>
        {:else}
          <p class="muted">No variables match “{filter}”.</p>
        {/if}
      {:else}
        <p class="muted">No variables.</p>
      {/if}
      <VariantBrowser registerFqid={node.fqid} />
    {:else if node.kind === "binding"}
      <!-- Pass the full node down: this no-query browse fetch already resolved
           the variable's metadata + embedded edges + default states. BindingLeafView
           renders those from `node` (always present — so a cold deep-link with
           `?period` isn't blank) and fetches only the period-NARROWED states from
           the URL query, reactive without a remount. -->
      <BindingLeafView {fqidPath} {node} {regMetaVersion} {steward} {vintageYear} />
    {:else if node.kind === "classification-root"}
      <!-- #516 umbrella folding: e.g. group:sun renders as ONE group row
           expanding to its dimension members; ungrouped classifications stay
           leaves. Children are terminal editions only (the backend drops
           superseded ones) — superseded editions are reached via a leaf's
           edition-chain panel. -->
      {@const clsRows = foldGroupedRows(node.children, node.groups)}
      <h2>{nodeLabel(node)}</h2>
      <h3>Classifications</h3>
      {#if clsRows.length > 0}
        <ul class="children">
          {#each clsRows as row (row.kind === "group" ? row.group.key : row.item.fqid)}
            <li>
              {#if row.kind === "group"}
                <ConceptGroupRow
                  group={row.group}
                  noun={axisNoun(row.group.axes)}
                />
              {:else}
                <a href={catalogHref(row.item.fqid)}>
                  <span class="label">{row.item.name}</span>
                  <code class="child-fqid">{row.item.short_name}</code>
                </a>
              {/if}
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No classifications.</p>
      {/if}
    {:else if node.kind === "classification"}
      <!-- #638 PR1: the classification leaf renders through the unified SubjectView
           shell, same as the binding leaf + concept group. -->
      <ClassificationLeafView {node} />
    {/if}
  </article>
{:else if notBrowsable}
  <!-- A no-`kind` response: a deep-link to a SUB-ENDPOINT path (e.g.
       `.../states`, `.../variants`) hits that endpoint and returns a
       StatesResponse/VariantsResponse, not a browsable node. Render a clear
       message instead of a blank page. -->
  <p class="error" role="alert">
    <code>{fqidPath}</code> isn't a browsable catalog node.
  </p>
{/if}

<style>
  .breadcrumbs {
    font-size: 0.9rem;
    margin-bottom: 1rem;
  }
  .breadcrumbs .sep {
    color: var(--muted);
    margin: 0 0.25rem;
  }
  .fqid {
    margin-top: -0.25rem;
    color: var(--muted);
  }
  .children {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .children li a {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .children .label {
    font-weight: 600;
  }
  .child-fqid {
    color: var(--muted);
    font-size: 0.85em;
  }
</style>
