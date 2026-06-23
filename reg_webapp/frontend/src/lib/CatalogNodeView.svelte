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
  DATA_BROWSER_LABEL,
  foldGroupedRows,
  groupFilterKeys,
  groupHref,
  leafSlug,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
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
// navigation so a new node opens unfiltered. `rankFilter` matches on the leaf
// slug + display name + FQID (registers also match their purpose blurb) and,
// under an active filter, RANKS the survivors (exact → prefix → other-substring)
// so a slug-named target jumps above a purpose-blurb-only match (#674); an empty
// needle leaves the incoming alphabetical order untouched. matchesFilter folds
// diacritics.
let filter = $state("");
$effect(() => {
  // `fqidPath` is the navigation key — touching it here clears the filter when
  // the route changes (the component is reused across catalog paths).
  void fqidPath;
  filter = "";
});
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">{DATA_BROWSER_LABEL}</a>
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
      {@const registers = rankFilter(node.children, filter, (r) => [
        leafSlug(r.fqid),
        r.name,
        r.fqid,
        r.purpose,
      ])}
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
          <ul class="children table">
            {#each registers as register (register.fqid)}
              <li>
                <a href={catalogHref(register.fqid)} title={register.fqid}>
                  <span class="label">{register.name ?? register.fqid}</span>
                  {#if register.purpose}
                    <span class="purpose muted">{register.purpose}</span>
                  {/if}
                  <code class="child-fqid">{register.fqid}</code>
                </a>
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
      {@const filteredRows = rankFilter(rows, filter, (row) =>
        row.kind === "group"
          ? groupFilterKeys(row.group)
          : [leafSlug(row.item.fqid), row.item.fqid, row.item.name],
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
          <ul class="children table">
            {#each filteredRows as row (row.kind === "group" ? row.group.key : row.item.fqid)}
              {#if row.kind === "group"}
                <!-- #673 (M6): a group row in the register arm LINKS to its
                     subject page (register-only route) instead of expanding
                     inline. The group row is a self-contained widget — it spans
                     the table's columns (`.group-row`) and owns its own layout. -->
                <li class="group-row">
                  <ConceptGroupRow
                    group={row.group}
                    noun="variables"
                    href={groupHref(node.fqid, row.group.key)}
                  />
                </li>
              {:else}
                <li>
                  <a href={catalogHref(row.item.fqid)} title={row.item.fqid}>
                    <span class="label">{row.item.name ?? row.item.fqid}</span>
                    <code class="child-fqid">{row.item.fqid}</code>
                  </a>
                </li>
              {/if}
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
        <ul class="children table">
          {#each clsRows as row (row.kind === "group" ? row.group.key : row.item.fqid)}
            {#if row.kind === "group"}
              <!-- #673: NO href — the classification-umbrella group has no
                   register-only subject page, so it KEEPS the inline <details>.
                   Spans the table columns + owns its layout (`.group-row`). -->
              <li class="group-row">
                <ConceptGroupRow
                  group={row.group}
                  noun={axisNoun(row.group.axes)}
                />
              </li>
            {:else}
              <li>
                <a href={catalogHref(row.item.fqid)} title={row.item.short_name}>
                  <span class="label">{row.item.name}</span>
                  <code class="child-fqid">{row.item.short_name}</code>
                </a>
              </li>
            {/if}
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
  }
  /* #673 (M5): a compact, column-aligned, scannable table. Rows are tight
     (one text line each); the name forms a scannable left column and the
     secondary info (purpose, fqid) aligns in its own column. A CSS grid on the
     <ul> aligns columns ACROSS rows without per-row width guessing — one link
     per row (the <li>/<a> are display:contents so the grid tracks the <a>'s
     cells, keeping exactly one interactive element per row, no nested links). */
  .children.table {
    display: grid;
    /* name (max-content, capped) · purpose (flexes, hidden when absent) · fqid. */
    grid-template-columns: minmax(auto, max-content) 1fr auto;
    row-gap: var(--space-1);
    column-gap: var(--space-3);
    align-items: baseline;
  }
  /* A LEAF row's <li>/<a> dissolve into the grid (display:contents) so the <a>'s
     three children become the row's grid cells — one link per row, no nesting. */
  .children.table li:not(.group-row),
  .children.table li:not(.group-row) > a {
    display: contents;
  }
  .children.table li:not(.group-row) > a > * {
    /* Tighten each cell to a single line; truncate the flexible purpose column. */
    min-width: 0;
    padding: var(--space-1) 0;
  }
  /* A GROUP row is a self-contained widget (ConceptGroupRow: link or <details>):
     span the full table width and let it own its internal layout. */
  .children.table li.group-row {
    grid-column: 1 / -1;
    padding: var(--space-1) 0;
  }
  .children .label {
    font-weight: 600;
  }
  /* The purpose blurb (provider arm) sits in the flexible middle column, muted
     and clamped to one line so the table stays scannable (the row links to the
     register page where the full purpose renders). */
  .purpose {
    grid-column: 2;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: var(--text-sm);
  }
  /* #673 (M3): de-emphasize the fqid/short_name — the NAME is primary. The fqid
     lives in the right column, muted and hidden by default, REVEALED on row
     hover AND keyboard focus (:focus-within covers keyboard users). It stays in
     the DOM (discoverable/greppable) and on the link's `title` (tooltip + AX). */
  .child-fqid {
    grid-column: 3;
    color: var(--muted);
    font-size: var(--text-sm);
    text-align: right;
    visibility: hidden;
  }
  .children.table li:not(.group-row) > a:hover .child-fqid,
  .children.table li:not(.group-row) > a:focus-within .child-fqid {
    visibility: visible;
  }
</style>
