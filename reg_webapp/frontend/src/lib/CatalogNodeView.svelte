<script lang="ts">
import { getCatalogNode, isCatalogNode } from "./api";
import { asyncResource } from "./async.svelte";
import BindingLeafView from "./BindingLeafView.svelte";
import ClassificationLeafView from "./ClassificationLeafView.svelte";
import ConceptGroupRow from "./ConceptGroupRow.svelte";
import {
  axisNoun,
  bindingChildren,
  catalogHref,
  classGroupHref,
  countFoldedMembers,
  foldGroupedRows,
  groupFilterKeys,
  groupHref,
  leafSlug,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";
import { type Column, DataTable, EmptyState } from "./ui";
import VariantBrowser from "./VariantBrowser.svelte";

// The provider arm renders its register list as a real DataTable: a Register
// (name → catalog link) column and a Description (the purpose blurb, 2-line
// clamped) column. The cell content is custom (a link / a clamped blurb), so the
// `cell` escape hatch owns rendering; the column `key`s just index the row.
type RegisterRow = {
  fqid: string;
  name?: string | null;
  purpose?: string | null;
};
const registerColumns: Column<RegisterRow>[] = [
  { key: "name", label: "Register" },
  { key: "purpose", label: "Description" },
];

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
      <h3 class="section-eyebrow">Registers</h3>
      {#if node.children.length > 0}
        <FilterInput
          bind:value={filter}
          total={node.children.length}
          shown={registers.length}
          placeholder="Filter registers…"
          label="Filter registers"
        />
        {#if registers.length > 0}
          <DataTable columns={registerColumns} rows={registers}>
            {#snippet cell(register, column)}
              {#if column.key === "name"}
                <a class="row-link" href={catalogHref(register.fqid)} title={register.fqid}>
                  {register.name ?? register.fqid}
                </a>
              {:else if register.purpose}
                <span class="clamp-2">{register.purpose}</span>
              {/if}
            {/snippet}
          </DataTable>
        {:else}
          <EmptyState title={`No registers match “${filter}”`} />
        {/if}
      {:else}
        <EmptyState title="No registers." />
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
      {#if node.purpose}<p class="purpose-text">{node.purpose}</p>{/if}
      <!-- "Variables" is the researcher-facing label for this list; the code/API
           term is "binding" (the addressable variable leaf) — display copy only. -->
      <h3 class="section-eyebrow">Variables</h3>
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
          <ul class="children table single">
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
                  </a>
                </li>
              {/if}
            {/each}
          </ul>
        {:else}
          <EmptyState title={`No variables match “${filter}”`} />
        {/if}
      {:else}
        <EmptyState title="No variables." />
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
      <h3 class="section-eyebrow">Classifications</h3>
      {#if clsRows.length > 0}
        <ul class="children table">
          {#each clsRows as row (row.kind === "group" ? row.group.key : row.item.fqid)}
            {#if row.kind === "group"}
              <!-- #756: the classification-umbrella group now LINKS to its own
                   first-class subject page (`classGroupHref`), like the register
                   groups link to theirs (#673) — the row renders as a link, not the
                   inline <details>. Spans the table columns + owns its layout
                   (`.group-row`). -->
              <li class="group-row">
                <ConceptGroupRow
                  group={row.group}
                  noun={axisNoun(row.group.axes)}
                  href={classGroupHref(row.group.key)}
                />
              </li>
            {:else}
              <li>
                <a href={catalogHref(row.item.fqid)} title={row.item.short_name}>
                  <span class="label">{row.item.name}</span>
                  <code class="short-name">{row.item.short_name}</code>
                </a>
              </li>
            {/if}
          {/each}
        </ul>
      {:else}
        <EmptyState title="No classifications." />
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
  /* Tracked uppercase section eyebrow — the same micro-label hierarchy device
     Panel/DataTable headers use. It differentiates the list LEVEL (registers
     under a provider vs variables under a register vs classifications). */
  .section-eyebrow {
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
    margin: var(--space-4) 0 var(--space-2);
  }
  /* The register's own subject text (register arm). */
  .purpose-text {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* A register-list / provider-list name link — the NAME is primary. */
  .row-link {
    font-weight: 600;
    /* Long Swedish compound words otherwise force a min-content width past the
       375px mobile canvas (#806); break them only when they can't fit. */
    overflow-wrap: anywhere;
  }
  /* Clamp a register's description to ~2 lines in the DataTable cell; the full
     text lives on the register's own subject page. */
  .clamp-2 {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .children {
    list-style: none;
    padding: 0;
    margin: 0;
  }
  /* #673 (M5): a compact, column-aligned, scannable list. Rows are tight (one
     text line each); the name forms a scannable left column, a secondary label
     (a classification's short_name) aligns in its own column. A CSS grid on the
     <ul> aligns columns ACROSS rows without per-row width guessing — one link
     per row (the <li>/<a> are display:contents so the grid tracks the <a>'s
     cells, keeping exactly one interactive element per row, no nested links).
     The variable list (register arm) has only the name column; the
     classification list adds the short_name column. */
  .children.table {
    display: grid;
    grid-template-columns: minmax(auto, max-content) auto;
    row-gap: var(--space-1);
    column-gap: var(--space-3);
    align-items: baseline;
  }
  /* The variable arm (register) has only a name column, so a leaf contributes a
     single grid item. Force one column — otherwise CSS auto-placement packs two
     consecutive ungrouped variables into one row's two tracks. */
  .children.table.single {
    grid-template-columns: minmax(0, 1fr);
  }
  /* A LEAF row's <li>/<a> dissolve into the grid (display:contents) so the <a>'s
     children become the row's grid cells — one link per row, no nesting. */
  .children.table li:not(.group-row),
  .children.table li:not(.group-row) > a {
    display: contents;
  }
  .children.table li:not(.group-row) > a > * {
    min-width: 0;
    padding: var(--space-1) 0;
  }
  /* A GROUP row is a self-contained widget (ConceptGroupRow: link or <details>):
     span the full list width and let it own its internal layout. */
  .children.table li.group-row {
    grid-column: 1 / -1;
    padding: var(--space-1) 0;
  }
  .children .label {
    font-weight: 600;
    /* Break long compound variable/classification names rather than overflow the
       mobile canvas (#806). */
    overflow-wrap: anywhere;
  }
  /* A classification's short_name is a meaningful human classification code (not
     a raw FQID), so it stays VISIBLE as a muted secondary label in column 2. */
  .short-name {
    grid-column: 2;
    color: var(--text-muted);
    font-size: var(--text-sm);
    text-align: right;
  }
</style>
