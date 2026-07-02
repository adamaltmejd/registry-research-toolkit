<script lang="ts">
import {
  type BindingChild,
  type ClassificationNodeData,
  type ConceptGroup,
  getCatalogNode,
  isCatalogNode,
} from "./api";
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
  type GroupedRow,
  groupFilterKeys,
  groupHref,
  leafSlug,
  narrowCatalogNode,
  nodeLabel,
  rankFilter,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";
import RelatedDocumentsPanel from "./RelatedDocumentsPanel.svelte";
import { type Column, DataTable, EmptyState, Panel, Tag } from "./ui";
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

type VariableBrowseRow =
  | {
      id: string;
      kind: "group";
      group: ConceptGroup;
      label: string;
      href: string;
    }
  | {
      id: string;
      kind: "leaf";
      fqid: string;
      label: string;
    };

const variableColumns: Column<VariableBrowseRow>[] = [
  { key: "label", label: "Variable" },
];

type ClassificationBrowseRow =
  | {
      id: string;
      kind: "group";
      group: ConceptGroup;
      label: string;
      noun: string;
      href: string;
      shortName: "";
    }
  | {
      id: string;
      kind: "leaf";
      fqid: string;
      label: string;
      shortName: string;
    };

const classificationColumns: Column<ClassificationBrowseRow>[] = [
  { key: "label", label: "Name" },
  { key: "shortName", label: "Short name", align: "end" },
];

function variableBrowseRows(
  rows: GroupedRow<BindingChild>[],
  registerFqid: string,
): VariableBrowseRow[] {
  return rows.map((row) =>
    row.kind === "group"
      ? {
          id: `group:${row.group.key}`,
          kind: "group",
          group: row.group,
          label: row.group.label,
          href: groupHref(registerFqid, row.group.key),
        }
      : {
          id: row.item.fqid,
          kind: "leaf",
          fqid: row.item.fqid,
          label: row.item.name ?? row.item.fqid,
        },
  );
}

function classificationBrowseRows(
  rows: GroupedRow<ClassificationNodeData>[],
): ClassificationBrowseRow[] {
  return rows.map((row) =>
    row.kind === "group"
      ? {
          id: `group:${row.group.key}`,
          kind: "group",
          group: row.group,
          label: row.group.label,
          noun: axisNoun(row.group.axes),
          href: classGroupHref(row.group.key),
          shortName: "",
        }
      : {
          id: row.item.fqid,
          kind: "leaf",
          fqid: row.item.fqid,
          label: row.item.name,
          shortName: row.item.short_name,
        },
  );
}

function browseRowId(row: { id: string }): string {
  return row.id;
}

function registerRowId(row: RegisterRow): string {
  return row.fqid;
}

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
      {#if node.children.length > 0}
        <FilterInput
          bind:value={filter}
          total={node.children.length}
          shown={registers.length}
          placeholder="Filter registers…"
          label="Filter registers"
        />
        {#if registers.length > 0}
          <Panel title="Registers" flush>
            <DataTable columns={registerColumns} rows={registers} getRowId={registerRowId}>
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
          </Panel>
        {:else}
          <Panel title="Registers">
            <EmptyState title={`No registers match “${filter}”`} />
          </Panel>
        {/if}
      {:else}
        <Panel title="Registers">
          <EmptyState title="No registers." />
        </Panel>
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
      {#if node.tags && node.tags.length > 0}
        <div class="tag-strip" aria-label="Thematic tags">
          {#each node.tags as tag (tag.slug)}
            <Tag tone="neutral">{tag.label}</Tag>
          {/each}
        </div>
      {/if}
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
          {@const variableRows = variableBrowseRows(filteredRows, node.fqid)}
          <Panel title="Variables" flush>
            <DataTable
              columns={variableColumns}
              rows={variableRows}
              getRowId={browseRowId}
            >
              {#snippet cell(row)}
                {#if row.kind === "group"}
                  <!-- #673 (M6): register-arm group rows link to their subject page.
                       Browse-link rows omit the slug pill; picker disclosure rows keep it. -->
                  <ConceptGroupRow
                    group={row.group}
                    noun="variables"
                    href={row.href}
                    showGroupKey={false}
                  />
                {:else}
                  <a class="row-link" href={catalogHref(row.fqid)} title={row.fqid}>
                    {row.label}
                  </a>
                {/if}
              {/snippet}
            </DataTable>
          </Panel>
        {:else}
          <Panel title="Variables">
            <EmptyState title={`No variables match “${filter}”`} />
          </Panel>
        {/if}
      {:else}
        <Panel title="Variables">
          <EmptyState title="No variables." />
        </Panel>
      {/if}
      <VariantBrowser registerFqid={node.fqid} />
      <RelatedDocumentsPanel register={leafSlug(node.fqid)} />
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
      {#if clsRows.length > 0}
        {@const classificationRows = classificationBrowseRows(clsRows)}
        <div class="classification-table">
          <DataTable
            columns={classificationColumns}
            rows={classificationRows}
            getRowId={browseRowId}
          >
            {#snippet cell(row, column)}
              {#if column.key === "label"}
                {#if row.kind === "group"}
                  <!-- #756: classification-umbrella groups link to their subject page.
                       Browse-link rows omit the slug pill; picker disclosure rows keep it. -->
                  <ConceptGroupRow
                    group={row.group}
                    noun={row.noun}
                    href={row.href}
                    showGroupKey={false}
                  />
                {:else}
                  <a class="row-link" href={catalogHref(row.fqid)} title={row.shortName}>
                    {row.label}
                  </a>
                {/if}
              {:else if row.kind === "leaf"}
                <code class="short-name">{row.shortName}</code>
              {/if}
            {/snippet}
          </DataTable>
        </div>
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
  /* The register's own subject text (register arm). */
  .purpose-text {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .tag-strip {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    margin: 0.5rem 0 1rem;
  }
  /* Browse-list name links (inside DataTable cells) — the NAME is primary.
     Long-name breaking comes from DataTable's cell-level `overflow-wrap:
     anywhere`, which inherits into these links (#832). */
  .row-link {
    font-weight: 600;
  }
  /* Clamp a register's description to ~2 lines in the DataTable cell; the full
     text lives on the register's own subject page. (Breaking inherits from the
     DataTable cell's `overflow-wrap: anywhere`, #832.) */
  .clamp-2 {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    color: var(--text-muted);
  }
  /* A classification's short_name is a meaningful human classification code (not
     a raw FQID), so it stays VISIBLE as a muted secondary label in column 2. */
  .short-name {
    color: var(--text-muted);
    font-size: var(--text-sm);
    text-align: right;
  }
  .classification-table {
    margin-top: var(--space-3);
  }
  /* The classification root page is an index whose visible heading is already
     "Classifications"; keep DataTable's column semantics for assistive tech but
     remove the visual header row and stacked-card micro-labels here only. */
  .classification-table :global(thead) {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
    border: 0;
  }
  .classification-table :global(td:not(.first)::before) {
    content: none;
  }
</style>
