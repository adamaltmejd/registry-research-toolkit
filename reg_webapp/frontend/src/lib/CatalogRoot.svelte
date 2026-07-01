<script lang="ts">
import { getCatalogRoot } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref, DATA_BROWSER_LABEL, matchesFilter } from "./catalog";
import FilterInput from "./FilterInput.svelte";
import { type Column, DataTable, EmptyState, Panel, Tag } from "./ui";

// The catalog root: every provider plus the classification-root sentinel
// (`class`). Children are a `kind`-tagged union (`provider` | `classification-
// root`); both link via path-based URLs mirroring the API.
const root = asyncResource(() => getCatalogRoot());

// Same type-to-filter affordance as the deeper browse lists, for consistency.
// Match on display name, FQID, row type, and the scope hint.
let filter = $state("");
const children = $derived(root.data?.children ?? []);
type Child = (typeof children)[number];
type RootRow = {
  fqid: string;
  kind: Child["kind"];
  name?: string | null;
  typeLabel: string;
  scope: string;
};

function rootRow(child: Child): RootRow {
  if (child.kind === "classification-root") {
    return {
      fqid: child.fqid,
      kind: child.kind,
      name: child.name,
      typeLabel: "Classification",
      scope: "Catalog-wide classification systems and code lists",
    };
  }
  return {
    fqid: child.fqid,
    kind: child.kind,
    name: child.name,
    typeLabel: "Provider",
    scope: "Provider catalog of registers and variables",
  };
}

const rows = $derived(children.map(rootRow));
const filtered = $derived(
  rows.filter((r) =>
    matchesFilter(filter, r.name, r.fqid, r.typeLabel, r.scope),
  ),
);

// Root rows use the same DataTable treatment as deeper browse pages: the first
// column is the catalog link, the second gives the row grain, and the scope hint
// keeps `/catalog` from reading as a one-column provider list wearing table chrome.
const columns: Column<RootRow>[] = [
  { key: "name", label: "Section" },
  { key: "typeLabel", label: "Type", width: "9.5rem" },
  { key: "scope", label: "Scope" },
];
</script>

<article>
  <h2>{DATA_BROWSER_LABEL}</h2>
  {#if root.loading}
    <p class="muted" aria-busy="true">Loading…</p>
  {:else if root.error}
    <p class="error" role="alert">Failed to load catalog: {root.error}</p>
  {:else if root.data}
    <FilterInput
      bind:value={filter}
      total={rows.length}
      shown={filtered.length}
      placeholder="Filter catalog sections…"
      label="Filter catalog sections"
    />
    {#if filtered.length > 0}
      <Panel title="Catalog sections" flush>
        <DataTable {columns} rows={filtered}>
          {#snippet cell(row, column)}
            {#if column.key === "name"}
              <a class="row-link" href={catalogHref(row.fqid)} title={row.fqid}>
                {row.name ?? row.fqid}
              </a>
            {:else if column.key === "typeLabel"}
              <Tag tone={row.kind === "classification-root" ? "class" : "reg"}>
                {row.typeLabel}
              </Tag>
            {:else}
              <span class="scope">{row.scope}</span>
            {/if}
          {/snippet}
        </DataTable>
      </Panel>
    {:else}
      <EmptyState title={`No catalog sections match “${filter}”`} />
    {/if}
  {/if}
</article>

<style>
  .row-link {
    font-weight: 600;
    /* Long-name breaking now comes from DataTable's cell-level
       `overflow-wrap: anywhere`, which inherits into this link (#832). */
  }
  .scope {
    color: var(--text-muted);
  }
</style>
