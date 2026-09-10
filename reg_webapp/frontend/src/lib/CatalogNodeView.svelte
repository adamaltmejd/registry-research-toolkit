<script lang="ts">
import {
  type BindingChild,
  type ClassificationFamilyNodeData,
  type ClassificationNodeData,
  type ConceptGroup,
  getCatalogNode,
  getRegisterVariants,
  isCatalogNode,
  type VariableDeliveryModel,
  type VariantsResponse,
} from "./api";
import { asyncResource } from "./async.svelte";
import BindingLeafView from "./BindingLeafView.svelte";
import ClassificationGroupView from "./ClassificationGroupView.svelte";
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
  memberCoverageUnion,
  narrowCatalogNode,
  narrowGroupsToMembers,
  nodeLabel,
  rankFilter,
  variantLabel,
} from "./catalog";
import FilterInput from "./FilterInput.svelte";
import type { Coverage } from "./period";
import RelatedDocumentsPanel from "./RelatedDocumentsPanel.svelte";
import {
  Button,
  type Column,
  DataTable,
  EmptyState,
  FilterChip,
  Panel,
  Skeleton,
  Tag,
} from "./ui";
import VariantsSummary from "./VariantsSummary.svelte";

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

/** One delivery column beside a variable (Y-82): the name SCB delivers it under
 * — what a researcher who knows LISA by its columns is hunting for — and the
 * years that name was delivered, shown only when the variable has more than one
 * (there the years say WHICH era each name belongs to). */
type DeliveryColumn = { name: string; years: string };

type VariableBrowseRow =
  | {
      id: string;
      kind: "group";
      group: ConceptGroup;
      label: string;
      href: string;
      /** A group row names no column: its members carry their own, shown in the
       * facet navigator the row expands to. */
      columns: DeliveryColumn[];
    }
  | {
      id: string;
      kind: "leaf";
      fqid: string;
      label: string;
      columns: DeliveryColumn[];
    };

const variableColumns: Column<VariableBrowseRow>[] = [
  { key: "label", label: "Variable" },
  { key: "columns", label: "Delivery column", mono: true },
];

type ClassificationBrowseRow =
  | {
      id: string;
      kind: "family";
      family: ClassificationFamilyNodeData;
      label: string;
      href: string;
      shortName: string;
    }
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
  { key: "shortName", label: "Current edition", align: "end" },
];

function variableBrowseRows(
  rows: GroupedRow<BindingChild>[],
  registerFqid: string,
  columnsByFqid: Map<string, DeliveryColumn[]>,
): VariableBrowseRow[] {
  return rows.map((row) =>
    row.kind === "group"
      ? {
          id: `group:${row.group.key}`,
          kind: "group",
          group: row.group,
          label: row.group.label,
          href: groupHref(registerFqid, row.group.key),
          columns: [],
        }
      : {
          id: row.item.fqid,
          kind: "leaf",
          fqid: row.item.fqid,
          label: row.item.name ?? row.item.fqid,
          columns: columnsByFqid.get(row.item.fqid) ?? [],
        },
  );
}

/** A register child's delivery columns, de-duplicated by NAME across the
 * variants that deliver it (one name = one column of the register, whichever
 * variants ship it) and carrying the union of those deliveries' windows. A
 * state SCB named no column for contributes nothing to name. Reads whatever
 * deliveries the child carries, so a child already narrowed by the chip lens
 * names only that variant's columns. */
function deliveryColumns(child: BindingChild): DeliveryColumn[] {
  const byName = new Map<string, VariableDeliveryModel[]>();
  for (const delivery of child.deliveries ?? []) {
    if (delivery.column == null) {
      continue;
    }
    const named = byName.get(delivery.column);
    if (named) {
      named.push(delivery);
    } else {
      byName.set(delivery.column, [delivery]);
    }
  }
  return (
    [...byName]
      .map(([name, deliveries]) => ({
        name,
        span: memberCoverageUnion(deliveries.map((d) => d.coverage)),
      }))
      .sort(
        (a, b) =>
          (a.span?.from ?? END) - (b.span?.from ?? END) ||
          a.name.localeCompare(b.name),
      )
      // A lone column needs no era label — there is nothing to tell it apart
      // from. Decided here, where the count is known, so the cell just renders
      // whatever `years` holds.
      .map(({ name, span }) => ({
        name,
        years: byName.size > 1 ? yearsLabel(span) : "",
      }))
  );
}

/** Sort key for a column whose window has no finite start: it prints no years,
 * so it can't join the chronological run — it goes last. */
const END = Number.POSITIVE_INFINITY;

/** A delivery window as years: "2018", "1990–2021", or "2022–" while still
 * delivered. Empty when the span has no finite start — the years would say
 * nothing then, so the column name stands alone. */
function yearsLabel(span: Coverage | null): string {
  if (!span || span.from === null) {
    return "";
  }
  if (span.to === null) {
    return `${span.from}–`;
  }
  return span.from === span.to ? `${span.from}` : `${span.from}–${span.to}`;
}

function classificationBrowseRows(
  rows: GroupedRow<ClassificationNodeData>[],
  families: ClassificationFamilyNodeData[],
): ClassificationBrowseRow[] {
  const familyRows: ClassificationBrowseRow[] = families.map((family) => ({
    id: `family:${family.key}`,
    kind: "family",
    family,
    label: `${family.label} (${family.editions.length} editions)`,
    href: classGroupHref(family.key),
    shortName: familyCurrentLabel(family),
  }));
  const itemRows: ClassificationBrowseRow[] = rows.map(
    (row): ClassificationBrowseRow =>
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
  return [...familyRows, ...itemRows];
}

function familyCurrentLabel(family: ClassificationFamilyNodeData): string {
  const current = family.editions.filter((edition) => edition.is_current);
  if (current.length === 1) {
    return current[0]?.slug ?? "";
  }
  if (current.length > 1) {
    return `${current.length} current`;
  }
  return `${family.editions.length} editions`;
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
  windowMinYear,
  vintageYear,
  windowMaxYear = vintageYear,
  enforcePeriodBounds = false,
}: {
  fqidPath: string;
  // C1: the deployment seed, threaded to BindingLeafView's "Add to project" so a
  // pristine store can implicitly create the project (App → here → BindingLeafView).
  regMetaVersion: string;
  steward: string;
  // #1037: steward-aware period-control bounds, threaded to BindingLeafView's
  // period picker. Kept separate from `vintageYear`, which is the true catalog
  // vintage for open-ended graph timelines.
  windowMinYear: number;
  // #631: the true catalog vintage year (App derives it from
  // context.reg_meta.import_date), used by graph/picker timelines that need to
  // extend open-ended histories to the catalog build vintage.
  vintageYear: number;
  windowMaxYear?: number;
  // #1037: true only when App's bounds came from steward.catalog_period_span,
  // making them hard picker limits rather than global fallback hints.
  enforcePeriodBounds?: boolean;
} = $props();

const resource = asyncResource(() => getCatalogNode(fqidPath));
// A browsable path resolves to a `kind`-tagged CatalogNode. A SUB-ENDPOINT path
// (e.g. a deep-link to `.../states`) hits that endpoint and returns a no-`kind`
// StatesResponse — narrow it OUT of `node` (so the kind-switch type-checks) and
// flag it as `notBrowsable` so we render a clear message instead of a blank
// page. (`.../variants` is its own SPA route now, Y-79, so it never lands here.)
const node = $derived(narrowCatalogNode(resource.data));
const notBrowsable = $derived(
  resource.data !== null && !isCatalogNode(resource.data),
);
const classificationSubjectKey = $derived.by((): string | null => {
  if (node?.kind !== "classification") {
    return null;
  }
  return node.family?.key ?? node.dimensions?.[0]?.key ?? null;
});

function classificationTabFocusFqid(node: ClassificationNodeData): string {
  return (
    node.edition_chain?.find(
      (edition) => edition.is_self && edition.fqid != null,
    )?.fqid ?? node.fqid
  );
}

// In-memory type-to-filter over the current node's child list (a provider's 238
// registers / a register's 740 bindings render flat otherwise). Reset on
// navigation so a new node opens unfiltered. `rankFilter` matches on the leaf
// slug + display name + FQID (registers also match their purpose blurb) and,
// under an active filter, RANKS the survivors (exact → prefix → other-substring)
// so a slug-named target jumps above a purpose-blurb-only match (#674); an empty
// needle leaves the incoming alphabetical order untouched. matchesFilter folds
// diacritics.
let filter = $state("");
// The register arm's variant chips (Y-82): multi-select, OR within the
// selection, the same interaction as the group page's per-axis facet filters.
// Filter-only — it narrows what the list shows, never the project.
let selectedVariants = $state(new Set<string>());
$effect(() => {
  // `fqidPath` is the navigation key — touching it here clears the filter when
  // the route changes (the component is reused across catalog paths).
  void fqidPath;
  filter = "";
  selectedVariants = new Set();
});

function toggleVariant(variant: string): void {
  const next = new Set(selectedVariants);
  if (!next.delete(variant)) {
    next.add(variant);
  }
  // Reassign so the `$state` proxy tracks the change (as ConceptGroupNavigator).
  selectedVariants = next;
}

/** Lift the variant lens — the way back to the whole register. Leaves the text
 * filter alone, which is why the control says "variant filter". */
function clearVariants(): void {
  selectedVariants = new Set();
}

// Everything the register arm derives from the node, hoisted OUT of the template
// so it is computed once per node instead of once per keystroke: the text filter
// re-runs its whole `{@const}` chain on every character, and none of this depends
// on what was typed. (Empty for every other node kind — `bindingChildren` returns
// [] unless the node is a register.)
const registerChildren = $derived(node ? bindingChildren(node) : []);
const registerGroups = $derived(
  node && node.kind === "register" ? node.groups : undefined,
);

// The register's variants, fetched ONCE for the page and handed to the Variants
// section below as well: the chips must spell a variant the way that section
// does, and two fetches of one list is two chances to drift. Every other node
// kind resolves to null without a request.
const variantsResource = asyncResource(
  (): Promise<VariantsResponse | null> =>
    node && node.kind === "register"
      ? getRegisterVariants(node.fqid)
      : Promise.resolve(null),
);
/** How a variant is SPELLED on its chip: its catalog name — the word the Variants
 * section uses. The slug is the fallback, for the moment before that list lands
 * and for a slug this list does not name. */
const variantNames = $derived(
  new Map(
    (variantsResource.data?.variants ?? []).map((v) => [
      v.slug,
      variantLabel(v),
    ]),
  ),
);

// The variants that deliver at least one of this register's variables — the chip
// set, read off the children rather than the register's variant list so a chip
// can never narrow the list to nothing. A register delivered by ONE variant has
// no variant axis to filter on, so it shows no chips. Ordered by SLUG, which is
// stable across the variant fetch: naming a chip must not move it.
const registerVariants = $derived.by(() => {
  const seen = new Set<string>();
  for (const child of registerChildren) {
    for (const delivery of child.deliveries ?? []) {
      seen.add(delivery.variant);
    }
  }
  return [...seen].sort();
});

// The chip lens, applied ONCE and to the data: each surviving child keeps only the
// selected variants' deliveries (OR within the selection), and a child none of
// them delivers is dropped. Everything downstream — the rows, the column cells,
// the filter keys — then reads one already-narrowed list and never has to know
// that variants exist. An empty selection is the whole register, untouched.
const lensedChildren = $derived.by(() => {
  if (selectedVariants.size === 0) {
    return registerChildren;
  }
  const kept: BindingChild[] = [];
  for (const child of registerChildren) {
    const deliveries = (child.deliveries ?? []).filter((d) =>
      selectedVariants.has(d.variant),
    );
    if (deliveries.length > 0) {
      kept.push({ ...child, deliveries });
    }
  }
  return kept;
});

// The delivery columns per child, off the LENSED children — so the cell and the
// filter's column keys both say only what the selected variants deliver. The
// filter reads them on every keystroke (twice per row — match, then rank) and the
// surviving rows render them.
const columnsByFqid = $derived(
  new Map(lensedChildren.map((child) => [child.fqid, deliveryColumns(child)])),
);

/** A child's delivery column NAMES, for the filter's match keys. */
function columnNames(fqid: string): string[] {
  return (columnsByFqid.get(fqid) ?? []).map((column) => column.name);
}

// #303 concept-group folding: grouped bindings become one expandable group row,
// ungrouped bindings stay leaf rows. `registerRows` folds the WHOLE register —
// the "of N" the readouts count against. `narrowedRows` folds the chip lens: the
// lens narrows the CHILDREN and the groups' members FIRST, so a surviving group
// row counts and indexes only what the selected variants deliver, and a group
// left with one member gives way to that member's own leaf row. The text
// filter ranks what is left, in the template, because only IT changes per
// keystroke.
const registerRows = $derived(
  foldGroupedRows(registerChildren, registerGroups),
);
const narrowedRows = $derived(
  selectedVariants.size === 0
    ? registerRows
    : foldGroupedRows(
        lensedChildren,
        narrowGroupsToMembers(registerGroups, lensedChildren),
      ),
);
</script>

{#if resource.loading}
  <section
    class="route-loading"
    class:classification-loading={fqidPath.startsWith("class/")}
    aria-busy="true"
    aria-live="polite"
  >
    <p class="muted loading-status">Loading…</p>
    <div class="loading-geometry">
      <Skeleton width="min(24rem, 70%)" count={2} />
      <Skeleton
        variant="block"
        count={fqidPath.startsWith("class/") ? 3 : 1}
      />
    </div>
  </section>
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
          <DataTable
            framed
            columns={registerColumns}
            rows={registers}
            getRowId={registerRowId}
            rowNavigation
          >
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
      <!-- The rows are folded and variant-narrowed in the script (both depend on
           the node, not on the needle); only the TEXT ranking belongs here, where
           it re-runs per keystroke. Y-82: it matches DELIVERY COLUMN names too,
           so a researcher who knows LISA as `ForvErs` finds `forvink-ers`. -->
      {@const filteredRows = rankFilter(narrowedRows, filter, (row) =>
        row.kind === "group"
          ? groupFilterKeys(row.group, columnNames)
          : [
              leafSlug(row.item.fqid),
              row.item.fqid,
              row.item.name,
              ...columnNames(row.item.fqid),
            ],
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
      {#if registerRows.length > 0}
        {#if registerVariants.length > 1}
          <!-- Y-82: a register delivered by SEVERAL variants gets a chip per
               variant, so a researcher can read the list as the one variant they
               will order from. A chip READS as the variant's catalog name — the
               word the Variants section below spells it with — while its identity
               stays the `?variant=` slug. Value-only chips, like the picker's
               dimension filters. -->
          <div class="variant-filters">
            <fieldset class="variant-filter">
              <legend><span class="micro-label">Variant</span></legend>
              {#if variantsResource.loading}
                <!-- A chip can only be NAMED once the variant list lands. Painting
                     the slug first and swapping to the name re-flows the strip
                     under the pointer, so hold its shape instead. On a FAILED
                     load the chips render their slugs and the lens keeps working
                     — losing the filter would cost more than a machine-readable
                     label, and the Variants section below reports the failure. -->
                <div class="filter-options" aria-busy="true">
                  <Skeleton width="16rem" />
                </div>
              {:else}
                <div class="filter-options">
                  {#each registerVariants as variant (variant)}
                    <FilterChip
                      selected={selectedVariants.has(variant)}
                      onToggle={() => toggleVariant(variant)}
                    >
                      {variantNames.get(variant) ?? variant}
                    </FilterChip>
                  {/each}
                </div>
              {/if}
            </fieldset>
            <!-- The live region is mounted WITH the strip and left empty until a
                 chip is on: a region inserted together with its first text is not
                 announced, so the first narrowing — the one that matters — would
                 pass in silence. It has something to say because the chips narrow
                 silently otherwise: with the text box empty FilterInput shows no
                 "x of y". While typing, FilterInput reports the SAME pair (its
                 `shown` is already the post-chip count), so this one steps aside
                 rather than say it twice. -->
            <div class="variant-status">
              <span aria-live="polite">
                {#if selectedVariants.size > 0 && !filter.trim()}
                  Showing {countFoldedMembers(filteredRows)} of {countFoldedMembers(
                    registerRows,
                  )} variables
                {/if}
              </span>
              {#if selectedVariants.size > 0}
                <Button size="sm" onclick={clearVariants}>
                  Clear variant filter
                </Button>
              {/if}
            </div>
          </div>
        {/if}
        <!-- Counts stay in VARIABLE units after folding (a group row counts its
             members), so the "x of y" readout still reflects register size. -->
        <FilterInput
          bind:value={filter}
          total={countFoldedMembers(registerRows)}
          shown={countFoldedMembers(filteredRows)}
          placeholder="Filter variables…"
          label="Filter variables"
        />
        {#if filteredRows.length > 0}
          {@const variableRows = variableBrowseRows(
            filteredRows,
            node.fqid,
            columnsByFqid,
          )}
          <DataTable
            framed
            columns={variableColumns}
            rows={variableRows}
            getRowId={browseRowId}
            rowNavigation
          >
            {#snippet cell(row, column)}
              {#if column.key === "columns"}
                <!-- Y-82: the delivery column names, one per line. The years ride
                     along only when a variable has SEVERAL — there they say which
                     era each name belongs to (`ForvErs` 1990–2021, then
                     `ForvErsNetto`); a single column needs no disambiguation. -->
                {#each row.columns as col (col.name)}
                  <span class="delivery-column">
                    {col.name}
                    {#if col.years}
                      <span class="column-years">{col.years}</span>
                    {/if}
                  </span>
                {/each}
              {:else if row.kind === "group"}
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
        {:else}
          <Panel title="Variables">
            <!-- Only the text filter can empty the list: every chip is read off a
                 variant that DELIVERS something, so a selection always keeps a
                 row. The chips can still be narrowing what was searched,
                 though, so the copy points back at the strip that lifts them. -->
            <EmptyState
              title={`No variables match “${filter}”`}
              description={selectedVariants.size > 0
                ? "The variant filter above is also narrowing this list."
                : undefined}
            />
          </Panel>
        {/if}
      {:else}
        <Panel title="Variables">
          <EmptyState title="No variables." />
        </Panel>
      {/if}
      <VariantsSummary
        registerFqid={node.fqid}
        variants={variantsResource.data}
        error={variantsResource.error}
      />
      <RelatedDocumentsPanel register={leafSlug(node.fqid)} />
    {:else if node.kind === "binding"}
      <!-- Pass the full node down: this no-query browse fetch already resolved
           the variable's metadata + embedded edges + default states. BindingLeafView
           renders those from `node` (always present — so a cold deep-link with
           `?period` isn't blank) and fetches only the period-NARROWED states from
           the URL query, reactive without a remount. -->
      <BindingLeafView
        {fqidPath}
        {node}
        {regMetaVersion}
        {steward}
        {windowMinYear}
        {windowMaxYear}
        {enforcePeriodBounds}
        {vintageYear}
      />
    {:else if node.kind === "classification-root"}
      <!-- #516 umbrella folding: e.g. group:sun renders as ONE group row
           expanding to its dimension members; #771 one-dimensional succession
           families render as stable family rows; remaining ungrouped
           classifications stay leaves. The backend drops superseded editions
           and every edition represented by a family row — those editions are
           reached via a leaf/family edition-chain panel. -->
      {@const clsRows = foldGroupedRows(node.children, node.groups)}
      {@const familyNodes = node.families ?? []}
      <h2>{nodeLabel(node)}</h2>
      {#if clsRows.length > 0 || familyNodes.length > 0}
        {@const classificationRows = classificationBrowseRows(clsRows, familyNodes)}
        <div class="classification-table">
          <Panel title="Classification systems" flush>
            <DataTable
              columns={classificationColumns}
              rows={classificationRows}
              getRowId={browseRowId}
              rowNavigation
            >
              {#snippet cell(row, column)}
                {#if column.key === "label"}
                  {#if row.kind === "family"}
                    <a class="row-link" href={row.href} title={`${row.family.editions.length} editions`}>
                      {row.label}
                    </a>
                  {:else if row.kind === "group"}
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
                {:else if row.kind === "family"}
                  <span class="short-name">{row.shortName}</span>
                {/if}
              {/snippet}
            </DataTable>
          </Panel>
        </div>
      {:else}
        <EmptyState title="No classifications." />
      {/if}
    {:else if node.kind === "classification"}
      {#if classificationSubjectKey}
        <!-- #1116: grouped / family classification FQIDs stay valid as shareable
             deep-links, but the canonical surface is the group/family page with
             this edition's tab active. Ungrouped classifications still use the
             standalone leaf view. -->
        <ClassificationGroupView
          key={classificationSubjectKey}
          activeFqid={classificationTabFocusFqid(node)}
          initialActiveNode={node}
        />
      {:else}
        <!-- #638 PR1: the ungrouped classification leaf renders through the unified
             SubjectView shell, same as the binding leaf + concept group. -->
        <ClassificationLeafView {node} />
      {/if}
    {/if}
  </article>
{:else if notBrowsable}
  <!-- A no-`kind` response: a deep-link to a SUB-ENDPOINT path (e.g.
       `.../states`) hits that endpoint and returns a StatesResponse, not a
       browsable node. Render a clear message instead of a blank page. -->
  <p class="error" role="alert">
    <code>{fqidPath}</code> isn't a browsable catalog node.
  </p>
{/if}

<style>
  /* Match the successful route's title/detail rhythm while the first catalog
     request is unresolved. Classification routes reserve the extra graph/value
     surfaces through three standard Skeleton blocks; ordinary browse routes use
     one. The placeholder remains bounded and responsive because it follows the
     content column instead of measuring the viewport in JavaScript. */
  .route-loading {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
  }
  .loading-status {
    margin: 0;
  }
  .loading-geometry {
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    max-width: 64rem;
  }
  .classification-loading .loading-geometry {
    gap: var(--space-4);
  }
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
  /* The register arm's variant chip strip (Y-82) — one fieldset, its legend
     naming the axis in TEXT (never hue), over `ui/FilterChip` checkboxes. Same
     shape as the group page's per-axis filters, at one axis. */
  .variant-filters {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-3);
    margin: 0 0 var(--space-3);
  }
  .variant-filter {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-2) var(--space-3) var(--space-3);
    margin: 0;
    /* Hug the chips: a bare fieldset is block-level, so at 1920 it would frame a
       canvas-wide box around a handful of chips. */
    inline-size: fit-content;
    min-inline-size: 0;
  }
  .variant-filter legend {
    padding: 0 var(--space-1);
  }
  .filter-options {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1);
  }
  .variant-status {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-2);
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  /* One delivery column per line in the (mono-faced) column cell. The years —
     shown when a variable has several columns — stay in the cell's mono face
     (a year is a machine identifier, like the Variants panel's Years column
     below) and are merely dimmed, so the name a researcher hunts for leads. */
  .delivery-column {
    display: block;
    /* Pin the NAME against DataTable's stacked-card rule, which mutes every
       non-primary cell: below 48rem that flattened the name and its years to one
       grey and the column a researcher is hunting for stopped leading. */
    color: var(--text);
  }
  .column-years {
    color: var(--text-muted);
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
