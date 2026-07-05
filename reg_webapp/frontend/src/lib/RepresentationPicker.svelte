<script lang="ts">
import type {
  GroupAxisModel,
  GroupFacetModel,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  type BandIdentity,
  type BandLabel,
  catalogHref,
  clusterBands,
  encodeCodesParam,
  facetLabelJoin,
  leafSlug,
  type PickerDimension,
  type PickerRepresentation,
  pickerFilterDimensions,
  pickerLabeling,
  pickerRowPasses,
  pickerRowVariantFamilyLabel,
  representationInWindow,
  rowFacet,
  yearOf,
} from "./catalog";
import {
  axisTicks,
  CELL_MIN_W,
  cellsOf,
  clustersOf,
  graphColumnMatches,
  graphEdgeVisibleInGraph,
  type NodeCluster,
  PX_PER_YEAR,
  type RenderNode,
  type ResolvedEdge,
  type RunCell,
  resolveEdges,
  type VariableLane,
  type YearScale,
  yearScaleOf,
} from "./picker_graph";
import type { StagedPeriodChange } from "./project_store.svelte";
import { router } from "./router.svelte";
import {
  nullBindingCommittedRowKeys,
  type PickerCommittedRow,
  pickerRowKey,
  type StagedPickerBand,
} from "./staged_picker";
import { Button, Tag } from "./ui";

// The direct COLUMN picker (#678 redesign): ONE compact, integrated list of a
// concept's delivery columns with a staged diff footer whose commit label follows the
// diff shape. The binding leaf passes its single variable; the concept-group page passes
// one entry per member variable — and the two render essentially identically (the
// user is selecting a CONCEPT's columns, not reasoning about the underlying
// variables). Light hierarchy, no card chrome, no default collapse: every column is
// visible. A multi-column variable gets a thin subheading row (its distinguishing
// identity + a "select all" toggle) over its column rows; a single-column variable
// collapses to ONE selectable row. Thin + presentational: the parent owns the data
// (enumerates each variable's `rows`) and the store wiring (`onapply`); this owns the
// cross-variable staging + the layout.

/** One variable the picker lists — its identity + its delivery-column rows. `key` is
 * GLOBALLY unique (the member fqid for a group, the leaf fqid for the leaf) so it
 * namespaces the variable's column selection keys in the cross-variable set. (The
 * type name keeps "Band" for continuity with the consumers; the UI says "column".) */
export interface PickerBand {
  key: string;
  name: string;
  registerPrefix: string;
  facetLabel?: string | null;
  /** Per-DELIVERY-COLUMN human facet label (#678): a representation group can carry
   * several members on ONE variable, each a distinct `delivery_column` with its own
   * facet (e.g. CDISP "Inkl. kapitalvinst" vs CDISP5 "Exkl. kapitalvinst"). The band
   * is built per DISTINCT fqid, so without this the later members' facet labels never
   * reach their rows. Keyed by `delivery_column`, it lets the picker show the human
   * facet label per column rather than only the technical column name. The GROUP view
   * sets it; the binding LEAF leaves it undefined (its single member has no per-column
   * facet split). */
  facetByColumn?: Record<string, string>;
  /** Per-DELIVERY-COLUMN structured facets (#908): the (axis, value, label) tuples
   * a representation member carries, keyed by `delivery_column`. The dimension
   * marking + per-axis filter controls read these to mark each row with its facet
   * dimension and to narrow the list to one axis value. The GROUP view sets it; the
   * binding LEAF leaves it undefined (no facet split on a single member). */
  facetsByColumn?: Record<string, GroupFacetModel[]>;
  /** BAND-LEVEL structured facets (#908 C1): the facets of a WHOLE-VARIABLE faceted
   * member — one whose `delivery_column` is null, so its facets can't key by column
   * (e.g. a month-faceted group: one variable per month). These apply to ALL of the
   * band's rows, as `rowFacet`'s fallback after the per-column lookup. The GROUP view
   * sets it for such members; otherwise undefined. */
  facets?: GroupFacetModel[];
  isSensitive?: boolean;
  isIdentifier?: boolean;
  /** The member variable's OPERATIONAL DEFINITION (#892/#932): SCB's per-(split-)variable
   * distinguishing text — the one field that tells parallel concept-group members apart
   * when their name/definition/description coincide (e.g. fordonsreg näringsgren: owner /
   * previous-owner). The GROUP view sets it per member (from its graph node) so a band
   * surfaces its own distinguishing text inline; the binding LEAF leaves it undefined (its
   * meta list already renders the op-def, so the single band would duplicate it). */
  operationalDefinition?: string | null;
  rows: PickerRepresentation[];
  /** The variable's leaf page href (the GROUP view sets it per member —
   * `catalogHref(member.fqid)` — so the picker can link to each member's own page).
   * Undefined for the binding LEAF (it's already that page — no self-link). When set,
   * the variable IDENTITY becomes a navigation link, kept DISTINCT from the selection
   * checkbox. */
  href?: string;
  /** The SUPERSEDED predecessor editions this band folds (#902): when an inter-variable
   * `succession` edge runs between two members of the group, the predecessor is NOT a
   * co-equal selectable band — the chain HEAD (latest edition) leads, and its superseded
   * predecessor(s) are surfaced here as quiet HISTORY (a "supersedes …" disclosure), each
   * still reachable via its own leaf page. Oldest-first along the chain; the head (this
   * band) is excluded. Undefined / empty when the member heads no in-group succession
   * (the binding leaf, or a member with no superseded predecessor). The GROUP view sets
   * it. */
  supersedes?: {
    name: string;
    href: string;
    /** The year the predecessor was replaced by its successor (the edge
     * `effective_year`), or null when the edge carries none — shown as a quiet
     * "until <year>" qualifier on the history entry. */
    effectiveYear: number | null;
    /** Optional selectable predecessor band (#926). Axis-less group views keep the
     * predecessor folded visually, but expose its era-specific rows inside this
     * disclosure so an old study window can add the covering variable. */
    band?: Omit<PickerBand, "supersedes">;
  }[];
}

/** A staged add — the variable it belongs to plus the picked column, so the parent can
 * write the final project binding in one `applyStagedDiff` mutation. */
export interface PickerSelection {
  band: PickerBand;
  row: PickerRepresentation;
}

export interface PickerRemoval {
  band: PickerBand;
  row: PickerRepresentation;
  committed: PickerCommittedRow;
}

export interface PickerApplyPayload {
  adds: PickerSelection[];
  removes: PickerRemoval[];
  periodChanges: StagedPeriodChange[];
}

type PickerApplyResult = boolean | undefined;

let {
  bands,
  axes = [],
  includeRowDimensionFilters = true,
  window,
  canAdd,
  committedRows = new Map<string, PickerCommittedRow>(),
  activePeriod = null,
  focusKey = null,
  graph = null,
  graphMemberHrefs = null,
  vintageYear,
  onapply,
  onstagechange,
}: {
  /** The variables, in render order. One element for the leaf; one per member for
   * the group page. */
  bands: PickerBand[];
  /** The group's declared facet axes (#819/#908): the per-axis filter controls match
   * on `axis.name` and display `axis.label`. Empty (the default — the binding leaf,
   * or an axis-less group) → no facet-dimension filters. */
  axes?: readonly GroupAxisModel[];
  /** Whether to add built-in row-level Population/Coding filters after the curated
   * facet axes. Group pages can disable these when succession/coding should remain
   * row metadata, not a browse facet. */
  includeRowDimensionFilters?: boolean;
  /** The active period window as an inclusive year pair, or null (no narrowing → no
   * column dims). Columns whose span doesn't overlap render dimmed (still selectable). */
  window: [number, number] | null;
  /** Whether the Add action is permitted (the deployment seed is ready). When false
   * the button stays disabled regardless of selection. */
  canAdd: boolean;
  /** Rows already present in the active project, keyed by `pickerRowKey`. */
  committedRows?: ReadonlyMap<string, PickerCommittedRow>;
  /** The explicit `?period` value. The picker uses it only as parent-supplied
   * context; partial leaf/group views must not stage source-level period
   * replacements because a source period applies to every binding on the source. */
  activePeriod?: string | null;
  /** The `band.key` of a band to visually MARK as the deep-link focus (#678): the
   * group page passes the `?member=` hint's band so a `?member=<slug>` deep link
   * renders with that member highlighted, restoring the focus affordance. Null (the
   * leaf, or no hint) marks nothing. */
  focusKey?: string | null;
  /** Optional relationship graph for the graph/time-band picker mode (#904). When
   * absent, too large, or edge-less, the picker renders the compact list fallback. */
  graph?: RelationshipGraph | null;
  /** Explicit group-member leaf links keyed by member fqid. Group pages pass this so
   * graph mode can distinguish folded members (history) from unrelated graph neighbors
   * that must not leak into the picker. The binding leaf leaves it null. */
  graphMemberHrefs?: Readonly<Record<string, string>> | null;
  /** Catalog vintage ceiling for open-ended graph cells. */
  vintageYear?: number;
  /** Commit the staged diff. The parent maps add rows to final `StagedAdd` payloads
   * and calls `projectStore.applyStagedDiff` once. Return false when an async parent
   * guard rejects the apply so local staging remains visible. */
  onapply: (
    payload: PickerApplyPayload,
  ) => PickerApplyResult | Promise<PickerApplyResult>;
  /** Notify the host when a user starts a new staged diff, so page-level applied
   * confirmations do not sit beside conflicting staged status. */
  onstagechange?: (hasDiff: boolean) => void;
} = $props();

/** The local staged diff. A key in `stagedAddKeys` means an uncommitted row will be
 * added; a key in `stagedRemoveKeys` means a committed row will be removed. Desired
 * checkbox state is derived from committed project state plus these two sets. */
let stagedAddKeys = $state(new Set<string>());
let stagedRemoveKeys = $state(new Set<string>());
let applying = $state(false);
/** The active filter selection (#908): dimension key → set of selected values. An
 * empty / absent set imposes no constraint (the initial all-empty state shows every
 * row). Reassigned (not mutated) so the `$state` record/Set stay reactive. Reset
 * alongside staged row keys when the band set changes (the effect below). */
let filterSelection = $state<Record<string, Set<string>>>({});
const selectableBands = $derived.by((): PickerBand[] => {
  const out = [...bands];
  const seen = new Set(bands.map((band) => band.key));
  for (const band of bands) {
    for (const predecessor of band.supersedes ?? []) {
      if (predecessor.band && !seen.has(predecessor.band.key)) {
        seen.add(predecessor.band.key);
        out.push(predecessor.band);
      }
    }
  }
  return out;
});
const bandsSignature = $derived(
  selectableBands
    .map((b) => `${b.key}:${b.rows.map((r) => r.key).join(",")}`)
    .join("|"),
);
const committedSignature = $derived(
  [...committedRows.values()]
    .map((r) => `${r.key}:${r.registerVariant}:${r.representation ?? ""}`)
    .join("|"),
);
$effect(() => {
  void bandsSignature;
  stagedAddKeys = new Set<string>();
  stagedRemoveKeys = new Set<string>();
  // Reset the filters too (#908): a stale axis value from a different group could
  // otherwise hide everything in the new one.
  filterSelection = {};
});
$effect(() => {
  void committedSignature;
  stagedAddKeys = new Set<string>();
  stagedRemoveKeys = new Set<string>();
});

// ── Dimension marking + per-dimension filters (#908) ─────────────────────────
// The filterable dimensions across the bands (facet axes first, then variant, then
// coding), each emitted only when it DISCRIMINATES (≥2 distinct values). A
// single-population, single-coding, single-axis-value group surfaces none → the
// controls collapse. Filtering is a CLIENT-SIDE LENS: it narrows which rows show,
// never the selection or the commit.
const dimensions = $derived(
  pickerFilterDimensions(selectableBands, axes, {
    includeRowDimensions: includeRowDimensionFilters,
  }),
);

const anyFilterActive = $derived(
  Object.values(filterSelection).some((s) => s.size > 0),
);

/** Whether a value is selected in its dimension's filter. */
function isFilterOn(key: string, value: string): boolean {
  return filterSelection[key]?.has(value) ?? false;
}

/** Toggle a value in a dimension's filter set (multi-select within a dimension). */
function toggleFilter(key: string, value: string): void {
  const current = filterSelection[key] ?? new Set<string>();
  const next = new Set(current);
  if (next.has(value)) {
    next.delete(value);
  } else {
    next.add(value);
  }
  filterSelection = { ...filterSelection, [key]: next };
}

/** Clear every dimension filter (back to "show all columns"). */
function clearFilters(): void {
  filterSelection = {};
}

function filteredRowsForBand(band: PickerBand): PickerRepresentation[] {
  if (!anyFilterActive) {
    return band.rows;
  }
  return band.rows.filter((row) =>
    pickerRowPasses(row, band, dimensions, filterSelection),
  );
}

const visibleHistoryRows = $derived.by(
  (): { band: PickerBand; row: PickerRepresentation }[] => {
    const out: { band: PickerBand; row: PickerRepresentation }[] = [];
    const seen = new Set<string>();
    for (const band of bands) {
      for (const predecessor of band.supersedes ?? []) {
        if (!predecessor.band) {
          continue;
        }
        for (const row of filteredRowsForBand(predecessor.band)) {
          const key = rowKey(predecessor.band, row);
          if (rowSelectable(row) && !seen.has(key)) {
            seen.add(key);
            out.push({ band: predecessor.band, row });
          }
        }
      }
    }
    return out;
  },
);

/** The bands with their rows narrowed to those passing the active filters. A head band
 * remains visible if only its selectable history matches, so filters do not hide the
 * disclosure that contains matching predecessor rows. */
const filteredBands = $derived.by((): PickerBand[] => {
  if (!anyFilterActive) {
    return bands;
  }
  const out: PickerBand[] = [];
  for (const band of bands) {
    const rows = filteredRowsForBand(band);
    const hasVisibleHistory = (band.supersedes ?? []).some(
      (predecessor) =>
        predecessor.band && filteredRowsForBand(predecessor.band).length > 0,
    );
    if (rows.length > 0 || hasVisibleHistory) {
      out.push({ ...band, rows });
    }
  }
  return out;
});

/** The total visible rows under the active filters (the "showing N of M" count). */
const totalRows = $derived(
  selectableBands.reduce((n, b) => n + b.rows.length, 0),
);
const visibleRows = $derived(
  filteredBands.reduce((n, b) => n + b.rows.length, 0) +
    visibleHistoryRows.length,
);

function rowKey(band: PickerBand, row: PickerRepresentation): string {
  return pickerRowKey(band as StagedPickerBand, row);
}

function rowSelectable(row: PickerRepresentation): boolean {
  return row.selectable !== false;
}

function rowCanToggle(band: PickerBand, row: PickerRepresentation): boolean {
  return rowSelectable(row) || committedRows.has(rowKey(band, row));
}

function rowChecked(band: PickerBand, row: PickerRepresentation): boolean {
  const key = rowKey(band, row);
  const committed = committedRows.has(key);
  return committed ? !stagedRemoveKeys.has(key) : stagedAddKeys.has(key);
}

type RowStage = "none" | "committed" | "staged-add" | "staged-remove";

function rowStage(band: PickerBand, row: PickerRepresentation): RowStage {
  const key = rowKey(band, row);
  if (stagedAddKeys.has(key)) {
    return "staged-add";
  }
  if (stagedRemoveKeys.has(key)) {
    return "staged-remove";
  }
  return committedRows.has(key) ? "committed" : "none";
}

function rowStageLabel(stage: RowStage): string {
  if (stage === "committed") {
    return "In project";
  }
  if (stage === "staged-add") {
    return "Will be added";
  }
  if (stage === "staged-remove") {
    return "Will be removed";
  }
  return "";
}

function rowStageGlyph(stage: RowStage): string {
  if (stage === "staged-add") {
    return "+";
  }
  if (stage === "staged-remove") {
    return "-";
  }
  return "i";
}

function setRowDesired(
  nextAdds: Set<string>,
  nextRemoves: Set<string>,
  band: PickerBand,
  row: PickerRepresentation,
  desired: boolean,
): void {
  const key = rowKey(band, row);
  if (committedRows.has(key)) {
    const committed = committedRows.get(key);
    const removeKeys = committed
      ? nullBindingCommittedRowKeys(committedRows.values(), committed)
      : [key];
    nextAdds.delete(key);
    if (desired) {
      for (const removeKey of removeKeys) {
        nextRemoves.delete(removeKey);
      }
    } else {
      for (const removeKey of removeKeys) {
        nextRemoves.add(removeKey);
      }
    }
  } else {
    nextRemoves.delete(key);
    if (desired) {
      nextAdds.add(key);
    } else {
      nextAdds.delete(key);
    }
  }
}

/** Toggle one column's desired project membership. */
function toggleRow(band: PickerBand, row: PickerRepresentation): void {
  if (applying || !rowCanToggle(band, row)) {
    return;
  }
  const adds = new Set(stagedAddKeys);
  const removes = new Set(stagedRemoveKeys);
  setRowDesired(adds, removes, band, row, !rowChecked(band, row));
  stagedAddKeys = adds;
  stagedRemoveKeys = removes;
}

/** Whether EVERY column of a variable is selected — the variable-level "select all"
 * checked state (and the indeterminate complement: some-but-not-all). */
function allOfBandSelected(band: PickerBand): boolean {
  const rows = band.rows.filter(rowSelectable);
  return rows.length > 0 && rows.every((r) => rowChecked(band, r));
}
function someOfBandSelected(band: PickerBand): boolean {
  return band.rows.filter(rowSelectable).some((r) => rowChecked(band, r));
}

/** Select or clear every column of one variable in a single move (the per-variable
 * "select all columns of <identity>" affordance). */
function toggleBand(band: PickerBand): void {
  if (applying) {
    return;
  }
  const adds = new Set(stagedAddKeys);
  const removes = new Set(stagedRemoveKeys);
  const select = !allOfBandSelected(band);
  for (const r of band.rows) {
    if (!rowSelectable(r)) {
      continue;
    }
    setRowDesired(adds, removes, band, r, select);
  }
  stagedAddKeys = adds;
  stagedRemoveKeys = removes;
}

/** The variable currently hovered at the SUBHEADING level — its column rows get the
 * row-hover highlight (signalling they move together). Set on subhead enter/leave;
 * the rows are sibling `<li>`s, so a JS `$state` flag scopes the highlight. */
let hoveredBandKey = $state<string | null>(null);

/** Every VISIBLE column key — the global select-all target (#908): select-all acts
 * on the rows the active filters leave showing, never the hidden ones (filtering is
 * a presentation lens — it must not let "select all" grab a row the user has filtered
 * out of view). A hidden row's staged state persists regardless. */
const allVisibleRows = $derived([
  ...filteredBands.flatMap((b) =>
    b.rows.filter(rowSelectable).map((r) => ({ band: b, row: r })),
  ),
  ...visibleHistoryRows,
]);
const allKeys = $derived(
  allVisibleRows.map(({ band, row }) => rowKey(band, row)),
);
const visibleSelectableBandCount = $derived(
  new Set(allVisibleRows.map(({ band }) => band.key)).size,
);
const allSelected = $derived(
  allVisibleRows.length > 0 &&
    allVisibleRows.every(({ band, row }) => rowChecked(band, row)),
);
const someSelected = $derived(
  allVisibleRows.some(({ band, row }) => rowChecked(band, row)),
);

/** Select or clear every VISIBLE column in one move — leaving any hidden-but-selected
 * row's selection untouched (clear removes only the visible keys). */
function toggleAll(): void {
  if (applying) {
    return;
  }
  const adds = new Set(stagedAddKeys);
  const removes = new Set(stagedRemoveKeys);
  for (const { band, row } of allVisibleRows) {
    setRowDesired(adds, removes, band, row, !allSelected);
  }
  stagedAddKeys = adds;
  stagedRemoveKeys = removes;
}

const stagedAdds = $derived.by((): PickerSelection[] => {
  const out: PickerSelection[] = [];
  for (const band of selectableBands) {
    for (const row of band.rows) {
      if (rowSelectable(row) && stagedAddKeys.has(rowKey(band, row))) {
        out.push({ band, row });
      }
    }
  }
  return out;
});
const stagedRemoves = $derived.by((): PickerRemoval[] => {
  const out: PickerRemoval[] = [];
  for (const band of selectableBands) {
    for (const row of band.rows) {
      const key = rowKey(band, row);
      const committed = committedRows.get(key);
      if (committed && stagedRemoveKeys.has(key)) {
        out.push({ band, row, committed });
      }
    }
  }
  return out;
});
const periodChanges = $derived.by((): StagedPeriodChange[] => {
  void activePeriod;
  return [];
});
const selectedCount = $derived(stagedAdds.length);
const removeCount = $derived(stagedRemoves.length);
const periodChangeCount = $derived(periodChanges.length);
const diffCount = $derived(selectedCount + removeCount + periodChangeCount);
const rowDiffCount = $derived(selectedCount + removeCount);
const canApply = $derived(diffCount > 0 && (selectedCount === 0 || canAdd));
const applyLabel = $derived.by(() => {
  if (periodChangeCount > 0 || (selectedCount > 0 && removeCount > 0)) {
    return "Apply changes";
  }
  if (removeCount > 0) {
    return "Remove from project";
  }
  return "Add to project";
});

$effect(() => {
  if (diffCount > 0) {
    onstagechange?.(true);
  }
});

async function commit(): Promise<void> {
  if (!canApply || applying) {
    return;
  }
  applying = true;
  try {
    const applied = await onapply({
      adds: stagedAdds,
      removes: stagedRemoves,
      periodChanges,
    });
    if (applied !== false) {
      stagedAddKeys = new Set<string>();
      stagedRemoveKeys = new Set<string>();
    }
  } finally {
    applying = false;
  }
}

function resetStaging(): void {
  stagedAddKeys = new Set<string>();
  stagedRemoveKeys = new Set<string>();
}

/** The DISTINCT delivery columns a variable's rows address. A member whose rows all
 * deliver the SAME one column is a single-column member — its several rows are
 * POPULATIONS of that column, not distinct columns. */
function distinctColumns(band: PickerBand): string[] {
  return [...new Set(band.rows.map((r) => r.column).filter(Boolean))];
}

/** Whether the band is a SINGLE-COLUMN member — every row delivers one and the same
 * delivery column. Its identity is then that column (rendered as the chip-LINK); only
 * a genuinely multi-column member falls back to the leaf slug. */
function distinguisherIsColumn(band: PickerBand): boolean {
  return distinctColumns(band).length === 1;
}

/** A variable's distinguishing technical differentiator: its sole delivery column when
 * every row delivers the same one (a column-led group reads `SNI2002`/`SNI2007_Ag` —
 * the several rows are populations), else the member leaf slug — the fallback for a
 * genuinely multi-column variable. */
function distinguisherOf(band: PickerBand): string {
  const cols = distinctColumns(band);
  return cols.length === 1 ? cols[0] : leafSlug(band.key);
}

/** A band's identity dimensions for `bandLabeling`/`clusterBands` (#678). */
function bandIdentity(b: PickerBand): BandIdentity {
  return {
    name: b.name,
    registerPrefix: b.registerPrefix,
    facetLabel: b.facetLabel ?? null,
    distinguisher: distinguisherOf(b),
    distinguisherIsColumn: distinguisherIsColumn(b),
  };
}

/** Group the bands into name-CLUSTERS, each labeled independently (#901): a
 * heterogeneous group (several distinct concept names) used to make `bandLabeling`'s
 * GLOBAL `nameVaries` true, so every band led with its (repeated) name and buried the
 * real distinguisher. Clustering by name first lets the existing per-band labeling run
 * per cluster — inside a cluster the name is constant, so each band leads with its
 * facet → column/slug exactly as it already does for a homogeneous group. With one
 * cluster (all members share the name, or the lone leaf) `showClusterHeadings` is
 * false → render as today (the name is already the page title); with several, each
 * name renders ONCE as a group heading over its distinguisher-led bands. */
// Cluster + label the FILTERED bands (#908) so the adaptive labeling adapts to the
// VISIBLE rows (e.g. once a filter leaves a band one column, its labels re-collapse).
const clustered = $derived(clusterBands(filteredBands, bandIdentity));

/** Per-variable adaptive COLUMN labels (#678 1b) — show only what varies within the
 * variable, constants hoisted to a thin context line. Keyed by variable key. Built
 * over the FILTERED bands (#908). */
const labelingByBand = $derived(
  new Map(filteredBands.map((b) => [b.key, pickerLabeling(b.rows)])),
);

function normalizedOperationalDefinition(band: PickerBand): string {
  return band.operationalDefinition?.trim().replace(/\s+/g, " ") ?? "";
}

function normalizedSearchText(text: string): string {
  return text.trim().toLocaleLowerCase().replace(/\s+/g, " ");
}

function commonDefinitionStem(definitions: readonly string[]): string {
  if (definitions.length < 2) {
    return "";
  }
  const wordLists = definitions.map((definition) =>
    normalizedSearchText(definition)
      .split(/\s+/)
      .filter((word) => word !== ""),
  );
  const stem: string[] = [];
  const maxLen = Math.min(...wordLists.map((words) => words.length));
  for (let i = 0; i < maxLen; i++) {
    const word = wordLists[0]?.[i] ?? "";
    if (word === "" || !wordLists.every((words) => words[i] === word)) {
      break;
    }
    stem.push(word);
  }
  const joined = stem.join(" ");
  return stem.length >= 2 && joined.length >= 10 ? joined : "";
}

function axisFacetTexts(band: PickerBand): string[] {
  const texts = new Set<string>();
  for (const row of band.rows) {
    for (const axis of axes) {
      const facet = rowFacet(band, row, axis.name);
      if (!facet) {
        continue;
      }
      texts.add(facet.label);
      texts.add(facet.value);
    }
  }
  return [...texts].filter((text) => text.trim() !== "");
}

function bandHasAxisMarker(band: PickerBand): boolean {
  return axes.length > 0 && axisFacetTexts(band).length > 0;
}

function operationalDefinitionCarriedByAxis(band: PickerBand): boolean {
  const definition = normalizedSearchText(
    normalizedOperationalDefinition(band),
  );
  return axisFacetTexts(band).some((text) => {
    const facetText = normalizedSearchText(text);
    return facetText.length >= 3 && definition.includes(facetText);
  });
}

const suppressOperationalDefinitions = $derived.by(() => {
  const withDefinitions = filteredBands.filter(
    (band) => normalizedOperationalDefinition(band) !== "",
  );
  if (withDefinitions.length < 2) {
    return false;
  }
  const definitions = withDefinitions.map(normalizedOperationalDefinition);
  if (new Set(definitions).size === 1) {
    return true;
  }
  return (
    withDefinitions.every(bandHasAxisMarker) &&
    commonDefinitionStem(definitions) !== "" &&
    withDefinitions.every(operationalDefinitionCarriedByAxis)
  );
});

function pickerOperationalDefinition(band: PickerBand): string | null {
  if (suppressOperationalDefinitions) {
    return null;
  }
  const text = normalizedOperationalDefinition(band);
  return text === "" ? null : text;
}

/** Whether the active window starts BEFORE a row's data actually begins — the
 * "data starts late" warning trigger (#678). Only when a window is set, the row's
 * start year resolves (skip the open/unknown-start `0001-01-01` sentinel → `yearOf`
 * null or 1), and that start year is STRICTLY after the window's start. The data
 * start year, or null when no warning applies. */
function dataStartsLate(
  row: PickerRepresentation,
): { dataStart: number; windowStart: number } | null {
  if (!window) {
    return null;
  }
  const start = yearOf(row.from);
  // Skip unknown/open starts (the yearless sentinel reads as year 1): no real
  // "data begins later" claim there.
  if (start === null || row.from === "0001-01-01" || start <= window[0]) {
    return null;
  }
  return { dataStart: start, windowStart: window[0] };
}

/** The render model for ONE variable within its cluster: its leading identity
 * (`id`, from the cluster's `bandLabeling`), whether it is a single column (→ one
 * merged row, no subheading), the hoisted COLUMN chip + the quiet value-set context,
 * the adaptive per-row column labels, and whether EVERY one of its rows is out of the
 * active window (→ dim the subheading too). `showPrefix` is the cluster's prefix-hoist
 * flag. */
function bandView(band: PickerBand, id: BandLabel, showPrefix: boolean) {
  const labeling = labelingByBand.get(band.key);
  const single = band.rows.length === 1;
  // The hoisted constant delivery column → a prominent chip in the context. But when
  // the variable's IDENTITY already IS that column (a single-column member — its
  // primary is the column chip-link, whether single-row or a multi-population
  // subheading), it's shown once as that identity, so suppress the duplicate context
  // chip here.
  const column = id.primaryIsColumn ? null : (labeling?.column ?? null);
  // ALL-OUT: every row out of the active window → the (multi-column) subheading
  // greys at the variable level too. A 0-row band is NOT all-out (nothing to scope).
  const allOut =
    band.rows.length > 0 &&
    band.rows.every((r) => !representationInWindow(r, window));
  return {
    band,
    primary: id.primary,
    primaryIsColumn: id.primaryIsColumn,
    primaryIsFacet: id.primaryIsFacet,
    showPrefix,
    single,
    column,
    allOut,
    // The deep-link `?member=` focus (#678): mark this band when its key matches.
    focused: focusKey != null && band.key === focusKey,
    context: labeling?.headerContext ?? [],
    rowLabels: new Map((labeling?.rows ?? []).map((r) => [r.key, r])),
    // The superseded predecessor editions folded onto this chain head (#902).
    supersedes: band.supersedes ?? [],
    operationalDefinition: pickerOperationalDefinition(band),
  };
}

/** The render model per NAME-CLUSTER (#901): the cluster's heading name + whether to
 * show cluster headings at all (more than one cluster), and each member band's view
 * labeled by THAT cluster's `bandLabeling` (so each leads with its within-cluster
 * distinguisher, the name hoisted to the heading). One cluster → no headings, rendered
 * exactly as today. */
const view = $derived(
  clustered.clusters.map((cluster) => ({
    name: cluster.name,
    showHeading: clustered.showClusterHeadings,
    bands: cluster.bands.map((band, i) =>
      bandView(band, cluster.labeling.bands[i], cluster.labeling.showPrefix),
    ),
  })),
);

// ── Graph/time-band mode (#904) ──────────────────────────────────────────────
// The picker has two render modes:
//   - graph mode for small edge-bearing variable graphs, where succession context is
//     load-bearing and the cells remain selectable;
//   - the existing compact list for large, edge-less, or infeasible graphs.
// Keep this threshold deliberately conservative: the known dense `disponibel-inkomst`
// group (~53 members) must fall back to the list.
const GRAPH_MAX_NODES = 18;
const GRAPH_MAX_EDGES = 24;
const GRAPH_MAX_CELLS = 48;
const GRAPH_GUTTER_W = 188;
const GRAPH_TRACK_MIN = 360;
const GRAPH_TRACK_PAD = 14;
const GRAPH_LANE_BASE_H = 58;
const GRAPH_ROW_H = 46;
const GRAPH_CELL_H = 40;
const GRAPH_CODINGS_NUDGE_MIN_READABLE_W = 160;

function variableGraphNodes(g: RelationshipGraph): VariableGraphNode[] {
  return g.nodes.filter((n): n is VariableGraphNode => n.kind === "variable");
}

function graphNodeFqids(node: VariableGraphNode): string[] {
  return [
    ...(node.fqid == null ? [] : [node.fqid]),
    ...node.same_as.map((sa) => sa.fqid),
  ];
}

function graphNodeMatchesKey(
  node: VariableGraphNode,
  key: string | null | undefined,
): boolean {
  return key != null && graphNodeFqids(node).includes(key);
}

interface GraphCellCandidate {
  band: PickerBand;
  row: PickerRepresentation;
  columns: string[];
}

interface GraphCellMatch {
  band: PickerBand;
  row: PickerRepresentation;
  column: string;
}

function rowConcreteVariants(row: PickerRepresentation): string[] {
  return row.variantSegments && row.variantSegments.length > 0
    ? row.variantSegments.map((segment) => segment.variant)
    : [row.variant];
}

function graphMemberHrefForNode(node: VariableGraphNode): string | null {
  if (graphMemberHrefs == null) {
    return null;
  }
  for (const fqid of graphNodeFqids(node)) {
    const href = graphMemberHrefs[fqid];
    if (href) {
      return href;
    }
  }
  return null;
}

function cellMatchedColumns(
  row: PickerRepresentation,
  cell: RunCell,
): string[] {
  if (!rowConcreteVariants(row).includes(cell.variant)) {
    return [];
  }
  const columns = new Set(cell.columns);
  const out: string[] = [];
  if (columns.has(row.column)) {
    out.push(row.column);
  }
  for (const col of row.renamedColumns) {
    if (columns.has(col)) {
      out.push(col);
    }
  }
  return [...new Set(out)];
}

function graphCellCandidates(
  band: PickerBand,
  cell: RunCell,
): GraphCellCandidate[] {
  return band.rows
    .map((row) => ({ band, row, columns: cellMatchedColumns(row, cell) }))
    .filter((candidate) => candidate.columns.length > 0);
}

function graphFocusIsNavigable(g: RelationshipGraph): boolean {
  if (focusKey == null || graphMemberHrefs == null) {
    return true;
  }
  const focusedNode = variableGraphNodes(g).find((node) =>
    graphNodeMatchesKey(node, focusKey),
  );
  if (!focusedNode) {
    return false;
  }
  const focusedBand = graphOriginalBandForNode(focusedNode);
  return focusedBand?.href != null;
}

const graphBands = $derived(filteredBands);

function graphOriginalBandForNode(node: VariableGraphNode): PickerBand | null {
  for (const fqid of graphNodeFqids(node)) {
    const band = bands.find((b) => b.key === fqid);
    if (band) {
      return band;
    }
  }
  return null;
}

function graphCandidateIsOneToOne(
  candidates: GraphCellCandidate[],
  cell: RunCell,
): boolean {
  return (
    candidates.length === 1 &&
    candidates[0].columns.length === 1 &&
    cell.columns.length === 1 &&
    candidates[0].columns[0] === cell.columns[0]
  );
}

function graphCellFullyCoveredByBand(band: PickerBand, cell: RunCell): boolean {
  if (cell.columns.length === 0) {
    return true;
  }
  const covered = new Set<string>();
  for (const candidate of graphCellCandidates(band, cell)) {
    for (const column of candidate.columns) {
      covered.add(column);
    }
  }
  return cell.columns.every((column) => covered.has(column));
}

function graphTrackInnerWidthForScale(scale: YearScale | null): number {
  return scale
    ? Math.max(GRAPH_TRACK_MIN, (scale.maxYear - scale.minYear) * PX_PER_YEAR)
    : GRAPH_TRACK_MIN;
}

function graphXForScale(year: number, scale: YearScale): number {
  if (!Number.isFinite(year)) {
    return GRAPH_TRACK_PAD;
  }
  const span = scale.maxYear - scale.minYear || 1;
  return (
    GRAPH_TRACK_PAD +
    ((year - scale.minYear) / span) * graphTrackInnerWidthForScale(scale)
  );
}

function graphCellWidthForScale(
  cell: RunCell,
  scale: YearScale | null,
): number {
  if (!scale) {
    return CELL_MIN_W;
  }
  const fromYear =
    cell.openStart || !Number.isFinite(cell.fromYear)
      ? scale.minYear
      : cell.fromYear;
  const toYear =
    cell.openEnd || !Number.isFinite(cell.toYear) ? scale.maxYear : cell.toYear;
  return Math.max(
    CELL_MIN_W,
    graphXForScale(toYear, scale) - graphXForScale(fromYear, scale),
  );
}

function graphReadableWithCurrentRows(g: RelationshipGraph): boolean {
  const scale = yearScaleOf(g, vintageYear);
  for (const band of graphBands) {
    const node = variableGraphNodes(g).find((n) =>
      graphNodeMatchesKey(n, band.key),
    );
    if (!node) {
      return false;
    }
    for (const cell of cellsOf(node)) {
      const matches = graphCellCandidates(band, cell);
      if (matches.length !== 1) {
        continue;
      }
      const match = matches[0];
      const column = match.columns[0];
      if (
        column &&
        match.row.codingsVary &&
        graphCellWidthForScale(cell, scale) < GRAPH_CODINGS_NUDGE_MIN_READABLE_W
      ) {
        return false;
      }
    }
  }
  return true;
}

/** Strict graph-mode gate: selectable graph cells render only when they are a
 * lossless, one-to-one projection of the picker rows. Leaf-only graph context that has
 * no selectable picker row is still allowed, but renders as unavailable context rather
 * than becoming a checkbox. Group graphs remain stricter about non-member cells so the
 * picker cannot leak columns outside the browsed concept. */
function graphCoversEveryPickerRow(g: RelationshipGraph): boolean {
  if (graphBands.length === 0) {
    return false;
  }
  const nodes = variableGraphNodes(g);
  const bandNodes = new Map<string, VariableGraphNode>();
  const usedNodeIds = new Set<string>();
  for (const band of graphBands) {
    const matches = nodes.filter((node) => graphNodeMatchesKey(node, band.key));
    if (matches.length !== 1) {
      return false;
    }
    if (usedNodeIds.has(matches[0].id)) {
      return false;
    }
    usedNodeIds.add(matches[0].id);
    bandNodes.set(band.key, matches[0]);
  }

  for (const node of nodes) {
    if (graphMemberHrefs != null && graphOriginalBandForNode(node) == null) {
      return false;
    }
    const originalBand = graphOriginalBandForNode(node);
    if (
      !originalBand &&
      graphMemberHrefs != null &&
      graphMemberHrefForNode(node) != null &&
      cellsOf(node).length > 0
    ) {
      return false;
    }
  }

  const coveredRows = new Set<string>();
  for (const band of graphBands) {
    const node = bandNodes.get(band.key);
    if (!node) {
      return false;
    }
    const originalBand = graphOriginalBandForNode(node) ?? band;
    for (const cell of cellsOf(node)) {
      const originalMatches = graphCellCandidates(originalBand, cell);
      if (
        originalMatches.length > 0 &&
        !graphCandidateIsOneToOne(originalMatches, cell)
      ) {
        return false;
      }
      if (
        graphMemberHrefs != null &&
        originalMatches.length === 0 &&
        cell.columns.length > 0
      ) {
        return false;
      }
      const matches = graphCellCandidates(band, cell);
      if (matches.length > 0 && !graphCandidateIsOneToOne(matches, cell)) {
        return false;
      }
      if (matches.length === 1) {
        coveredRows.add(rowKey(matches[0].band, matches[0].row));
      } else if (originalMatches.length > 0 && !anyFilterActive) {
        return false;
      }
    }
  }
  return graphBands.every((band) =>
    band.rows.every((row) => coveredRows.has(rowKey(band, row))),
  );
}

function graphHasDrawableContext(g: RelationshipGraph): boolean {
  if (resolveEdges(g).length > 0) {
    return true;
  }
  return (
    graphMemberHrefs == null &&
    variableGraphNodes(g).some(
      (node) => node.same_as.length > 0 || cellsOf(node).length > 1,
    )
  );
}

function graphNodeWithVisibleStates(
  node: VariableGraphNode,
): VariableGraphNode {
  if (graphMemberHrefs == null) {
    return node;
  }
  const band = graphBandForNode(node);
  if (!band) {
    return node;
  }
  const visibleColumnsByVariant = new Map<string, Set<string>>();
  for (const row of band.rows) {
    for (const variant of rowConcreteVariants(row)) {
      let cols = visibleColumnsByVariant.get(variant);
      if (!cols) {
        cols = new Set<string>();
        visibleColumnsByVariant.set(variant, cols);
      }
      cols.add(row.column);
      for (const renamed of row.renamedColumns) {
        cols.add(renamed);
      }
    }
  }
  return {
    ...node,
    states: node.states.filter((state) => {
      if (!state.delivery_column_name) {
        return false;
      }
      return (
        visibleColumnsByVariant
          .get(state.variant)
          ?.has(state.delivery_column_name) ?? false
      );
    }),
  };
}

function graphForVisibleRows(g: RelationshipGraph): RelationshipGraph | null {
  if (graphMemberHrefs == null) {
    return g;
  }
  const memberNodes = variableGraphNodes(g);
  if (g.nodes.length !== memberNodes.length) {
    return null;
  }
  if (memberNodes.some((node) => graphOriginalBandForNode(node) == null)) {
    return null;
  }
  for (const node of memberNodes) {
    const band = graphBandForNode(node);
    if (!band) {
      continue;
    }
    const originalBand = graphOriginalBandForNode(node);
    if (!originalBand) {
      return null;
    }
    for (const cell of cellsOf(node)) {
      if (!graphCellFullyCoveredByBand(originalBand, cell)) {
        return null;
      }
      if (
        graphCellCandidates(band, cell).length > 0 &&
        !graphCandidateIsOneToOne(graphCellCandidates(originalBand, cell), cell)
      ) {
        return null;
      }
    }
  }
  const visibleNodeIds = new Set(
    memberNodes
      .filter((node) => graphBandForNode(node) != null)
      .map((node) => node.id),
  );
  const visibleGraph: RelationshipGraph = {
    ...g,
    nodes: g.nodes
      .filter((node) => visibleNodeIds.has(node.id))
      .map((node) =>
        node.kind === "variable" ? graphNodeWithVisibleStates(node) : node,
      ),
    edges: [],
    focus_id:
      g.focus_id != null && visibleNodeIds.has(g.focus_id) ? g.focus_id : null,
  };
  return {
    ...visibleGraph,
    edges: g.edges.filter(
      (edge) =>
        visibleNodeIds.has(edge.source) &&
        visibleNodeIds.has(edge.target) &&
        graphEdgeVisibleInGraph(edge, visibleGraph),
    ),
  };
}

function graphFitsPicker(g: RelationshipGraph): boolean {
  const renderGraph = graphForVisibleRows(g);
  if (!renderGraph) {
    return false;
  }
  const variableNodes = variableGraphNodes(renderGraph);
  const cellCount = variableNodes.reduce(
    (n, node) => n + cellsOf(node).length,
    0,
  );
  return (
    graphHasDrawableContext(g) &&
    renderGraph.nodes.length === variableNodes.length &&
    renderGraph.nodes.length <= GRAPH_MAX_NODES &&
    renderGraph.edges.length <= GRAPH_MAX_EDGES &&
    cellCount <= GRAPH_MAX_CELLS &&
    graphCoversEveryPickerRow(renderGraph) &&
    graphReadableWithCurrentRows(renderGraph) &&
    graphFocusIsNavigable(g)
  );
}

const useGraphMode = $derived(graph != null && graphFitsPicker(graph));
const graphRenderGraph = $derived.by((): RelationshipGraph | null => {
  if (!useGraphMode || !graph) {
    return null;
  }
  return graphForVisibleRows(graph);
});
const graphScale = $derived<YearScale | null>(
  graphRenderGraph ? yearScaleOf(graphRenderGraph, vintageYear) : null,
);
const graphTrackInnerW = $derived(graphTrackInnerWidthForScale(graphScale));
const graphTrackW = $derived(graphTrackInnerW + GRAPH_TRACK_PAD * 2);
const graphTicks = $derived(graphScale ? axisTicks(graphScale) : []);
const graphSuccessionEndpointIds = $derived.by((): Set<string> => {
  const ids = new Set<string>();
  for (const edge of graph?.edges ?? []) {
    if (edge.kind === "succession") {
      ids.add(edge.source);
      ids.add(edge.target);
    }
  }
  return ids;
});

function graphX(year: number): number {
  if (!graphScale || !Number.isFinite(year)) {
    return GRAPH_TRACK_PAD;
  }
  const span = graphScale.maxYear - graphScale.minYear || 1;
  return (
    GRAPH_TRACK_PAD + ((year - graphScale.minYear) / span) * graphTrackInnerW
  );
}

interface GraphLaneBox {
  rn: RenderNode;
  top: number;
  height: number;
  center: number;
  rowCount: number;
}

interface GraphRenderCluster {
  cluster: NodeCluster;
  edges: ResolvedEdge[];
  lanes: GraphLaneBox[];
  byId: Map<string, GraphLaneBox>;
  height: number;
}

function graphLaneHeight(rowCount: number): number {
  if (rowCount > 1) {
    return GRAPH_LANE_BASE_H + (rowCount - 1) * GRAPH_ROW_H;
  }
  return GRAPH_LANE_BASE_H;
}

const graphClusters = $derived.by((): GraphRenderCluster[] => {
  if (!graphRenderGraph) {
    return [];
  }
  const clusters = clustersOf(graphRenderGraph, graphScale);
  const out = clusters.map((cluster) => {
    let top = 0;
    const lanes = cluster.nodes.map((rn) => {
      const rowCount = graphLaneDisplayRowCount(rn);
      const height = graphLaneHeight(rowCount);
      const box = { rn, top, height, center: top + height / 2, rowCount };
      top += height;
      return box;
    });
    return {
      cluster,
      edges: [] as ResolvedEdge[],
      lanes,
      byId: new Map(lanes.map((l) => [l.rn.node.id, l])),
      height: top,
    };
  });
  const clusterOfNode = new Map<string, number>();
  out.forEach((rc, i) => {
    for (const rn of rc.cluster.nodes) {
      clusterOfNode.set(rn.node.id, i);
    }
  });
  for (const edge of resolveEdges(graphRenderGraph)) {
    const source = clusterOfNode.get(edge.source.id);
    const target = clusterOfNode.get(edge.target.id);
    if (source !== undefined && source === target) {
      out[source].edges.push(edge);
    }
  }
  return out;
});

function graphCellTop(
  laneHeight: number,
  row: number,
  rowCount: number,
): number {
  if (rowCount <= 1) {
    return (laneHeight - GRAPH_CELL_H) / 2;
  }
  const inset = (GRAPH_LANE_BASE_H - GRAPH_CELL_H) / 2;
  return inset + row * GRAPH_ROW_H;
}

function graphBandForNode(node: VariableGraphNode): PickerBand | null {
  for (const fqid of graphNodeFqids(node)) {
    const band = graphBands.find((b) => b.key === fqid);
    if (band) {
      return band;
    }
  }
  return null;
}

function graphCellMatch(
  lane: VariableLane,
  cell: RunCell,
): GraphCellMatch | null {
  const band = graphBandForNode(lane.node);
  if (!band) {
    return null;
  }
  const matches = graphCellCandidates(band, cell);
  if (matches.length !== 1 || matches[0].columns.length !== 1) {
    return null;
  }
  return { band, row: matches[0].row, column: matches[0].columns[0] };
}

function graphCellInWindow(cell: RunCell): boolean {
  if (!window) {
    return true;
  }
  const from = cell.openStart
    ? Number.NEGATIVE_INFINITY
    : Number.isFinite(cell.fromYear)
      ? cell.fromYear
      : Number.NEGATIVE_INFINITY;
  const to = cell.openEnd
    ? Number.POSITIVE_INFINITY
    : Number.isFinite(cell.toYear)
      ? cell.toYear
      : Number.POSITIVE_INFINITY;
  return from <= window[1] && window[0] <= to;
}

type GraphLaneItem =
  | {
      kind: "cell";
      cell: RunCell;
      match: GraphCellMatch | null;
      index: number;
      rowIndex: number;
    }
  | {
      kind: "row";
      band: PickerBand;
      row: PickerRepresentation;
      rowIndex: number;
    };

function graphLaneItems(rn: RenderNode): GraphLaneItem[] {
  if (rn.kind !== "variable") {
    return [];
  }
  const band = graphBandForNode(rn.node);
  const matchedRows = new Set<string>();
  const items: GraphLaneItem[] = [];
  for (const [index, cell] of rn.cells.entries()) {
    const match = graphCellMatch(rn, cell);
    if (match) {
      matchedRows.add(rowKey(match.band, match.row));
      items.push({ kind: "cell", cell, match, index, rowIndex: cell.row });
    } else if (
      graphMemberHrefs == null &&
      (!band || band.rows.length === 0 || cell.columns.length === 0)
    ) {
      items.push({
        kind: "cell",
        cell,
        match: null,
        index,
        rowIndex: cell.row,
      });
    }
  }
  if (!band) {
    return items;
  }
  const cellRows = rn.cells.length > 0 ? rn.rowCount : 0;
  let nextRow = cellRows;
  for (const row of band.rows) {
    if (matchedRows.has(rowKey(band, row))) {
      continue;
    }
    items.push({ kind: "row", band, row, rowIndex: nextRow });
    nextRow += 1;
  }
  return items;
}

function graphLaneDisplayRowCount(rn: RenderNode): number {
  const maxRow = graphLaneItems(rn).reduce(
    (max, item) => Math.max(max, item.rowIndex),
    -1,
  );
  return Math.max(1, maxRow + 1);
}

function graphLaneItemKey(item: GraphLaneItem): string {
  if (item.kind === "cell") {
    return `cell:${item.cell.runId}:${item.cell.variant}:${item.cell.columns.join("|")}`;
  }
  return `row:${rowKey(item.band, item.row)}`;
}

function graphLaneItemLeft(item: GraphLaneItem): number {
  if (item.kind === "row") {
    return GRAPH_TRACK_PAD;
  }
  return graphScale
    ? graphX(item.cell.fromYear)
    : GRAPH_TRACK_PAD + item.index * (CELL_MIN_W + 8);
}

function graphLaneItemWidth(item: GraphLaneItem): number {
  if (item.kind === "row") {
    return graphTrackInnerW;
  }
  return graphScale
    ? Math.max(
        CELL_MIN_W,
        graphX(item.cell.toYear) - graphX(item.cell.fromYear),
      )
    : CELL_MIN_W;
}

interface GraphEdgeEndpoint {
  left: number;
  right: number;
  centerX: number;
  y: number;
}

interface GraphEdgeSegment {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  representation: boolean;
}

type GraphRepresentationEndpointRole = "source" | "target";

function graphEndpointYearScore(
  cell: RunCell,
  role: GraphRepresentationEndpointRole,
  effectiveYear: number | null | undefined,
): number {
  if (effectiveYear == null || !Number.isFinite(effectiveYear)) {
    return 0;
  }
  if (role === "source") {
    if (cell.toYear < effectiveYear) {
      return effectiveYear - cell.toYear;
    }
    if (cell.fromYear <= effectiveYear && effectiveYear <= cell.toYear) {
      return 0.5;
    }
    return 10000 + Math.abs(cell.fromYear - effectiveYear);
  }
  if (cell.fromYear >= effectiveYear) {
    return cell.fromYear - effectiveYear;
  }
  if (cell.fromYear <= effectiveYear && effectiveYear <= cell.toYear) {
    return 0.5;
  }
  return 10000 + Math.abs(effectiveYear - cell.toYear);
}

function graphRepresentationEdgeEndpoint(
  lane: GraphLaneBox,
  column: string | null | undefined,
  variant: string | null | undefined,
  role: GraphRepresentationEndpointRole,
  effectiveYear: number | null | undefined,
): GraphEdgeEndpoint | null {
  if (lane.rn.kind !== "variable" || column == null) {
    return null;
  }
  const candidates = lane.rn.cells
    .map((cell, index) => ({ cell, index }))
    .filter(
      ({ cell }) =>
        (variant == null || cell.variant === variant) &&
        cell.columns.some((candidate) => graphColumnMatches(candidate, column)),
    )
    .sort(
      (a, b) =>
        graphEndpointYearScore(a.cell, role, effectiveYear) -
          graphEndpointYearScore(b.cell, role, effectiveYear) ||
        a.index - b.index,
    );
  const match = candidates[0];
  if (!match) {
    return null;
  }
  const item: GraphLaneItem = {
    kind: "cell",
    cell: match.cell,
    match: null,
    index: match.index,
    rowIndex: match.cell.row,
  };
  const left = GRAPH_GUTTER_W + graphLaneItemLeft(item);
  const width = graphLaneItemWidth(item);
  const top = graphCellTop(lane.height, match.cell.row, lane.rowCount);
  return {
    left,
    right: left + width,
    centerX: left + width / 2,
    y: lane.top + top + GRAPH_CELL_H / 2,
  };
}

function graphEdgeSegment(
  edge: ResolvedEdge,
  source: GraphLaneBox,
  target: GraphLaneBox,
): GraphEdgeSegment {
  const sourceEndpoint = graphRepresentationEdgeEndpoint(
    source,
    edge.edge.source_column,
    edge.edge.variant,
    "source",
    edge.edge.effective_year,
  );
  const targetEndpoint = graphRepresentationEdgeEndpoint(
    target,
    edge.edge.target_column,
    edge.edge.variant,
    "target",
    edge.edge.effective_year,
  );
  if (sourceEndpoint && targetEndpoint) {
    const forward = sourceEndpoint.centerX <= targetEndpoint.centerX;
    let x1 = forward ? sourceEndpoint.right : sourceEndpoint.left;
    let x2 = forward ? targetEndpoint.left : targetEndpoint.right;
    const endpointGap = forward ? x2 - x1 : x1 - x2;
    if (endpointGap < 8) {
      x1 = sourceEndpoint.centerX;
      x2 = targetEndpoint.centerX;
    }
    if (
      Math.abs(x2 - x1) < 1 &&
      Math.abs(targetEndpoint.y - sourceEndpoint.y) < 1
    ) {
      x2 += 22;
    }
    return {
      x1,
      y1: sourceEndpoint.y,
      x2,
      y2: targetEndpoint.y,
      representation: true,
    };
  }
  return {
    x1: GRAPH_GUTTER_W - 10,
    y1: source.center,
    x2: GRAPH_GUTTER_W - 10,
    y2: target.center,
    representation: false,
  };
}

function graphEdgeLabelLeft(segment: GraphEdgeSegment): number {
  if (!segment.representation) {
    return GRAPH_GUTTER_W + 6;
  }
  return Math.max(GRAPH_GUTTER_W + 6, (segment.x1 + segment.x2) / 2 + 6);
}

function graphCellSubLabel(cell: RunCell, column: string): string {
  return cell.label === column || cell.columns.includes(cell.label)
    ? ""
    : cell.label;
}

function graphCellDisplayWindow(
  cell: RunCell,
  match: GraphCellMatch,
): string | null {
  return match.row.variantSegments && match.row.variantSegments.length > 1
    ? match.row.period
    : cell.window;
}

function graphCellTitle(cell: RunCell, match: GraphCellMatch): string {
  const column = match.column;
  const sub = graphCellSubLabel(cell, column);
  return [column, sub || null, graphCellDisplayWindow(cell, match)]
    .filter(Boolean)
    .join(" · ");
}

function graphRenameHint(match: GraphCellMatch): string[] {
  return match.column === match.row.column ? match.row.renamedColumns : [];
}

function graphNodeFocused(rn: RenderNode): boolean {
  return (
    rn.node.id === graph?.focus_id ||
    (focusKey != null &&
      rn.node.kind === "variable" &&
      graphNodeMatchesKey(rn.node, focusKey))
  );
}

function graphNodeIsRenamed(rn: RenderNode): boolean {
  return (
    rn.kind === "variable" &&
    rn.node.states.length === 0 &&
    graphSuccessionEndpointIds.has(rn.node.id)
  );
}

function graphNodeMuted(rn: RenderNode): boolean {
  return (
    rn.kind === "variable" &&
    (graphBandForNode(rn.node) == null || graphNodeIsRenamed(rn))
  );
}

function graphNodeLabel(rn: RenderNode): string {
  if (rn.kind !== "variable") {
    return rn.node.label;
  }
  if (rn.node.facets.length > 0) {
    return facetLabelJoin(rn.node.facets);
  }
  if ((rn.node.group_label != null || graphNodeIsRenamed(rn)) && rn.node.fqid) {
    const label = leafSlug(rn.node.fqid);
    return graphNodeIsRenamed(rn) ? `${label} (renamed)` : label;
  }
  return rn.node.label;
}

function graphNodeHref(rn: RenderNode): string | null {
  if (rn.kind === "variable") {
    const band = graphBandForNode(rn.node);
    if (band?.href) {
      return band.href;
    }
    const memberHref = graphMemberHrefForNode(rn.node);
    if (memberHref) {
      return memberHref;
    }
  }
  if (rn.node.id === graph?.focus_id || rn.node.fqid == null) {
    return null;
  }
  return catalogHref(rn.node.fqid);
}

function graphEdgeLabel(edge: ResolvedEdge): string | null {
  const representation =
    edge.edge.source_column != null && edge.edge.target_column != null
      ? edge.edge.source_column === edge.edge.target_column
        ? edge.edge.source_column
        : `${edge.edge.source_column} → ${edge.edge.target_column}`
      : null;
  const base = representation ?? edge.edge.label;
  if (edge.edge.effective_year != null) {
    return base
      ? `${base} · ${edge.edge.effective_year}`
      : `→ ${edge.edge.effective_year}`;
  }
  return base;
}

function graphLaneA11y(rn: RenderNode): string {
  if (rn.kind !== "variable") {
    return rn.node.label;
  }
  const items = graphLaneItems(rn);
  if (items.length === 0) {
    return graphNodeIsRenamed(rn)
      ? "renamed predecessor with no live states"
      : "no delivered state rows";
  }
  return items
    .map((item) => {
      if (item.kind === "row") {
        // A flat (unmatched) folded row: label with the FAMILY, not the head
        // `row.variant` (#376) — the cell branch below uses the concrete `cell.variant`.
        return [
          item.row.column,
          item.row.period,
          pickerRowVariantFamilyLabel(item.row),
        ]
          .filter(Boolean)
          .join(", ");
      }
      if (item.match) {
        const sub = graphCellSubLabel(item.cell, item.match.column);
        return [
          item.match.column,
          sub || null,
          graphCellDisplayWindow(item.cell, item.match),
          item.cell.variant,
        ]
          .filter(Boolean)
          .join(", ");
      }
      return [item.cell.label, item.cell.window, item.cell.variant]
        .filter(Boolean)
        .join(", ");
    })
    .join("; ");
}

/** A row's facet DIMENSION markers (#908): one `{ axis, value }` per declared axis
 * the row's column carries a facet on (axis = the curator label, value = the facet
 * label), in `axes` order. Generalizes the quiet single facet line into an
 * axis-named marker so the user sees WHAT KIND of dimension (e.g. "Hushållsbegrepp:
 * Individ") distinguishes the row. Empty for a row with no structured facets (the
 * binding leaf, or an axis-less group) — the existing `facetByColumn` line still
 * renders there. */
function rowFacetMarkers(
  band: PickerBand,
  column: string,
): { name: string; axis: string; value: string }[] {
  const out: { name: string; axis: string; value: string }[] = [];
  for (const axis of axes) {
    // Reuse catalog's single facetsByColumn[column] lookup; the marker shape maps
    // the curator label (display) over the stable axis NAME (Svelte #each key).
    const f = rowFacet(band, { column } as PickerRepresentation, axis.name);
    if (f) {
      out.push({ name: axis.name, axis: axis.label, value: f.label });
    }
  }
  return out;
}

/** Selected columns currently HIDDEN by the active filters (#908) — a presentation
 * lens never silently drops a selection, so the footer signals that N of the
 * committed columns aren't visible. Compared by namespaced key against the visible
 * set. */
const hiddenSelectedCount = $derived.by((): number => {
  if (!anyFilterActive) {
    return 0;
  }
  const visible = new Set(allKeys);
  for (const { band, row } of visibleHistoryRows) {
    visible.add(rowKey(band, row));
  }
  let n = 0;
  for (const { band, row } of [...stagedAdds, ...stagedRemoves]) {
    if (!visible.has(rowKey(band, row))) {
      n += 1;
    }
  }
  return n;
});

const footerLabel = $derived.by(() => {
  const parts: string[] = [];
  if (selectedCount > 0) {
    parts.push(
      `+${selectedCount} ${selectedCount === 1 ? "column" : "columns"}`,
    );
  }
  if (removeCount > 0) {
    parts.push(`-${removeCount} ${removeCount === 1 ? "column" : "columns"}`);
  }
  if (periodChangeCount > 0) {
    parts.push(
      `${periodChangeCount} ${periodChangeCount === 1 ? "period change" : "period changes"}`,
    );
  }
  if (parts.length === 0) {
    return "";
  }
  const label = parts.join(" · ");
  return (
    label +
    (hiddenSelectedCount > 0
      ? ` (${hiddenSelectedCount} hidden by filters)`
      : "")
  );
});

/** Navigate an in-picker identity link (the column chip / subhead title) through
 * the SPA ROUTER. The link sits inside a <label> wrapping the row's checkbox, so a
 * plain click would (a) toggle the checkbox and (b) — because we must keep it from
 * toggling — previously `stopPropagation`'d, which ALSO stopped the app-level
 * `use:link` delegated handler from intercepting the bubbled click → a full page
 * reload (losing app state, tripping the dirty-project beforeunload). Instead:
 * `preventDefault()` (kills the label toggle) + navigate via the router directly.
 * The bubbled click still reaches `use:link`'s `onNavClick`, but it early-returns on
 * `defaultPrevented`, so there's no double navigation. Modifier / non-primary
 * clicks (open-in-new-tab etc.) fall through to the browser, matching `onNavClick`. */
function navigateChip(event: MouseEvent, href: string): void {
  if (
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  ) {
    return;
  }
  event.preventDefault();
  router.navigate(href);
}

/** The deep-link target for a row's "codings vary" nudge (#905): the value-set
 * viewer focused on that `(variant, column)` coding, scrolled to the States
 * section. The `codes` param carries the CONCRETE-segment IDENTITY (`encodeCodesParam`
 * → `variant::column`), NOT just the column — two rows can share one delivery column
 * across different variants/populations, so the focus must target the clicked cell's
 * coding, not another variant's latest era. `variant` is the CONCRETE segment that
 * delivered `column` (#376): the head `row.variant` for a plain/latest-era row, but the
 * matched `cell.variant` for a folded-family graph cell whose column belongs to a
 * PREDECESSOR era — collapsing to `row.variant` there would deep-link to a
 * `(variant, column)` pair the successor never delivered. This is a dedicated `?codes=` encoding,
 * distinct from the `?variant` RESOLUTION modifier (which narrows the picker + drives
 * the "Narrowed by" chips), so the focus never perturbs the resolution.
 *
 * The nudge means "this column's coding changed OVER TIME — see the value sets", which
 * is inherently a FULL-HISTORY inspection. So the href carries ONLY the focus and
 * DROPS any inherited query (`?period`/`?variant`/…): a period-narrowed leaf, when
 * focused via `?codes`, must show the column's full coding history, not the
 * period-scoped (and possibly out-of-era) subset.
 *   - GROUP view (`band.href` set → the member's own leaf): the member's PATH only
 *     (`band.href.split("?")[0]`), discarding any `?period` carried by `memberHref`.
 *   - BINDING leaf (`band.href` undefined → already this page): the CURRENT path with
 *     NO search (`globalThis.location.pathname`), discarding the live `?period`/
 *     `?variant`.
 * Both branches yield a clean `<path>?codes=<variant>::<column>#states-heading`; the
 * `#states-heading` hash targets the `<section id="states-heading">` anchor
 * BindingLeafView keeps; the router preserves both query + hash on navigate. */
function codingsVaryHref(
  band: PickerBand,
  row: PickerRepresentation,
  column = row.column,
  variant = row.variant,
): string {
  const codes = encodeCodesParam(variant, column);
  // `band.href` (group branch) may carry `?period=…` (memberHref); the leaf branch
  // reads the live path off `globalThis` (`window` is the period-window prop here,
  // shadowing the global). Either way, take ONLY the path — drop the query.
  const path = band.href
    ? band.href.split("?")[0]
    : globalThis.location.pathname;
  // Emit `codes` as the SOLE param via URLSearchParams so the value round-trips through
  // `router.getQueryParam` (one decode) back to the `encodeCodesParam` composite that
  // `parseCodesParam` expects.
  const search = new URLSearchParams();
  search.set("codes", codes);
  return `${path}?${search.toString()}#states-heading`;
}
</script>

<!-- The delivery COLUMN chip (#678): the main selection signal, rendered as a small
     categorical pill (mono text + a subtle --cat-var tint, distinct from the rost
     selection accent) wherever a delivery column shows. When `href` is set (a single-
     column variable's IDENTITY chip in the group view), the chip is a NAVIGATION LINK
     to that variable's leaf page — clicking it navigates (via the SPA router, see
     `navigateChip`) and must NOT toggle the row selection. Otherwise a plain <code>. -->
{#snippet colChip(text: string, href?: string)}
  {#if href}
    <a
      class="col-chip link"
      {href}
      title={`Open ${text}`}
      onclick={(e) => navigateChip(e, href)}
      >{text}<span class="chip-arrow" aria-hidden="true">↗</span></a
    >
  {:else}
    <code class="col-chip" title={`Delivery column ${text}`}>{text}</code>
  {/if}
{/snippet}

<!-- The SUPERSEDED-editions history (#902): an inter-variable succession fold leads
     with the LATEST edition (this band) and tucks its superseded predecessor(s) behind
     a quiet disclosure — "supersedes <name>" — rather than rendering each as a co-equal
     selectable band. Each predecessor stays reachable via its own leaf-page link and,
     when rows are available, exposes era-specific selection controls inside the
     disclosure (#926). Closed by default; a thin, low-key affordance, never card
     chrome. -->
<!-- The sequential-RENAME progression hint (#902): a column delivered under several
     names over non-overlapping eras (`DINF` → `DINF83` → `DINF86`) collapses to ONE row
     led by the latest column; this quiet sub-text names the superseded column(s) so the
     rename is legible without the full Gantt era view (#904). Empty for an ordinary
     single column or a genuinely parallel column. -->
{#snippet renameHint(renamed: string[])}
  {#if renamed.length > 0}
    <span class="rename-hint" title={`Earlier delivery columns: ${renamed.join(", ")}`}
      >was {renamed.join(", ")}</span
    >
  {/if}
{/snippet}

{#snippet historyDisclosure(
  supersedes: {
    name: string;
    href: string;
    effectiveYear: number | null;
    band?: Omit<PickerBand, "supersedes">;
  }[],
)}
  {#if supersedes.length > 0}
    <details class="history">
      <summary
        >supersedes {supersedes.length}
        {supersedes.length === 1 ? "edition" : "editions"}</summary
      >
      <ul class="history-list">
        {#each supersedes as p (p.href)}
          {@const predecessorBand = p.band}
          {@const predecessorRows = predecessorBand
            ? filteredRowsForBand(predecessorBand)
            : []}
          <li>
            <a
              class="history-link"
              href={p.href}
              onclick={(e) => navigateChip(e, p.href)}>{p.name}</a
            >{#if p.effectiveYear != null}<span class="history-until"
                >until {p.effectiveYear}</span
              >{/if}
            {#if predecessorBand && predecessorRows.length > 0}
              <ul class="history-rows" aria-label={`Selectable rows for ${p.name}`}>
                {#each predecessorRows as row (row.key)}
                  {@const checked = rowChecked(predecessorBand, row)}
                  {@const stage = rowStage(predecessorBand, row)}
                  {@const inWindow = representationInWindow(row, window)}
                  <li>
                    <label
                      class="history-row"
                      class:selected={checked}
                      class:committed={stage === "committed"}
                      class:staged-add={stage === "staged-add"}
                      class:staged-remove={stage === "staged-remove"}
                      class:dimmed={!inWindow}
                    >
                      <input
                        type="checkbox"
                        class="cbox"
                        disabled={applying || !rowCanToggle(predecessorBand, row)}
                        checked={checked}
                        onchange={() => toggleRow(predecessorBand, row)}
                      />
                      <span class="history-row-main">
                        {@render colChip(row.column)}
                        {@render renameHint(row.renamedColumns)}
                        {#if stage !== "none"}
                          {@render stageTag(stage)}
                        {/if}
                      </span>
                      {#if row.codingsVary}
                        {@render codingsVaryNudge(predecessorBand, row)}
                      {/if}
                      {#if inWindow}
                        {@render lateWarn(row)}
                      {/if}
                      {#if row.period}
                        <span class="period">{row.period}</span>
                      {/if}
                    </label>
                  </li>
                {/each}
              </ul>
            {/if}
          </li>
        {/each}
      </ul>
    </details>
  {/if}
{/snippet}

<!-- The OPERATIONAL-DEFINITION line (#892/#932): a member variable's per-(split-)variable
     distinguishing text — what tells parallel concept-group members apart when their name/
     definition coincide (e.g. owner vs previous-owner näringsgren). A quiet prose line under
     the identity, rendered only when it adds member-level distinction in the picker. -->
{#snippet opDefLine(text: string)}
  <span class="op-def">
    <span class="op-def-text">{text}</span>
  </span>
{/snippet}

<!-- The DATA-STARTS-LATE warning (#678): a quiet --warn marker shown immediately
     before the period when the active window starts BEFORE this column's data begins
     (the user asked from <windowStart> but data only starts <dataStart>). A text glyph
     (no icon webfont); kept outside the right-aligned year text so it never breaks the
     tabular-nums alignment. Only on IN-window rows — a fully-out row is already dimmed. -->
{#snippet lateWarn(row: PickerRepresentation)}
  {@const late = dataStartsLate(row)}
  {#if late}
    {@const msg = `Data starts ${late.dataStart} — your selected period begins ${late.windowStart}`}
    <span class="late-warn" title={msg} aria-label={msg}>⚠</span>
  {/if}
{/snippet}

{#snippet stageTag(stage: RowStage)}
  {#if stage === "committed"}
    <Tag tone="info">
      {#snippet glyph()}{rowStageGlyph(stage)}{/snippet}
      {rowStageLabel(stage)}
    </Tag>
  {:else if stage === "staged-add"}
    <Tag tone="ok">
      {#snippet glyph()}{rowStageGlyph(stage)}{/snippet}
      {rowStageLabel(stage)}
    </Tag>
  {:else}
    <Tag tone="warn">
      {#snippet glyph()}{rowStageGlyph(stage)}{/snippet}
      {rowStageLabel(stage)}
    </Tag>
  {/if}
{/snippet}

<!-- The "codings vary" nudge (#905/#1058): a quiet DEEP LINK (no longer a passive span)
     to the value-set viewer focused on this row/cell coding (`?codes=<variant>::<column>`
     + `#states-heading`). A nudge, not a control — token-styled, must not dominate the
     row. Routed through `navigateChip` (preventDefault + SPA-router navigate) because
     it sits inside the row's <label>: a plain click would toggle the checkbox AND a
     stopPropagation would full-reload the app (same reasoning as `colChip`). A real
     anchor keeps it keyboard-accessible; the title/aria are unchanged. -->
{#snippet codingsVaryNudge(
  band: PickerBand,
  row: PickerRepresentation,
  column = row.column,
  variant = row.variant,
)}
  {@const href = codingsVaryHref(band, row, column, variant)}
  <a
    class="codings-vary"
    {href}
    title="Coding changes over time — see the value sets"
    aria-label="Coding changes over time — see the value sets"
    onclick={(e) => navigateChip(e, href)}>codings vary</a
  >
{/snippet}

<!-- One dimension FILTER fieldset (#908): a tracked micro-label naming the dimension
     KIND (an axis label, "Population", or "Coding") over its pill-checkboxes. The
     pills reuse the #819 navigator pattern — a visually-hidden native checkbox
     (keyboard + a11y + labelled) under a selectable accent chip — so axis identity is
     carried by TEXT (the legend), never hue. Multi-select within a dimension (OR),
     AND across dimensions. Filter-only: it narrows the visible rows, never the
     selection or the commit. -->
{#snippet dimFilter(dim: PickerDimension)}
  <fieldset class="dim-filter">
    <legend>
      <span class="dim-kind">{dim.label}</span>
    </legend>
    <div class="filter-options">
      {#each dim.values as v (v.value)}
        <label class="filter-pill" class:on={isFilterOn(dim.key, v.value)}>
          <input
            class="visually-hidden"
            type="checkbox"
            checked={isFilterOn(dim.key, v.value)}
            onchange={() => toggleFilter(dim.key, v.value)}
          />
          <span>{v.label}</span>
        </label>
      {/each}
    </div>
  </fieldset>
{/snippet}

{#snippet graphPicker()}
  <div class="graph-picker" role="group" aria-label="Graph column picker">
    {#each graphClusters as { cluster, edges: cEdges, lanes, byId, height: stackH }, ci (cluster.groupKey ?? `u${ci}`)}
      <div class="graph-cluster">
        {#if cluster.label}
          <h3 class="graph-cluster-heading">{cluster.label}</h3>
        {/if}

        <div
          class="graph-timeline"
          role="group"
          aria-label={cluster.label
            ? `Graph picker for ${cluster.label}: ${cluster.nodes.length} variables`
            : `Graph picker: ${cluster.nodes.length} variables`}
        >
          <div
            class="graph-grid"
            style={`--graph-track-w:${graphTrackW}px; --graph-gutter-w:${GRAPH_GUTTER_W}px;`}
          >
            {#if graphScale}
              <div class="graph-axis-row" aria-hidden="true">
                <div class="graph-axis-gutter"></div>
                <div class="graph-axis-track">
                  {#each graphTicks as tick (tick.year)}
                    <span class="graph-tick" style={`left:${graphX(tick.year)}px`}
                      >{tick.label}</span
                    >
                  {/each}
                </div>
              </div>
            {/if}

            <div class="graph-lanes" style={`height:${stackH}px`}>
              {#if graphScale}
                <div class="graph-gridlines" aria-hidden="true">
                  {#each graphTicks as tick (tick.year)}
                    <span
                      class="graph-gridline"
                      style={`left:${graphX(tick.year)}px`}
                    ></span>
                  {/each}
                </div>
              {/if}

              <svg
                class="graph-connectors"
                width={GRAPH_GUTTER_W + graphTrackW}
                height={stackH}
                aria-hidden="true"
              >
                <defs>
                  <marker
                    id={`picker-arrow-${ci}`}
                    viewBox="0 0 8 8"
                    refX="6"
                    refY="4"
                    markerWidth="6"
                    markerHeight="6"
                    orient="auto"
                  >
                    <path d="M0,0 L8,4 L0,8 z" class="graph-arrow-head" />
                  </marker>
                </defs>
                {#each cEdges as edge (edge.edge.id)}
                  {@const source = byId.get(edge.source.id)}
                  {@const target = byId.get(edge.target.id)}
                  {#if source && target}
                    {@const segment = graphEdgeSegment(edge, source, target)}
                    <line
                      data-edge-id={edge.edge.id}
                      x1={segment.x1}
                      y1={segment.y1}
                      x2={segment.x2}
                      y2={segment.y2}
                      class="graph-edge"
                      class:representation={segment.representation}
                      marker-end={`url(#picker-arrow-${ci})`}
                    />
                  {/if}
                {/each}
              </svg>

              {#each cEdges as edge (edge.edge.id)}
                {@const source = byId.get(edge.source.id)}
                {@const target = byId.get(edge.target.id)}
                {@const label = graphEdgeLabel(edge)}
                {#if label && source && target}
                  {@const segment = graphEdgeSegment(edge, source, target)}
                  <div
                    class="graph-reason"
                    style={`top:${(segment.y1 + segment.y2) / 2}px; left:${graphEdgeLabelLeft(segment)}px`}
                    title={label}
                    aria-hidden="true"
                  >
                    {label}
                  </div>
                {/if}
              {/each}

              {#each lanes as { rn, top, height, rowCount } (rn.node.id)}
                {@const focused = graphNodeFocused(rn)}
                {@const muted = graphNodeMuted(rn)}
                {@const href = graphNodeHref(rn)}
                <div
                  class="graph-lane"
                  class:focused
                  class:muted
                  style={`top:${top}px; height:${height}px`}
                >
                  <div class="graph-gutter">
                    <span class="graph-marker" class:focused></span>
                    <span class="graph-gutter-text">
                      {#if href}
                        <a class="graph-name" {href} title={rn.node.label}
                          >{graphNodeLabel(rn)}</a
                        >
                      {:else}
                        <span class="graph-name" title={rn.node.label}
                          >{graphNodeLabel(rn)}</span
                        >
                      {/if}
                      {#if rn.kind === "variable" && rn.node.fqid}
                        <span class="graph-slug">{leafSlug(rn.node.fqid)}</span>
                      {/if}
                      {#if focused}
                        <span class="graph-viewed">viewed</span>
                      {/if}
                      <span class="visually-hidden">{graphLaneA11y(rn)}</span>
                    </span>
                    {#if rn.kind === "variable" && rn.node.same_as.length > 0}
                      <span class="graph-same-as">
                        <span class="graph-sa-prefix">also in</span>
                        {#each rn.node.same_as as sa (sa.fqid)}
                          <a class="graph-sa-chip" href={catalogHref(sa.fqid)}
                            >{sa.register}</a
                          >
                        {/each}
                      </span>
                    {/if}
                  </div>

                  <div class="graph-track" style={`width:${graphTrackW}px`}>
                    {#if rn.kind === "variable"}
                      {#each graphLaneItems(rn) as item (graphLaneItemKey(item))}
                        {@const left = graphLaneItemLeft(item)}
                        {@const width = graphLaneItemWidth(item)}
                        {@const cellTopValue = graphCellTop(
                          height,
                          item.rowIndex,
                          rowCount,
                        )}
                        {#if item.kind === "cell" && item.match}
                          {@const band = item.match.band}
                          {@const row = item.match.row}
                          {@const column = item.match.column}
                          {@const cell = item.cell}
                          {@const checked = rowChecked(band, row)}
                          {@const stage = rowStage(band, row)}
                          {@const inWindow = graphCellInWindow(cell)}
                          {@const cellSub = graphCellSubLabel(cell, column)}
                          {@const facetMarkers = rowFacetMarkers(band, column)}
                          <label
                            class="graph-cell"
                            class:selected={checked}
                            class:committed={stage === "committed"}
                            class:staged-add={stage === "staged-add"}
                            class:staged-remove={stage === "staged-remove"}
                            class:dimmed={!inWindow}
                            class:open-start={cell.openStart}
                            class:open-end={cell.openEnd}
                            style={`left:${left}px; width:${width}px; top:${cellTopValue}px`}
                            title={graphCellTitle(cell, item.match)}
                          >
                            <input
                              type="checkbox"
                              class="cbox"
                              disabled={applying || !rowCanToggle(band, row)}
                              checked={checked}
                              onchange={() => toggleRow(band, row)}
                            />
                            <span class="graph-cell-main">
                              {@render colChip(column)}
                              {#if cellSub}
                                <span class="graph-cell-sub">{cellSub}</span>
                              {/if}
                              {#if facetMarkers.length > 0}
                                <span class="facet-markers graph-facet-markers">
                                  {#each facetMarkers as m (m.name)}
                                    <span class="facet-marker"
                                      ><span class="dim-kind facet">{m.axis}</span
                                      >{m.value}</span
                                    >
                                  {/each}
                                </span>
                              {/if}
                              {@render renameHint(graphRenameHint(item.match))}
                            </span>
                            {#if stage !== "none"}
                              {@render stageTag(stage)}
                            {/if}
                            {#if row.codingsVary}
                              {@render codingsVaryNudge(
                                band,
                                row,
                                column,
                                cell.variant,
                              )}
                            {/if}
                            {#if inWindow}
                              {@render lateWarn(row)}
                            {/if}
                            {#if graphCellDisplayWindow(cell, item.match)}
                              <span class="graph-cell-window"
                                >{graphCellDisplayWindow(cell, item.match)}</span
                              >
                            {/if}
                          </label>
                        {:else if item.kind === "cell"}
                          {@const cell = item.cell}
                          <div
                            class="graph-cell unavailable"
                            class:open-start={cell.openStart}
                            class:open-end={cell.openEnd}
                            style={`left:${left}px; width:${width}px; top:${cellTopValue}px`}
                            title={[cell.label, cell.window]
                              .filter(Boolean)
                              .join(" · ")}
                          >
                            <span class="graph-cell-main">
                              <span class="graph-unavailable-label">{cell.label}</span>
                            </span>
                            {#if cell.window}
                              <span class="graph-cell-window">{cell.window}</span>
                            {/if}
                          </div>
                        {:else}
                          {@const row = item.row}
                          <div
                            class="graph-cell unavailable dimmed"
                            style={`left:${left}px; width:${width}px; top:${cellTopValue}px`}
                            title={[row.column, row.period]
                              .filter(Boolean)
                              .join(" · ")}
                          >
                            <span class="graph-cell-main">
                              {@render colChip(row.column)}
                              {#if row.valueSetLabel}
                                <span class="graph-cell-sub">{row.valueSetLabel}</span>
                              {/if}
                            </span>
                            {#if row.period}
                              <span class="graph-cell-window">{row.period}</span>
                            {/if}
                          </div>
                        {/if}
                      {/each}
                    {/if}
                  </div>
                </div>
              {/each}
            </div>
          </div>
        </div>

        {#if cEdges.length > 0}
          <ul class="graph-fallback">
            {#each cEdges as edge (edge.edge.id)}
              {@const source = byId.get(edge.source.id)?.rn}
              {@const target = byId.get(edge.target.id)?.rn}
              <li>
                {source ? graphNodeLabel(source) : edge.source.label}
                →
                {target ? graphNodeLabel(target) : edge.target.label}
                {#if graphEdgeLabel(edge)}({graphEdgeLabel(edge)}){/if}
              </li>
            {/each}
          </ul>
        {/if}
      </div>
    {/each}
  </div>
{/snippet}

{#if totalRows > 0 || useGraphMode || graphMemberHrefs != null}
<div class="rep-picker">
  {#if dimensions.length > 0}
    <!-- The per-dimension filters sit ABOVE the picker surface (#908): they narrow the
         compact list and the graph/time-band mode through the same filtered row model. -->
    <div class="dim-filters" role="group" aria-label="Filter columns by dimension">
      {#each dimensions as dim (dim.key)}
        {@render dimFilter(dim)}
      {/each}
      <div class="dim-filters-status">
        <span class="showing" aria-live="polite"
          >Showing {visibleRows} of {totalRows} columns</span
        >
        {#if anyFilterActive}
          <button type="button" class="clear-filters" onclick={clearFilters}>
            Clear filters
          </button>
        {/if}
      </div>
    </div>
  {/if}

  {#if anyFilterActive && visibleRows === 0}
    <p class="no-match" role="status">No columns match the active filters.</p>
  {/if}

  {#if useGraphMode}
    {@render graphPicker()}
  {:else}
    <ul class="col-list integrated-list">
    {#if visibleSelectableBandCount > 1 && allKeys.length > 1}
      <!-- Global select-all: grab every visible column of the concept in one move.
           Rendered as the first integrated list row so hover, click-anywhere, and the
           full-selection gutter match the column rows below. Omitted when there's only
           ONE selectable band, where that variable's own select-all already covers it,
           or one selectable row, where a global one would just duplicate it. -->
      <li class="select-all-row">
        <label
          class="select-all row-btn integrated-list-row"
          class:selected={allSelected}
        >
          <input
            type="checkbox"
            class="cbox"
            checked={allSelected}
            indeterminate={someSelected && !allSelected}
            aria-label="Select all columns"
            disabled={applying}
            onchange={toggleAll}
          />
          <span>Select all columns</span>
        </label>
      </li>
    {/if}
    {#each view as cluster (cluster.name)}
      {#if cluster.showHeading}
        <!-- The name-CLUSTER heading (#901): a heterogeneous group renders each
             distinct concept name ONCE as a group label over its
             distinguisher-led bands, de-duplicating the repeated names that used to
             lead every band. PRESENTATIONAL only — not a selection unit and not a
             leaf link (a concept name spans variables). A heading element gives it the
             group-label semantics over the rows that follow. -->
        <li class="cluster-head">
          <h3>{cluster.name}</h3>
        </li>
      {/if}
      {#each cluster.bands as v (v.band.key)}
        {@const band = v.band}
        {#if v.single}
          {@const row = band.rows[0]}
          {@const checked = rowChecked(band, row)}
          {@const stage = rowStage(band, row)}
          {@const inWindow = representationInWindow(row, window)}
          <!-- The column's facet leads the quiet `.sub` context (#678) — but when the
               band's PRIMARY already IS that facet (#901 facet-led single-column band,
               `primaryIsFacet`), drop it from the sub so the same facet text isn't
               rendered twice (bold primary AND quiet sub). Value-set context still shows. -->
          {@const facet = v.primaryIsFacet
            ? undefined
            : band.facetByColumn?.[row.column]}
          <!-- #908: the row's structured facet DIMENSION markers (axis label : value).
               When the band carries declared-axis facets, mark each row with its axis
               so the dimension TYPE is legible; this generalizes the single `facet`
               line above. Empty for the leaf / axis-less group (the `facet` line stays). -->
          {@const facetMarkers = rowFacetMarkers(band, row.column)}
          <!-- A single-column variable = ONE selectable row, led by the variable's
               distinguishing identity (the leaf ≈ one-variable group case). The row is a
               click-anywhere container (mouse toggles selection); a real checkbox owns
               keyboard. When the variable has an `href` (group view) the COLUMN CHIP is
               itself the navigation link to its leaf — clicking the chip navigates
               (via the SPA router, `navigateChip`), not toggles; there's no separate
               "View" link. -->
          <li class="col-row single" class:focused={v.focused}>
            <!-- The whole row is a <label> wrapping the checkbox: clicking ANYWHERE in
                 it toggles selection natively (no JS, keyboard via the input). The chip-
                 link inside `preventDefault`s + router-navigates so a nav click neither
                 toggles the row NOR full-reloads the app. -->
            <label
              class="row-btn integrated-list-row"
              class:selected={checked}
              class:committed={stage === "committed"}
              class:staged-add={stage === "staged-add"}
              class:staged-remove={stage === "staged-remove"}
              class:dimmed={!inWindow}
            >
              <!-- No aria-label: the wrapping <label>'s text content (the column chip +
                   population + value set + period) names the checkbox for AT. -->
              <input
                type="checkbox"
                class="cbox"
                disabled={applying || !rowCanToggle(band, row)}
                checked={checked}
                onchange={() => toggleRow(band, row)}
              />
              <span class="row-main">
                <span class="primary-line">
                  {#if v.primary.mono}
                    <!-- The primary IS the delivery column → the prominent column chip,
                         a nav LINK when the variable has its own leaf page (group). -->
                    {@render colChip(v.primary.text, band.href)}
                  {:else}
                    <span class="primary">{v.primary.text}</span>
                  {/if}
                  {#if v.column}
                    <!-- A constant delivery column hoisted alongside a non-column
                         primary (e.g. a name-led row) → the column chip (nav link). -->
                    {@render colChip(v.column, band.href)}
                  {/if}
                  {#if v.showPrefix}
                    <code class="register-prefix">{band.registerPrefix}</code>
                  {/if}
                  {#if band.isIdentifier}
                    <span class="badge" title="Identifier">id</span>
                  {/if}
                  {#if band.isSensitive}
                    <span class="badge sensitive" title="Sensitive">sensitive</span
                    >
                  {/if}
                  {#if stage !== "none"}
                    {@render stageTag(stage)}
                  {/if}
                </span>
                <!-- #908: the per-axis facet markers — each an axis-named dimension
                     marker (e.g. "Hushållsbegrepp: Individ") so the user sees what KIND
                     of dimension distinguishes the row. -->
                {#if facetMarkers.length > 0}
                  <span class="facet-markers">
                    {#each facetMarkers as m (m.name)}
                      <span class="facet-marker"
                        ><span class="dim-kind facet">{m.axis}</span
                        >{m.value}</span
                      >
                    {/each}
                  </span>
                {/if}
                <!-- The column's human facet label (#678) leads the quiet context for a
                     single-column representation member (e.g. CDISP "Inkl. kapitalvinst")
                     so the row shows the human distinction, not only the column name.
                     Suppressed when the structured axis markers already render (#908). -->
                {#if (facet && facetMarkers.length === 0) || v.context.length > 0}
                  <span class="sub"
                    >{[facetMarkers.length === 0 ? facet : undefined, ...v.context]
                      .filter(Boolean)
                      .join(" · ")}</span
                  >
                {/if}
                {#if v.operationalDefinition}
                  {@render opDefLine(v.operationalDefinition)}
                {/if}
                {@render renameHint(row.renamedColumns)}
              </span>
              {#if row.codingsVary}
                <!-- A coding change over time on this ONE column (distinct value_set_id
                     across years). A quiet DEEP LINK to the value-set viewer focused on
                     this column (#905). Placed BEFORE the period so the period stays the
                     last, right-aligned element on every row (aligned column). -->
                {@render codingsVaryNudge(band, row)}
              {/if}
              {#if inWindow}
                {@render lateWarn(row)}
              {/if}
              {#if row.period}
                <span class="period">{row.period}</span>
              {/if}
            </label>
            {@render historyDisclosure(v.supersedes)}
          </li>
        {:else}
          <!-- A multi-column variable: a thin, quiet subheading (its distinguishing
               identity + a "select all" toggle) over its column rows. No card chrome —
               a hairline separates the group from the rest of the list. HOVERING the
               subheading highlights ALL its column rows (they move together); CLICKING
               anywhere on it toggles ALL its columns (mirrors the select-all checkbox),
               except the title nav link (which `preventDefault`s + router-navigates) +
               the checkbox. -->
          {@const empty = band.rows.length === 0}
          <!-- Grey the whole subheading when EVERY column is out of the active window —
               the variable reads as out-of-scope at the variable level, not just per
               row (#678). A FULLY-selected variable carries the same rust left bar the
               selected rows do (only full selection — not partial — mirrors the fill). -->
          {@const fullySelected = allOfBandSelected(band)}
          <li
            class="subhead integrated-list-row"
            class:empty
            class:dimmed={v.allOut}
            class:selected={fullySelected}
            class:focused={v.focused}
          >
            <!-- The identity chrome (primary + name/prefix/badges). When the variable
                 has an `href` (group view) the title is a navigation LINK; otherwise
                 plain text. The select-all checkbox is the control; the title link is
                 separate, so navigation and selection never share a target. -->
            <!-- The leading identity. A SINGLE-COLUMN member (`primaryIsColumn`) leads
                 with its delivery column as the prominent chip-LINK (the chip itself
                 navigates to the member's leaf — no outer link/↗, like the single-row
                 identity); a multi-column member leads with its mono slug (wrapped in
                 the subhead-title nav link below). -->
            {#snippet identityPrimary()}
              {#if v.primaryIsColumn}
                {@render colChip(v.primary.text, band.href)}
              {:else if v.primary.mono}
                <code class="primary mono">{v.primary.text}</code>
              {:else}
                <span class="primary">{v.primary.text}</span>
              {/if}
            {/snippet}
            {#snippet identityMeta()}
              {#if v.showPrefix}
                <code class="register-prefix">{band.registerPrefix}</code>
              {/if}
              {#if band.isIdentifier}
                <span class="badge" title="Identifier">id</span>
              {/if}
              {#if band.isSensitive}
                <span class="badge sensitive" title="Sensitive">sensitive</span>
              {/if}
              {#if empty}
                <span class="empty-note">No columns</span>
              {/if}
            {/snippet}
            {#snippet identityInner()}
              {@render identityPrimary()}
              {@render identityMeta()}
            {/snippet}
            {#snippet subheadContext()}
              {#if v.column || v.context.length > 0}
                <span class="subhead-context">
                  {#if v.column}
                    <!-- The constant delivery column (when it doesn't vary across this
                         variable's rows) → the prominent column chip (NOT a nav link for
                         a multi-column member, so it is part of the select-all surface). -->
                    {@render colChip(v.column)}
                  {/if}
                  {#if v.context.length > 0}
                    <span class="ctx-text">{v.context.join(" · ")}</span>
                  {/if}
                </span>
              {/if}
              <!-- The member's operational definition (#892/#932): the per-variable
                   distinguishing text, on its OWN line below the heading/context (it's a
                   sentence, not a chip) so parallel members are told apart at a glance. -->
              {#if v.operationalDefinition}
                {@render opDefLine(v.operationalDefinition)}
              {/if}
            {/snippet}
            {#if empty}
              <div class="subhead-row">
                <span class="subhead-title">{@render identityInner()}</span>
              </div>
              {@render subheadContext()}
            {:else}
              <!-- The WHOLE subheading is one <label> wrapping the select-all checkbox:
                   a click ANYWHERE on it — the title, the column chip, OR the description
                   line — toggles all columns natively (the title nav link inside
                   `preventDefault`s + router-navigates so a nav click never toggles).
                   Hovering anywhere sets the
                   band-hover key → all this variable's column rows highlight together. -->
              <label
                class="subhead-label"
                onmouseenter={() => (hoveredBandKey = band.key)}
                onmouseleave={() => (hoveredBandKey = null)}
              >
                <input
                  type="checkbox"
                  class="cbox"
                  checked={allOfBandSelected(band)}
                  indeterminate={someOfBandSelected(band) &&
                    !allOfBandSelected(band)}
                  aria-label={`Select all columns of ${v.primary.text}`}
                  disabled={applying}
                  onchange={() => toggleBand(band)}
                />
                <!-- The title + description share ONE wrapping line: when they fit they
                     sit on one row (a dot separates them); when they don't, the
                     description wraps WHOLE to its own row so the heading stays intact
                     (the description is a single flex item — it never breaks mid-line
                     beside the heading). Keeps the table compact. -->
                <span class="subhead-body">
                  {#if v.primaryIsColumn}
                    <!-- Single-column member: the column chip-LINK IS the identity (the
                         chip navigates; its color-deepen hover is the affordance). No
                         outer nav link / ↗ — the chip itself is the link, mirroring the
                         single-row identity. -->
                    <span class="subhead-title">{@render identityInner()}</span>
                  {:else if band.href}
                    {@const href = band.href}
                    <a
                      class="subhead-title link"
                      {href}
                      title={`Open ${v.primary.text}`}
                      onclick={(e) => navigateChip(e, href)}
                    >
                      {@render identityInner()}
                      <span class="open-marker" aria-hidden="true">↗</span>
                    </a>
                  {:else}
                    <span class="subhead-title">{@render identityInner()}</span>
                  {/if}
                  {@render subheadContext()}
                </span>
              </label>
            {/if}
            {@render historyDisclosure(v.supersedes)}
          </li>
          {#each band.rows as row (row.key)}
            {@const checked = rowChecked(band, row)}
            {@const stage = rowStage(band, row)}
            {@const inWindow = representationInWindow(row, window)}
            {@const label = v.rowLabels.get(row.key)}
            {@const facet = band.facetByColumn?.[row.column]}
            {@const facetMarkers = rowFacetMarkers(band, row.column)}
            <!-- A nested column row: the SAME <label>-wraps-checkbox click-anywhere
                 pattern as the single row, minus the nav link (a nested column is not
                 its own variable). Gets the band-hover highlight when its subheading is
                 hovered. -->
            <li class="col-row nested">
              <label
                class="row-btn integrated-list-row"
                class:selected={checked}
                class:committed={stage === "committed"}
                class:staged-add={stage === "staged-add"}
                class:staged-remove={stage === "staged-remove"}
                class:dimmed={!inWindow}
                class:band-hover={hoveredBandKey === band.key}
              >
                <!-- No aria-label: the <label> text (column chip + value-set + period)
                     names the checkbox for AT. -->
                <input
                  type="checkbox"
                  class="cbox"
                  disabled={applying || !rowCanToggle(band, row)}
                  checked={checked}
                  onchange={() => toggleRow(band, row)}
                />
                <span class="row-main">
                  {#if label?.primary.mono}
                    <!-- A mono primary here is the varying DELIVERY COLUMN → chip (NOT a
                         link — these columns aren't separate variables). -->
                    {@render colChip(label.primary.text)}
                  {:else}
                    <span class="primary">{label?.primary.text}</span>
                  {/if}
                  <!-- #908: the per-axis facet dimension markers (axis label : value)
                       so a multi-axis column reads its dimension TYPE at a glance. -->
                  {#if facetMarkers.length > 0}
                    <span class="facet-markers">
                      {#each facetMarkers as m (m.name)}
                        <span class="facet-marker"
                          ><span class="dim-kind facet">{m.axis}</span
                          >{m.value}</span
                        >
                      {/each}
                    </span>
                  {/if}
                  <!-- The human FACET label for this column (#678): a representation
                       group with several members on one variable distinguishes its
                       columns by facet ("Inkl./Exkl. kapitalvinst"), not just the
                       technical column name. Shown as the leading qualifier so the row
                       reads as the human distinction, not only `CDISP`/`CDISP5`.
                       Suppressed when the structured axis markers already render (#908). -->
                  {#if (facet && facetMarkers.length === 0) || (label && label.qualifiers.length > 0)}
                    <span class="sub"
                      >{[
                        facetMarkers.length === 0 ? facet : undefined,
                        ...(label?.qualifiers ?? []),
                      ]
                        .filter(Boolean)
                        .join(" · ")}</span
                    >
                  {/if}
                  {#if stage !== "none"}
                    {@render stageTag(stage)}
                  {/if}
                  {@render renameHint(row.renamedColumns)}
                </span>
                <!-- The codings-vary nudge (#905 deep link) sits BEFORE the period so
                     the period is the last, right-aligned element on every row (a clean
                     aligned column whether or not a row carries the nudge — #678 fix). -->
                {#if row.codingsVary}
                  {@render codingsVaryNudge(band, row)}
                {/if}
                {#if inWindow}
                  {@render lateWarn(row)}
                {/if}
                <!-- Every row shows its own period on the right (the period is never in
                     the hoisted context now — #678 fix 5). Use the raw `row.period` so
                     a constant-period band still shows each row's span. -->
                {#if row.period}
                  <span class="period">{row.period}</span>
                {/if}
              </label>
            </li>
          {/each}
        {/if}
      {/each}
    {/each}
    </ul>
  {/if}

  {#if diffCount > 0}
    <div class="picker-footer">
      <span class="count" role="status">{footerLabel}</span>
      {#if rowDiffCount > 0}
        <Button
          type="button"
          variant="default"
          size="sm"
          disabled={applying}
          onclick={resetStaging}
        >
          Reset
        </Button>
      {/if}
      <Button
        type="button"
        variant="primary"
        size="sm"
        disabled={!canApply || applying}
        onclick={commit}
      >
        {applying ? "Applying..." : applyLabel}
      </Button>
    </div>
  {/if}
</div>
{/if}

<style>
  .rep-picker {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
    overflow: hidden;
  }

  /* ── Graph/time-band mode (#904) ───────────────────────────────────────────
     Small edge-bearing graphs render as the picker itself: variable lanes in the
     sticky gutter, selectable representation cells on the year axis, and succession
     edges on the rail. Large/no-edge graphs fall back to the compact list above. */
  .graph-picker {
    display: flex;
    flex-direction: column;
  }
  .graph-cluster + .graph-cluster {
    border-top: 1px solid var(--border);
  }
  .graph-cluster-heading {
    margin: 0;
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    background: var(--surface-sunken);
    font-size: var(--text-sm);
    font-weight: 600;
    color: var(--text-muted);
  }
  .graph-timeline {
    overflow-x: auto;
    overflow-y: hidden;
    background: var(--surface);
  }
  .graph-grid {
    min-width: 100%;
    width: max-content;
  }
  .graph-axis-row {
    position: sticky;
    top: 0;
    z-index: 3;
    display: flex;
    height: 28px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
  }
  .graph-axis-gutter {
    position: sticky;
    left: 0;
    z-index: 1;
    flex: 0 0 var(--graph-gutter-w);
    background: var(--surface);
    border-right: 1px solid var(--border);
  }
  .graph-axis-track {
    position: relative;
    flex: 0 0 var(--graph-track-w);
    width: var(--graph-track-w);
  }
  .graph-tick {
    position: absolute;
    top: 7px;
    transform: translateX(-50%);
    font-size: var(--text-micro);
    font-variant-numeric: tabular-nums;
    color: var(--text-muted);
    white-space: nowrap;
  }
  .graph-lanes {
    position: relative;
  }
  .graph-gridlines {
    position: absolute;
    inset: 0;
    left: var(--graph-gutter-w);
    pointer-events: none;
  }
  .graph-gridline {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 1px;
    background: color-mix(in srgb, var(--border) 35%, transparent);
  }
  .graph-connectors {
    position: absolute;
    top: 0;
    left: 0;
    z-index: 2;
    overflow: visible;
    pointer-events: none;
  }
  .graph-edge {
    stroke: var(--viz-edge-succession);
    stroke-width: 1.5;
  }
  .graph-edge.representation {
    stroke-dasharray: 4 3;
  }
  .graph-arrow-head {
    fill: var(--viz-edge-succession);
  }
  .graph-reason {
    position: absolute;
    z-index: 2;
    transform: translateY(-50%);
    max-width: 220px;
    padding: 0 5px;
    border-radius: var(--radius-sm);
    background: color-mix(in srgb, var(--surface) 90%, transparent);
    color: var(--text-muted);
    font-size: var(--text-micro);
    line-height: 1.5;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    pointer-events: none;
  }
  .graph-lane {
    position: absolute;
    left: 0;
    right: 0;
    display: flex;
    align-items: stretch;
  }
  .graph-lane.focused {
    background: var(--accent-bg);
  }
  .graph-lane.muted {
    opacity: 0.45;
  }
  .graph-gutter {
    position: sticky;
    left: 0;
    z-index: 1;
    flex: 0 0 var(--graph-gutter-w);
    width: var(--graph-gutter-w);
    max-width: var(--graph-gutter-w);
    min-width: 0;
    box-sizing: border-box;
    padding: var(--space-2) var(--space-3);
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 2px;
    background: var(--surface);
    border-right: 1px solid var(--border);
  }
  .graph-lane.focused .graph-gutter {
    background: var(--accent-bg);
    box-shadow: inset 3px 0 0 var(--accent);
  }
  .graph-marker {
    position: absolute;
    right: -5px;
    top: 50%;
    z-index: 2;
    width: 9px;
    height: 9px;
    border: 1.5px solid var(--text-muted);
    border-radius: 50%;
    background: var(--surface);
    transform: translateY(-50%);
  }
  .graph-marker.focused {
    border-color: var(--accent);
    background: var(--accent);
  }
  .graph-gutter-text {
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
  }
  .graph-name {
    color: var(--text);
    font-size: var(--text-sm);
    font-weight: 600;
    line-height: 1.2;
    text-decoration: none;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
  }
  a.graph-name:hover {
    color: var(--accent);
  }
  a.graph-name:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .graph-slug {
    font-family: var(--font-mono);
    font-size: var(--text-micro);
    color: var(--text-muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .graph-viewed {
    align-self: flex-start;
    margin-top: 1px;
    padding: 0 4px;
    border-radius: var(--radius-sm);
    background: color-mix(in srgb, var(--accent) 12%, transparent);
    color: var(--accent-ink);
    font-size: 0.6rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    text-transform: uppercase;
  }
  .graph-same-as {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 3px;
    margin-top: 2px;
  }
  .graph-sa-prefix {
    color: var(--text-muted);
    font-size: 0.62rem;
  }
  .graph-sa-chip {
    padding: 0 4px;
    border-radius: var(--radius-sm);
    background: var(--accent-bg);
    color: var(--accent-ink);
    font-family: var(--font-mono);
    font-size: 0.62rem;
    line-height: 1.4;
    text-decoration: none;
  }
  .graph-track {
    position: relative;
    flex: 0 0 auto;
  }
  .graph-cell {
    position: absolute;
    box-sizing: border-box;
    height: 40px;
    padding: 4px 8px;
    display: flex;
    align-items: center;
    gap: var(--space-2);
    overflow: hidden;
    border: 1px solid var(--border);
    border-left: 3px solid transparent;
    border-radius: var(--radius-sm);
    background: var(--surface);
    cursor: pointer;
  }
  .graph-cell:hover {
    z-index: 4;
    border-color: var(--accent);
    box-shadow: 0 2px 8px color-mix(in srgb, var(--accent) 18%, transparent);
  }
  .graph-cell.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  .graph-cell.committed:not(.selected) {
    background: var(--surface);
  }
  .graph-cell.staged-add {
    background: var(--ok-bg);
    border-left-color: var(--ok);
  }
  .graph-cell.staged-remove {
    background: var(--warn-bg);
    border-left-color: var(--warn);
  }
  .graph-cell.staged-remove .graph-cell-main,
  .graph-cell.staged-remove .graph-cell-window,
  .graph-cell.staged-remove .col-chip {
    text-decoration: line-through;
    text-decoration-thickness: 1px;
    text-decoration-color: var(--warn);
  }
  .graph-cell.dimmed,
  .graph-cell.unavailable {
    opacity: 0.45;
  }
  .graph-cell.dimmed:hover {
    opacity: 0.7;
  }
  .graph-cell.unavailable {
    cursor: default;
    color: var(--text-muted);
  }
  .graph-cell.open-end {
    border-right-color: transparent;
    -webkit-mask-image: linear-gradient(
      to right,
      #000 calc(100% - 28px),
      transparent
    );
    mask-image: linear-gradient(to right, #000 calc(100% - 28px), transparent);
  }
  .graph-cell.open-start {
    border-left-color: transparent;
    -webkit-mask-image: linear-gradient(to left, #000 calc(100% - 22px), transparent);
    mask-image: linear-gradient(to left, #000 calc(100% - 22px), transparent);
  }
  .graph-cell.open-start.open-end {
    -webkit-mask-image: linear-gradient(to right, #000 calc(100% - 28px), transparent),
      linear-gradient(to left, #000 calc(100% - 22px), transparent);
    -webkit-mask-composite: source-in;
    mask-image: linear-gradient(to right, #000 calc(100% - 28px), transparent),
      linear-gradient(to left, #000 calc(100% - 22px), transparent);
    mask-composite: intersect;
  }
  .graph-cell-main {
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
    flex: 1 1 auto;
  }
  .graph-cell-sub,
  .graph-cell-window,
  .graph-unavailable-label {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .graph-cell-sub,
  .graph-cell-window {
    color: var(--text-muted);
    font-size: var(--text-micro);
    font-variant-numeric: tabular-nums;
  }
  .graph-unavailable-label {
    font-size: var(--text-sm);
    font-weight: 600;
  }
  .graph-cell :global(.tag),
  .graph-cell .codings-vary,
  .graph-cell .late-warn {
    flex: 0 0 auto;
  }
  .graph-fallback {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
    border: 0;
  }

  /* ── Per-dimension filters (#908) ───────────────────────────────────────────
     A quiet strip of per-dimension fieldsets above the column list. NEUTRAL
     throughout — no `--cat-*` type palette (that sub-system tags result/node TYPE;
     reusing it here would read a facet/coding value as a CODE/REG chip). Dimension
     identity is carried by TEXT (the legend's tracked micro-label), never hue —
     mirroring the #819 ConceptGroupNavigator. */
  .dim-filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-3);
    align-items: flex-start;
    padding: var(--space-3) var(--space-3);
    border-bottom: 1px solid var(--border);
    background: var(--surface-sunken);
  }
  .dim-filter {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-1) var(--space-2) var(--space-2);
    margin: 0;
    min-inline-size: 0;
  }
  .dim-filter legend {
    padding: 0 var(--space-1);
  }
  /* The dimension-KIND eyebrow: a tracked uppercase micro-label naming the dimension
     (an axis label, "Population", or "Coding"). The design system's hierarchy device
     — it reads as a section label, not a value. Reused as the per-row facet-marker
     axis prefix so the marking and the filter legend share one visual language. */
  .dim-kind {
    font-size: var(--text-micro);
    letter-spacing: 0.04em;
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
  }
  .filter-options {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1);
  }
  /* A filter pill: a checkbox styled as a selectable neutral chip (the #819 navigator
     pattern). The native input is visually hidden (the `.visually-hidden` global
     utility) but kept in the DOM for keyboard + a11y + labelling; `.on` paints the
     selected state with the brand accent (the one interactive-chrome use). */
  .filter-pill {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: 0.1em 0.5em;
    border: 1px solid var(--border);
    border-radius: 1rem;
    font-size: var(--text-sm);
    cursor: pointer;
    user-select: none;
    background: var(--surface);
    color: var(--text);
  }
  .filter-pill.on {
    background: var(--accent-bg);
    border-color: var(--accent);
    color: var(--accent-ink);
    font-weight: 600;
  }
  /* Keyboard focus ring on the (hidden) input projects onto its pill label. */
  .filter-pill:focus-within {
    box-shadow: var(--focus-ring);
  }
  .dim-filters-status {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-2);
    align-self: center;
  }
  .showing {
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  .clear-filters {
    background: none;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    padding: var(--space-1) var(--space-3);
    font: inherit;
    font-size: var(--text-sm);
    color: var(--text-muted);
    cursor: pointer;
  }
  .clear-filters:hover {
    color: var(--text);
    border-color: var(--border-strong);
  }
  .clear-filters:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  /* The empty-result line when every column is filtered out. */
  .no-match {
    margin: 0;
    padding: var(--space-3) var(--space-3);
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  /* The per-row facet dimension markers (#908): one axis-named marker per declared
     axis the row carries, sitting in the quiet sub-context line. Each pairs the
     uppercase axis eyebrow (`.dim-kind`) with the facet value, so a multi-axis row
     reads "HUSHÅLLSBEGREPP Individ · KAPITALVINST Inkl." — the dimension type made
     legible, not just the value. */
  .facet-markers {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-1) var(--space-3);
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  .facet-marker {
    display: inline-flex;
    align-items: baseline;
    gap: var(--space-1);
    overflow-wrap: anywhere;
  }

  /* ONE dense list — hairlines, no per-variable boxes. */
  .col-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .col-list > li + li {
    border-top: 1px solid var(--border);
  }

  /* The name-CLUSTER heading (#901): a quiet group label over the bands of one
     concept name in a heterogeneous group, de-duplicating the name that used to lead
     every band. A small, muted, uppercase-ish label — NOT the bold band identity and
     NOT a link (a concept name spans variables). It sits a touch tinted so the eye
     reads it as a section divider above its distinguisher-led rows. */
  .cluster-head {
    padding: 0.5rem 0.75rem 0.3rem;
    background: var(--surface-sunken);
    /* Its OWN deliberate top hairline, so the FIRST cluster head (a first-child, which
       the `.col-list > li + li` rule skips) reads identically to the subsequent ones —
       no inconsistent first-vs-rest seam above the sunken tint. A non-first cluster head
       already gets the same single 1px from `li + li`; borders don't stack on one
       element, so this just makes the first match the rest. */
    border-top: 1px solid var(--border);
  }
  .cluster-head h3 {
    margin: 0;
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--text-muted);
    letter-spacing: 0.01em;
  }

  /* A thin, quiet variable subheading — the distinguishing identity + select-all.
     NOT a card and NO fill: the `.col-list` hairline top divider alone separates it,
     so several stacked subheadings read flat and integrated. Click-anywhere toggles
     all its columns (cursor: pointer); its own hover tint reinforces the band-hover
     highlight on the rows below. */
  .subhead {
    padding: 0.4rem 0.75rem 0.3rem;
    /* The same 3px transparent left bar the rows carry — lines up with `.row-btn`'s
       border-left so a fully-selected variable's rust bar is continuous down the
       variable. Turns rust only on FULL selection (below), mirroring the fill rule. */
    border-left: 3px solid transparent;
    cursor: pointer;
  }
  .subhead.empty {
    cursor: default;
  }
  /* Fully-selected variable → the rust left bar + accent fill, matching the selected
     rows below (`.row-btn.selected`) so the variable's own row reads as selected too. A
     partial (indeterminate) selection deliberately does NOT get the bar. The left
     BORDER is the selected distinguisher; `.focused` below shares the same fill but is
     marked by an inset box-shadow instead, so the two stay distinct even when combined. */
  .subhead.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  /* All columns out of the active window → the subheading greys at the variable level
     (same muted treatment as a dimmed row; un-dims a touch on hover). */
  .subhead.dimmed {
    opacity: 0.45;
  }
  .subhead.dimmed:hover {
    opacity: 0.7;
  }
  .subhead:not(.empty):hover {
    background: var(--accent-bg);
  }
  /* The whole non-empty subheading is one <label> (the hover-all + click-all surface):
     the checkbox beside a wrapping body holding the title + description. */
  .subhead-label {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    cursor: pointer;
    /* Fill the whole subheading: pull the label out to the `.subhead` content edges
       (cancel its padding) then re-pad inside, so the hover/click surface covers the
       entire item — no dead padding ring at the top/sides that highlights the subhead
       but not its rows. */
    box-sizing: border-box;
    margin: -0.4rem -0.75rem -0.3rem;
    padding: 0.4rem 0.75rem 0.3rem;
  }
  /* The checkbox stays vertically centered against the (possibly wrapping) body. */
  .subhead-label > .cbox {
    align-self: center;
  }
  /* The title + description flow on ONE wrapping line: both fit → one row (a dot
     separates); the description (a single flex item) drops WHOLE to its own row when
     it can't fit beside the heading, so the heading never shares a line with a
     fragment of the description. */
  .subhead-body {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.1rem 0.5rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  /* The empty-band header keeps the simple one-row layout (no description to wrap). */
  .subhead-row {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }
  .subhead-title {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem 0.6rem;
  }
  .subhead-title .primary {
    font-weight: 600;
  }
  /* The identity-as-navigation link (group view): inherits the text color so it
     reads as the heading, shifting to the accent color on hover (no underline —
     matching the app's other links) — the `↗` marks it as a link. Distinct from
     the select-all checkbox beside it. */
  .subhead-title.link {
    text-decoration: none;
    color: inherit;
  }
  .subhead-title.link:hover {
    color: var(--accent);
  }
  .subhead-title.link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .open-marker {
    font-size: 0.8em;
    color: var(--text-muted);
  }
  /* The description rides inline after the title (one flex item in `.subhead-body`),
     so it sits on the same row when it fits and wraps WHOLE to its own row otherwise.
     Its OWN content flows as inline text (not an inner flex) so the leading dot glues
     to the text and never strands on a line by itself; `min-width: 0` lets the text
     wrap once the description is on its own row. */
  .subhead-context {
    min-width: 0;
    font-size: 0.78rem;
    color: var(--text-muted);
  }
  /* The dot that separates the heading from the description when they share a row.
     (It leads the description's own row after a wrap — a quiet continuation cue,
     glued to the first word so it can't sit alone.) */
  .subhead-context::before {
    content: "·";
    margin-right: 0.3rem;
  }
  /* A multi-column member's context column chip sits before the text. */
  .subhead-context .col-chip {
    margin-right: 0.3rem;
  }
  .subhead-context .ctx-text {
    overflow-wrap: anywhere;
  }
  /* A 0-column variable: a plain subheading with a quiet "No columns" marker. */
  .empty-note {
    font-size: 0.8rem;
    font-style: italic;
    color: var(--text-muted);
  }

  /* The deep-link `?member=` FOCUS marker (#678): the band the link named is marked
     with a subtle accent tint + a left accent rule so a `?member=<slug>` deep link
     lands with that member visibly highlighted. Distinct from `.selected` (a rust
     fill on the rows) — focus is a softer attention cue on the band itself, and the
     `box-shadow` inset rule reads even alongside the selected left border. */
  .subhead.focused,
  .col-row.single.focused {
    background: var(--accent-bg);
    box-shadow: inset 3px 0 0 0 var(--accent);
  }

  /* A column row: a click-anywhere checkbox. The whole row toggles (real <button> +
     role=checkbox for keyboard/AT). Nested rows indent under their subheading. */
  /* A column row: a click-anywhere container (a <div> — the real checkbox inside owns
     keyboard). Hovering OR band-hovering it highlights; clicking it toggles. */
  .row-btn {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    width: 100%;
    /* A <label> defaults to content-box (unlike the <button> this replaced), so
       width:100% + padding would overflow the row to the right — clip the years.
       Border-box folds the padding back in. */
    box-sizing: border-box;
    padding: 0.4rem 0.75rem;
    font: inherit;
    text-align: left;
    background: transparent;
    border: none;
    border-left: 3px solid transparent;
    cursor: pointer;
  }
  .col-row.nested .row-btn {
    padding-left: 1.6rem;
  }
  /* Hover (the row itself) AND band-hover (its subheading is hovered → all rows
     highlight together) share one highlight. */
  .row-btn:hover,
  .row-btn.band-hover {
    background: var(--accent-bg);
  }
  .row-btn.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  .row-btn.committed:not(.selected) {
    background: var(--surface);
  }
  .row-btn.staged-add {
    background: var(--ok-bg);
    border-left-color: var(--ok);
  }
  .row-btn.staged-remove {
    background: var(--warn-bg);
    border-left-color: var(--warn);
  }
  .row-btn.staged-remove .primary,
  .row-btn.staged-remove .col-chip,
  .row-btn.staged-remove .facet-markers,
  .row-btn.staged-remove .sub,
  .row-btn.staged-remove .period {
    text-decoration: line-through;
    text-decoration-thickness: 1px;
    text-decoration-color: var(--warn);
  }
  .row-btn.staged-remove .row-main :global(.tag) {
    text-decoration: none;
  }
  .row-btn.dimmed {
    opacity: 0.45;
  }
  .row-btn.dimmed:hover {
    opacity: 0.7;
  }
  .select-all-row .select-all {
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  .select-all-row .select-all.selected {
    color: var(--text);
  }

  /* The shared checkbox visual — every box is now a real native <input> (the row's
     keyboard control AND the select-all), styled identically: same size / border /
     radius. OS chrome is stripped so the shared box + pseudo-element show through. The
     check itself is a single CENTERED pseudo-element (a rotated stub with a right +
     bottom border), never the old crossing-gradient X. */
  .cbox {
    position: relative;
    flex: 0 0 auto;
    width: 1rem;
    height: 1rem;
    margin: 0;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: var(--surface);
    appearance: none;
    -webkit-appearance: none;
    cursor: pointer;
  }
  .cbox:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  /* FULL selection → the accent fill + the centered check. Indeterminate is NOT here:
     a partial box keeps the default surface bg + border, with only a visible dash. */
  input.cbox:checked {
    border-color: var(--accent);
    background: var(--accent);
  }
  /* The CENTERED checkmark: a short rotated stub (border-right + border-bottom)
     positioned at the box centre and nudged so the corner sits centred. Drawn only on
     a :checked input (full selection). */
  input.cbox:checked::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 48%;
    width: 0.25rem;
    height: 0.5rem;
    border: solid var(--accent-fg);
    border-width: 0 2px 2px 0;
    transform: translate(-50%, -55%) rotate(45deg);
  }
  /* The indeterminate (partial-selection) visual: NO accent fill — the box keeps its
     surface bg + border — with a clearly visible centred --accent dash drawn ON that
     unfilled box. :indeterminate beats :checked so a partial box never draws a check. */
  input.cbox:indeterminate::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 50%;
    width: 0.55rem;
    height: 2px;
    border: none;
    background: var(--accent);
    transform: translate(-50%, -50%);
  }

  .row-main {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .primary-line {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem 0.6rem;
  }
  /* A single-column row leads with the variable identity (prominent); a nested
     column row leads with its adaptive varying dimension (regular). */
  .col-row.single .primary {
    font-weight: 600;
  }
  .primary {
    font-size: 0.9rem;
    overflow-wrap: anywhere;
  }
  .primary.mono {
    font-family: var(--font-mono);
  }
  /* The DELIVERY COLUMN chip (#678): the main selection signal, a small categorical
     pill in the --cat-var hue (the "variable"/column dimension tint), tuned like the
     Tag primitive — 10% fill + 35% border + the AA-cleared --cat-var-ink text. A
     DISTINCT, recognizable "column" mark, deliberately NOT the rost --accent/-bg
     (which mean "selected"). Light/dark-safe via color-mix. */
  .col-chip {
    font-family: var(--font-mono);
    font-size: 0.82rem;
    font-weight: 500;
    line-height: 1.3;
    padding: 0.05rem 0.4rem;
    border-radius: var(--radius-sm);
    color: var(--cat-var-ink);
    border: 1px solid color-mix(in srgb, var(--cat-var) 35%, transparent);
    background: color-mix(in srgb, var(--cat-var) 10%, var(--surface));
    overflow-wrap: anywhere;
    /* Hug the column text — never stretch to fill the row's flex column. */
    align-self: flex-start;
    width: fit-content;
    max-width: 100%;
  }
  /* The navigable column chip (single-column identity in the group view): a real <a>
     to the variable's leaf. Reads as the column chip, gaining a stronger border +
     underline on hover/focus so it's discoverable as a link, distinct from selection. */
  a.col-chip.link {
    text-decoration: none;
    cursor: pointer;
  }
  /* The link-out affordance: a ↗ inside the chip pill (the whole chip is the link).
     Sized up from the mono text (the glyph reads small at text size) and kept a touch
     lighter so the column name still leads. */
  .col-chip.link .chip-arrow {
    margin-left: 0.2rem;
    font-size: 1.15em;
    line-height: 1;
    opacity: 0.85;
  }
  a.col-chip.link:hover,
  a.col-chip.link:focus-visible {
    /* Color change on hover — deepen the chip's own hue, NOT an underline —
       matching the variable-name link's color-shift hover affordance. */
    border-color: color-mix(in srgb, var(--cat-var) 55%, transparent);
    background: color-mix(in srgb, var(--cat-var) 22%, var(--surface));
  }
  a.col-chip.link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .register-prefix {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: var(--text-muted);
  }
  .badge {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 0.1rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-muted);
  }
  .badge.sensitive {
    border-color: var(--accent);
    color: var(--accent);
  }
  .sub {
    font-size: 0.8rem;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .row-main :global(.tag) {
    align-self: flex-start;
  }
  /* The operational-definition line (#892/#932): a member's per-variable distinguishing
     text, on its own quiet line. In `.row-main` (single row, a flex column) it sits below
     the identity naturally; inside `.subhead-body` (a wrapping flex row) `flex-basis:100%`
     drops it WHOLE to its own row beneath the heading — never sharing a line with the title.
     Muted prose reads as a distinguishing annotation, not a control. */
  .op-def {
    display: flex;
    align-items: baseline;
    gap: var(--space-1);
    flex-basis: 100%;
    min-width: 0;
    font-size: 0.8rem;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .op-def-text {
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* The sequential-rename progression hint (#902): a quiet inline "was DINF, DINF83"
     beneath the collapsed row's identity — the rename made legible without the full era
     view. Muted, mono for the column names so they read as columns, never a control. */
  .rename-hint {
    font-size: 0.78rem;
    color: var(--text-muted);
    font-family: var(--font-mono);
    overflow-wrap: anywhere;
  }
  /* The superseded-editions history disclosure (#902): a thin, low-key <details> on a
     succession chain head, listing the predecessor editions it folds. Closed by default;
     it indents under the band identity and never reads as card chrome. */
  .history {
    margin: 0.1rem 0 0.2rem 1.6rem;
    font-size: 0.78rem;
  }
  .history > summary {
    color: var(--text-muted);
    cursor: pointer;
    list-style: revert;
  }
  .history > summary:hover {
    color: var(--text);
  }
  .history-list {
    list-style: none;
    margin: 0.15rem 0 0;
    padding: 0 0 0 0.8rem;
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .history-link {
    color: var(--text);
    text-decoration: none;
  }
  .history-link:hover {
    color: var(--accent);
  }
  .history-link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .history-until {
    margin-left: 0.4rem;
    color: var(--text-muted);
    font-variant-numeric: tabular-nums;
  }
  .history-rows {
    list-style: none;
    margin: 0.2rem 0 0.25rem;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
  }
  .history-row {
    box-sizing: border-box;
    width: 100%;
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto auto auto;
    align-items: center;
    gap: var(--space-2);
    padding: 0.2rem 0.35rem;
    border-left: 2px solid transparent;
    border-radius: var(--radius-sm);
    color: var(--text);
    cursor: pointer;
  }
  .history-row:hover {
    background: var(--surface-sunken);
  }
  .history-row.selected {
    border-left-color: var(--accent);
    background: var(--accent-bg);
  }
  .history-row.staged-remove {
    border-left-color: var(--warn);
    background: var(--warn-bg);
  }
  .history-row.dimmed {
    opacity: 0.58;
  }
  .history-row-main {
    min-width: 0;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: var(--space-2);
  }
  .period {
    flex: 0 0 auto;
    font-size: 0.8rem;
    color: var(--text-muted);
    text-align: right;
    white-space: nowrap;
    /* Tabular numerals so every same-format year string is identical width and the
       right-aligned year column lines up cleanly (#678 — the proportional font's
       digits otherwise differ in width). */
    font-variant-numeric: tabular-nums;
  }
  /* A quiet nudge for a column whose CODING changed over time (#678): a tiny muted
     pill before the period. As of #905 it's a DEEP LINK (an <a>) to the value-set
     viewer focused on that column — still a hint, not a control, so it stays
     token-styled and low-key (no link-blue, no default underline); a hover deepen +
     underline is the link affordance, consistent with the picker's other in-row
     links. */
  .codings-vary {
    flex: 0 0 auto;
    font-size: 0.7rem;
    letter-spacing: 0.02em;
    padding: 0.05rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-muted);
    white-space: nowrap;
    text-decoration: none;
    cursor: pointer;
  }
  .codings-vary:hover {
    color: var(--accent-ink);
    border-color: var(--accent);
    text-decoration: underline;
  }
  .codings-vary:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  /* The data-starts-late warning marker (#678): a quiet --warn glyph just before the
     period. Sized so it sits in the row gap and never shifts the right-aligned period
     column. May appear on many rows, so it stays small + low-key (no fill). */
  .late-warn {
    flex: 0 0 auto;
    font-size: 0.75rem;
    line-height: 1;
    color: var(--warn);
    cursor: help;
  }

  /* ONE footer spanning the whole list: the selected count + the single Add. */
  .picker-footer {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    padding: 0.5rem 0.75rem;
    border-top: 1px solid var(--border);
  }
  .count {
    font-size: 0.85rem;
    color: var(--text-muted);
    margin-right: auto;
  }
</style>
