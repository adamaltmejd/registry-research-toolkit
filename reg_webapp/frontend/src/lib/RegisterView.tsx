import { useEffect, useId, useMemo, useRef, useState } from "react";
import type {
  ConceptGroup,
  RegisterShow,
  VariableChild,
  VariableDeliveryModel,
} from "./api";
import {
  browseFilter,
  useBrowseFilter,
  useRegisterMatches,
} from "./browse_filter";
import type { PeriodProps } from "./CatalogNodeView";
import styles from "./CatalogNodeView.module.css";
import {
  addWindowBounds,
  bindingChildren,
  catalogHref,
  countFoldedMembers,
  deliveredYears,
  deliveryColumnRows,
  deliveryWindows,
  distinctMemberCount,
  foldGroupedRows,
  type GroupedRow,
  groupFilterKeys,
  groupHref,
  leafSlug,
  matchesFilter,
  narrowGroupsToMembers,
  nodeLabel,
  OPEN_ENDED_VALID_TO,
  type PickerRepresentation,
  pickerWindowYears,
  rankFilter,
  registerPrefixOf,
  rowCoversColumn,
  variantLabel,
  windowsOverlapWindow,
  YEARLESS_VALID_FROM,
} from "./catalog";
import { catalogRootChildren } from "./catalog_names.svelte";
import { clampYearWindow, yearWindowLabel } from "./period";
import { projectStore } from "./project_store";
import viewStyles from "./RegisterView.module.css";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import ScopedDataWarnings from "./ScopedDataWarnings.svelte";
import { StagedAddStatus } from "./StagedAddStatus";
import { SvelteIsland } from "./SvelteIsland";
import {
  ADD_WINDOW_REQUIRED_MESSAGE,
  applyStagedPicks,
  type CommittedMarker,
  committedMarker,
  committedPickerRows,
  type PickerCommitScope,
  pickerRowKey,
  rowDeliversInScope,
  type StagedApplyOutcome,
  type StagedPick,
  type StagedPickerBand,
  stagedApplyRefusal,
  windowsOverlapPeriod,
} from "./staged_picker";
import { useRune } from "./svelte_bridge.svelte";
import { Timeline } from "./Timeline";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { type Definition, Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { FilterChip } from "./ui/FilterChip";
import { Tag } from "./ui/Tag";
import { VariableRow, VariableRowList, type YearSpan } from "./ui/VariableRow";
import { useProject, useStudyWindow } from "./use_project";
import { VariantsSummary } from "./VariantsSummary";
import { windowStore } from "./window";

type Era = { from: string; to: string };

/** One delivery column beside a variable (Y-82): the name a register delivers
 * it under, and the eras THAT NAME was delivered over. `windows` pools the
 * name's eras across every variant that ships it: the label prints it and it
 * offers the tick, since one tick covers them all. `windowsByVariant` keeps the
 * same eras per variant, the grain an add is decided at, so a sibling still
 * shipping a retired name never lends its years to the variant that renamed it.
 * `rows` are the variable's own picker rows that deliver the name (Y-83): one
 * per variant, or the one row a #902 rename folded it into, whose window spans
 * the whole chain and so never gates the tick (Y-104). */
interface DeliveryColumn {
  name: string;
  years: string[];
  windows: Era[];
  windowsByVariant: Map<string, Era[]>;
  rows: PickerRepresentation[];
}

/** A list row: a concept group folded to one row (#303), or a variable. A group
 * names no column of its own: its members carry theirs, on its page. */
type ListRow =
  | {
      id: string;
      kind: "group";
      group: ConceptGroup;
      label: string;
      columns: [];
    }
  | {
      id: string;
      kind: "leaf";
      fqid: string;
      label: string;
      columns: DeliveryColumn[];
    };

/** The years a variable's deliveries cover, merged and ordered, for its
 * coverage bar. An era still delivered runs to the catalog vintage; an era
 * whose start the record does not know is left out, since it has no year to
 * draw from. */
function coverageYears(
  deliveries: readonly VariableDeliveryModel[],
  vintageYear: number,
): YearSpan[] {
  const dated = deliveryWindows(
    deliveries.flatMap((d) =>
      d.windows
        .filter((w) => w.valid_from !== YEARLESS_VALID_FROM)
        .map((w) => ({ from: w.valid_from, to: w.valid_to })),
    ),
  );
  const spans: [number, number][] = [];
  for (const era of dated) {
    const from = Number(era.from.slice(0, 4));
    const to =
      era.to === OPEN_ENDED_VALID_TO
        ? Math.max(from, vintageYear)
        : Number(era.to.slice(0, 4));
    const last = spans.at(-1);
    // Eras a year apart or less draw as one segment: the bar is year-grain.
    if (last && from <= last[1] + 1) last[1] = Math.max(last[1], to);
    else spans.push([from, to]);
  }
  return spans;
}

/** A child's delivery columns, one per NAME across the variants that deliver
 * it, sorted by the year the name was first delivered. A state with no named
 * column contributes nothing. */
function deliveryColumns(
  child: VariableChild,
  rows: readonly PickerRepresentation[],
): DeliveryColumn[] {
  const byName = new Map<string, VariableDeliveryModel[]>();
  for (const delivery of child.deliveries) {
    if (delivery.column == null) continue;
    byName.set(delivery.column, [
      ...(byName.get(delivery.column) ?? []),
      delivery,
    ]);
  }
  // A lone column needs no era label, UNLESS its own delivery was interrupted:
  // then the gap is what the years say (Y-104).
  const several = byName.size > 1;
  return [...byName]
    .map(([name, deliveries]) => {
      const byVariant = new Map<string, Era[]>();
      for (const d of deliveries) {
        byVariant.set(d.variant, [
          ...(byVariant.get(d.variant) ?? []),
          ...d.windows.map((w) => ({ from: w.valid_from, to: w.valid_to })),
        ]);
      }
      const windowsByVariant = new Map(
        [...byVariant].map(([variant, eras]) => [
          variant,
          deliveryWindows(eras),
        ]),
      );
      const windows = deliveryWindows([...byVariant.values()].flat());
      return {
        name,
        years: several || windows.length > 1 ? eraLabels(windows) : [],
        windows,
        windowsByVariant,
        rows: rows.filter((row) => rowCoversColumn(row, name)),
      };
    })
    .sort(
      (a, b) =>
        firstYear(a.windows).localeCompare(firstYear(b.windows)) ||
        a.name.localeCompare(b.name),
    );
}

/** The year a column's earliest DATED era began; all-undated sorts last. */
function firstYear(windows: readonly Era[]): string {
  const dated = windows.find((w) => w.from !== YEARLESS_VALID_FROM);
  return (dated?.from ?? OPEN_ENDED_VALID_TO).slice(0, 4);
}

/** One label per disjoint era, in order: ["1968", "1995–1996", "1998–"] (Y-104).
 * Eras that differ below the year can label the same (a monthly family missing
 * a month), and a repeat says nothing, so adjacent repeats are dropped. */
function eraLabels(windows: readonly Era[]): string[] {
  const labels: string[] = [];
  for (const window of windows) {
    const label = eraYears(window);
    if (label && label !== labels.at(-1)) labels.push(label);
  }
  return labels;
}

/** "2018", "1990–2021", "2022–" while still delivered, "–1968" where delivery
 * began before the record does. An era undated at both ends says nothing. */
function eraYears(window: Era): string {
  const open = window.to === OPEN_ENDED_VALID_TO;
  if (window.from === YEARLESS_VALID_FROM) {
    return open ? "" : `–${window.to.slice(0, 4)}`;
  }
  const from = window.from.slice(0, 4);
  return open
    ? `${from}–`
    : yearWindowLabel({
        from: Number(from),
        to: Number(window.to.slice(0, 4)),
      });
}

/** Past this many eras the label prints only the first and last behind a `+N`
 * (Y-110): a dozen interrupted eras would push the row's name onto its own line. */
const ERA_LABEL_LIMIT = 3;

function visibleEras(years: readonly string[]) {
  return years.length <= ERA_LABEL_LIMIT
    ? { shown: years, hidden: [] as string[] }
    : {
        shown: [years[0], years.at(-1) ?? years[0]],
        hidden: years.slice(1, -1),
      };
}

/** The tick identity: a delivery column of one variable. Not the picker row
 * key: one tick covers every variant that delivers the name. */
function columnKey(fqid: string, name: string): string {
  return `${fqid}::${name}`;
}

/** The add scope: the register list has no period control of its own, so the
 * study window (clamped to the steward's span) IS the add's period. Without one
 * an open-ended column has no finite period, and the Add says so. */
function addScopeOf(
  studyWindow: typeof windowStore.value,
  bounds: Pick<
    PeriodProps,
    "windowMinYear" | "windowMaxYear" | "enforcePeriodBounds"
  >,
) {
  const bounded =
    studyWindow === null || !bounds.enforcePeriodBounds
      ? studyWindow
      : clampYearWindow(
          studyWindow,
          bounds.windowMinYear,
          bounds.windowMaxYear,
        );
  const scope: PickerCommitScope = {
    period: null,
    window: pickerWindowYears(null, bounded),
  };
  return {
    bounded,
    scope,
    bounds: addWindowBounds(null, scope.window ?? null),
    years: scope.window
      ? yearWindowLabel({ from: scope.window[0], to: scope.window[1] })
      : "",
  };
}

/**
 * A register's page: its purpose, warnings and tags, the variant chips, and the
 * variable list, which is an authoring surface (Y-83): every delivery column
 * carries a tick, and one Add commits them all through the same staged add a
 * variable's own page uses (reg_webapp/DESIGN.md → Adding columns from the
 * register list).
 */
export function RegisterView({
  node,
  ...period
}: PeriodProps & { node: RegisterShow }) {
  const { regMetaVersion, steward } = period;
  const filter = useBrowseFilter();
  // The variant chips (Y-82): OR within the selection, a lens on the list only.
  const [selectedVariants, setSelectedVariants] = useState<Set<string>>(
    () => new Set(),
  );
  // Set by Clear so the live region announces the return to the whole register.
  const chipsRef = useRef<HTMLFieldSetElement>(null);
  const listLabelId = useId();
  const listHeadRef = useRef<HTMLHeadingElement>(null);
  const addBarRef = useRef<HTMLDivElement>(null);

  // Everything derived from the node and the lens, computed once per change of
  // either rather than on every keystroke of the filter (740 variables on LISA).
  const derived = useMemo(() => {
    const children = bindingChildren(node);
    // The chip set: variants that deliver something here, so a chip can never
    // narrow the list to nothing. One variant means no axis to filter on.
    const variants = [
      ...new Set(children.flatMap((c) => c.deliveries.map((d) => d.variant))),
    ].sort();
    const lensed =
      selectedVariants.size === 0
        ? children
        : children.flatMap((child) => {
            const deliveries = child.deliveries.filter((d) =>
              selectedVariants.has(d.variant),
            );
            return deliveries.length > 0 ? [{ ...child, deliveries }] : [];
          });
    // The picker rows per variable, over the exact eras its deliveries carry
    // (Y-104): the unit an Add commits in. The columns point at these same
    // objects, and a #902 rename is one row both its names lead to.
    const rowsByFqid = new Map(
      lensed.map((child) => [child.fqid, deliveryColumnRows(child.deliveries)]),
    );
    const columnsByFqid = new Map(
      lensed.map((child) => [
        child.fqid,
        deliveryColumns(child, rowsByFqid.get(child.fqid) ?? []),
      ]),
    );
    // One band per listed variable, its rows the variable's own picker rows:
    // a tick stages one add per (variable, variant, representation).
    const bands: StagedPickerBand[] = lensed.map((child) => ({
      key: child.fqid,
      registerPrefix: registerPrefixOf(child.fqid),
      rows: rowsByFqid.get(child.fqid) ?? [],
    }));
    const allRows = foldGroupedRows(children, node.groups);
    // A group the lens leaves with one member gives way to that member's row.
    const lensGroups =
      selectedVariants.size === 0
        ? (node.groups ?? [])
        : narrowGroupsToMembers(node.groups, lensed);
    return { variants, columnsByFqid, bands, allRows, lensed, lensGroups };
  }, [node, selectedVariants]);
  const { variants, columnsByFqid, bands, allRows, lensed, lensGroups } =
    derived;
  const variantNames = new Map(
    node.variants.map((v) => [v.slug, variantLabel(v)]),
  );
  const columnNames = (fqid: string) =>
    (columnsByFqid.get(fqid) ?? []).map((column) => column.name);
  // Y-82: the filter matches delivery column names too, so `ForvErs` finds its
  // variable.
  const leafKeys = (item: VariableChild) => [
    leafSlug(item.fqid),
    item.fqid,
    item.name,
    ...columnNames(item.fqid),
  ];
  // While the filter holds a settled query, the server's match decides which
  // variables show (#1788): it reads definitions the page never loads, and it
  // is what the browse tree counts, so the tree's "N matching variables" is
  // what this list shows. A group narrows to its matched members. Until the
  // answer lands, the names on the page filter.
  const serverMatches = useRegisterMatches(node.fqid);
  // The filter narrows a group to its matching members, as the variant lens
  // does, so a group row counts only the variables that matched. A group
  // whose own label or key matches keeps every member: the server counts
  // variables only, so a group-label hit is admitted here, deliberately.
  const filterGroups = lensGroups.map((group) =>
    matchesFilter(filter, group.label, group.key)
      ? group
      : serverMatches
        ? {
            ...group,
            members: group.members.filter((m) => serverMatches.has(m.fqid)),
          }
        : {
            ...group,
            members: group.members.filter((member) =>
              matchesFilter(
                filter,
                ...groupFilterKeys(
                  { ...group, members: [member] },
                  columnNames,
                ),
              ),
            ),
          },
  );
  const memberFqids = new Set(
    filterGroups.flatMap((group) => group.members.map((m) => m.fqid)),
  );
  const filteredItems = lensed.filter(
    (item) =>
      memberFqids.has(item.fqid) ||
      (serverMatches
        ? serverMatches.has(item.fqid)
        : matchesFilter(filter, ...leafKeys(item))),
  );
  // A group the filter leaves with one member gives way to that member's row.
  const narrowedRows = foldGroupedRows(
    filteredItems,
    filterGroups.filter((group) => distinctMemberCount(group.members) > 1),
  );
  // Every row here matched; ranking only orders them. A member that matched
  // through a key its own leaf row does not carry (a facet label) and stands
  // alone keeps its place after the ranked rows.
  const ranked = rankFilter(narrowedRows, filter, (row) =>
    row.kind === "group"
      ? groupFilterKeys(row.group, columnNames)
      : leafKeys(row.item),
  );
  const rankedSet = new Set(ranked);
  const filteredRows = [
    ...ranked,
    ...narrowedRows.filter((row) => !rankedSet.has(row)),
  ];

  // ── Y-83: add delivery columns straight from the list ────────────────────
  const studyWindow = useStudyWindow();
  const add = addScopeOf(studyWindow, period);
  const draft = useProject((store) => store.draft);
  const seedReady = regMetaVersion !== "" && steward !== "";

  /** The ticked columns, each with the variants the list showed it under when
   * ticked. The lens is live and a tick is not: an Add stages the intersection,
   * never wider than what was on screen at the tick or is on screen now. */
  const [selectedColumns, setSelectedColumns] = useState<
    Map<string, ReadonlySet<string>>
  >(() => new Map());
  const [applying, setApplying] = useState(false);
  // The last Add's refusal, with the window it was made under: moving the window
  // is exactly what the refusal asks for, so it retires it.
  const [refusal, setRefusal] = useState<{
    message: string;
    years: string;
  } | null>(null);
  const [outcome, setOutcome] = useState<StagedApplyOutcome | null>(null);
  const shownRefusal = refusal?.years === add.years ? refusal.message : null;

  /** Is this column offered for a tick, and staged by an Add? Its own NAME's
   * eras met with the scope, never the folded row's (Y-104). A name with no
   * eras is never offered. One predicate for the cell and the batch. */
  const columnOffered = (column: DeliveryColumn) =>
    column.windows.length > 0 &&
    windowsOverlapWindow(column.windows, add.bounds);

  // Which listed columns are already wholly in the draft, by tick identity,
  // each with its marker against the study window (the most severe across the
  // variants that deliver it). Partial cover reads as not added.
  const committed = new Map<string, CommittedMarker>();
  const committedRows = committedPickerRows(draft, bands, add.scope);
  if (committedRows.size > 0) {
    for (const band of bands) {
      for (const column of columnsByFqid.get(band.key) ?? []) {
        const markers: CommittedMarker[] = [];
        const all =
          column.rows.length > 0 &&
          column.rows.every((row) => {
            const match = committedRows.get(pickerRowKey(band, row));
            if (match === undefined) return false;
            markers.push(committedMarker(match.sourcePeriod, add.bounded));
            // THIS variant's eras for the name must be inside what was
            // committed, not just the row's (a rename chain spans the chain).
            return windowsOverlapPeriod(
              column.windowsByVariant.get(row.variant) ?? [],
              match.period,
            );
          });
        if (all) {
          committed.set(
            columnKey(band.key, column.name),
            markers.find((m) => m.tone === "error") ??
              markers.find((m) => m.detail !== null) ??
              markers[0],
          );
        }
      }
    }
  }

  // The staged batch: the live list met with the ticks, in two units read off
  // one sweep: the rows an Add commits, and the COLUMNS the bar counts and the
  // confirmation reports. A rename ticked under both names is two columns and
  // one add; a column two variants deliver is one column and two adds.
  const adds: StagedPick[] = [];
  const stagedKeys = new Set<string>();
  for (const band of selectedColumns.size > 0 ? bands : []) {
    const staged = new Set<PickerRepresentation>();
    for (const column of columnsByFqid.get(band.key) ?? []) {
      const key = columnKey(band.key, column.name);
      const tickedUnder = selectedColumns.get(key);
      if (tickedUnder === undefined || !columnOffered(column)) continue;
      for (const row of column.rows) {
        // Per (variant, name) as well as per name: the tick was offered off the
        // pooled eras, so a retired name must not stage the variant that
        // renamed it.
        if (
          !tickedUnder.has(row.variant) ||
          !windowsOverlapWindow(
            column.windowsByVariant.get(row.variant) ?? [],
            add.bounds,
          ) ||
          !rowDeliversInScope(row, add.scope)
        ) {
          continue;
        }
        stagedKeys.add(key);
        if (!staged.has(row)) {
          staged.add(row);
          adds.push({ band, row });
        }
      }
    }
  }
  const columnCount = `${stagedKeys.size} ${stagedKeys.size === 1 ? "column" : "columns"}`;

  // Leaving the page mid-add abandons the batch rather than committing it into a
  // draft the researcher has navigated away from.
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  function toggleVariant(variant: string): void {
    setSelectedVariants((current) => {
      const next = new Set(current);
      if (!next.delete(variant)) next.add(variant);
      return next;
    });
  }

  /** Lift the variant lens. Focus moves to the first chip first: the button
   * this runs from unmounts once the selection is empty (Y-94). */
  function clearVariants(): void {
    chipsRef.current?.querySelector("input")?.focus();
    setSelectedVariants(new Set());
  }

  function toggleColumn(fqid: string, column: DeliveryColumn): void {
    const key = columnKey(fqid, column.name);
    setSelectedColumns((current) => {
      const next = new Map(current);
      // Off what the box shows: a tick the lens moved out of the batch shows
      // unticked, and clicking it re-ticks under the variants on screen now.
      if (stagedKeys.has(key)) next.delete(key);
      else next.set(key, new Set(column.rows.map((row) => row.variant)));
      return next;
    });
  }

  async function addSelected(): Promise<void> {
    // The button is never natively disabled, so every reason it reads as
    // refused is re-checked here.
    if (applying || stagedKeys.size === 0 || !seedReady) return;
    // The batch is bound to the project, the page and the window it was
    // pressed under; `applyStagedPicks` asks again after every await.
    const stagedAgainst = projectStore.replacementGeneration;
    const lapsed = () =>
      !mounted.current ||
      projectStore.replacementGeneration !== stagedAgainst ||
      addScopeOf(windowStore.value, period).years !== add.years;
    const addedKeys = stagedKeys;
    setApplying(true);
    try {
      const result = await applyStagedPicks(
        { adds, removes: [] },
        {
          scope: add.scope,
          studyWindow: add.bounded,
          seed: { regMetaVersion, steward },
          cancelled: lapsed,
        },
      );
      if (!mounted.current) return;
      if (result.kind !== "applied") {
        // Refused or abandoned: nothing was authored, and an earlier
        // confirmation must not go on reading as current.
        const message = stagedApplyRefusal(
          result,
          add.scope,
          ADD_WINDOW_REQUIRED_MESSAGE,
        );
        setRefusal(message === null ? null : { message, years: add.years });
        setOutcome(null);
        return;
      }
      setRefusal(null);
      setOutcome(result.outcome && { added: addedKeys.size, removed: 0 });
      // The bar unmounts with the consumed ticks: focus it held goes to the
      // list's heading rather than to the document body.
      if (addBarRef.current?.contains(document.activeElement)) {
        listHeadRef.current?.focus();
      }
      // The committed ticks are consumed: they now read as in the project.
      setSelectedColumns(
        (current) =>
          new Map([...current].filter(([key]) => !addedKeys.has(key))),
      );
    } finally {
      if (mounted.current) setApplying(false);
    }
  }

  // The provider's display name, from the browse tree's one root read.
  const root = useRune(catalogRootChildren, undefined);
  const providerFqid = node.fqid.split("/")[0] ?? "";
  const provider = root.value?.find((child) => child.fqid === providerFqid);
  const definitions: Definition[] = [];
  if (node.purpose) definitions.push({ term: "Purpose", value: node.purpose });
  if (node.tags && node.tags.length > 0) {
    definitions.push({
      term: "Themes",
      value: node.tags.map((tag) => tag.label).join(", "),
    });
  }

  const warningProps = useMemo(
    () => ({ fqid: node.fqid, registerOnly: true, collapsed: true }),
    [node.fqid],
  );

  const total = countFoldedMembers(allRows);
  const shown = countFoldedMembers(filteredRows);
  const filtering = filter.trim() !== "";
  const narrowed = filtering || selectedVariants.size > 0;
  const childrenByFqid = new Map(lensed.map((child) => [child.fqid, child]));
  const axis: YearSpan = [period.windowMinYear, period.windowMaxYear];
  // The register's delivered years, every variable's windows together: the
  // Timeline's coverage and edition ticks.
  const registerYears = deliveredYears(
    bindingChildren(node).flatMap((child) =>
      child.deliveries.flatMap((d) =>
        d.windows.map((w) => ({ from: w.valid_from, to: w.valid_to })),
      ),
    ),
    period.vintageYear,
  );
  const windowSpan: YearSpan | null = add.bounded
    ? [add.bounded.from, add.bounded.to]
    : null;
  // A variable is in the project when one of its listed columns is.
  const inProject = new Set(
    [...committed.keys()].map((key) => key.slice(0, key.indexOf("::"))),
  );
  const listRows: ListRow[] = filteredRows.map(
    (row: GroupedRow<VariableChild>) =>
      row.kind === "group"
        ? {
            id: `group:${row.group.key}`,
            kind: "group",
            group: row.group,
            label: row.group.label,
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

  return (
    <article className={styles.page}>
      <Definitions
        title={nodeLabel(node)}
        memberOf={[
          {
            relation: "Published by",
            label: provider?.name ?? providerFqid,
            href: catalogHref(providerFqid),
          },
        ]}
        definitions={definitions}
      />
      <SvelteIsland component={ScopedDataWarnings} props={warningProps} />
      <Timeline
        axis={{ from: axis[0], to: axis[1] }}
        window={add.bounded}
        coverage={
          registerYears.length === 0
            ? null
            : { from: registerYears[0], to: registerYears.at(-1) ?? null }
        }
        years={registerYears}
        vintageYear={period.vintageYear}
      />
      {allRows.length === 0 ? (
        <section className={viewStyles.variables} aria-labelledby={listLabelId}>
          <h2 id={listLabelId} className="micro-label">
            Variables
          </h2>
          <EmptyState title="This register lists no variables." />
        </section>
      ) : (
        <section className={viewStyles.variables} aria-labelledby={listLabelId}>
          <div className={viewStyles.listHead}>
            <h2
              id={listLabelId}
              ref={listHeadRef}
              tabIndex={-1}
              className={`micro-label ${viewStyles.listLabel}`}
            >
              Variables
            </h2>
            {/* Mounted empty: a live region inserted with its first text is
                not announced. Counts stay in variable units after folding: a
                group row counts its members. */}
            <span className={viewStyles.shown} aria-live="polite">
              {narrowed ? `${shown} of ${total} shown` : ""}
            </span>
          </div>
          {variants.length > 1 && (
            <div className={viewStyles.variantFilters}>
              <fieldset ref={chipsRef} className={viewStyles.variantFilter}>
                <legend className="micro-label">Variant</legend>
                <div className={viewStyles.chips}>
                  {variants.map((variant) => (
                    <FilterChip
                      key={variant}
                      isSelected={selectedVariants.has(variant)}
                      onChange={() => toggleVariant(variant)}
                    >
                      {variantNames.get(variant) ?? variant}
                    </FilterChip>
                  ))}
                </div>
              </fieldset>
              {selectedVariants.size > 0 && (
                <Button variant="ghost" onPress={clearVariants}>
                  Clear variant filter
                </Button>
              )}
            </div>
          )}
          {listRows.length > 0 ? (
            <VariableRowList label="Variables">
              {listRows.map((row) =>
                row.kind === "group" ? (
                  <VariableRow
                    key={row.id}
                    name={row.label}
                    href={groupHref(node.fqid, row.group.key)}
                    fqid={row.group.fqid}
                    inProject={row.group.members.some((m) =>
                      inProject.has(m.fqid),
                    )}
                    coverage={coverageYears(
                      row.group.members.flatMap(
                        (m) => childrenByFqid.get(m.fqid)?.deliveries ?? [],
                      ),
                      period.vintageYear,
                    )}
                    axis={axis}
                    window={windowSpan}
                  />
                ) : (
                  <VariableRow
                    key={row.id}
                    name={row.label}
                    href={catalogHref(row.fqid)}
                    fqid={row.fqid}
                    inProject={inProject.has(row.fqid)}
                    coverage={coverageYears(
                      childrenByFqid.get(row.fqid)?.deliveries ?? [],
                      period.vintageYear,
                    )}
                    axis={axis}
                    window={windowSpan}
                  >
                    {row.columns.map((column) => (
                      <DeliveryColumnTick
                        key={column.name}
                        column={column}
                        offered={columnOffered(column)}
                        ticked={stagedKeys.has(
                          columnKey(row.fqid, column.name),
                        )}
                        marker={committed.get(columnKey(row.fqid, column.name))}
                        isDisabled={applying}
                        windowYears={add.years}
                        onToggle={() => toggleColumn(row.fqid, column)}
                      />
                    ))}
                  </VariableRow>
                ),
              )}
            </VariableRowList>
          ) : (
            // Only the text filter can empty the list: every chip is a
            // variant that delivers something.
            <EmptyState
              title={`No variables match “${filter}”`}
              description={
                selectedVariants.size > 0
                  ? "The variant filter above is also narrowing this list."
                  : "Nothing in this register contains that text."
              }
              action={
                <Button onPress={() => browseFilter.set("")}>
                  Clear filter
                </Button>
              }
            />
          )}
          {/* Only while something is ticked (DESIGN.md → VariableRow); it
              stays through a filter that empties the list, so ticks made
              across several searches are still addable. Sticky against the
              viewport (Y-97). */}
          {(stagedKeys.size > 0 || applying) && (
            <div ref={addBarRef} className={viewStyles.addBar}>
              <Button
                variant="primary"
                isInert={!seedReady}
                isBusy={applying}
                onPress={addSelected}
              >
                Add {columnCount} to project
              </Button>
            </div>
          )}
          <StagedAddStatus outcome={outcome} blocked={shownRefusal} />
        </section>
      )}
      <VariantsSummary registerFqid={node.fqid} variants={node.variants} />
      <RelatedDocumentsPanel register={node.fqid} />
    </article>
  );
}

interface TickProps {
  column: DeliveryColumn;
  offered: boolean;
  ticked: boolean;
  marker: CommittedMarker | undefined;
  isDisabled: boolean;
  windowYears: string;
  onToggle: () => void;
}

/** One delivery column (Y-83): its `ColumnChip` and eras, with a tick when it
 * can be added. The label carries the checkbox, so the name being read is what
 * is clicked, and its eras and any reason it cannot be added join the
 * accessible name. A column already in the project shows its `in` chip and no
 * tick; one the study window has moved off shows its `out` chip, the tick
 * disabled. */
function DeliveryColumnTick({
  column,
  offered,
  ticked,
  marker,
  isDisabled,
  windowYears,
  onToggle,
}: TickProps) {
  const eras = visibleEras(column.years);
  const separator = eras.hidden.length > 0 ? " … " : ", ";
  const years = column.years.length > 0 && (
    <span className={viewStyles.years}>
      {/* One span per era, so a line breaks between eras, never inside a
          range, where "1995–" would read as still delivered (Y-104). */}
      {eras.shown.map((era, i) => (
        <span key={era}>
          {i > 0 && separator}
          <span className={viewStyles.era}>{era}</span>
        </span>
      ))}
      {eras.hidden.length > 0 && (
        <>
          {" "}
          {/* A generic span takes no accessible name, so the hidden eras are
              text a screen reader reads after the count. */}
          <span className={viewStyles.era} title={column.years.join(", ")}>
            +{eras.hidden.length}
            <span className="visually-hidden">
              {` more era${eras.hidden.length === 1 ? "" : "s"}: ${eras.hidden.join(", ")}`}
            </span>
          </span>
        </>
      )}
    </span>
  );

  if (marker) {
    // In the project: the chip says so. A marker that says more (the source's
    // years differ from the window, or lie outside it) keeps its words, in
    // its status colour.
    const plain = marker.tone === "info" && marker.detail === null;
    return (
      <span className={viewStyles.column}>
        <ColumnChip name={column.name} state="in" />
        {years}
        {plain ? (
          <span className="visually-hidden">in the project</span>
        ) : (
          <Tag tone={marker.tone} glyph={marker.glyph}>
            {marker.label}
            {marker.detail && (
              <span className="visually-hidden">{marker.detail}</span>
            )}
          </Tag>
        )}
      </span>
    );
  }

  // The study window is this page's period, so a column delivered wholly
  // outside it has nothing to commit; the reason names the window.
  const reason = offered
    ? null
    : column.windows.length === 0
      ? "not delivered"
      : `not delivered in ${windowYears}`;
  return (
    <label
      className={`${viewStyles.column} ${offered ? viewStyles.pick : ""}`}
      title={reason ? `${column.name}, ${reason}` : undefined}
    >
      <input
        className={`cbox ${viewStyles.tick}`}
        type="checkbox"
        checked={ticked}
        disabled={isDisabled || !offered}
        onChange={onToggle}
      />
      <ColumnChip name={column.name} state={offered ? "default" : "out"} />
      {years}
      {/* Visible, so the disabled tick explains itself without a hover. */}
      {reason && <span className={viewStyles.reason}>{reason}</span>}
    </label>
  );
}
