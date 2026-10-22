import { useEffect, useMemo, useRef, useState } from "react";
import type {
  ConceptGroup,
  RegisterShow,
  VariableChild,
  VariableDeliveryModel,
} from "./api";
import { useBrowseFilter } from "./browse_filter";
import type { PeriodProps } from "./CatalogNodeView";
import { ConceptGroupLink } from "./CatalogNodeView";
import styles from "./CatalogNodeView.module.css";
import {
  addWindowBounds,
  bindingChildren,
  catalogHref,
  countFoldedMembers,
  deliveryColumnRows,
  deliveryWindows,
  foldGroupedRows,
  type GroupedRow,
  groupFilterKeys,
  groupHref,
  leafSlug,
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
import { FilterCount } from "./FilterCount";
import { clampYearWindow, yearWindowLabel } from "./period";
import type { RawDraft } from "./project_data";
import { projectStore } from "./project_store.svelte";
import viewStyles from "./RegisterView.module.css";
import RelatedDocumentsPanel from "./RelatedDocumentsPanel.svelte";
import ScopedDataWarnings from "./ScopedDataWarnings.svelte";
import StagedAddStatus from "./StagedAddStatus.svelte";
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
import { draftSnapshot, useRune } from "./svelte_bridge.svelte";
import { Button } from "./ui/Button";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { FilterChip } from "./ui/FilterChip";
import { Panel } from "./ui/Panel";
import { Tag } from "./ui/Tag";
import type { Column } from "./ui/types";
import VariantsSummary from "./VariantsSummary.svelte";
import { windowStore } from "./window.svelte";

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
type VariableRow =
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

const variableColumns: Column<VariableRow>[] = [
  { key: "label", label: "Variable" },
  { key: "columns", label: "Delivery column", mono: true },
];

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

const readWindow = () => windowStore.value;

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
  const [variantsLifted, setVariantsLifted] = useState(false);
  const chipsRef = useRef<HTMLFieldSetElement>(null);

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
    const narrowedRows =
      selectedVariants.size === 0
        ? allRows
        : foldGroupedRows(lensed, narrowGroupsToMembers(node.groups, lensed));
    return { variants, columnsByFqid, bands, allRows, narrowedRows };
  }, [node, selectedVariants]);
  const { variants, columnsByFqid, bands, allRows, narrowedRows } = derived;
  const variantNames = new Map(
    node.variants.map((v) => [v.slug, variantLabel(v)]),
  );
  const columnNames = (fqid: string) =>
    (columnsByFqid.get(fqid) ?? []).map((column) => column.name);
  // Y-82: the filter matches delivery column names too, so `ForvErs` finds its
  // variable.
  const filteredRows = rankFilter(narrowedRows, filter, (row) =>
    row.kind === "group"
      ? groupFilterKeys(row.group, columnNames)
      : [
          leafSlug(row.item.fqid),
          row.item.fqid,
          row.item.name,
          ...columnNames(row.item.fqid),
        ],
  );

  // ── Y-83: add delivery columns straight from the list ────────────────────
  const studyWindow = useRune(readWindow, undefined);
  const add = addScopeOf(studyWindow, period);
  const draft = useRune(draftSnapshot, undefined) as RawDraft | null;
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
    setVariantsLifted(false);
  }

  /** Lift the variant lens. Focus moves to the first chip first: the button
   * this runs from unmounts once the selection is empty (Y-94). */
  function clearVariants(): void {
    chipsRef.current?.querySelector("input")?.focus();
    setVariantsLifted(true);
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
      // The committed ticks are consumed: they now read as in the project.
      setSelectedColumns(
        (current) =>
          new Map([...current].filter(([key]) => !addedKeys.has(key))),
      );
    } finally {
      if (mounted.current) setApplying(false);
    }
  }

  const statusProps = useMemo(
    () => ({ outcome, blocked: shownRefusal }),
    [outcome, shownRefusal],
  );
  const warningProps = useMemo(
    () => ({ fqid: node.fqid, registerOnly: true, collapsed: true }),
    [node.fqid],
  );
  const variantsProps = useMemo(
    () => ({ registerFqid: node.fqid, variants: node.variants }),
    [node],
  );
  const documentsProps = useMemo(() => ({ register: node.fqid }), [node.fqid]);

  const total = countFoldedMembers(allRows);
  const shown = countFoldedMembers(filteredRows);
  const filtering = filter.trim() !== "";
  const variableRows: VariableRow[] = filteredRows.map(
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
      <h2 className={styles.title}>{nodeLabel(node)}</h2>
      {node.purpose && <p className={viewStyles.purpose}>{node.purpose}</p>}
      <SvelteIsland component={ScopedDataWarnings} props={warningProps} />
      {node.tags && node.tags.length > 0 && (
        <ul className={viewStyles.tags} aria-label="Thematic tags">
          {node.tags.map((tag) => (
            <li key={tag.slug}>
              <Tag>{tag.label}</Tag>
            </li>
          ))}
        </ul>
      )}
      {allRows.length === 0 ? (
        <Panel title="Variables">
          <EmptyState title="This register lists no variables." />
        </Panel>
      ) : (
        <div className={viewStyles.variables}>
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
              {/* Mounted with the strip and empty until a chip is on: a region
                  inserted with its first text is not announced. While typing,
                  the filter's own count says the same pair, so this steps
                  aside. */}
              <div className={viewStyles.variantStatus}>
                <span aria-live="polite">
                  {!filtering &&
                    (selectedVariants.size > 0
                      ? `Showing ${shown} of ${total} variables`
                      : variantsLifted
                        ? `Showing all ${total} variables`
                        : "")}
                </span>
                {selectedVariants.size > 0 && (
                  <Button size="sm" onPress={clearVariants}>
                    Clear variant filter
                  </Button>
                )}
              </div>
            </div>
          )}
          {/* Counts stay in variable units after folding: a group row counts
              its members. */}
          <FilterCount
            filter={filter}
            shown={shown}
            total={total}
            noun="variables"
          />
          {variableRows.length > 0 ? (
            <DataTable
              framed
              rowNavigation
              columns={variableColumns}
              rows={variableRows}
              getRowId={(row) => row.id}
              renderCell={(row, column) =>
                column.key === "label" ? (
                  row.kind === "group" ? (
                    <ConceptGroupLink
                      group={row.group}
                      noun="variables"
                      href={groupHref(node.fqid, row.group.key)}
                    />
                  ) : (
                    <a
                      className={styles.rowLink}
                      href={catalogHref(row.fqid)}
                      title={row.fqid}
                    >
                      {row.label}
                    </a>
                  )
                ) : (
                  row.kind === "leaf" &&
                  row.columns.map((column) => (
                    <DeliveryColumnTick
                      key={column.name}
                      column={column}
                      offered={columnOffered(column)}
                      ticked={stagedKeys.has(columnKey(row.fqid, column.name))}
                      marker={committed.get(columnKey(row.fqid, column.name))}
                      isDisabled={applying}
                      windowYears={add.years}
                      onToggle={() => toggleColumn(row.fqid, column)}
                    />
                  ))
                )
              }
            />
          ) : (
            <Panel title="Variables">
              {/* Only the text filter can empty the list: every chip is a
                  variant that delivers something. */}
              <EmptyState
                title={`No variables match “${filter}”`}
                description={
                  selectedVariants.size > 0
                    ? "The variant filter above is also narrowing this list."
                    : "Press Escape in the search bar to see every variable."
                }
              />
            </Panel>
          )}
          {/* The add bar stays mounted through a filter that empties the list,
              so ticks made across several searches are still addable. Sticky
              against the viewport (Y-97). */}
          <div className={viewStyles.addBar}>
            <output className={viewStyles.addCount}>
              {stagedKeys.size === 0
                ? "Tick a delivery column to add it to the project."
                : `${columnCount} selected`}
            </output>
            <Button
              variant="primary"
              size="sm"
              isInert={stagedKeys.size === 0 || !seedReady || applying}
              onPress={addSelected}
            >
              {applying
                ? "Adding…"
                : stagedKeys.size === 0
                  ? "Add columns to project"
                  : `Add ${columnCount} to project`}
            </Button>
          </div>
          <SvelteIsland component={StagedAddStatus} props={statusProps} />
        </div>
      )}
      <SvelteIsland component={VariantsSummary} props={variantsProps} />
      <SvelteIsland component={RelatedDocumentsPanel} props={documentsProps} />
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

/** One delivery column and its tick, one target (Y-83): the label carries the
 * checkbox, so the name being read is what is clicked, and its era, "In
 * project" marker and any reason it cannot be added join the accessible name. */
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
  return (
    <label
      className={`${viewStyles.column} ${offered ? "" : viewStyles.outOfWindow}`}
    >
      <input
        className={`cbox ${viewStyles.tick}`}
        type="checkbox"
        checked={ticked}
        disabled={isDisabled || !offered}
        onChange={onToggle}
      />
      {/* Name, eras and markers wrap as one line beside the tick, so a marker
          that will not fit drops under the name it qualifies. */}
      <span className={viewStyles.columnLine}>
        {column.name}
        {column.years.length > 0 && (
          <span className={viewStyles.years}>
            {/* One span per era, so a line breaks between eras, never inside a
                range, where "1995–" would read as still delivered (Y-104).
                Keyed by position: the list is rebuilt whole from the windows. */}
            {eras.shown.map((era, i) => (
              <span key={era}>
                {i > 0 && separator}
                <span className={viewStyles.era}>{era}</span>
              </span>
            ))}
            {eras.hidden.length > 0 && (
              <>
                {" "}
                <span
                  className={viewStyles.era}
                  title={column.years.join(", ")}
                  aria-label={`+${eras.hidden.length} more era${eras.hidden.length === 1 ? "" : "s"}: ${eras.hidden.join(", ")}`}
                >
                  +{eras.hidden.length}
                </span>
              </>
            )}
          </span>
        )}
        {marker && (
          // Already in the draft, and still tickable: adding it again folds
          // into the same source and changes nothing.
          <Tag tone={marker.tone} glyph={marker.glyph}>
            {marker.label}
            {marker.detail && (
              <span className="visually-hidden">{marker.detail}</span>
            )}
          </Tag>
        )}
        {!offered && (
          // The study window is this page's period, so a column delivered
          // wholly outside it has nothing to commit; the reason names the one
          // control that lifts it.
          <Tag>
            {column.windows.length === 0 ? (
              "Not delivered"
            ) : (
              <>
                Not delivered in{" "}
                <span className={viewStyles.windowYears}>{windowYears}</span>
              </>
            )}
          </Tag>
        )}
      </span>
    </label>
  );
}
