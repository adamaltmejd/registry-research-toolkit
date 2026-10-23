import { type ReactNode, useRef, useState } from "react";
import { flushSync } from "react-dom";
import type { GroupAxisModel, RelationshipGraph } from "./api";
import {
  type PickerDimension,
  pickerFilterDimensions,
  pickerRowPasses,
} from "./catalog";
import { type LaneCoverage, LaneGraph } from "./LaneGraph";
import { type PickerBand, pickerLanes } from "./picker_lanes";
import type { ProjectStudyWindow } from "./project_data";
import styles from "./RepresentationPicker.module.css";
import {
  nullBindingCommittedRowKeys,
  type PickerCommittedRow,
  pickerRowKey,
  type StagedApplyPayload,
  type StagedPick,
  stagedDiffSummary,
} from "./staged_picker";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { FilterChip } from "./ui/FilterChip";

export type { PickerBand } from "./picker_lanes";

/** The variable page's Timeline drawn into the picker's graph (#1833): its
 * header, coverage row, period slider and the words under them. */
export interface PickerTimeline {
  head: ReactNode;
  /** The band the graph shades: the active window, which mid-drag is the
   * selection. */
  band: [number, number] | null;
  coverage: LaneCoverage;
  /** The period slider over the coverage row, on the axis the graph draws. */
  overlay?: (axis: [number, number]) => ReactNode;
  foot: ReactNode;
}

interface Props {
  /** One for the binding leaf; one per member variable on a group page. */
  bands: PickerBand[];
  /** The group's facet axes: lane order and the facet filters. */
  axes?: readonly GroupAxisModel[];
  /** Offer the population and coding filters beside the facet axes. */
  includeRowDimensionFilters?: boolean;
  /** The active window (a `?period`, else the study window) as inclusive
   * years: shaded on the axis, and a span outside it reads as out. */
  window: [number, number] | null;
  /** The deployment seed is ready, so an add can mint a project. */
  canAdd: boolean;
  /** Rows already in the project, by `pickerRowKey`. */
  committedRows: ReadonlyMap<string, PickerCommittedRow>;
  /** What a committed row's source period is marked against. */
  studyWindow: ProjectStudyWindow | null;
  /** The lane of the variable this page is about. */
  focusKey?: string | null;
  graph?: RelationshipGraph | null;
  /** Draw the graph's related variables as lanes to read (the binding leaf). */
  neighbours?: boolean;
  /** The catalog's build year: where a still-delivered column ends. */
  vintageYear: number;
  /** The catalog's first and last year: the lanes' axis. */
  catalogYears: readonly [number, number];
  /** Commit the diff; false keeps the staging (a refused or abandoned add). */
  onApply: (payload: StagedApplyPayload) => Promise<boolean>;
  /** Whether a diff is now staged, so the page retires a stale status line. */
  onStageChange?: (hasDiff: boolean) => void;
  /** Draw the graph as the variable page's one Timeline. */
  timeline?: PickerTimeline;
}

interface Staging {
  adds: ReadonlySet<string>;
  removes: ReadonlySet<string>;
}

const EMPTY: Staging = { adds: new Set(), removes: new Set() };

/**
 * Choose columns for the project on a LaneGraph (DESIGN.md → Time): every span
 * is a column a researcher can tick, read against the study window and the
 * relations between variables. Ticks stage a diff against the project; one
 * Apply commits it through the host. The facet chips are a lens: they narrow
 * the lanes, never the staging.
 */
export function RepresentationPicker({
  bands,
  axes = [],
  includeRowDimensionFilters = true,
  window,
  canAdd,
  committedRows,
  studyWindow,
  focusKey = null,
  graph = null,
  neighbours = false,
  vintageYear,
  catalogYears,
  onApply,
  onStageChange,
  timeline,
}: Props) {
  const [staging, setStaging] = useState<Staging>(EMPTY);
  const pickerRef = useRef<HTMLElement>(null);
  const footerRef = useRef<HTMLDivElement>(null);
  const [filters, setFilters] = useState<Record<string, Set<string>>>({});
  const [applying, setApplying] = useState(false);

  const dimensions = pickerFilterDimensions(bands, axes, {
    includeRowDimensions: includeRowDimensionFilters,
  });
  const filtering = Object.values(filters).some((set) => set.size > 0);
  const shown = filtering
    ? bands.flatMap((band) => {
        const rows = band.rows.filter((row) =>
          pickerRowPasses(row, band, dimensions, filters),
        );
        return rows.length > 0 ? [{ ...band, rows }] : [];
      })
    : bands;

  // Every row the staging may name, shown or not: a filter hides a tick, it
  // never drops it. A staged key the bands or the project no longer carry is
  // pruned here, on read, rather than reset in an effect.
  const all = new Map<string, StagedPick>();
  for (const band of bands) {
    for (const row of band.rows) {
      const key = pickerRowKey(band, row);
      if (row.selectable !== false || committedRows.has(key)) {
        all.set(key, { band, row });
      }
    }
  }
  const adds = new Set(
    [...staging.adds].filter((key) => all.has(key) && !committedRows.has(key)),
  );
  const removes = new Set(
    [...staging.removes].filter((key) => committedRows.has(key)),
  );
  const graphModel = pickerLanes({
    bands: shown,
    axes,
    graph,
    neighbours: neighbours && !filtering,
    focusKey,
    window,
    studyWindow,
    vintageYear,
    catalogYears,
    committed: committedRows,
    stagedAdds: adds,
    stagedRemoves: removes,
  });

  /** The add bar unmounts once nothing is staged: focus it held goes to the
   * graph's first column rather than to the document body. Call it while the
   * columns are enabled (not mid-apply), or the focus is refused. */
  function keepFocus(held: boolean): void {
    if (!held) return;
    pickerRef.current
      ?.querySelector<HTMLInputElement>(
        "ol input[type=checkbox]:not(:disabled)",
      )
      ?.focus();
  }

  const barHasFocus = () =>
    footerRef.current?.contains(document.activeElement) ?? false;

  function stage(next: Staging): void {
    if (next.adds.size + next.removes.size === 0) keepFocus(barHasFocus());
    setStaging(next);
    onStageChange?.(next.adds.size + next.removes.size > 0);
  }

  function toggle(key: string): void {
    if (applying || !all.has(key)) return;
    const nextAdds = new Set(adds);
    const nextRemoves = new Set(removes);
    const committed = committedRows.get(key);
    if (committed) {
      // A null-representation binding covers every row of its variable and
      // period, so unticking one unticks them all.
      const keys = nullBindingCommittedRowKeys(
        committedRows.values(),
        committed,
      );
      const removing = !nextRemoves.has(key);
      for (const k of keys) {
        if (removing) nextRemoves.add(k);
        else nextRemoves.delete(k);
      }
    } else if (!nextAdds.delete(key)) {
      nextAdds.add(key);
    }
    stage({ adds: nextAdds, removes: nextRemoves });
  }

  function toggleFilter(dim: PickerDimension, value: string): void {
    setFilters((current) => {
      const next = new Set(current[dim.key]);
      if (!next.delete(value)) next.add(value);
      return { ...current, [dim.key]: next };
    });
  }

  const visible = new Set(graphModel.rows.keys());
  // Committed columns whose source and the study window disagree: the
  // reason, in words, under the graph. Under the Timeline the ▲ row already
  // says the years differ, so only a column outside the window is named.
  const notes = graphModel.spans.filter(
    (span) => span.note && (!timeline || span.note.tone === "error"),
  );
  const hidden = [...adds, ...removes].filter((k) => !visible.has(k)).length;
  const summary = stagedDiffSummary({
    added: adds.size,
    removed: removes.size,
  });
  const summaryText =
    summary === ""
      ? ""
      : `${summary}${hidden > 0 ? ` (${hidden} hidden by the filters)` : ""}`;
  const canApply = adds.size + removes.size > 0 && (adds.size === 0 || canAdd);
  const applyLabel =
    adds.size > 0 && removes.size > 0
      ? "Apply changes"
      : removes.size > 0
        ? "Remove from project"
        : "Add to project";

  async function apply(): Promise<void> {
    if (!canApply || applying) return;
    const payload: StagedApplyPayload = {
      adds: [...adds].flatMap((key): StagedPick[] => {
        const entry = all.get(key);
        return entry ? [entry] : [];
      }),
      removes: [...removes].flatMap((key) => {
        const committed = committedRows.get(key);
        return committed ? [{ committed }] : [];
      }),
    };
    setApplying(true);
    try {
      if (await onApply(payload)) {
        const held = barHasFocus();
        // Commit first, so the columns are enabled again when focus moves.
        flushSync(() => {
          setApplying(false);
          setStaging(EMPTY);
        });
        keepFocus(held);
      }
    } finally {
      setApplying(false);
    }
  }

  return (
    <section
      ref={pickerRef}
      className={styles.picker}
      // Under the Timeline the card around it names the region.
      aria-label={timeline ? undefined : "Columns"}
    >
      {timeline && <div className={styles.timelineHead}>{timeline.head}</div>}
      {dimensions.length > 0 && (
        <div className={styles.filters}>
          {dimensions.map((dim) => (
            <fieldset key={dim.key} className={styles.filter}>
              <legend className="micro-label">{dim.label}</legend>
              <div className={styles.chips}>
                {dim.values.map((v) => (
                  <FilterChip
                    key={v.value}
                    isSelected={filters[dim.key]?.has(v.value) ?? false}
                    onChange={() => toggleFilter(dim, v.value)}
                  >
                    {v.label}
                    {v.sharedLabel && (
                      <code className={styles.variantKey}>{v.value}</code>
                    )}
                  </FilterChip>
                ))}
              </div>
            </fieldset>
          ))}
          {filtering && (
            <Button variant="ghost" onPress={() => setFilters({})}>
              Show every column
            </Button>
          )}
        </div>
      )}
      {(graphModel.lanes.length > 0 || timeline) && (
        <LaneGraph
          label={
            bands.length === 1 ? "Columns over time" : "Variables over time"
          }
          lanes={graphModel.lanes}
          spans={graphModel.spans}
          edges={graphModel.edges}
          axis={graphModel.axis}
          window={timeline ? timeline.band : window}
          coverage={timeline?.coverage}
          overlay={timeline?.overlay}
          isDisabled={applying}
          onToggle={toggle}
        />
      )}
      {graphModel.lanes.length === 0 && filtering && (
        <p className="muted">
          No column matches these filters.{" "}
          <Button variant="ghost" onPress={() => setFilters({})}>
            Show every column
          </Button>
        </p>
      )}
      {timeline?.foot}
      {notes.length > 0 && (
        <ul className={styles.notes}>
          {notes.map((span) => (
            <li key={span.id} data-tone={span.note?.tone}>
              {span.note?.tone === "error" && (
                <span aria-hidden="true">✕ </span>
              )}
              <ColumnChip name={span.name} state={span.state} />{" "}
              {span.note?.text}
            </li>
          ))}
        </ul>
      )}
      {/* One standing live region, so the first tick is announced; the bar
          that shows the same words mounts with it. */}
      <output className="visually-hidden" aria-live="polite">
        {summaryText}
      </output>
      {/* Only while something is staged, like the register's add bar
          (DESIGN.md → VariableRow); the graph says how to tick. */}
      {(summary !== "" || applying) && (
        <div ref={footerRef} className={styles.footer}>
          <span className={styles.summary} aria-hidden="true">
            {summaryText}
          </span>
          <div className={styles.actions}>
            <Button isInert={applying} onPress={() => stage(EMPTY)}>
              Clear selection
            </Button>
            <Button
              variant="primary"
              isInert={!canApply || applying}
              isBusy={applying}
              onPress={() => void apply()}
            >
              {applyLabel}
            </Button>
          </div>
        </div>
      )}
    </section>
  );
}
