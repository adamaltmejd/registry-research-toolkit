import { useState } from "react";
import type { GroupAxisModel, RelationshipGraph } from "./api";
import {
  type PickerDimension,
  pickerFilterDimensions,
  pickerRowPasses,
} from "./catalog";
import { LaneGraph } from "./LaneGraph";
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
}: Props) {
  const [staging, setStaging] = useState<Staging>(EMPTY);
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

  function stage(next: Staging): void {
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
  // reason, in words, under the graph.
  const notes = graphModel.spans.filter((span) => span.note);
  const hidden = [...adds, ...removes].filter((k) => !visible.has(k)).length;
  const summary = stagedDiffSummary({
    added: adds.size,
    removed: removes.size,
  });
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
      if (await onApply(payload)) setStaging(EMPTY);
    } finally {
      setApplying(false);
    }
  }

  return (
    <section className={styles.picker} aria-label="Columns">
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
      {graphModel.lanes.length > 0 ? (
        <LaneGraph
          label={
            bands.length === 1 ? "Columns over time" : "Variables over time"
          }
          lanes={graphModel.lanes}
          spans={graphModel.spans}
          edges={graphModel.edges}
          axis={graphModel.axis}
          window={window}
          isDisabled={applying}
          onToggle={toggle}
        />
      ) : (
        <p className="muted">
          No column matches these filters.{" "}
          <Button variant="ghost" onPress={() => setFilters({})}>
            Show every column
          </Button>
        </p>
      )}
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
      <div className={styles.footer}>
        <output className={styles.summary} aria-live="polite">
          {summary === ""
            ? "Tick a column to add it to the project."
            : `${summary}${hidden > 0 ? ` (${hidden} hidden by the filters)` : ""}`}
        </output>
        <div className={styles.actions}>
          {/* Always mounted, inert with nothing staged: a press that empties
              the selection keeps focus here instead of dropping to body. */}
          <Button
            isInert={applying || summary === ""}
            onPress={() => stage(EMPTY)}
          >
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
    </section>
  );
}
