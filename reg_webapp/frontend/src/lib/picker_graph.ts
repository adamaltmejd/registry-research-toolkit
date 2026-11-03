/** Projection of 5.1 catalog segments onto the variable page's first lane. */
import type { VariableColumn } from "./add_columns";
import type { VariableShow } from "./api";
import { sourcePeriodLabel } from "./catalog_period";
import type { LaneSpanModel } from "./LaneGraph";
import { periodWindowRelation, yearWindowLabel } from "./period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
import { columnYearRuns, studyWindow } from "./study_window";

export function variableSpans({
  columns,
  variable,
  names,
  project,
  selection,
  axis,
  catalogBounds,
  selected,
}: {
  columns: readonly VariableColumn[];
  variable: VariableShow;
  names: ReadonlyMap<string, string>;
  project: ProjectData | null;
  selection: ProjectStudyWindow;
  axis: [number, number];
  catalogBounds: [number, number];
  selected: ReadonlySet<string>;
}): LaneSpanModel[] {
  const study = studyWindow(project);
  return columns.flatMap((column) => {
    const eras = columnYearRuns(column.segments, catalogBounds, axis[0]);
    if (!eras.length) return [];
    const first = eras[0].from;
    const last = eras[eras.length - 1].to;
    const source = project?.sources.find(
      (entry) =>
        entry.table === column.table &&
        entry.bindings.some((binding) => binding.variable === variable.fqid),
    );
    const inside = eras.some(
      (era) => era.to >= selection.from && era.from <= selection.to,
    );
    const relation = source
      ? periodWindowRelation(
          source.period,
          study,
          eras.map((era) => [`${era.from}-01-01`, `${era.to}-12-31`]),
        )
      : null;
    const note =
      relation && relation !== "same"
        ? {
            text:
              relation === "disjoint"
                ? `In project, outside the study window (${sourcePeriodLabel(source?.period)}).`
                : `In project for ${sourcePeriodLabel(source?.period)}, which differs from the study window.`,
            tone:
              relation === "disjoint" ? ("error" as const) : ("info" as const),
          }
        : null;
    const years = eras.map(yearWindowLabel).join(", ");
    const coding = [...column.segments].sort((a, b) =>
      (a.period.start ?? "").localeCompare(b.period.start ?? ""),
    );
    const changes = coding.flatMap((segment, index) =>
      index > 0 &&
      (segment.classification !== coding[index - 1].classification ||
        segment.value_set?.hash !== coding[index - 1].value_set?.hash) &&
      segment.period.start
        ? [Number(segment.period.start.slice(0, 4))]
        : [],
    );
    return [
      {
        id: column.id,
        lane: variable.fqid,
        name: column.spelling,
        from: first,
        to: last,
        segments: eras.map((era) => ({ name: null, ...era })),
        years,
        state: source ? "in" : inside ? "default" : "out",
        selectable: !!project && !source,
        isSelected: selected.has(column.id),
        staged: selected.has(column.id) && !source ? "add" : null,
        blocked: relation === "disjoint",
        note,
        changes: [...new Set(changes)],
        accessibleName: `${column.spelling}, ${variable.name}, ${names.get(column.table) ?? "Table name unavailable"}, ${years}, ${note?.text ?? (source ? "in project" : inside ? "click to select" : "outside the add window")}${changes.length ? `. Coding changes in ${[...new Set(changes)].join(", ")}.` : ""}`,
      },
    ];
  });
}
