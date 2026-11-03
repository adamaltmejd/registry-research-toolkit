import { useRef, useState } from "react";
import { variableColumns } from "./add_columns";
import { type CatalogSegment, errMessage, type RegisterShow } from "./api";
import { catalogHref } from "./catalog";
import { catalogLastDate, sourcePeriodLabel } from "./catalog_period";
import { focusAfterRemoval } from "./focus_after_removal";
import {
  LaneGraph,
  LaneList,
  type LaneModel,
  type LaneSpanModel,
} from "./LaneGraph";
import { PeriodThumbs } from "./PeriodThumbs";
import { sameYearWindow, uncoveredYears, yearWindowLabel } from "./period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import {
  checkProject,
  overlapIntervals,
  sourcePeriodIntervals,
  sourcePeriodYears,
} from "./reg_core";
import styles from "./SourceCard.module.css";
import {
  pinClassification,
  removeBinding,
  removeSource,
  setBindingType,
  setSourcePeriod,
} from "./source_edits";
import { columnYearRuns, tableYears } from "./study_window";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { Panel } from "./ui/Panel";
import { Select } from "./ui/Select";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { LocatedIssue } from "./ValidationPanel";
import { issueId, type ValidationIssue } from "./validation";

type Table = RegisterShow["tables"][number];
const TYPES = [
  { id: "id", name: "Identifier" },
  { id: "categorical", name: "Categorical" },
  { id: "numeric", name: "Numeric" },
  { id: "date", name: "Date" },
  { id: "datetime", name: "Date and time" },
  { id: "opaque", name: "Opaque" },
];

export function SourceCard({
  draft,
  sourceIndex,
  issues,
  raw,
  names,
  segments,
  table,
  axis,
  bounds,
  loading,
  loadError,
  onRemoved,
}: {
  draft: ProjectData;
  sourceIndex: number;
  issues: ValidationIssue[];
  raw: string | null;
  names: ReadonlyMap<string, string>;
  segments: ReadonlyMap<string, CatalogSegment[]>;
  table: Table | undefined;
  axis: [number, number];
  bounds: [number, number];
  loading: boolean;
  loadError: string | null;
  onRemoved: () => void;
}) {
  const source = draft.sources[sourceIndex];
  const heading = useRef<HTMLSpanElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [removing, setRemoving] = useState<{
    binding: number | null;
    label: string;
  } | null>(null);
  const [dragging, setDragging] = useState<ProjectStudyWindow | null>(null);
  const stored = sourcePeriodYears(source.period);
  const hull = stored
    ? { from: stored[0][0], to: stored[stored.length - 1][1] }
    : null;
  const covered = tableYears(table, bounds);
  const timeless = table?.timeless === true;
  const resting =
    hull ?? (source.period === "_default" && !timeless ? covered : null);
  const selection = dragging ?? resting;
  // A stored period outside table coverage is preserved, including both thumbs.
  const thumbBounds =
    covered && resting
      ? {
          from: Math.min(covered.from, resting.from),
          to: Math.max(covered.to, resting.to),
        }
      : null;
  const unbounded = source.period === "_default" || table?.timeless;
  const graph =
    !loading && !loadError && !!table && (hull !== null || unbounded);
  const sourcePath = `/sources/${sourceIndex}`;
  const sourceIssues = issues.filter(
    (issue) =>
      (issue.path === sourcePath || issue.path.startsWith(`${sourcePath}/`)) &&
      !issue.path.startsWith(`${sourcePath}/bindings/`),
  );

  function edit(candidate: ProjectData): boolean {
    try {
      const checked = checkProject(JSON.stringify(candidate));
      if (!checked.ok)
        throw new Error(checked.issues.map((issue) => issue.message).join(" "));
      projectStore.updateField({
        sources: candidate.sources,
        panels: candidate.panels,
      });
      setError(null);
      return true;
    } catch (cause) {
      setError(`The source was not changed. ${errMessage(cause)}`);
      return false;
    }
  }

  const spans: LaneSpanModel[] = [];
  const lanes: LaneModel[] = source.bindings.map((binding, bindingIndex) => {
    const id = binding.variable;
    const name = names.get(id) ?? "Variable name unavailable";
    const path = `${sourcePath}/bindings/${bindingIndex}`;
    const findings = issues.filter(
      (issue) => issue.path === path || issue.path.startsWith(`${path}/`),
    );
    const onTable = (segments.get(id) ?? []).filter(
      (segment) => segment.table === source.table,
    );
    const columns = variableColumns(onTable);
    const period = sourcePeriodIntervals(source.period);
    const candidates = [
      ...new Set(
        onTable
          .filter((segment) => {
            if ("error" in period) return false;
            if (period.intervals === null) return true;
            return (
              overlapIntervals(period.intervals, [
                [
                  segment.period.start ?? "0001-01-01",
                  catalogLastDate(segment.period) ?? "9999-12-31",
                ],
              ]).length > 0
            );
          })
          .flatMap((segment) =>
            segment.classification ? [segment.classification] : [],
          ),
      ),
    ];
    const pinOptions = [
      ...new Set([
        ...candidates,
        ...(binding.classification ? [binding.classification] : []),
      ]),
    ];
    for (const column of columns) {
      const runs = columnYearRuns(column.segments, bounds, axis[0]);
      if (!runs.length) continue;
      const years = runs.map(yearWindowLabel).join(", ");
      spans.push({
        id: `${id}:${column.id}`,
        lane: id,
        name: column.spelling,
        from: runs[0].from,
        to: runs[runs.length - 1].to,
        segments: runs.map((run) => ({ name: null, ...run })),
        years,
        state: "in",
        selectable: false,
        accessibleName: `${column.spelling}, ${name}, ${years}, in project`,
      });
    }
    return {
      id,
      name,
      href: catalogHref(id),
      action: (
        <Button
          variant="ghost"
          aria-label={`Remove ${columns[0]?.spelling ?? name} from ${source.name}`}
          onPress={() =>
            setRemoving({
              binding: bindingIndex,
              label: columns[0]?.spelling ?? name,
            })
          }
        >
          Remove
        </Button>
      ),
      below: (
        <div className={styles.below}>
          {!graph && (
            <div className={styles.columns}>
              {columns.map((column) => (
                <ColumnChip key={column.id} name={column.spelling} state="in" />
              ))}
            </div>
          )}
          <div className={styles.controls}>
            <Select
              label="Type"
              aria-label={`Type for ${name} in ${source.name}`}
              value={binding.type}
              options={TYPES}
              onChange={(type) =>
                edit(
                  setBindingType(
                    draft,
                    sourceIndex,
                    bindingIndex,
                    type as typeof binding.type,
                  ),
                )
              }
            />
            {(binding.classification ||
              findings.some(
                (issue) => issue.code === "classification_version_ambiguous",
              )) && (
              <Select
                label="Classification"
                aria-label={`Classification for ${name} in ${source.name}`}
                value={binding.classification ?? ""}
                options={[
                  { id: "", name: "Not pinned" },
                  ...pinOptions.map((ref) => ({
                    id: ref,
                    name: `${names.get(ref) ?? "Classification name unavailable"}${!candidates.includes(ref) ? " (not delivered in this period)" : ""}`,
                  })),
                ]}
                onChange={(ref) => {
                  const changed = edit(
                    pinClassification(
                      draft,
                      sourceIndex,
                      bindingIndex,
                      ref || null,
                    ),
                  );
                  // Clearing the pin can remove this select with its finding.
                  if (changed && !ref)
                    focusAfterRemoval([() => heading.current]);
                }}
              />
            )}
          </div>
          {binding.display_name && (
            <span>
              Output name: <code>{binding.display_name}</code>
            </span>
          )}
          <ul className={styles.plainList}>
            {findings.map((issue) => (
              <LocatedIssue
                key={issueId(issue)}
                issue={issue}
                raw={raw}
                omitPlace={["source", "variable", "table"]}
              />
            ))}
          </ul>
        </div>
      ),
    };
  });
  const removesSource =
    removing?.binding === null || source.bindings.length === 1;
  return (
    <Panel
      headingLevel={3}
      title={
        <span ref={heading} tabIndex={-1} className={styles.heading}>
          {source.name}
        </span>
      }
      action={
        <Button
          variant="ghost"
          aria-label={`Remove source ${source.name}`}
          onPress={() => setRemoving({ binding: null, label: source.name })}
        >
          Remove source
        </Button>
      }
    >
      {sourceIssues.length > 0 && (
        <ul className={`${styles.plainList} ${styles.sourceFindings}`}>
          {sourceIssues.map((issue) => (
            <LocatedIssue
              key={issueId(issue)}
              issue={issue}
              raw={raw}
              omitPlace={["source", "table"]}
            />
          ))}
        </ul>
      )}
      <p className={styles.meta}>
        <a className="link" href={catalogHref(source.table)}>
          {names.get(source.table) ?? "Table name unavailable"}
        </a>
        <span>
          Period:{" "}
          {unbounded ? (
            "Unbounded"
          ) : (
            <code>{sourcePeriodLabel(source.period)}</code>
          )}
        </span>
      </p>
      {loading && (
        <div aria-busy="true">
          <Skeleton />
        </div>
      )}
      {loadError && (
        <StatusRow tone="error">
          Coverage did not load. Reload the page to try again, or check the
          catalog for columns that are no longer available.
        </StatusRow>
      )}
      {graph ? (
        <LaneGraph
          label={`Columns of ${source.name}`}
          lanes={lanes}
          spans={spans}
          edges={[]}
          axis={axis}
          window={
            selection && !timeless ? [selection.from, selection.to] : null
          }
          coverage={{
            covered,
            years: null,
            outside:
              selection && !timeless ? uncoveredYears(selection, covered) : [],
            bandName: selection && !timeless ? "Source period" : null,
          }}
          overlay={
            selection && thumbBounds && !timeless ? (
              <PeriodThumbs
                axis={axis}
                selection={selection}
                bounds={thumbBounds}
                label={`Period for ${source.name} (years)`}
                onMove={setDragging}
                onRest={(next) => {
                  if (!hull || !sameYearWindow(next, hull))
                    edit(setSourcePeriod(draft, sourceIndex, next));
                  setDragging(null);
                }}
              />
            ) : undefined
          }
        />
      ) : (
        <LaneList label={`Columns of ${source.name}`} lanes={lanes} />
      )}
      {stored && Array.isArray(source.period) && (
        <p className={styles.note}>
          Moving a thumb replaces these years with one span.
        </p>
      )}
      {!stored && !unbounded && (
        <p className={styles.note}>
          The year thumbs cannot express this period. Change it in
          project_data.json and open the file again.
        </p>
      )}
      {error && (
        <StatusRow tone="error" live="alert">
          {error}
        </StatusRow>
      )}
      <ConfirmDialog
        isOpen={removing !== null}
        onOpenChange={(open) => {
          if (!open) setRemoving(null);
        }}
        title={
          removesSource
            ? `Remove source ${source.name}?`
            : `Remove ${removing?.label ?? "column"} from ${source.name}?`
        }
        actions={
          <>
            <Button onPress={() => setRemoving(null)}>Cancel</Button>
            <Button
              variant="danger"
              onPress={() => {
                if (!removing) return;
                const candidate =
                  removing.binding === null
                    ? removeSource(draft, sourceIndex)
                    : removeBinding(draft, sourceIndex, removing.binding);
                if (edit(candidate)) {
                  setRemoving(null);
                  if (removesSource) onRemoved();
                  else focusAfterRemoval([() => heading.current]);
                } else setRemoving(null);
              }}
            >
              {removesSource ? "Remove source" : "Remove column"}
            </Button>
          </>
        }
      >
        {removesSource
          ? source.bindings.length === 1 && removing?.binding !== null
            ? "This is the source's last column. Removing it also removes the source from this project."
            : "The source and its columns leave this project."
          : "The column leaves this source only."}
        {removesSource &&
          draft.panels?.some((panel) => panel.members.includes(source.name)) &&
          " The source is removed from its panels. Any panel left without members is also removed."}
      </ConfirmDialog>
    </Panel>
  );
}
