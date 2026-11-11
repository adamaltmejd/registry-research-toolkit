import { useRef, useState } from "react";
import { columnInSource, columnRows, variableColumns } from "./add_columns";
import { type CatalogSegment, errMessage, type RegisterShow } from "./api";
import { catalogHref } from "./catalog";
import {
  catalogLastDate,
  catalogPeriodWithin,
  segmentYears,
  sourcePeriodLabel,
} from "./catalog_period";
import { LocatedIssue } from "./Finding";
import { focusAfterRemoval } from "./focus_after_removal";
import {
  LaneGraph,
  LaneList,
  type LaneModel,
  type LaneSpanModel,
} from "./LaneGraph";
import { PeriodThumbs } from "./PeriodThumbs";
import {
  sameYearWindow,
  sourcePeriodYearWindows,
  uncoveredYears,
} from "./period";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import {
  checkProject,
  overlapIntervals,
  sourcePeriodIntervals,
  sourcePeriodYears,
  studyWindowInterval,
} from "./reg_core";
import styles from "./SourceCard.module.css";
import {
  pinClassification,
  removeBinding,
  removeSource,
  setSourcePeriod,
} from "./source_edits";
import { columnDateRuns, tableYears } from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EntityPill } from "./ui/EntityPill";
import { Panel } from "./ui/Panel";
import { Select } from "./ui/Select";
import { NameSkeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { issueId, issueNames, type ValidationIssue } from "./validation";

type Table = RegisterShow["tables"][number];

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
  sharedHeading = false,
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
  /** Another table of the project has the same register heading: the
   * heading then names this table too (maintainer ruling, #2221). */
  sharedHeading?: boolean;
  onRemoved: () => void;
}) {
  const source = draft.sources[sourceIndex];
  // The card is headed by its register and names its table in every label,
  // as the page's findings do: the source's own name is the file's join key.
  const register = names.get(source.table.split("/").slice(0, 2).join("/"));
  const tableName = names.get(source.table) ?? source.name;
  // While the card's reads are in flight a missing name is a placeholder, not
  // the join key it would otherwise flash.
  const naming = loading && !names.has(source.table);
  const shownName = naming ? (
    <NameSkeleton label="Loading table name" />
  ) : (
    tableName
  );
  const heading = useRef<HTMLSpanElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [removing, setRemoving] = useState<{
    binding: number | null;
    label: string;
  } | null>(null);
  const [dragging, setDragging] = useState<ProjectStudyWindow | null>(null);
  // Any dated period rests the thumbs on its years, a date period's too
  // (#2409); only whole years are what the thumbs write.
  const stored = sourcePeriodYearWindows(source.period);
  const wholeYears = sourcePeriodYears(source.period);
  const hull = stored
    ? { from: stored[0].from, to: stored[stored.length - 1].to }
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
  const periodYears = unbounded ? null : sourcePeriodLabel(source.period);
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
      setError(`The table was not changed. ${errMessage(cause)}`);
      return false;
    }
  }

  const period = sourcePeriodIntervals(source.period);
  // The days the source asks for, or the dragged window's while a thumb
  // moves; null (every day) for a year-independent source.
  const within = dragging
    ? [studyWindowInterval(dragging)].filter((days) => days !== null)
    : "intervals" in period
      ? period.intervals
      : null;
  const spans: LaneSpanModel[] = [];
  const lanes: LaneModel[] = source.bindings.map((binding, bindingIndex) => {
    const id = binding.variable;
    const path = `${sourcePath}/bindings/${bindingIndex}`;
    const findings = issues.filter(
      (issue) => issue.path === path || issue.path.startsWith(`${path}/`),
    );
    // A variable this steward does not offer has no page to read its name
    // from; the check's finding about it names it (#2220).
    const name =
      names.get(id) ??
      findings.map((issue) => issueNames(issue).get(id)).find(Boolean) ??
      "Variable name unavailable";
    const onTable = (segments.get(id) ?? []).filter(
      (segment) => segment.table === source.table,
    );
    const columns = variableColumns(onTable);
    // The server's verdict that the chosen database lacks the column: its lane
    // draws it blocked, as the variable page draws a column outside the study
    // years, not with the plain in-project fill (#2343).
    const elsewhere = findings.some(
      (issue) => issue.code === "not_in_database",
    );
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
    // One row per run of the variable's columns, successive spellings on one
    // row (#2408). A lane is in the project for its source's days only, so a
    // column is drawn over those days and one outside them is not drawn.
    columnRows(columns).forEach((row, index) => {
      const drawn = row.flatMap((column) => {
        const inProject = within
          ? column.segments.flatMap((segment) =>
              catalogPeriodWithin(segment.period, within).map((period) => ({
                period,
              })),
            )
          : column.segments;
        return inProject.length ? [{ column, inProject }] : [];
      });
      // In date order, so a spelling that returns draws its changes.
      const segments = drawn
        .flatMap(({ column, inProject }) =>
          columnDateRuns(inProject, bounds, axis[0]).map((run) => ({
            name: column.spelling,
            ...run,
          })),
        )
        .sort((a, b) => a.from - b.from);
      if (!segments.length) return;
      const spellings = drawn.map(({ column }) => column.spelling);
      const years = segmentYears(drawn.flatMap(({ inProject }) => inProject));
      spans.push({
        id: `${id}:${index}`,
        lane: id,
        name: spellings.join(", "),
        from: Math.min(...segments.map((segment) => segment.from)),
        to: Math.max(...segments.map((segment) => segment.to)),
        segments,
        years,
        columns:
          drawn.length > 1
            ? drawn.map(({ column, inProject }) => ({
                name: column.spelling,
                years: segmentYears(inProject),
              }))
            : undefined,
        state: "in",
        blocked: elsewhere,
        selectable: false,
        accessibleName: [
          ...spellings,
          name,
          years,
          elsewhere ? "not in the chosen database" : "in project",
        ]
          .filter(Boolean)
          .join(", "),
      });
    });
    const pinned =
      !!binding.classification ||
      findings.some(
        (issue) => issue.code === "classification_version_ambiguous",
      );
    return {
      id,
      name,
      // A variable the catalog does not name has no page to link to.
      href: names.has(id) ? catalogHref(id) : null,
      action: (
        <Button
          variant="ghost"
          // Named by the variable, as the lane is.
          aria-label={`Remove ${name} from ${tableName}`}
          onPress={() => setRemoving({ binding: bindingIndex, label: name })}
        >
          Remove
        </Button>
      ),
      // Only a slot with something in it: an empty one still took a padded row
      // under the lane, with a sliver of the study band.
      below: (!graph || pinned || findings.length > 0) && (
        <div className={styles.below}>
          {!graph && (
            <div className={styles.columns}>
              {columns.map((column) => (
                <EntityPill
                  key={column.id}
                  entity="column"
                  name={column.spelling}
                  state={
                    !elsewhere && columnInSource(column, source) ? "in" : "out"
                  }
                />
              ))}
            </div>
          )}
          {pinned && (
            <div className={styles.controls} aria-busy={loading || undefined}>
              {/* The names come from the card's own rows, so until they land a
                pinned version has none: a placeholder, not a failure. */}
              {loading ? (
                <NameSkeleton label="Loading classification names" />
              ) : (
                <Select
                  label="Classification"
                  aria-label={`Classification for ${name} in ${tableName}`}
                  value={binding.classification ?? ""}
                  options={[
                    { id: "", name: "Not pinned" },
                    ...pinOptions.map((ref) => ({
                      id: ref,
                      // `catalogNames` names a version from the rows already
                      // read, or by its id, never with a `show` per version.
                      name: `${names.get(ref) ?? ref}${!candidates.includes(ref) ? " (not delivered in this period)" : ""}`,
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
          )}
          {findings.length > 0 && (
            <ul className={styles.plainList}>
              {findings.map((issue) => (
                <LocatedIssue
                  key={issueId(issue)}
                  issue={issue}
                  raw={raw}
                  omitPlace={["variable", "table", "period"]}
                />
              ))}
            </ul>
          )}
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
        <span
          ref={heading}
          tabIndex={-1}
          className={styles.heading}
          aria-busy={naming || undefined}
        >
          {register ?? shownName}
          {/* The table's pill (#2399), only where two tables of one register
              would share the heading (#2221). */}
          {register && sharedHeading && (
            <EntityPill entity="table" name={tableName} />
          )}
        </span>
      }
      action={
        <Button
          variant="ghost"
          aria-label={naming ? "Remove table" : `Remove table ${tableName}`}
          onPress={() => setRemoving({ binding: null, label: tableName })}
        >
          Remove table
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
              omitPlace={["table", "period"]}
            />
          ))}
        </ul>
      )}
      <p className={styles.meta}>
        {naming ? (
          shownName
        ) : (
          <EntityPill
            entity="table"
            name={tableName}
            href={catalogHref(source.table)}
          />
        )}
        {periodYears && (
          <span>
            Years: <code>{periodYears}</code>
          </span>
        )}
      </p>
      {loading && (
        <div aria-busy="true">
          <StatusRow tone="info">Loading years…</StatusRow>
        </div>
      )}
      {loadError && (
        <StatusRow tone="error">
          The years did not load. Reload the page to try again, or check the
          catalog for columns that are no longer available.
        </StatusRow>
      )}
      {graph ? (
        <LaneGraph
          label={`Columns of ${tableName}`}
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
          }}
          overlay={
            selection && thumbBounds && !timeless ? (
              <PeriodThumbs
                axis={axis}
                selection={selection}
                bounds={thumbBounds}
                label={`Years for ${tableName}`}
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
      ) : loading ? null : (
        <LaneList label={`Columns of ${tableName}`} lanes={lanes} />
      )}
      {graph && stored && wholeYears?.length !== 1 && (
        <p className={styles.note}>
          {wholeYears
            ? "Moving a thumb replaces these years with one span."
            : "Moving a thumb replaces this period with whole years."}
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
            ? naming
              ? "Remove this table?"
              : `Remove table ${tableName}?`
            : `Remove ${removing?.label ?? "column"} from ${tableName}?`
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
              {removesSource ? "Remove table" : "Remove column"}
            </Button>
          </>
        }
      >
        {removesSource
          ? source.bindings.length === 1 && removing?.binding !== null
            ? "This is the table's last column. Removing it also removes the table from this project."
            : "The table and its columns leave this project."
          : "The column leaves this table only."}
        {removesSource &&
          draft.panels?.some((panel) => panel.members.includes(source.name)) &&
          " The table is removed from its panels. Any panel left without members is also removed."}
      </ConfirmDialog>
    </Panel>
  );
}
