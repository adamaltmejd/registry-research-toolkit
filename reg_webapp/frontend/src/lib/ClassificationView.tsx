import { useState } from "react";
import type { ClassFamilyShow, Context } from "./api";
import styles from "./CatalogNodeView.module.css";
import classStyles from "./ClassificationView.module.css";
import { catalogPeriodLabel } from "./catalog_period";
import { LaneGraph, type LaneModel, type LaneSpanModel } from "./LaneGraph";
import { dataAxis } from "./period";
import { columnYearRuns, datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { useProject } from "./use_project";
import { ValueSetCodes, VersionChanges } from "./ValueSetCodes";
import { windowBounds } from "./window_bounds";

type Version = ClassFamilyShow["versions"][number];

/** A classification's page (DESIGN.md → Detail pane): its versions as lanes
 * on the year axis, then one version's codes and what changed since the
 * version before it. A version's address renders this page with that version
 * picked (`focus`). */
export function ClassificationView({
  node,
  focus,
  context,
}: {
  node: ClassFamilyShow;
  focus: string | null;
  context: Context | null;
}) {
  const { draft } = useProject();
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const versions = laneOrder(node.versions);
  // The axis reaches every version's start, past the catalog's years where a
  // version starts outside them (ICD-11, ICD-8 on a later-starting steward), so
  // every version has a span to pick (maintainer ruling, 2026-11-05).
  const starts = versions.flatMap((v) =>
    v.period.start ? [Number(v.period.start.slice(0, 4))] : [],
  );
  const bounds = [
    Math.min(catalogBounds.min, ...starts),
    Math.max(catalogBounds.max, ...starts),
  ] as const;
  const byFqid = new Map(versions.map((version) => [version.fqid, version]));
  // The latest version by start (the API's order) when no address picks one,
  // passing over a version that starts after the catalog's last year (ICD-11).
  const fallback =
    node.versions.findLast(
      (v) =>
        v.period.start === null ||
        Number(v.period.start.slice(0, 4)) <= catalogBounds.max,
    )?.fqid ??
    node.versions.at(-1)?.fqid ??
    null;
  // Only the reader's own pick is state; until they pick, the address or the
  // default (which moves when the deployment's years arrive) decides.
  const [chosen, setChosen] = useState<string | null>(null);
  const [chosenFor, setChosenFor] = useState(focus);
  if (focus !== chosenFor) {
    setChosenFor(focus);
    setChosen(null);
  }
  const picked = chosen ?? focus ?? fallback;
  const version = picked ? byFqid.get(picked) : undefined;
  const axis = dataAxis(
    datedYearRuns(versions, bounds).flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  // One lane per part, named by it; a classification without parts is one
  // lane under its own name (the approved plan). Succession within a part
  // reads as order along its lane; only a version split across parts (SUN
  // 1996) draws arrows.
  const laneOf = (v: Version) => v.part ?? node.name;
  const spans: LaneSpanModel[] = versions.flatMap((v) =>
    // A dated open end stops at the catalog's last year, not at a later
    // version's start that stretched the axis: ICD-10 does not run into
    // ICD-11's 2027. An undated version (ATC) spans the axis and ends at its
    // end, never past it (DESIGN.md → Time, year-independent spans).
    columnYearRuns(
      [v],
      [
        bounds[0],
        v.period.end
          ? bounds[1]
          : v.period.start
            ? Math.max(catalogBounds.max, Number(v.period.start.slice(0, 4)))
            : axis[1],
      ],
      axis[0],
    ).map((run) => {
      const years = catalogPeriodLabel(v.period);
      const name = versionName(v);
      return {
        id: v.fqid,
        lane: laneOf(v),
        // No column name: the version is named in the tooltip, in the UI face.
        name: "",
        label: name,
        from: run.from,
        to: run.to,
        years,
        state: "default",
        // Pressing a version picks it: the graph is the picker at every width.
        selectable: true,
        isSelected: v.fqid === picked,
        accessibleName: name,
      };
    }),
  );
  const drawn = new Set(spans.map((span) => span.id));
  const crossings = versions.flatMap((v) =>
    v.succeeds.flatMap((older) => {
      const from = byFqid.get(older);
      return from &&
        drawn.has(older) &&
        drawn.has(v.fqid) &&
        laneOf(from) !== laneOf(v)
        ? [{ from, to: v }]
        : [];
    }),
  );
  const edges = crossings.map(({ from, to }) => ({
    id: `${from.fqid}>${to.fqid}`,
    from: from.fqid,
    to: to.fqid,
    kind: "succession" as const,
  }));
  const pickedLane = version ? laneOf(version) : null;
  const lanes: LaneModel[] = [...new Set(versions.map(laneOf))].map((lane) => ({
    id: lane,
    name: lane,
    wrap: true,
    current: lane === pickedLane,
    relations: crossings.flatMap(({ from, to }) =>
      laneOf(to) === lane ? [`${to.name} continues ${from.name}`] : [],
    ),
  }));
  return (
    <article className={styles.page}>
      <Definitions
        kind="classification"
        title={node.name}
        definitions={[
          ...(node.publisher
            ? [{ term: "Publisher", value: node.publisher }]
            : []),
          ...(node.description
            ? [{ term: "Description", value: node.description }]
            : []),
        ]}
      />
      {/* Unframed and unheaded, as the variable and group pages draw theirs
          (maintainer ruling, 2026-11-05). */}
      <LaneGraph
        label="Versions"
        lanes={lanes}
        spans={spans}
        edges={edges}
        axis={axis}
        window={study ? [study.from, study.to] : null}
        focusSpan={focus}
        hint="Click a version to see its codes."
        onToggle={setChosen}
      />
      {version && (
        <Panel title={`Codes in ${version.name}`}>
          <div className={classStyles.codes}>
            <ValueSetCodes
              key={version.fqid}
              subject={{ classification: version.fqid }}
              valueSet={version.value_set}
            />
            {version.succeeds.flatMap((older) => {
              const name = byFqid.get(older)?.name;
              return name
                ? [
                    <VersionChanges
                      key={`${older}>${version.fqid}`}
                      classification={version.fqid}
                      compare={older}
                      compareName={name}
                    />,
                  ]
                : [];
            })}
          </div>
        </Panel>
      )}
    </article>
  );
}

/** The versions part by part, each part where its first version starts, and
 * by start within a part: the lanes' order and the version picker's. */
function laneOrder(versions: readonly Version[]): Version[] {
  const parts = [...new Set(versions.map((version) => version.part ?? ""))];
  return parts.flatMap((part) =>
    versions.filter((version) => (version.part ?? "") === part),
  );
}

/** A version as the picker names it: its name, then its years unless the
 * name already states them ("Utbildningsnivå 2020", not "…2020, From 2020"). */
function versionName(version: Version): string {
  const years = catalogPeriodLabel(version.period);
  const stated = years.match(/\d{4}/g) ?? [];
  return stated.length > 0 &&
    stated.every((year) => version.name.includes(year))
    ? version.name
    : `${version.name}, ${years}`;
}
