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
import { Select } from "./ui/Select";
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
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const versions = laneOrder(node.versions);
  const byFqid = new Map(versions.map((version) => [version.fqid, version]));
  // The latest version by start (the API's order) when no address picks one,
  // passing over a version that starts after the catalog's last year (ICD-11).
  const fallback =
    node.versions.findLast(
      (v) =>
        v.period.start === null ||
        Number(v.period.start.slice(0, 4)) <= bounds[1],
    )?.fqid ??
    node.versions.at(-1)?.fqid ??
    null;
  const [picked, setPicked] = useState(focus ?? fallback);
  const [pickedFor, setPickedFor] = useState(focus);
  if (focus !== pickedFor) {
    setPickedFor(focus);
    setPicked(focus ?? fallback);
  }
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
    // A version that starts after the axis ends (ICD-11) has no span; it
    // stays in the version picker.
    columnYearRuns([v], bounds, axis[0]).map((run) => {
      const years = catalogPeriodLabel(v.period);
      return {
        id: v.fqid,
        lane: laneOf(v),
        // No column name: the version is named in the tooltip, in the UI face.
        name: "",
        label: v.name,
        from: run.from,
        to: run.to,
        years,
        state: "default",
        selectable: false,
        accessibleName: `${v.name}, ${years}`,
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
  const focused = focus ? byFqid.get(focus) : undefined;
  const focusLane = focused ? laneOf(focused) : null;
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
      <Panel title="Versions">
        <LaneGraph
          label="Versions"
          lanes={lanes}
          spans={spans}
          edges={edges}
          axis={axis}
          window={study ? [study.from, study.to] : null}
          focus={focusLane}
        />
      </Panel>
      {version && (
        <Panel title="Codes">
          <div className={classStyles.codes}>
            {versions.length > 1 && (
              <Select
                label="Version"
                value={version.fqid}
                options={versions.map((v) => ({
                  id: v.fqid,
                  name: `${v.name}, ${catalogPeriodLabel(v.period)}`,
                }))}
                onChange={setPicked}
              />
            )}
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
