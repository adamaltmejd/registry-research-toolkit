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
  const parts = new Set(versions.map((v) => v.part ?? ""));
  const successors = new Map<string, Version[]>();
  for (const v of versions)
    for (const older of v.succeeds)
      successors.set(older, [...(successors.get(older) ?? []), v]);
  const lanes: LaneModel[] = versions.map((v) => ({
    id: v.fqid,
    name: v.name,
    // A classification with parts says which each version belongs to; the
    // name alone often does too, but not always (SUN 1996).
    sub: parts.size > 1 && v.part ? { text: v.part } : null,
    wrap: true,
    current: v.fqid === picked,
    relations: [
      ...v.succeeds.flatMap((older) => {
        const name = byFqid.get(older)?.name;
        return name ? [`Continues ${name}`] : [];
      }),
      ...(successors.get(v.fqid) ?? []).map(
        (newer) => `Succeeded by ${newer.name}`,
      ),
    ],
  }));
  const spans: LaneSpanModel[] = versions.flatMap((v) =>
    columnYearRuns([v], bounds, axis[0]).map((run) => {
      const years = catalogPeriodLabel(v.period);
      return {
        id: v.fqid,
        lane: v.fqid,
        // The lane's name column names the version; the span is its years.
        name: "",
        from: run.from,
        to: run.to,
        years,
        state: "default",
        selectable: false,
        accessibleName: `${v.name}, ${years}`,
      };
    }),
  );
  const edges = versions.flatMap((v) =>
    v.succeeds
      .filter((older) => byFqid.has(older))
      .map((older) => ({
        id: `${older}>${v.fqid}`,
        from: older,
        to: v.fqid,
        kind: "succession" as const,
      })),
  );
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
          focus={focus}
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
 * by start within a part: a part's succession reads down its lanes, and an
 * arrow crosses parts only where a version is split (SUN 1996 into its
 * three parts). */
function laneOrder(versions: readonly Version[]): Version[] {
  const parts = [...new Set(versions.map((version) => version.part ?? ""))];
  return parts.flatMap((part) =>
    versions.filter((version) => (version.part ?? "") === part),
  );
}
