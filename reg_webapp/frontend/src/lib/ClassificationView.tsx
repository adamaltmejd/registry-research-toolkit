import { useCallback, useMemo } from "react";
import type { ClassFamilyShow, Context } from "./api";
import styles from "./CatalogNodeView.module.css";
import classStyles from "./ClassificationView.module.css";
import { CodeTimeline } from "./CodeTimeline";
import { catalogHref, svCollator } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { dataAxis } from "./period";
import { columnYearRuns, datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

/** A classification family's page (DESIGN.md → Detail pane): its versions in
 * start order, each over the steward's variables that use it, then the codes
 * those variables' columns use. A version's address renders this page with
 * that version `focus`ed (Operator Q4, #2252). */
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
  const bound = useMemo(
    () =>
      new Set(
        draft?.sources.flatMap((source) =>
          source.bindings.map((binding) => binding.variable),
        ) ?? [],
      ),
    [draft],
  );
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const studyYears: [number, number] | null = study
    ? [study.from, study.to]
    : null;
  const variables = [...node.variables].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  const axis = dataAxis(
    columnYearRuns(
      variables.flatMap((variable) => variable.segments),
      bounds,
    ).flatMap((run) => [run.from, run.to]),
    study,
    bounds,
  );
  // Like a grouped variable's lane: the focused version takes focus unless
  // the reader is already elsewhere, and scrolls only when out of view.
  const focusVersion = useCallback((heading: HTMLHeadingElement | null) => {
    if (!heading) return;
    const active = document.activeElement;
    if (active && active !== document.body) return;
    heading.focus({ preventScroll: true });
    const bar =
      Number.parseFloat(
        getComputedStyle(document.documentElement).getPropertyValue(
          "--top-bar-height",
        ),
      ) || 0;
    const { top, bottom } = heading.getBoundingClientRect();
    if (top < bar || bottom > window.innerHeight)
      heading.scrollIntoView({ block: "center" });
  }, []);
  return (
    <article className={styles.page}>
      <Definitions
        title={node.name}
        definitions={
          node.description
            ? [{ term: "Description", value: node.description }]
            : []
        }
      />
      <Panel title="Versions" flush>
        <ul className={classStyles.versions}>
          {node.versions.map((version) => {
            const users = variables.flatMap((variable) => {
              const rows = variable.segments.filter(
                (segment) => segment.classification === version.fqid,
              );
              return rows.length > 0 ? [{ variable, rows }] : [];
            });
            const current = version.fqid === focus;
            return (
              <li
                key={version.fqid}
                className={classStyles.version}
                data-current={current || undefined}
              >
                <div className={classStyles.versionHead}>
                  <h3
                    ref={current ? focusVersion : undefined}
                    tabIndex={current ? -1 : undefined}
                    className={classStyles.versionName}
                  >
                    {version.name}
                  </h3>
                  <span className={classStyles.years}>
                    {catalogPeriodLabel(version.period)}
                  </span>
                </div>
                {users.length > 0 ? (
                  <VariableRowList
                    label={`Variables using ${version.name}`}
                    axis={axis}
                  >
                    {users.map(({ variable, rows }) => (
                      <VariableRow
                        key={variable.fqid}
                        name={variable.name}
                        href={catalogHref(variable.fqid)}
                        inProject={bound.has(variable.fqid)}
                        coverage={datedYearRuns(rows, bounds)}
                        axis={axis}
                        window={studyYears}
                      />
                    ))}
                  </VariableRowList>
                ) : (
                  <p className={classStyles.unused}>
                    No variable here uses this version.
                  </p>
                )}
              </li>
            );
          })}
        </ul>
      </Panel>
      <CodeTimeline
        members={variables}
        axis={axis}
        window={studyYears}
        bounds={bounds}
      />
    </article>
  );
}
