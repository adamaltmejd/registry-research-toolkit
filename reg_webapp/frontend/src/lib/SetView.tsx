import type { Context, SetShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { registerRead, registerSubjectNames } from "./catalog_names";
import { LaneGraph } from "./LaneGraph";
import { dataAxis } from "./period";
import { memberSpans } from "./picker_graph";
import { columnYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { useProject } from "./use_project";
import { useCatalogRead } from "./use_store_read";
import { windowBounds } from "./window_bounds";

export function SetView({
  node,
  context,
}: {
  node: SetShow;
  context: Context | null;
}) {
  const { draft } = useProject();
  const register = useCatalogRead(registerRead, node.register).value;
  const names = registerSubjectNames(register);
  const axes = node.axes.flatMap((axis) => {
    const facets = node.members.map((member) =>
      member.facets.find((facet) => facet.axis === axis.id),
    );
    if (!facets.some((facet) => facet !== undefined)) return [];
    return [
      {
        ...axis,
        varying: new Set(facets.map((facet) => facet?.value)).size > 1,
        labelValue: facets[0]?.label ?? "Not stated",
      },
    ];
  });
  const varying = axes.filter((axis) => axis.varying);
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds: [number, number] = [catalogBounds.min, catalogBounds.max];
  const axis = dataAxis(
    node.members.flatMap((member) =>
      member.columns.flatMap((column) =>
        columnYearRuns(
          column.periods.map((period) => ({ period })),
          bounds,
        ).flatMap((run) => [run.from, run.to]),
      ),
    ),
    study,
    bounds,
  );
  return (
    <article className={styles.page}>
      <Definitions
        kind="group"
        title={node.name}
        memberOf={
          register
            ? [
                {
                  relation: "Variable group in",
                  label: register.name,
                  href: catalogHref(node.register),
                },
              ]
            : []
        }
        facets={
          node.members.length
            ? axes
                .filter((axis) => !axis.varying)
                .map((axis) => ({
                  label: axis.label,
                  value: axis.labelValue,
                }))
            : []
        }
        definitions={[
          ...(node.description
            ? [{ term: "Description", value: node.description }]
            : []),
          ...(node.tags.length
            ? [
                {
                  term: "Tags",
                  value: node.tags.map((tag) => tag.name).join(", "),
                },
              ]
            : []),
        ]}
      />
      <Panel title="Members">
        {node.members.length === 0 ? (
          <EmptyState title="No variables recorded for this group." />
        ) : (
          <LaneGraph
            label="Group members"
            lanes={node.members.map((member) => {
              // A lane's title is its varying facets; its name tells it apart otherwise.
              const facets = varying.flatMap((axis) => {
                const label = member.facets.find(
                  (facet) => facet.axis === axis.id,
                )?.label;
                return label ? [label] : [];
              });
              return {
                id: member.fqid,
                name: facets.length ? facets.join(", ") : member.name,
                sub: facets.length ? { text: member.name } : null,
                href: catalogHref(member.fqid),
              };
            })}
            spans={node.members.flatMap((member) =>
              memberSpans({
                member,
                names,
                project: draft,
                axis,
                catalogBounds: bounds,
              }),
            )}
            edges={[]}
            axis={axis}
            window={study ? [study.from, study.to] : null}
          />
        )}
      </Panel>
    </article>
  );
}
