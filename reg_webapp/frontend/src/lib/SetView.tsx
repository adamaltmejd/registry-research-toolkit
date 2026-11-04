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
      columnYearRuns(member.segments, bounds).flatMap((run) => [
        run.from,
        run.to,
      ]),
    ),
    study,
    bounds,
  );
  // A lane's title is its varying facets, else its name; the line under it is
  // the name, or the column names where title and name repeat another lane's.
  const titled = node.members.map((member) => {
    const facets = varying.flatMap((axis) => {
      const label = member.facets.find(
        (facet) => facet.axis === axis.id,
      )?.label;
      return label ? [label] : [];
    });
    return {
      member,
      title: facets.length ? facets.join(", ") : member.name,
      name: facets.length ? member.name : null,
    };
  });
  const seen = new Map<string, number>();
  for (const { title, name } of titled) {
    const key = JSON.stringify([title, name]);
    seen.set(key, (seen.get(key) ?? 0) + 1);
  }
  const lanes = titled.map(({ member, title, name }) => {
    const repeated = (seen.get(JSON.stringify([title, name])) ?? 0) > 1;
    const spellings = [
      ...new Set(member.segments.map((segment) => segment.column)),
    ].join(", ");
    return {
      id: member.fqid,
      name: title,
      sub:
        repeated && spellings
          ? name
            ? { text: `${name}, ${spellings}` }
            : { text: spellings, mono: true }
          : name
            ? { text: name }
            : null,
      href: catalogHref(member.fqid),
    };
  });
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
                  term: "Topics",
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
            lanes={lanes}
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
