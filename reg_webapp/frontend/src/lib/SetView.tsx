import type { SetShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { registerRead } from "./catalog_names";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { useCatalogRead } from "./use_store_read";

export function SetView({ node }: { node: SetShow }) {
  const register = useCatalogRead(registerRead, node.register).value;
  const axes = node.axes.map((axis) => {
    const facets = node.members.map((member) =>
      member.facets.find((facet) => facet.axis === axis.id),
    );
    return {
      ...axis,
      varying: new Set(facets.map((facet) => facet?.value)).size > 1,
      labelValue: facets[0]?.label ?? "Not stated",
    };
  });
  const varying = axes.filter((axis) => axis.varying);
  // Keep axis ids separate from the variable column key, even for an axis named "member".
  const rows: {
    member: SetShow["members"][number];
    [key: `axis:${string}`]: string;
  }[] = node.members.map((member) => ({
    member,
    ...Object.fromEntries(
      varying.map((axis) => [
        `axis:${axis.id}`,
        member.facets.find((facet) => facet.axis === axis.id)?.label ??
          "Not stated",
      ]),
    ),
  }));
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
      <Panel
        title="Members"
        flush={node.members.length > 0 && varying.length > 0}
      >
        {node.members.length === 0 ? (
          <EmptyState title="No variables recorded for this group." />
        ) : varying.length === 0 ? (
          <ul className={styles.plainList}>
            {node.members.map((member) => (
              <li key={member.fqid}>
                <a className="link" href={catalogHref(member.fqid)}>
                  {member.name}
                </a>
              </li>
            ))}
          </ul>
        ) : (
          <DataTable
            rowNavigation
            columns={[
              { key: "member", label: "Variable" },
              ...varying.map((axis) => ({
                key: `axis:${axis.id}` as const,
                label: axis.label,
              })),
            ]}
            rows={rows}
            getRowId={(row) => row.member.fqid}
            renderCell={(row, column) =>
              column.key === "member" ? (
                <a className="link" href={catalogHref(row.member.fqid)}>
                  {row.member.name}
                </a>
              ) : (
                row[column.key]
              )
            }
          />
        )}
      </Panel>
    </article>
  );
}
