import { useEffect } from "react";
import {
  type ClassificationRootShow,
  type ConceptGroup,
  getStates,
  type ProviderShow,
  type ShowNode,
  type VariableStateModel,
} from "./api";
import { BindingLeafView } from "./BindingLeafView";
import styles from "./CatalogNodeView.module.css";
import {
  ClassificationGroupView,
  ClassificationLeafView,
} from "./ClassificationView";
import {
  axisNoun,
  catalogHref,
  classGroupHref,
  distinctMemberCount,
  foldGroupedRows,
  nodeLabel,
} from "./catalog";
import { sharedShow } from "./catalog_names";
import { RegisterView } from "./RegisterView";
import { router } from "./router";
import { DataTable } from "./ui/DataTable";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { Tag } from "./ui/Tag";
import type { Column } from "./ui/types";
import { useAsyncResource } from "./use_async_resource";

/** The deployment seed and the steward's period bounds, threaded from App to
 * every authoring surface under a catalog page. */
export interface PeriodProps {
  /** Empty until /api/context resolves; an add waits for both. */
  regMetaVersion: string;
  steward: string;
  windowMinYear: number;
  windowMaxYear: number;
  /** True only when the bounds are the steward's own span, not a fallback. */
  enforcePeriodBounds: boolean;
  /** The catalog's build year, for open-ended graph timelines. */
  vintageYear: number;
}

interface Props extends PeriodProps {
  fqidPath: string;
}

/**
 * One catalog node by ref (`show`), switched on its `kind`. The provider,
 * register and classification-root pages render here; a variable and a
 * classification hand over to their own views. A variable's whole state
 * history is read with the node, so one loading and error surface covers both.
 * App keys this view on `fqidPath`, so a new node is a fresh mount.
 */
export function CatalogNodeView({ fqidPath, ...period }: Props) {
  // Shared with the variables column, which reads the same register now.
  const resource = useAsyncResource(fqidPath, async (signal) => {
    const node = await sharedShow(fqidPath, signal);
    // By the CANONICAL fqid: a bare-name ref resolves to it. A grouped
    // member's page is its group's, which reads the states itself.
    const states =
      node.kind === "variable" && !node.group ? await getStates(node.fqid) : [];
    return { node, states };
  });
  const classification = fqidPath.startsWith("class/");

  if (resource.loading) {
    return (
      <section className={styles.loading} aria-busy="true">
        <div className={styles.loadingGeometry}>
          <Skeleton width="min(24rem, 70%)" count={2} />
          <Skeleton variant="block" count={classification ? 3 : 1} />
        </div>
      </section>
    );
  }
  if (resource.error || !resource.data) {
    return (
      <StatusRow tone="error" live="alert">
        {resource.status === 404 ? (
          <>
            Nothing in the catalog at <code>{fqidPath}</code>. Check the link,
            or choose a register in the list.
          </>
        ) : (
          <>
            This page did not load ({resource.error}). Reload the page to try
            again.
          </>
        )}
      </StatusRow>
    );
  }

  // The loaded page crossfades in over the skeleton it replaces.
  return (
    <div className={`${styles.loaded} motion-swap`}>
      <NodeView
        fqidPath={fqidPath}
        node={resource.data.node}
        states={resource.data.states}
        period={period}
      />
    </div>
  );
}

function NodeView({
  fqidPath,
  node,
  states,
  period,
}: {
  fqidPath: string;
  node: ShowNode;
  states: VariableStateModel[];
  period: PeriodProps;
}) {
  switch (node.kind) {
    case "provider":
      return <ProviderView node={node} />;
    case "register":
      return <RegisterView node={node} {...period} />;
    case "variable":
      // A concept group is its members' variable page (#1909).
      return node.group ? (
        <MemberRedirect groupRef={node.group} fqid={node.fqid} />
      ) : (
        <BindingLeafView node={node} states={states} {...period} />
      );
    case "classification_root":
      return <ClassificationRootView node={node} />;
    case "classification": {
      // A grouped or family edition stays a valid deep link, but its page is
      // the group or family page with this edition's tab active (#1116).
      const key = node.family?.key ?? node.dimensions[0]?.key ?? null;
      return key ? (
        <ClassificationGroupView
          groupKey={key}
          activeFqid={node.fqid}
          initialActiveNode={node}
        />
      ) : (
        <ClassificationLeafView node={node} />
      );
    }
    default:
      // The router sends the root and group refs to their own views, so a kind
      // landing here is a routing mistake: say so rather than render nothing.
      return (
        <StatusRow tone="error" live="alert">
          This page cannot show a <code>{node.kind}</code> node:{" "}
          <code>{fqidPath}</code>
        </StatusRow>
      );
  }
}

/** A grouped member's URL opens its group's page focused on it, in place of
 * this history entry, carrying the URL's query (`?period`) along. History is
 * the external system the effect syncs with; a second run in StrictMode finds
 * the URL already there and does nothing. */
function MemberRedirect({
  groupRef,
  fqid,
}: {
  groupRef: string;
  fqid: string;
}) {
  useEffect(() => {
    const query = new URLSearchParams(window.location.search);
    query.set("member", fqid);
    router.navigate(`${catalogHref(groupRef)}?${query}`, { replace: true });
  }, [groupRef, fqid]);
  return null;
}

type RegisterRow = ProviderShow["children"][number];

const registerColumns: Column<RegisterRow>[] = [
  { key: "name", label: "Register" },
  { key: "purpose", label: "Description" },
];

/** A provider's registers, each linking to its page. The top-bar search
 * filters the browse columns, never this page (#1829). */
function ProviderView({ node }: { node: ProviderShow }) {
  return (
    <article className={styles.page}>
      <h1 className={styles.title}>{nodeLabel(node)}</h1>
      {node.children.length === 0 ? (
        <Panel title="Registers">
          <EmptyState title="This provider has no registers in the catalog." />
        </Panel>
      ) : (
        <DataTable
          framed
          rowNavigation
          columns={registerColumns}
          rows={node.children}
          getRowId={(r) => r.fqid}
          renderCell={(register, column) =>
            column.key === "name" ? (
              <a
                className={`link ${styles.rowLink}`}
                href={catalogHref(register.fqid)}
                title={register.fqid}
              >
                {register.name ?? register.fqid}
              </a>
            ) : (
              register.purpose && (
                <span className={styles.clamp}>{register.purpose}</span>
              )
            )
          }
        />
      )}
    </article>
  );
}

/** One concept-group row (#303): it links to the group's own page, where its
 * members and their facets live. The count is distinct member FQIDs (#819);
 * `noun` is the plural, and every noun here pluralizes with "s" (`axisNoun`). */
export function ConceptGroupLink({
  group,
  noun,
  href,
}: {
  group: ConceptGroup;
  noun: string;
  href: string;
}) {
  const count = distinctMemberCount(group.members);
  return (
    <a className={styles.groupLink} href={href}>
      <span className={`link ${styles.rowLink}`}>{group.label}</span>
      <Tag>
        {count} {count === 1 ? noun.slice(0, -1) : noun}
      </Tag>
    </a>
  );
}

type ClassificationChild = ClassificationRootShow["children"][number];
type ClassificationFamily = ClassificationRootShow["families"][number];

type ClassificationRow =
  | {
      id: string;
      kind: "family";
      family: ClassificationFamily;
      edition: string;
    }
  | { id: string; kind: "group"; group: ConceptGroup; edition: string }
  | { id: string; kind: "leaf"; item: ClassificationChild; edition: string };

const classificationColumns: Column<ClassificationRow>[] = [
  { key: "kind", label: "Name" },
  { key: "edition", label: "Current edition", align: "end" },
];

function familyEdition(family: ClassificationFamily): string {
  const current = family.editions.filter((edition) => edition.is_current);
  if (current.length === 1) return current[0]?.slug ?? "";
  if (current.length > 1) return `${current.length} current`;
  return `${family.editions.length} editions`;
}

/** The classification systems: one-dimensional succession families (#771),
 * umbrella groups folded to one row (#516), then the ungrouped editions. */
function ClassificationRootView({ node }: { node: ClassificationRootShow }) {
  const rows: ClassificationRow[] = [
    ...node.families.map((family) => ({
      id: `family:${family.key}`,
      kind: "family" as const,
      family,
      edition: familyEdition(family),
    })),
    ...foldGroupedRows(node.children, node.groups).map((row) =>
      row.kind === "group"
        ? {
            id: `group:${row.group.key}`,
            kind: "group" as const,
            group: row.group,
            edition: "",
          }
        : {
            id: row.item.fqid,
            kind: "leaf" as const,
            item: row.item,
            edition: row.item.short_name ?? "",
          },
    ),
  ];
  return (
    <article className={styles.page}>
      <h1 className={styles.title}>{nodeLabel(node)}</h1>
      {rows.length === 0 ? (
        <EmptyState title="The catalog has no classifications." />
      ) : (
        <Panel title="Classification systems" flush>
          {/* The panel title already names the columns' contents. */}
          <DataTable
            rowNavigation
            hideHeaders
            columns={classificationColumns}
            rows={rows}
            getRowId={(row) => row.id}
            renderCell={(row, column) => {
              if (column.key === "edition") {
                return row.kind === "leaf" ? (
                  <code className={styles.edition}>{row.edition}</code>
                ) : (
                  <span className={styles.edition}>{row.edition}</span>
                );
              }
              switch (row.kind) {
                case "family":
                  return (
                    <a
                      className={`link ${styles.rowLink}`}
                      href={classGroupHref(row.family.key)}
                      title={`${row.family.editions.length} editions`}
                    >
                      {row.family.label} ({row.family.editions.length} editions)
                    </a>
                  );
                case "group":
                  return (
                    <ConceptGroupLink
                      group={row.group}
                      noun={axisNoun(row.group.axes)}
                      href={classGroupHref(row.group.key)}
                    />
                  );
                case "leaf":
                  return (
                    <a
                      className={`link ${styles.rowLink}`}
                      href={catalogHref(row.item.fqid)}
                      title={row.edition}
                    >
                      {row.item.name ?? row.item.fqid}
                    </a>
                  );
              }
            }}
          />
        </Panel>
      )}
    </article>
  );
}
