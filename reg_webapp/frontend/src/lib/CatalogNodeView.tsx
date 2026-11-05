import type { Context, OrderCheck } from "./api";
import styles from "./CatalogNodeView.module.css";
import { sharedShow } from "./catalog_names";
import { GroupView } from "./GroupView";
import { ProviderView } from "./ProviderView";
import { RegisterView } from "./RegisterView";
import { TableView } from "./TableView";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { VariableView } from "./VariableView";

export function CatalogNodeView({
  fqidPath,
  context,
  validation,
}: {
  fqidPath: string;
  context: Context | null;
  validation: OrderCheck | null;
}) {
  // A grouped variable has no page of its own: its address shows its
  // group's page with its lane in focus (a variable is in at most one group).
  const resource = useAsyncResource(fqidPath, async (signal) => {
    const node = await sharedShow(fqidPath, signal);
    if (node.kind !== "variable" || !node.sets?.length)
      return { node, focus: null };
    return {
      node: await sharedShow(node.sets[0].fqid, signal),
      focus: node.fqid,
    };
  });
  if (resource.loading) {
    return (
      <section className={styles.loading} aria-busy="true">
        <Skeleton width="min(24rem, 70%)" count={2} />
        <Skeleton variant="block" />
      </section>
    );
  }
  if (resource.error || !resource.data) {
    return (
      <StatusRow tone="error" live="alert">
        {resource.status === 404 || resource.status === 422 ? (
          <>
            Nothing in the catalog at this address. Check the link, or choose a
            register in the list.
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
  const { node, focus } = resource.data;
  switch (node.kind) {
    case "provider":
      return <ProviderView node={node} context={context} />;
    case "register":
      return <RegisterView key={node.fqid} node={node} context={context} />;
    case "table":
      return <TableView node={node} context={context} />;
    case "variable":
      return (
        <VariableView
          key={node.fqid}
          node={node}
          context={context}
          validation={validation}
        />
      );
    case "set":
      return (
        <GroupView
          key={node.fqid}
          node={node}
          focus={focus}
          context={context}
          validation={validation}
        />
      );
    default:
      return (
        <StatusRow tone="info">Not ported to the current API yet</StatusRow>
      );
  }
}
