import styles from "./CatalogNodeView.module.css";
import { sharedShow } from "./catalog_names";
import { ProviderView } from "./ProviderView";
import { RegisterView } from "./RegisterView";
import { SetView } from "./SetView";
import { TableView } from "./TableView";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { VariableView } from "./VariableView";

// Retained for the disconnected authoring views restored in later workstreams.
export interface PeriodProps {
  regMetaVersion: string;
  steward: string;
  windowMinYear: number;
  windowMaxYear: number;
  enforcePeriodBounds: boolean;
  vintageYear: number;
}

export function CatalogNodeView({ fqidPath }: { fqidPath: string }) {
  const resource = useAsyncResource(fqidPath, (signal) =>
    sharedShow(fqidPath, signal),
  );
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
  const node = resource.data;
  switch (node.kind) {
    case "provider":
      return <ProviderView node={node} />;
    case "register":
      return <RegisterView node={node} />;
    case "table":
      return <TableView node={node} />;
    case "variable":
      return <VariableView key={node.fqid} node={node} />;
    case "set":
      return <SetView node={node} />;
    default:
      return (
        <StatusRow tone="info">Not ported to the current API yet</StatusRow>
      );
  }
}
