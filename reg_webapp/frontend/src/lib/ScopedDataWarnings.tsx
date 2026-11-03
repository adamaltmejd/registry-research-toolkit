import { getFindings } from "./api";
import { DataWarnings } from "./DataWarnings";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

export function ScopedDataWarnings({
  fqid,
  names,
}: {
  fqid: string;
  names?: ReadonlyMap<string, string>;
}) {
  const resource = useAsyncResource(fqid, (signal) =>
    getFindings(fqid, { signal }),
  );
  if (resource.loading)
    return (
      <section aria-label="Findings" aria-busy="true">
        <Skeleton />
      </section>
    );
  if (resource.error || !resource.data)
    return (
      <StatusRow tone="error" live="alert">
        Findings did not load ({resource.error}). Reload the page to try again.
      </StatusRow>
    );
  return <DataWarnings findings={resource.data} names={names} />;
}
