import { getFindings, type RegisterShow } from "./api";
import { DataWarnings } from "./DataWarnings";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

export function ScopedDataWarnings({
  fqid,
  register,
}: {
  fqid: string;
  register?: RegisterShow;
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
  const names = new Map<string, string>();
  if (register) {
    names.set(register.fqid, register.name);
    for (const subject of [...register.tables, ...register.variables]) {
      names.set(subject.fqid, subject.name);
    }
  }
  return <DataWarnings findings={resource.data} names={names} />;
}
