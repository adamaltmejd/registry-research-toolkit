import { getFindings } from "./api";
import { DataWarnings } from "./DataWarnings";
import { mergeFindingGroups } from "./finding_groups";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

/** The notes on the page's subjects: one, or each of a group's members, whose
 * notes name the member they are about. */
export function ScopedDataWarnings({
  fqids,
  names,
}: {
  fqids: readonly string[];
  names?: ReadonlyMap<string, string>;
}) {
  // simplify: one request per subject; the server has no group-level read.
  const resource = useAsyncResource(fqids.join("\n"), (signal) =>
    Promise.all(
      fqids.map(async (fqid) => ({
        ref: fqid,
        groups: await getFindings(fqid, { signal }),
      })),
    ),
  );
  if (resource.loading)
    return (
      <section aria-label="Notes on this data" aria-busy="true">
        <Skeleton />
      </section>
    );
  if (resource.error || !resource.data)
    return (
      <StatusRow tone="error" live="alert">
        Notes on this data did not load ({resource.error}). Reload the page to
        try again.
      </StatusRow>
    );
  // Members of one table share its table-level findings: the merge shows each once.
  return (
    <DataWarnings groups={mergeFindingGroups(resource.data)} names={names} />
  );
}
