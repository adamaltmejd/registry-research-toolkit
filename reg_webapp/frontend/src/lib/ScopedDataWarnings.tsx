import { getFindings } from "./api";
import { DataWarnings } from "./DataWarnings";
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
  const resource = useAsyncResource(fqids.join("\n"), async (signal) => {
    const findings = (
      await Promise.all(fqids.map((fqid) => getFindings(fqid, { signal })))
    ).flat();
    // Members of one table share its table-level findings: show each once.
    const seen = new Set<string>();
    return findings.filter((finding) => {
      const key = JSON.stringify(finding);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  });
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
  return <DataWarnings findings={resource.data} names={names} />;
}
