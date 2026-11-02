import { useBrowseFilter, useRegisterMatches } from "./browse_filter";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, leafSlug, rankFilter, svCollator } from "./catalog";
import { sharedShow } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { Highlight } from "./Highlight";
import { type ColumnItem, ColumnList } from "./ui/ColumnList";
import { useAsyncResource } from "./use_async_resource";

export function VariableList({
  registerFqid,
  selectedFqid,
  className,
}: {
  registerFqid: string;
  selectedFqid: string | null;
  className?: string;
}) {
  const resource = useAsyncResource(registerFqid, (signal) =>
    sharedShow(registerFqid, signal),
  );
  const needle = useBrowseFilter();
  const matches = useRegisterMatches(registerFqid);
  const node = resource.data?.kind === "register" ? resource.data : null;
  const variables = [...(node?.variables ?? [])].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
  const ranked = rankFilter(variables, needle, (row) => [
    row.name,
    leafSlug(row.fqid),
  ]);
  const rows = matches
    ? [
        ...ranked.filter((row) => matches.has(row.fqid)),
        ...variables.filter(
          (row) => matches.has(row.fqid) && !ranked.includes(row),
        ),
      ]
    : ranked;
  const items = rows.map((row) => ({
    id: row.fqid,
    name: row.name,
    href: catalogHref(row.fqid),
    inProject: 0,
    period: catalogPeriodLabel(row.period),
  }));
  const periods = new Map(items.map((item) => [item.id, item.period]));
  return (
    <ColumnList
      label="Variables"
      subject={node?.name}
      root={registerFqid}
      className={className}
      sections={
        resource.loading ? null : [{ id: "variables", label: null, items }]
      }
      selectedId={
        items.some((item) => item.id === selectedFqid) ? selectedFqid : null
      }
      selectedExact={items.some((item) => item.id === selectedFqid)}
      renderName={(item: ColumnItem) => (
        <span className={styles.variableName}>
          <Highlight text={item.name} needle={needle} />
          <span className={styles.period}>{periods.get(item.id)}</span>
        </span>
      )}
      empty={
        needle.trim()
          ? `No variables match “${needle.trim()}”. Press Escape to clear the search.`
          : "This register lists no variables."
      }
      error={
        resource.error || (!resource.loading && !node)
          ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
          : null
      }
    />
  );
}

export function NoRegisterList({ className }: { className?: string }) {
  return (
    <ColumnList
      label="Variables"
      className={className}
      sections={[]}
      selectedId={null}
      selectedExact={false}
      renderName={(item) => item.name}
      empty="Choose a register to list its variables."
      error={null}
    />
  );
}
