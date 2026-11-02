import { useCallback } from "react";
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
  const nameCounts = new Map<string, number>();
  for (const variable of variables) {
    nameCounts.set(variable.name, (nameCounts.get(variable.name) ?? 0) + 1);
  }
  const items = rows.map((row) => {
    const period = catalogPeriodLabel(row.period);
    return {
      id: row.fqid,
      name: row.name,
      label:
        (nameCounts.get(row.name) ?? 0) > 1
          ? `${row.name}, ${period}`
          : undefined,
      href: catalogHref(row.fqid),
      inProject: 0,
      period,
    };
  });
  // ColumnList passes this renderer into RAC's Collection dependencies. A fresh
  // function rebuilds every row, so keep it stable until the search text changes.
  const renderName = useCallback(
    (item: ColumnItem) => (
      <span className={styles.variableName}>
        <Highlight text={item.name} needle={needle} />
        <span className={styles.period}>{item.period}</span>
      </span>
    ),
    [needle],
  );
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
      renderName={renderName}
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
