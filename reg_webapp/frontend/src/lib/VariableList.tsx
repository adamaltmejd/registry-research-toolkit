import { useCallback } from "react";
import type { RegisterFacet, RegisterShow } from "./api";
import {
  useBrowseFilter,
  useOtherRegisterMatches,
  useRegisterMatches,
} from "./browse_filter";
import styles from "./CatalogNodeView.module.css";
import {
  catalogHref,
  leafSlug,
  rankFilter,
  sortedRegisters,
  svCollator,
} from "./catalog";
import { catalogRootChildren, sharedShow } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { Highlight } from "./Highlight";
import {
  type ColumnItem,
  ColumnList,
  type ColumnSection,
} from "./ui/ColumnList";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead } from "./use_store_read";

const MORE = "more:";

/** Other registers' flat matches, already limited and ordered by the hook.
 * `origin` keeps ids unique: a steward register can share a reference id. */
function otherRegisterSections(
  facets: readonly RegisterFacet[] | null,
  seen: Set<string>,
  origin: "reference" | "steward",
): ColumnSection[] {
  return (facets ?? []).flatMap((facet) => {
    if (!facet.fqid || !facet.variables) return [];
    const name = facet.name ?? "Unnamed register";
    const items: ColumnItem[] = facet.variables
      .filter((variable) => {
        if (seen.has(variable.fqid)) return false;
        seen.add(variable.fqid);
        return true;
      })
      .map((variable) => ({
        id: variable.fqid,
        name: variable.name,
        label: `${variable.name}, in ${name}`,
        href: catalogHref(variable.fqid),
        inProject: 0,
        leaves: true,
      }));
    const rest = facet.variable_matches - facet.variables.length;
    if (rest > 0) {
      items.push({
        id: `${MORE}${origin}:${facet.fqid}`,
        name: `${rest} more in ${name}`,
        href: catalogHref(facet.fqid),
        inProject: 0,
        leaves: true,
      });
    }
    if (items.length === 0) return [];
    return [
      {
        id: `${origin === "reference" ? "register" : "steward-register"}:${facet.fqid}`,
        label: name,
        items,
      },
    ];
  });
}

type VariableRef = RegisterShow["variables"][number];

function sortedVariables(node: RegisterShow | null): VariableRef[] {
  return [...(node?.variables ?? [])].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
}

function rankVariables(variables: VariableRef[], needle: string) {
  return rankFilter(variables, needle, (row) => [row.name, leafSlug(row.fqid)]);
}

/** Names only, except where two variables of one register share a name: then
 * the years tell them apart, on screen and in the accessible name. */
function variableItems(
  variables: readonly VariableRef[],
  rows: readonly VariableRef[],
): ColumnItem[] {
  const nameCounts = new Map<string, number>();
  for (const variable of variables) {
    nameCounts.set(variable.name, (nameCounts.get(variable.name) ?? 0) + 1);
  }
  return rows.map((row) => ({
    id: row.fqid,
    name: row.name,
    label:
      (nameCounts.get(row.name) ?? 0) > 1
        ? `${row.name}, ${catalogPeriodLabel(row.period)}`
        : undefined,
    href: catalogHref(row.fqid),
    inProject: 0,
  }));
}

/** ColumnList passes this renderer into RAC's Collection dependencies. A fresh
 * function rebuilds every row, so keep it stable until the search text changes. */
function useRenderName(needle: string) {
  return useCallback(
    // A "N more in <register>" row names a register, not a match: unmarked.
    (item: ColumnItem) =>
      item.id.startsWith(MORE) ? (
        item.name
      ) : (
        <span className={styles.variableLabel}>
          <Highlight text={item.label ?? item.name} needle={needle} />
        </span>
      ),
    [needle],
  );
}

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
  const otherMatches = useOtherRegisterMatches(registerFqid);
  const node = resource.data?.kind === "register" ? resource.data : null;
  const variables = sortedVariables(node);
  const ranked = rankVariables(variables, needle);
  const rows = matches
    ? [
        ...ranked.filter((row) => matches.has(row.fqid)),
        ...variables.filter(
          (row) => matches.has(row.fqid) && !ranked.includes(row),
        ),
      ]
    : ranked;
  const items = variableItems(variables, rows);
  const renderName = useRenderName(needle);
  const seen = new Set(items.map((item) => item.id));
  const others = [
    ...otherRegisterSections(
      otherMatches?.reference ?? null,
      seen,
      "reference",
    ),
    ...otherRegisterSections(otherMatches?.steward ?? null, seen, "steward"),
  ];
  const sections = [{ id: "variables", label: null, items }, ...others];
  const ownEmpty = others.length > 0 && items.length === 0;
  return (
    <ColumnList
      label="Variables"
      subject={node?.name}
      root={registerFqid}
      className={className}
      // Wait for other registers before declaring an empty search result.
      sections={
        resource.loading || (otherMatches === null && items.length === 0)
          ? null
          : sections
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
      notice={
        ownEmpty
          ? `No variables in ${node?.name ?? "this register"} match “${needle.trim()}”.`
          : null
      }
      error={
        resource.error || (!resource.loading && !node)
          ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
          : null
      }
    />
  );
}

/**
 * The variables column on a provider's page: every variable of its registers,
 * one section per register under the register's name, in the register
 * column's order. The search filters each section by name, as in a register's
 * list, and a register with no match drops out.
 */
export function ProviderVariableList({
  providerFqid,
  className,
}: {
  providerFqid: string;
  className?: string;
}) {
  const root = useCatalogRead(catalogRootChildren, undefined);
  const provider = root.value?.find((p) => p.fqid === providerFqid) ?? null;
  const registers = provider
    ? sortedRegisters(provider.registers).map((r) => r.fqid)
    : null;
  // simplify: one cached `show` per register. The largest SWECOV provider
  // (scb) has 36 registers and 4378 variables on the 0.76.0 pair, past the
  // ~30-register trigger already: the provider's own `show` should carry its
  // registers' variables (and the list may need measuring for virtualization).
  const resource = useAsyncResource(
    registers === null ? null : `${providerFqid}\u0000${registers.join(" ")}`,
    (signal) =>
      Promise.all((registers ?? []).map((fqid) => sharedShow(fqid, signal))),
  );
  const needle = useBrowseFilter();
  const renderName = useRenderName(needle);
  // simplify: the search matches names and slugs only, not the server's
  // variable matches the register page adds, which would be one facets request
  // per register on each query. Upgrade when `/search/facets` takes a provider.
  const sections: ColumnSection[] = (resource.data ?? []).flatMap((node) => {
    if (node.kind !== "register") return [];
    const variables = sortedVariables(node);
    const items = variableItems(variables, rankVariables(variables, needle));
    return [{ id: `register:${node.fqid}`, label: node.name, items }];
  });
  const failed = root.value !== null && provider === null;
  return (
    <ColumnList
      label="Variables"
      subject={provider?.name ?? undefined}
      root={providerFqid}
      className={className}
      sections={resource.data === null ? null : sections}
      selectedId={null}
      selectedExact={false}
      renderName={renderName}
      empty={
        needle.trim()
          ? `No variables match “${needle.trim()}”. Press Escape to clear the search.`
          : "This provider lists no variables."
      }
      error={
        resource.error || failed || (!root.loading && root.value === null)
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
