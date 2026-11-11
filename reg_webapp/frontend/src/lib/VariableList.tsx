import { type ComponentProps, useCallback, useMemo, useState } from "react";
import type { RegisterShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import {
  catalogHref,
  leafSlug,
  rankFilter,
  sortedRegisters,
  svCollator,
} from "./catalog";
import {
  CLASSIFICATIONS,
  CLASSIFICATIONS_LABEL,
  classFamiliesRead,
  sharedShow,
} from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { FilterInput } from "./FilterInput";
import { Highlight } from "./Highlight";
import {
  type ColumnItem,
  ColumnList,
  type ColumnSection,
} from "./ui/ColumnList";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead } from "./use_store_read";

// What the column reads of a variable: a provider's `show` carries no more.
type VariableRef = Pick<
  RegisterShow["variables"][number],
  "fqid" | "name" | "period"
>;

function sortedVariables(
  node: { variables: readonly VariableRef[] } | null,
): VariableRef[] {
  return [...(node?.variables ?? [])].sort((a, b) =>
    svCollator.compare(a.name, b.name),
  );
}

/** The rows the column's filter keeps (#2398), name and slug hits first; a
 * concept group's members also match on the group's name, so the group's row
 * stays when its own name matches. */
function rankVariables(
  variables: VariableRef[],
  needle: string,
  groupName: (fqid: string) => string | undefined = () => undefined,
) {
  return rankFilter(variables, needle, (row) => [
    row.name,
    leafSlug(row.fqid),
    groupName(row.fqid),
  ]);
}

/** Names only, except where two variables of one register share a name: then
 * the years tell them apart, on screen and in the accessible name. */
function variableItems(
  variables: readonly VariableRef[],
  rows: readonly VariableRef[],
  leaves = false,
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
        ? [row.name, catalogPeriodLabel(row.period)].filter(Boolean).join(", ")
        : undefined,
    href: catalogHref(row.fqid),
    inProject: 0,
    leaves,
  }));
}

/** A register's rows with each concept group folded to one row (#303) that opens
 * the group's page, at the place of its first member in `items`: under its own
 * name in the name order, or at its best-ranked member while filtering. The row
 * reads as the group's name, filtering or not: to a reader a group is a bigger
 * variable (maintainer 2026-10-22, #2398). */
function foldGroups(
  items: readonly ColumnItem[],
  setOf: ReadonlyMap<string, RegisterShow["sets"][number]>,
  filtering: boolean,
): ColumnItem[] {
  const folded = new Set<string>();
  const rows = items.flatMap((item) => {
    const set = setOf.get(item.id);
    if (!set) return [item];
    if (folded.has(set.fqid)) return [];
    folded.add(set.fqid);
    return [
      {
        ...item,
        id: set.fqid,
        name: set.name,
        label: undefined,
        href: catalogHref(set.fqid),
      },
    ];
  });
  return filtering
    ? rows
    : rows.sort((a, b) => svCollator.compare(a.name, b.name));
}

/** ColumnList passes this renderer into RAC's Collection dependencies. A fresh
 * function rebuilds every row, so keep it stable until the filter text changes. */
function useRenderName(needle: string) {
  return useCallback(
    (item: ColumnItem) => (
      <span className={styles.variableLabel}>
        <Highlight text={item.label ?? item.name} needle={needle} />
      </span>
    ),
    [needle],
  );
}

type ColumnProps = Omit<
  ComponentProps<typeof ColumnList>,
  "label" | "className" | "renderName" | "filter"
> & {
  /** How many rows the list holds with no filter, and with this one. */
  rows: { total: number; shown: number } | null;
};

/**
 * The variables column (DESIGN.md → Layout): a register's variables, or on a
 * provider's page every variable of its registers. One instance serves both,
 * so opening a variable from a provider's list keeps focus on its row while
 * the list re-roots under the variable's register (ColumnList's refocus).
 * Its filter box filters this column only and belongs to the list's root: it
 * starts empty when another register, provider or the classifications open
 * (#2398).
 */
export function VariableList({
  registerFqid,
  providerFqid,
  classPath,
  selectedFqid,
  className,
}: {
  registerFqid: string | null;
  providerFqid: string | null;
  /** A classification page's address (`class` or under it), or null. */
  classPath: string | null;
  selectedFqid: string | null;
  className?: string;
}) {
  const root =
    registerFqid ??
    providerFqid ??
    (classPath === null ? null : CLASSIFICATIONS);
  // Reset in render, not by remounting: the one instance keeps focus (above).
  const [filter, setFilter] = useState({ root, text: "" });
  if (filter.root !== root) setFilter({ root, text: "" });
  const needle = filter.root === root ? filter.text : "";
  const renderName = useRenderName(needle);
  const register = useRegisterColumn(registerFqid, selectedFqid, needle);
  const provider = useProviderColumn(providerFqid, needle);
  const classifications = useClassificationColumn(classPath, needle);
  const label = classifications ? CLASSIFICATIONS_LABEL : "Variables";
  const { rows, ...column } = register ??
    provider ??
    classifications ?? {
      rows: null,
      sections: [],
      size: 0,
      selectedId: null,
      selectedExact: false,
      empty: "Choose a register to list its variables.",
      error: null,
    };
  return (
    <ColumnList
      label={label}
      className={className}
      renderName={renderName}
      filter={
        rows && (
          <FilterInput
            value={needle}
            onChange={(text) => setFilter({ root, text })}
            total={rows.total}
            shown={rows.shown}
            label={`Filter ${label.toLowerCase()}`}
            placeholder={`Filter ${label.toLowerCase()}…`}
          />
        )
      }
      {...column}
    />
  );
}

/** A register's variables, the filter's matches first; null without a
 * register. */
function useRegisterColumn(
  registerFqid: string | null,
  selectedFqid: string | null,
  needle: string,
): ColumnProps | null {
  const resource = useAsyncResource(registerFqid, (signal) =>
    sharedShow(registerFqid ?? "", signal),
  );
  if (registerFqid === null) return null;
  const node = resource.data?.kind === "register" ? resource.data : null;
  const variables = sortedVariables(node);
  const sets = node?.sets ?? [];
  const setOf = new Map(
    sets.flatMap((set) => set.members.map((member) => [member, set])),
  );
  const filtering = needle.trim() !== "";
  const items = foldGroups(
    variableItems(
      variables,
      rankVariables(variables, needle, (fqid) => setOf.get(fqid)?.name),
    ),
    setOf,
    filtering,
  );
  // Rows, not variables: a group is one row, filtered or not.
  const total = filtering
    ? foldGroups(variableItems(variables, variables), setOf, false).length
    : items.length;
  // A member's address shows its group's page, so its group's row is selected.
  const selectedId =
    sets.find((set) => set.members.includes(selectedFqid ?? ""))?.fqid ??
    selectedFqid;
  return {
    rows: node && { total, shown: items.length },
    subject: node?.name,
    href: node ? catalogHref(registerFqid) : undefined,
    size: variables.length,
    root: registerFqid,
    sections: resource.loading
      ? null
      : [{ id: "variables", label: null, items }],
    selectedId: items.some((item) => item.id === selectedId)
      ? selectedId
      : null,
    // A member's address selects its group's row, which is not that page.
    selectedExact:
      selectedId === selectedFqid &&
      items.some((item) => item.id === selectedId),
    empty: filtering
      ? `No variables match “${needle.trim()}”. Clear the filter to list them all.`
      : "This register lists no variables.",
    error:
      resource.error || (!resource.loading && !node)
        ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
        : null,
  };
}

/**
 * A provider's page: every variable of its registers, one section per
 * register under the register's name, in the register column's order. The
 * filter filters each section as a register's list is filtered; a register
 * with no match drops out (ColumnList draws no empty section). Its rows open
 * a page under another register, so they are leaving rows: a click or Enter
 * opens one, never the arrows passing it. Null without a provider.
 */
function useProviderColumn(
  providerFqid: string | null,
  needle: string,
): ColumnProps | null {
  // The provider's own `show` carries its registers' variables: one request
  // for the column, shared with the page's (#2257's request budget).
  const resource = useAsyncResource(providerFqid, (signal) =>
    sharedShow(providerFqid ?? "", signal),
  );
  const provider = resource.data?.kind === "provider" ? resource.data : null;
  // Memoized: fresh sections rebuild RAC's whole collection, and the
  // virtualizer's layout cache, on every render (4,378 rows on /catalog/scb).
  const sections = useMemo(
    (): ColumnSection[] =>
      sortedRegisters(provider?.registers ?? []).map((node) => {
        const variables = sortedVariables(node);
        const items = variableItems(
          variables,
          rankVariables(variables, needle),
          true,
        );
        return { id: `register:${node.fqid}`, label: node.name, items };
      }),
    [provider, needle],
  );
  if (providerFqid === null) return null;
  const size = (provider?.registers ?? []).reduce(
    (sum, node) => sum + node.variables.length,
    0,
  );
  return {
    rows: provider && {
      total: size,
      shown: sections.reduce((sum, section) => sum + section.items.length, 0),
    },
    subject: provider?.name,
    href: provider ? catalogHref(providerFqid) : undefined,
    size,
    root: providerFqid,
    sections: resource.loading ? null : sections,
    selectedId: null,
    selectedExact: false,
    empty: needle.trim()
      ? `No variables match “${needle.trim()}”. Clear the filter to list them all.`
      : "This provider lists no variables.",
    error:
      resource.error || (!resource.loading && !provider)
        ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
        : null,
  };
}

/**
 * The classification pages' column: every classification, by name in Swedish
 * collation, as a register's column lists its variables (maintainer review
 * of the classification pages, 2026-11-05, point 2). The filter narrows it by
 * name and slug. A version's address selects its classification's row. Null
 * off the classification pages.
 */
function useClassificationColumn(
  classPath: string | null,
  needle: string,
): ColumnProps | null {
  const families = useCatalogRead(classFamiliesRead, undefined);
  if (classPath === null) return null;
  const items: ColumnItem[] = rankFilter(
    [...(families.value ?? [])].sort((a, b) =>
      svCollator.compare(a.name, b.name),
    ),
    needle,
    (family) => [family.name, leafSlug(family.fqid)],
  ).map((family) => ({
    id: family.fqid,
    name: family.name,
    href: catalogHref(family.fqid),
    inProject: 0,
  }));
  const selectedId =
    classPath === CLASSIFICATIONS
      ? null
      : classPath.split("/").slice(0, 2).join("/");
  const listed = items.some((item) => item.id === selectedId);
  return {
    rows: families.value && {
      total: families.value.length,
      shown: items.length,
    },
    size: families.value?.length ?? 0,
    href: catalogHref(CLASSIFICATIONS),
    root: CLASSIFICATIONS,
    sections: families.loading
      ? null
      : [{ id: "classifications", label: null, items }],
    selectedId: listed ? selectedId : null,
    selectedExact: listed && classPath === selectedId,
    empty: needle.trim()
      ? `No classifications match “${needle.trim()}”. Clear the filter to list them all.`
      : "The catalog has no classifications.",
    error:
      !families.loading && families.value === null
        ? "The classifications did not load. Reload the page to try again."
        : null,
  };
}
