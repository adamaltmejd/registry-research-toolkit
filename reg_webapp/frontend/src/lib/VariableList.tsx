import { type ComponentProps, useCallback } from "react";
import type { RegisterFacet, RegisterShow } from "./api";
import {
  useBrowseFilter,
  useBrowseOrder,
  useOtherRegisterMatches,
  useProviderMatches,
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
import { sharedShow } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import { Highlight } from "./Highlight";
import {
  type ColumnItem,
  ColumnList,
  type ColumnSection,
} from "./ui/ColumnList";
import { useAsyncResource } from "./use_async_resource";

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

function rankVariables(variables: VariableRef[], needle: string) {
  return rankFilter(variables, needle, (row) => [row.name, leafSlug(row.fqid)]);
}

/** The search's rows: name and slug hits first, then the server's other
 * matches (`matches`, once they arrive), which also drop name hits it lacks. */
function matchedVariables(
  variables: VariableRef[],
  needle: string,
  matches: ReadonlySet<string> | null,
): VariableRef[] {
  const ranked = rankVariables(variables, needle);
  return matches
    ? [
        ...ranked.filter((row) => matches.has(row.fqid)),
        ...variables.filter(
          (row) => matches.has(row.fqid) && !ranked.includes(row),
        ),
      ]
    : ranked;
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
        ? `${row.name}, ${catalogPeriodLabel(row.period)}`
        : undefined,
    href: catalogHref(row.fqid),
    inProject: 0,
    leaves,
  }));
}

/** The search-time label of a folded group: its one matching member by name, or
 * the count (member names contain commas, so a list would misread). */
function foldLabel(group: string, members: readonly string[] = []): string {
  const names = [...new Set(members)];
  return names.length === 1
    ? `${group}: ${names[0]}`
    : `${group}: ${names.length} matching`;
}

/** A register's rows with each concept group folded to one row (#303) that opens
 * the group's page, at the place of its first member in `items`: under its own
 * name in the name order, or at its best-ranked member while searching. While
 * searching the row names the one member that matched ("person: Kommun"), or counts
 * several ("Inkomstmått: 4 matching"): the group's own name may not contain the
 * search text. */
function foldGroups(
  items: readonly ColumnItem[],
  sets: RegisterShow["sets"],
  searching: boolean,
): ColumnItem[] {
  const setOf = new Map(
    sets.flatMap((set) => set.members.map((member) => [member, set])),
  );
  const matched = new Map<string, string[]>();
  for (const item of items) {
    const set = setOf.get(item.id);
    if (set)
      matched.set(set.fqid, [...(matched.get(set.fqid) ?? []), item.name]);
  }
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
        label: searching
          ? foldLabel(set.name, matched.get(set.fqid))
          : undefined,
        href: catalogHref(set.fqid),
      },
    ];
  });
  return searching
    ? rows
    : rows.sort((a, b) => svCollator.compare(a.name, b.name));
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

type ColumnProps = Omit<
  ComponentProps<typeof ColumnList>,
  "label" | "className" | "renderName"
>;

/**
 * The variables column (DESIGN.md → Layout): a register's variables, or on a
 * provider's page every variable of its registers. One instance serves both,
 * so opening a variable from a provider's list keeps focus on its row while
 * the list re-roots under the variable's register (ColumnList's refocus).
 */
export function VariableList({
  registerFqid,
  providerFqid,
  selectedFqid,
  className,
}: {
  registerFqid: string | null;
  providerFqid: string | null;
  selectedFqid: string | null;
  className?: string;
}) {
  const needle = useBrowseFilter();
  const renderName = useRenderName(needle);
  const register = useRegisterColumn(registerFqid, selectedFqid, needle);
  const provider = useProviderColumn(providerFqid, needle);
  return (
    <ColumnList
      label="Variables"
      className={className}
      renderName={renderName}
      {...(register ??
        provider ?? {
          sections: [],
          selectedId: null,
          selectedExact: false,
          empty: "Choose a register to list its variables.",
          error: null,
        })}
    />
  );
}

/** A register's variables, its search matches first, then other registers'
 * matches (#1828); null without a register. */
function useRegisterColumn(
  registerFqid: string | null,
  selectedFqid: string | null,
  needle: string,
): ColumnProps | null {
  const resource = useAsyncResource(registerFqid, (signal) =>
    sharedShow(registerFqid ?? "", signal),
  );
  const matches = useRegisterMatches(registerFqid);
  const otherMatches = useOtherRegisterMatches(registerFqid);
  if (registerFqid === null) return null;
  const node = resource.data?.kind === "register" ? resource.data : null;
  const variables = sortedVariables(node);
  const sets = node?.sets ?? [];
  const listed = variableItems(
    variables,
    matchedVariables(variables, needle, matches),
  );
  const items = foldGroups(listed, sets, !!needle.trim());
  // A member's address shows its group's page, so its group's row is selected.
  const selectedId =
    sets.find((set) => set.members.includes(selectedFqid ?? ""))?.fqid ??
    selectedFqid;
  const seen = new Set(listed.map((item) => item.id));
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
  return {
    subject: node?.name,
    root: registerFqid,
    // Wait for other registers before declaring an empty search result.
    sections:
      resource.loading || (otherMatches === null && items.length === 0)
        ? null
        : sections,
    selectedId: items.some((item) => item.id === selectedId)
      ? selectedId
      : null,
    selectedExact: items.some((item) => item.id === selectedId),
    empty: needle.trim()
      ? `No variables match “${needle.trim()}”. Press Escape to clear the search.`
      : "This register lists no variables.",
    notice: ownEmpty
      ? `No variables in ${node?.name ?? "this register"} match “${needle.trim()}”.`
      : null,
    error:
      resource.error || (!resource.loading && !node)
        ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
        : null,
  };
}

/**
 * A provider's page: every variable of its registers, one section per
 * register under the register's name, in the register column's order. The
 * search filters each section as a register's list is filtered, by the
 * server's matches for the whole provider in one request, so a section lists
 * what its tree row counts; a register with no match drops out. Its rows open
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
  const matches = useProviderMatches(providerFqid);
  const treeOrder = useBrowseOrder();
  if (providerFqid === null) return null;
  const provider = resource.data?.kind === "provider" ? resource.data : null;
  // While the search holds text the sections follow the register column,
  // which puts name hits first (#1828); a stable sort keeps the name order
  // otherwise.
  const place = (fqid: string) => {
    const at = treeOrder.indexOf(fqid);
    return at === -1 ? treeOrder.length : at;
  };
  const registers = sortedRegisters(provider?.registers ?? []).sort(
    (a, b) => place(a.fqid) - place(b.fqid),
  );
  const sections: ColumnSection[] = registers.map((node) => {
    const variables = sortedVariables(node);
    const items = variableItems(
      variables,
      matchedVariables(
        variables,
        needle,
        matches && (matches.get(node.fqid) ?? new Set()),
      ),
      true,
    );
    return { id: `register:${node.fqid}`, label: node.name, items };
  });
  return {
    subject: provider?.name,
    root: providerFqid,
    sections: resource.loading ? null : sections,
    selectedId: null,
    selectedExact: false,
    empty: needle.trim()
      ? `No variables match “${needle.trim()}”. Press Escape to clear the search.`
      : "This provider lists no variables.",
    error:
      resource.error || (!resource.loading && !provider)
        ? `The variables did not load${resource.error ? ` (${resource.error})` : ""}. Reload the page to try again.`
        : null,
  };
}
