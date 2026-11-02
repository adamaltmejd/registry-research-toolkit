import { useCallback } from "react";
import type { RegisterFacet } from "./api";
import {
  useBrowseFilter,
  useOtherRegisterMatches,
  useRegisterMatches,
} from "./browse_filter";
import {
  catalogHref,
  foldGroupedRows,
  groupFilterKeys,
  groupHref,
  keyVariablesFirst,
  leafSlug,
  matchesFilter,
  nodeLabel,
  rankFilter,
} from "./catalog";
import { registerRead } from "./catalog_names";
import { Highlight } from "./Highlight";
import { safeSourceBindings, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store";
import {
  type ColumnItem,
  ColumnList,
  type ColumnSection,
} from "./ui/ColumnList";
import { useCatalogRead, useProjectRead } from "./use_store_read";

/** What the open page is under the register: a variable (by FQID) or a concept
 * group (by key). */
export type VariableListFocus =
  | { kind: "variable"; fqid: string }
  | { kind: "group"; key: string };

/** Columns in the open project, per variable FQID. */
function readVariableCounts(): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const source of safeSourceSlots(projectStore.draft?.sources)) {
    for (const binding of safeSourceBindings(source)) {
      if (typeof binding.variable === "string") {
        counts[binding.variable] = (counts[binding.variable] ?? 0) + 1;
      }
    }
  }
  return counts;
}

const MORE = "more:";

/** Other registers' matches below the open register's (#1828): one section
 * per register under its name, its first matches, then a row for the rest
 * that opens the register, where the whole list is. */
function otherRegisterSections(
  facets: readonly RegisterFacet[] | null,
  counts: Record<string, number>,
): ColumnSection[] {
  return (facets ?? []).flatMap((facet) => {
    if (!facet.fqid || !facet.variables) return [];
    const name = facet.name ?? facet.fqid;
    const items: ColumnItem[] = facet.variables.map((variable) => ({
      id: variable.fqid,
      name: variable.name ?? leafSlug(variable.fqid),
      label: `${variable.name ?? leafSlug(variable.fqid)}, in ${name}`,
      href: catalogHref(variable.fqid),
      inProject: counts[variable.fqid] ?? 0,
      leaves: true,
    }));
    const rest = facet.variable_matches - facet.variables.length;
    if (rest > 0) {
      items.push({
        id: `${MORE}${facet.fqid}`,
        name: `${rest} more in ${name}`,
        href: catalogHref(facet.fqid),
        inProject: 0,
        leaves: true,
      });
    }
    return [{ id: `register:${facet.fqid}`, label: name, items }];
  });
}

/**
 * The second browse column (DESIGN.md → Layout): the selected register's
 * variables, a concept group folded to one item that opens the group's page
 * (#303), also on the group's page, where that row is selected (#1909). The
 * search bar's text filters it in place: by the server's match
 * set once the query settles, as on the register page, by names until then.
 */
export function VariableList({
  registerFqid,
  focus,
  className,
}: {
  registerFqid: string;
  focus: VariableListFocus | null;
  className?: string;
}) {
  // The register read the cart's names already share: one request per register.
  const read = useCatalogRead(registerRead, registerFqid);
  const counts = useProjectRead(readVariableCounts, undefined);
  const needle = useBrowseFilter();
  const serverMatches = useRegisterMatches(registerFqid);
  const otherMatches = useOtherRegisterMatches(registerFqid);
  const renderName = useCallback(
    // A "N more in <register>" row names a register, not a match: unmarked.
    (item: ColumnItem) =>
      item.id.startsWith(MORE) ? (
        item.name
      ) : (
        <Highlight text={item.name} needle={needle} />
      ),
    [needle],
  );

  const node = read.value;
  const children = node?.children ?? [];
  const columnsByFqid = new Map(
    children.map((c) => [
      c.fqid,
      c.deliveries.flatMap((d) => (d.column == null ? [] : [d.column])),
    ]),
  );
  const columnNames = (fqid: string) => columnsByFqid.get(fqid) ?? [];
  const folded = keyVariablesFirst(
    foldGroupedRows(children, node?.groups),
    children,
    node?.variants ?? [],
  );
  const keys = (row: (typeof folded)[number]) =>
    row.kind === "group"
      ? groupFilterKeys(row.group, columnNames)
      : [row.item.name, leafSlug(row.item.fqid), ...columnNames(row.item.fqid)];
  // With a settled query the server's match admits rows, as on the register
  // page beside this column, so the two panes agree. A group is admitted by
  // its own label or key, or by any matched member. Name hits rank first.
  let rows = rankFilter(folded, needle, keys);
  if (serverMatches) {
    const admitted = folded.filter((row) =>
      row.kind === "group"
        ? matchesFilter(needle, row.group.label, row.group.key) ||
          row.group.members.some((m) => serverMatches.has(m.fqid))
        : serverMatches.has(row.item.fqid),
    );
    const ranked = rankFilter(admitted, needle, keys);
    rows = [...ranked, ...admitted.filter((row) => !ranked.includes(row))];
  }
  const items: ColumnItem[] = rows.map((row) =>
    row.kind === "group"
      ? {
          id: `group:${row.group.key}`,
          name: row.group.label,
          href: groupHref(registerFqid, row.group.key),
          inProject: row.group.members.reduce(
            (n, member) => n + (counts[member.fqid] ?? 0),
            0,
          ),
        }
      : {
          id: row.item.fqid,
          name: row.item.name ?? row.item.fqid,
          href: catalogHref(row.item.fqid),
          inProject: counts[row.item.fqid] ?? 0,
        },
  );

  const others = otherRegisterSections(otherMatches, counts);
  const sections = [{ id: "variables", label: null, items }, ...others];
  const ownItems = items.length;
  // Other registers match but this one does not: say so above their runs.
  const ownEmpty = others.length > 0 && ownItems === 0;
  // The open page's row: a variable's own, or its group's folded row (a group
  // is its members' page, #1909).
  const selectedId =
    focus?.kind === "group"
      ? `group:${focus.key}`
      : focus?.kind === "variable"
        ? (items.find((item) => item.href === catalogHref(focus.fqid))?.id ??
          null)
        : null;

  return (
    <ColumnList
      label="Variables"
      subject={node ? nodeLabel(node) : undefined}
      root={registerFqid}
      className={className}
      // With nothing of its own to show, the column waits for the other
      // registers' answer rather than saying nothing matches.
      sections={
        read.loading || (otherMatches === null && ownItems === 0)
          ? null
          : sections
      }
      selectedId={selectedId}
      selectedExact={selectedId !== null}
      renderName={renderName}
      empty={
        needle.trim() === ""
          ? "This register lists no variables."
          : `No variables match “${needle.trim()}”. Press Escape to clear the search.`
      }
      notice={
        ownEmpty
          ? `No variables in ${node ? nodeLabel(node) : "this register"} match “${needle.trim()}”.`
          : null
      }
      error={
        !read.loading && node === null
          ? "The variables did not load. Reload the page to try again."
          : null
      }
    />
  );
}

const plainName = (item: ColumnItem) => item.name;

/** The second browse column before a register is chosen: present, so the
 * layout does not shift when one is, with its header aligned to the register
 * column's and a prompt in place of the list (maintainer ruling 2026-10-22). */
export function NoRegisterList({ className }: { className?: string }) {
  return (
    <ColumnList
      label="Variables"
      className={className}
      sections={[]}
      selectedId={null}
      selectedExact={false}
      renderName={plainName}
      empty="Choose a register to list its variables."
      error={null}
    />
  );
}
