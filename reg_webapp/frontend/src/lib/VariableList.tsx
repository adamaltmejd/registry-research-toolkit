import { useCallback } from "react";
import { useBrowseFilter, useRegisterMatches } from "./browse_filter";
import {
  catalogHref,
  foldGroupedRows,
  groupFilterKeys,
  groupHref,
  leafSlug,
  matchesFilter,
  rankFilter,
} from "./catalog";
import { registerRead } from "./catalog_names.svelte";
import { Highlight } from "./Highlight";
import { safeSourceBindings, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { useRune } from "./svelte_bridge.svelte";
import { type ColumnItem, ColumnList } from "./ui/ColumnList";

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

/**
 * The second browse column (DESIGN.md → Layout): the selected register's
 * variables, a concept group folded to one item that opens the group's page
 * (#303). The search bar's text filters it in place: by the server's match
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
  const read = useRune(registerRead, registerFqid);
  const counts = useRune(readVariableCounts, undefined);
  const needle = useBrowseFilter();
  const serverMatches = useRegisterMatches(registerFqid);
  const renderName = useCallback(
    (item: ColumnItem) => <Highlight text={item.name} needle={needle} />,
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
  const folded = foldGroupedRows(children, node?.groups);
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

  // A variable folded into a group selects its group's item.
  let selectedId: string | null = null;
  if (focus?.kind === "group") {
    selectedId = `group:${focus.key}`;
  } else if (focus?.kind === "variable") {
    const group = node?.groups.find((g) =>
      g.members.some((m) => m.fqid === focus.fqid),
    );
    selectedId = group ? `group:${group.key}` : focus.fqid;
  }

  return (
    <ColumnList
      label="Variables"
      className={className}
      items={read.loading ? null : items}
      selectedId={selectedId}
      selectedExact={
        focus !== null && (focus.kind === "group" || selectedId === focus.fqid)
      }
      renderName={renderName}
      empty={
        needle.trim() === ""
          ? "This register lists no variables."
          : `No variables match “${needle.trim()}”. Press Escape in the search bar to clear it.`
      }
      error={
        !read.loading && node === null
          ? "The variables did not load. Reload the page to try again."
          : null
      }
    />
  );
}
