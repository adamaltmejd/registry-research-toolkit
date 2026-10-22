import { useCallback } from "react";
import { useBrowseFilter } from "./browse_filter";
import {
  catalogHref,
  foldGroupedRows,
  groupFilterKeys,
  groupHref,
  leafSlug,
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
 * (#303). The search bar's text filters it in place, by the same keys as the
 * register page's own filter (names, slugs and delivery column names).
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
  const rows = rankFilter(
    foldGroupedRows(children, node?.groups),
    needle,
    (row) =>
      row.kind === "group"
        ? groupFilterKeys(row.group, columnNames)
        : [
            row.item.name,
            leafSlug(row.item.fqid),
            ...columnNames(row.item.fqid),
          ],
  );
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
          : `No variables match “${needle.trim()}”.`
      }
      error={
        !read.loading && node === null
          ? "The variables did not load. Reload the page to try again."
          : null
      }
    />
  );
}
