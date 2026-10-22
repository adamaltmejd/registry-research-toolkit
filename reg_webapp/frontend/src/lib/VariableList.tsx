import { useCallback } from "react";
import type { RegisterShow } from "./api";
import { useBrowseFilter, useRegisterMatches } from "./browse_filter";
import {
  catalogHref,
  facetLabelJoin,
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
import {
  type ColumnItem,
  ColumnList,
  type ColumnSection,
} from "./ui/ColumnList";

/** What the open page is under the register: a variable (by FQID) or a concept
 * group (by key). */
export type VariableListFocus =
  | { kind: "variable"; fqid: string }
  | { kind: "group"; key: string };

type Group = RegisterShow["groups"][number];

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

/** A concept group's members as sections under its first axis's facet value,
 * in member order, each member named by its remaining facets (DESIGN.md →
 * ColumnList: on a group page the members are grouped by facet). A group with
 * no axes is one unlabelled run. The filter keeps a member by its name, facets,
 * slug or column, or by the server's match once the query settles. */
function memberSections(
  group: Group,
  needle: string,
  serverMatches: ReadonlySet<string> | null,
  counts: Record<string, number>,
): ColumnSection[] {
  const axis = group.axes[0]?.name ?? null;
  const sections = new Map<string, ColumnSection & { items: ColumnItem[] }>();
  // One variable can be several members (one per facet combination it
  // delivers, e.g. a monthly wage per month): each is its own row, keyed by
  // the member's place, and every one opens the variable.
  for (const [index, member] of group.members.entries()) {
    const head =
      axis === null ? null : member.facets.find((f) => f.axis === axis);
    const rest = member.facets.filter((f) => f !== head);
    // Under its facet header a member is named by its other facets, else by
    // the variable's own name (the header already says the facet).
    const name =
      facetLabelJoin(rest) ||
      member.name ||
      head?.label ||
      member.delivery_column ||
      leafSlug(member.fqid);
    const keys = [
      name,
      member.name ?? "",
      leafSlug(member.fqid),
      member.delivery_column ?? "",
      ...member.facets.map((f) => f.label),
    ];
    const kept =
      needle.trim() === "" ||
      (serverMatches?.has(member.fqid) ?? false) ||
      keys.some((key) => matchesFilter(needle, key));
    if (!kept) continue;
    const id = `facet:${head?.value ?? ""}`;
    const section = sections.get(id) ?? {
      id,
      label: head?.label ?? null,
      items: [],
    };
    section.items.push({
      id: `member:${index}`,
      name,
      href: catalogHref(member.fqid),
      inProject: counts[member.fqid] ?? 0,
    });
    sections.set(id, section);
  }
  return [...sections.values()];
}

/**
 * The second browse column (DESIGN.md → Layout): the selected register's
 * variables, a concept group folded to one item that opens the group's page
 * (#303); inside a group (its page or a member's) the group's members, grouped
 * by facet. The search bar's text filters it in place: by the server's match
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

  const group =
    focus?.kind === "group"
      ? node?.groups.find((g) => g.key === focus.key)
      : focus?.kind === "variable"
        ? node?.groups.find((g) => g.members.some((m) => m.fqid === focus.fqid))
        : undefined;
  const sections: ColumnSection[] = group
    ? memberSections(group, needle, serverMatches, counts)
    : [{ id: "variables", label: null, items }];

  // A member selects itself; the group's own page selects nothing in its
  // member list.
  // A variable's page selects its row (its first, where it is several
  // members); the group's own page selects nothing in its member list.
  const variableHref =
    focus?.kind === "variable" ? catalogHref(focus.fqid) : null;
  const selectedId =
    variableHref !== null
      ? (sections
          .flatMap((s) => s.items)
          .find((item) => item.href === variableHref)?.id ?? null)
      : focus?.kind === "group" && !group
        ? `group:${focus.key}`
        : null;

  return (
    <ColumnList
      label="Variables"
      className={className}
      sections={read.loading ? null : sections}
      selectedId={selectedId}
      selectedExact={selectedId !== null}
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
