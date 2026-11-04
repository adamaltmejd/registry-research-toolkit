import type { CatalogFindingGroup } from "./api";

/** A group of findings and the ref whose `next_cursor` it carries (`null`: no
 * further page). `partial`: more than one ref had further pages, so the group cannot
 * page on and its items may stop short of its count. */
export type WarningGroup = CatalogFindingGroup & {
  ref: string | null;
  partial: boolean;
};

const groupKey = (group: { severity: string; code: string }) =>
  JSON.stringify([group.severity, group.code]);

/** The server's group order: review before notice, then by code. */
const byGroup = (a: WarningGroup, b: WarningGroup) =>
  Number(a.severity !== "review") - Number(b.severity !== "review") ||
  (a.code < b.code ? -1 : a.code > b.code ? 1 : 0);

/** Group lists from several refs merged by severity and code. Items are
 * de-duplicated, and the count drops each duplicate seen: exact once every ref's group
 * is fully loaded, otherwise an upper bound (a shared finding not yet loaded counts
 * once per ref). A group pages on only when a single ref has more, since one cursor
 * continues one ref. */
export function mergeFindingGroups(
  sources: readonly {
    ref: string;
    groups: readonly CatalogFindingGroup[];
  }[],
): WarningGroup[] {
  const merged = new Map<
    string,
    { group: WarningGroup; seen: Set<string>; paging: number }
  >();
  for (const { ref, groups } of sources) {
    for (const group of groups) {
      const key = groupKey(group);
      const entry = merged.get(key) ?? {
        group: {
          ...group,
          count: 0,
          items: [],
          next_cursor: null,
          ref: null,
          partial: false,
        },
        seen: new Set<string>(),
        paging: 0,
      };
      merged.set(key, entry);
      for (const item of group.items) {
        const id = JSON.stringify(item);
        if (entry.seen.has(id)) {
          entry.group.count -= 1;
          continue;
        }
        entry.seen.add(id);
        entry.group.items.push(item);
      }
      entry.group.count += group.count;
      if (group.next_cursor !== null) {
        entry.paging += 1;
        entry.group.next_cursor = group.next_cursor;
        entry.group.ref = ref;
      }
    }
  }
  return [...merged.values()]
    .map(({ group, paging }) =>
      paging > 1
        ? { ...group, next_cursor: null, ref: null, partial: true }
        : group,
    )
    .sort(byGroup);
}
