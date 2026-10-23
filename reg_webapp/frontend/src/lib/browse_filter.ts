import { useEffect, useState, useSyncExternalStore } from "react";
import {
  type RegisterFacet,
  SEARCH_MIN_QUERY_LENGTH,
  searchFacets,
} from "./api";
import { useAsyncResource } from "./use_async_resource";

/** The text in the top bar's search field, which filters both browse columns in
 * place and never the open page (DESIGN.md → Layout → Search filters in
 * place). App state, not URL
 * state: it outlives every route and is not part of a shared link. */
let value = "";
const listeners = new Set<() => void>();

export const browseFilter = {
  get value(): string {
    return value;
  },
  set(next: string): void {
    if (next === value) return;
    value = next;
    for (const notify of listeners) notify();
  },
  subscribe(listener: () => void): () => void {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};

const read = () => value;

/** How many registers the browse tree keeps for the filter, published by the
 * tree (the one place that ranks them) so the search field can say so. Null
 * while there is no filter or the tree has not answered. */
let matches: number | null = null;

export const browseMatches = {
  set(next: number | null): void {
    if (next === matches) return;
    matches = next;
    for (const notify of listeners) notify();
  },
};

const readMatches = () => matches;

export function useBrowseMatches(): number | null {
  return useSyncExternalStore(browseFilter.subscribe, readMatches);
}

/** The browse filter's text, as typed (callers fold it through `rankFilter`). */
export function useBrowseFilter(): string {
  return useSyncExternalStore(browseFilter.subscribe, read);
}

const readFiltering = () => value.trim() !== "";

/** Whether the filter holds text: for a reader that needs only that, so it
 * does not re-render on every keystroke. */
export function useBrowseFiltering(): boolean {
  return useSyncExternalStore(browseFilter.subscribe, readFiltering);
}

/** How long the filter text rests before a server request goes out. */
const SETTLE_MS = 200;

/** The filter text the server is asked about: the trimmed text once it has
 * rested, or null while it is still moving or too short to search. Keyed on
 * the settled text, a newer query aborts the older request, so an answer
 * never lands on other text. */
export function useSettledFilter(): string | null {
  const needle = useBrowseFilter().trim();
  const [settled, setSettled] = useState("");
  useEffect(() => {
    const timer = setTimeout(() => setSettled(needle), SETTLE_MS);
    return () => clearTimeout(timer);
  }, [needle]);
  return needle !== "" &&
    settled === needle &&
    settled.length >= SEARCH_MIN_QUERY_LENGTH
    ? settled
    : null;
}

/** The register's variables the server matches for the filter (#1788): the
 * same admission the browse tree counts, so the variables column lists exactly
 * the number its tree row says. Null while there is no settled query or no
 * answer yet, or when the request failed: the caller then filters on the
 * names it holds. */
export function useRegisterMatches(
  registerFqid: string,
): ReadonlySet<string> | null {
  const query = useSettledFilter();
  const matches = useAsyncResource(
    query === null ? null : `${registerFqid}\u0000${query}`,
    (signal) => searchFacets(query ?? "", { signal, register: registerFqid }),
  );
  if (query === null || matches.data === null) return null;
  return new Set(
    (
      matches.data.find((facet) => facet.fqid === registerFqid)?.variables ?? []
    ).map((variable) => variable.fqid),
  );
}

/** The registers the browse tree shows for the filter, in its order (name
 * hits first within a provider), published by the tree so the variables
 * column lists other registers' matches in the same order (#1828). */
let order: readonly string[] = [];

// It notifies the filter's listeners: each reads its own snapshot.
export const browseOrder = {
  set(next: readonly string[]): void {
    if (next.join(" ") === order.join(" ")) return;
    order = next;
    for (const notify of listeners) notify();
  },
};

const readOrder = () => order;

/** How many of another register's matches the variables column lists before
 * its "N more" row (#1828); a register with one more than this lists them all,
 * since a "1 more" row would take the place of the row it hides. */
const OTHER_REGISTER_LIMIT = 5;

/** The filter's matches in every register but the open one (#1828), each
 * register's first few in FQID order with the whole count, in the browse
 * tree's order: the last answer until the next one lands, null before the
 * first; empty when nothing else matches or the request failed, so the open
 * register's list never waits on it. */
export function useOtherRegisterMatches(
  registerFqid: string,
): readonly RegisterFacet[] | null {
  const needle = useBrowseFilter().trim();
  const query = useSettledFilter();
  const treeOrder = useSyncExternalStore(browseFilter.subscribe, readOrder);
  const matches = useAsyncResource(query, (signal) =>
    searchFacets(query ?? "", { signal, limit: OTHER_REGISTER_LIMIT + 1 }),
  );
  // The last answer stands while the next query settles and loads, so the
  // sections do not drop out and come back on every keystroke.
  const [last, setLast] = useState<RegisterFacet[] | null>(null);
  if (matches.data !== null && matches.data !== last) setLast(matches.data);
  if (needle.length < SEARCH_MIN_QUERY_LENGTH) return [];
  const pending = query === null || matches.loading;
  const data = pending ? last : matches.data;
  if (data === null) return pending ? null : [];
  const place = (facet: RegisterFacet) => {
    const at = treeOrder.indexOf(facet.fqid ?? "");
    return at === -1 ? treeOrder.length : at;
  };
  return data
    .filter((facet) => facet.fqid !== registerFqid)
    .map((facet) =>
      facet.variable_matches > OTHER_REGISTER_LIMIT + 1
        ? {
            ...facet,
            variables: facet.variables?.slice(0, OTHER_REGISTER_LIMIT),
          }
        : facet,
    )
    .sort((a, b) => place(a) - place(b));
}
