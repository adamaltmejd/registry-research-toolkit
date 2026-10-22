import { useEffect, useState, useSyncExternalStore } from "react";
import { SEARCH_MIN_QUERY_LENGTH, searchFacets } from "./api";
import { useAsyncResource } from "./use_async_resource";

/** The text in the top bar's search field, which filters both browse columns in
 * place (DESIGN.md → Layout → Search filters in place). App state, not URL
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
 * same admission the browse tree counts, so a register page lists exactly the
 * number its tree row says. Null while there is no settled query or no
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
    matches.data.find((facet) => facet.fqid === registerFqid)?.variables ?? [],
  );
}
