import { useSyncExternalStore } from "react";

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

/** The browse filter's text, as typed (callers fold it through `rankFilter`). */
export function useBrowseFilter(): string {
  return useSyncExternalStore(browseFilter.subscribe, read);
}
