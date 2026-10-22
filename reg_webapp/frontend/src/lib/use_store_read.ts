import { useMemo, useSyncExternalStore } from "react";
import { subscribeCatalogNames } from "./catalog_names";
import { projectStore } from "./project_store";

/** Same value for React's purposes: identical, or a plain object or array whose
 * entries are identical. Lets a reader build a fresh snapshot object each run
 * without re-rendering when nothing in it moved. */
function sameSnapshot(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== "object" || typeof b !== "object" || !a || !b) return false;
  const left = Object.entries(a);
  const right = b as Record<string, unknown>;
  return (
    Array.isArray(a) === Array.isArray(b) &&
    left.length === Object.keys(b).length &&
    left.every(([k, v]) => Object.is(v, right[k]))
  );
}

type Subscribe = (listener: () => void) => () => void;

/** A derived read of a store: recomputed only when the store notifies, so its
 * snapshot is the same object between notifications (what
 * `useSyncExternalStore` requires) and across a notification that moved nothing
 * in it. */
function derivedRead<T>(subscribe: Subscribe, read: () => T) {
  let value = read();
  return {
    subscribe(listener: () => void): () => void {
      // The store may have moved between render and subscribe.
      const refresh = () => {
        const next = read();
        if (!sameSnapshot(next, value)) {
          value = next;
          listener();
        }
      };
      refresh();
      return subscribe(refresh);
    },
    getSnapshot: (): T => value,
  };
}

function useStoreRead<A, T>(
  subscribe: Subscribe,
  read: (arg: A) => T,
  arg: A,
): T {
  const store = useMemo(
    () => derivedRead(subscribe, () => read(arg)),
    [subscribe, read, arg],
  );
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

/** Read something derived from the project store. `read` is a module-level
 * function of `arg`, so the subscription is rebuilt only when either changes; it
 * may return a fresh object or array (compared entry by entry). */
export function useProjectRead<A, T>(read: (arg: A) => T, arg: A): T {
  return useStoreRead(projectStore.subscribe, read, arg);
}

/** Read from the catalog display-name cache (`catalog_names.ts`), starting the
 * read on first ask. `read` is a module-level function of `arg`, as for
 * `useProjectRead`; the component re-renders when a read behind it lands. */
export function useCatalogRead<A, T>(read: (arg: A) => T, arg: A): T {
  return useStoreRead(subscribeCatalogNames, read, arg);
}
