// TEMPORARY (React port, REACT_PORT.md): the glue between the React shell and the
// Svelte runes stores and views it has not replaced yet. Each part goes when its
// store or last view is ported; the file is deleted in port bundle C.
import { useMemo, useSyncExternalStore } from "react";
import { untrack } from "svelte";
import { projectStore } from "./project_store.svelte";

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

function runeStore<T>(read: () => T) {
  let value = untrack(read);
  const listeners = new Set<() => void>();
  let stop: (() => void) | null = null;
  return {
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      // One effect for every subscriber: it re-runs `read` whenever a rune it
      // read changes, and wakes React only when the snapshot moved.
      stop ??= $effect.root(() => {
        $effect(() => {
          const next = read();
          if (!sameSnapshot(next, value)) {
            value = next;
            for (const notify of listeners) notify();
          }
        });
      });
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          stop?.();
          stop = null;
        }
      };
    },
    getSnapshot: (): T => value,
  };
}

/** Read Svelte runes state from React. `read` is a module-level function of
 * `arg`, so the subscription is rebuilt only when either changes; it should
 * return primitives, a stable object, or a flat snapshot object (compared entry
 * by entry). */
export function useRune<A, T>(read: (arg: A) => T, arg: A): T {
  const store = useMemo(() => runeStore(() => read(arg)), [read, arg]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

/** The project draft, for `useRune`: replaced (never mutated) on every edit, so
 * its identity is what wakes React. */
export function draftSnapshot() {
  return projectStore.draft;
}

/** A props object a Svelte island can be mounted with and later updated through
 * (`Object.assign` on it re-renders the island). */
export function islandProps<P extends Record<string, unknown>>(initial: P): P {
  const props = $state(initial);
  return props;
}
