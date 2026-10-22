import { useSyncExternalStore } from "react";
import type { ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import { windowStore } from "./window";

/** Read the project store. `read` returns a primitive or an object the store
 * holds (the draft, the validation result), never a fresh object: React compares
 * snapshots by identity. */
export function useProject<T>(read: (store: typeof projectStore) => T): T {
  return useSyncExternalStore(projectStore.subscribe, () => read(projectStore));
}

const readWindow = () => windowStore.value;

/** The active study window (the draft's, or the no-project fallback). */
export function useStudyWindow(): ProjectStudyWindow | null {
  return useSyncExternalStore(windowStore.subscribe, readWindow);
}
