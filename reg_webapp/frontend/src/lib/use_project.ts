import { useSyncExternalStore } from "react";
import type { ProjectStudyWindow } from "./project_data";
import { type ProjectSnapshot, projectStore } from "./project_store";
import { windowStore } from "./window";

export function useProject(): ProjectSnapshot {
  return useSyncExternalStore(projectStore.subscribe, projectStore.getSnapshot);
}

const readWindow = () => windowStore.value;

export function useStudyWindow(): ProjectStudyWindow | null {
  return useSyncExternalStore(windowStore.subscribe, readWindow);
}
