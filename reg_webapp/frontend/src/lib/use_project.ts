import { useState, useSyncExternalStore } from "react";
import { type ValidationResultModel, validateProject } from "./api";
import type { ProjectStudyWindow } from "./project_data";
import { type ProjectSnapshot, projectStore } from "./project_store";
import { type AsyncResource, useAsyncResource } from "./use_async_resource";
import { windowStore } from "./window";

export function useProject(): ProjectSnapshot {
  return useSyncExternalStore(projectStore.subscribe, projectStore.getSnapshot);
}

const readWindow = () => windowStore.value;

export function useStudyWindow(): ProjectStudyWindow | null {
  return useSyncExternalStore(windowStore.subscribe, readWindow);
}

export interface ProjectValidation
  extends AsyncResource<ValidationResultModel> {
  refresh: () => void;
}

/** One catalog check shared by the page and the banner across route changes. */
export function useProjectValidation(): ProjectValidation {
  const { draft, raw } = useProject();
  const [revision, setRevision] = useState(0);
  const resource = useAsyncResource(
    draft ? `${raw}:${revision}` : null,
    async (signal) => {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 350);
        signal.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            reject(signal.reason);
          },
          { once: true },
        );
      });
      if (!draft) throw new Error("Open a project before checking the catalog");
      return validateProject(draft, { signal });
    },
  );
  return { ...resource, refresh: () => setRevision((value) => value + 1) };
}
