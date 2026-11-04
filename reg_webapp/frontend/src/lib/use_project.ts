import { useState, useSyncExternalStore } from "react";
import { checkProjectOrder, type OrderCheck } from "./api";
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

export interface ProjectCheck extends AsyncResource<OrderCheck> {
  /** Run the check again after a refused request; edits re-run it anyway. */
  retry: () => void;
}

/** The project's one catalog check, shared by the page and the banner across
 * route changes: `order`, 350 ms after the last edit WASM accepted. */
export function useProjectCheck(): ProjectCheck {
  const { draft, raw } = useProject();
  const [attempt, setAttempt] = useState(0);
  const resource = useAsyncResource(
    draft ? `${raw}:${attempt}` : null,
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
      if (!draft) throw new Error("Open a project before checking it");
      return checkProjectOrder(draft, { signal });
    },
  );
  return { ...resource, retry: () => setAttempt((value) => value + 1) };
}

export type Verdict =
  | "checking"
  | "empty"
  | "ready"
  | "warnings"
  | "problems"
  | "blocked";

/** One verdict for the banner and /project, in the same words. A file that
 * fails the structure check has problems; a draft the catalog refuses to order
 * can't be ordered yet; a check that did not run is a problem too, which
 * /project explains. */
export function projectVerdict(
  { draft }: ProjectSnapshot,
  check: ProjectCheck,
): Verdict {
  if (!draft) return "problems";
  if (draft.sources.length === 0) return "empty";
  if (check.loading) return "checking";
  if (check.error || !check.data) return "problems";
  if (check.data.order === null) return "blocked";
  return check.data.issues.some((issue) => issue.level === "warning")
    ? "warnings"
    : "ready";
}

export const VERDICT_WORDS: Record<Verdict, string> = {
  checking: "Checking",
  empty: "Empty",
  ready: "Ready to order",
  warnings: "Has warnings",
  problems: "Has problems",
  blocked: "Can't be ordered yet",
};
