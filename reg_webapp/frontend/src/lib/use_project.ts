import { useState, useSyncExternalStore } from "react";
import { checkProjectOrder, type OrderCheck } from "./api";
import type { ProjectData, ProjectStudyWindow } from "./project_data";
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
  /** While a check is in flight, the last answer, so /project keeps
   * its verdict, findings and delivery on screen instead of a skeleton on every
   * keystroke. Findings and the order are located by index
   * (`/sources/0/bindings/1`), so it is `null` once a table or column was added,
   * removed or moved since: those indices would point at the wrong card. */
  previous: Pick<AsyncResource<OrderCheck>, "data" | "error"> | null;
}

/** Where each finding's index points: the tables and their columns, in order. */
const layoutOf = (draft: ProjectData) =>
  JSON.stringify(
    draft.sources.map((source) => [
      source.name,
      source.table,
      source.bindings.map((binding) => binding.variable),
    ]),
  );

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
  const layout = draft ? layoutOf(draft) : null;
  const [last, setLast] = useState<{
    layout: string;
    data: OrderCheck | null;
    error: string | null;
  } | null>(null);
  // Remember each settled check during render (state derived from the previous
  // render, so no effect); the guard stops after one update per result.
  if (
    layout !== null &&
    !resource.loading &&
    (resource.data !== last?.data || resource.error !== last?.error)
  )
    setLast({ layout, data: resource.data, error: resource.error });
  // Only an answer is kept: after a check that did not run, Retry shows the
  // Skeleton, not the same error row as if nothing happened.
  const previous =
    resource.loading && last?.data && last.layout === layout
      ? { data: last.data, error: null }
      : null;
  return {
    ...resource,
    previous,
    retry: () => setAttempt((value) => value + 1),
  };
}

export type Verdict =
  | "checking"
  | "empty"
  | "ready"
  | "warnings"
  | "problems"
  | "blocked"
  | "unchecked";

/** One verdict for the banner and /project, in the same words. A file that
 * fails the structure check has problems; a draft the catalog refuses to order
 * can't be ordered yet; a check that did not run (rate limit, network) says so,
 * and /project explains it with Retry. */
export function projectVerdict(
  { draft }: ProjectSnapshot,
  check: Pick<ProjectCheck, "loading" | "data" | "error">,
): Verdict {
  if (!draft) return "problems";
  if (draft.sources.length === 0) return "empty";
  if (check.loading) return "checking";
  if (check.error || !check.data) return "unchecked";
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
  unchecked: "Check did not run",
};
