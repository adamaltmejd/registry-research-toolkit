import { useMemo, useState } from "react";
import type { VariableColumn } from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  errMessage,
  getSegments,
  type RegisterShow,
  type ValidationResultModel,
} from "./api";
import { projectStore } from "./project_store";
import {
  addVariables,
  rowColumns,
  rowWindow,
  type VariableSelection,
} from "./register_add";
import { studyWindow } from "./study_window";
import { useAsyncResource } from "./use_async_resource";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

export function useRegisterAdd({
  node,
  context,
  validation,
}: {
  node: RegisterShow;
  context: Context | null;
  validation: ValidationResultModel | null;
}) {
  const { draft } = useProject();
  const bound = useMemo(
    () =>
      new Set(
        draft?.sources.flatMap((source) =>
          source.bindings.map((binding) => binding.variable),
        ) ?? [],
      ),
    [draft],
  );
  const study = studyWindow(draft);
  const bounds = windowBounds(context);
  const [ticks, setTicks] = useState<ReadonlySet<string>>(() => new Set());
  const [cache, setCache] = useState<
    ReadonlyMap<string, readonly CatalogSegment[]>
  >(() => new Map());
  const [error, setError] = useState<string | null>(null);
  const [addedVariables, setAddedVariables] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  const [selectionDraft, setSelectionDraft] = useState(draft);
  if (selectionDraft !== draft) {
    setSelectionDraft(draft);
    setTicks(new Set());
    setError(null);
    setAddedVariables(new Set());
  }
  const eligible = draft
    ? node.variables.filter(
        (variable) =>
          ticks.has(variable.fqid) &&
          !bound.has(variable.fqid) &&
          rowWindow(variable.period, study, bounds) !== null,
      )
    : [];
  const selected = new Set(eligible.map((variable) => variable.fqid));
  const pending =
    eligible.find((variable) => !cache.has(variable.fqid)) ?? null;
  // Read ticked rows one at a time through the shared cancellable resource.
  // Changing the next pending row aborts the previous request.
  useAsyncResource(pending?.fqid ?? null, async (signal) => {
    if (!pending) return null;
    try {
      const segments = await getSegments(pending.fqid, { signal });
      if (signal.aborted) return null;
      setCache((previous) => new Map(previous).set(pending.fqid, segments));
      const current = projectStore.draft;
      const window = rowWindow(pending.period, studyWindow(current), bounds);
      if (
        !window ||
        rowColumns(segments, pending.fqid, current, window).length === 0
      ) {
        setTicks((previous) => {
          const next = new Set(previous);
          next.delete(pending.fqid);
          return next;
        });
        setError(`${pending.name} has no columns to add in the study window.`);
      }
      return segments;
    } catch (cause) {
      if (!signal.aborted) {
        setTicks((previous) => {
          const next = new Set(previous);
          next.delete(pending.fqid);
          return next;
        });
        setError(`Could not read ${pending.name}'s columns. Try again.`);
      }
      throw cause;
    }
  });
  const columns = new Map<string, readonly VariableColumn[]>();
  const selections: VariableSelection[] = [];
  for (const variable of eligible) {
    const segments = cache.get(variable.fqid);
    const window = rowWindow(variable.period, study, bounds);
    if (segments && window) {
      const picked = rowColumns(segments, variable.fqid, draft, window);
      columns.set(variable.fqid, picked);
      selections.push({ variable: variable.fqid, columns: picked, window });
    }
  }
  const count = selections.reduce(
    (sum, selection) => sum + selection.columns.length,
    0,
  );
  const added = addedVariables.size > 0;
  const classificationWarning =
    added &&
    validation?.issues.some(
      (issue) =>
        issue.code === "classification_version_ambiguous" &&
        issue.variable !== null &&
        addedVariables.has(issue.variable),
    );
  const toggle = (
    variable: RegisterShow["variables"][number],
    checked: boolean,
  ) => {
    const segments = cache.get(variable.fqid);
    const window = rowWindow(variable.period, study, bounds);
    if (
      checked &&
      segments &&
      (!window ||
        rowColumns(segments, variable.fqid, draft, window).length === 0)
    ) {
      setError(`${variable.name} has no columns to add in the study window.`);
      setAddedVariables(new Set());
      return;
    }
    setTicks((previous) => {
      const next = new Set(previous);
      if (checked) next.add(variable.fqid);
      else next.delete(variable.fqid);
      return next;
    });
    setError(null);
    setAddedVariables(new Set());
  };
  const add = () => {
    const current = projectStore.draft;
    if (!current || pending || count === 0) return false;
    try {
      const candidate = addVariables(current, selections, node.tables);
      projectStore.updateField("sources", candidate.sources);
      setSelectionDraft(projectStore.draft);
      setTicks(new Set());
      setError(null);
      setAddedVariables(
        new Set(selections.map((selection) => selection.variable)),
      );
      return true;
    } catch (cause) {
      setError(`Columns were not added. ${errMessage(cause)}`);
      return false;
    }
  };
  return {
    draft,
    bound,
    study,
    bounds,
    selected,
    columns,
    count,
    loading: pending !== null,
    error,
    added,
    classificationWarning,
    toggle,
    add,
  };
}
