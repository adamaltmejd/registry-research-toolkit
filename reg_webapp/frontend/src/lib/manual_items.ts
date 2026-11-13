import {
  isManualBinding,
  type ProjectBinding,
  type ProjectManualBinding,
} from "./project_data";

/** What every place that shows data entered by hand says about it. */
export const MANUAL_LABEL = "Not in the catalog, entered by hand";

/** The columns entered by hand among a source's bindings, with their place in
 * the file (the index its findings and edits use). */
export function manualColumns(
  bindings: readonly ProjectBinding[],
): { binding: ProjectManualBinding; index: number }[] {
  return bindings.flatMap((binding, index) =>
    isManualBinding(binding) ? [{ binding, index }] : [],
  );
}

/** "3 codes", or null for a column that states none. */
export function codeCount(binding: ProjectManualBinding): string | null {
  const count = binding.manual.codes?.length ?? 0;
  return count === 0 ? null : `${count} ${count === 1 ? "code" : "codes"}`;
}
