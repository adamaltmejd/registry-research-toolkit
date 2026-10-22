/**
 * Arrow keys across the browse columns (DESIGN.md → Layout): → on a register row
 * moves to the variables column, ← in the variables column moves back to the
 * register. Within a column RAC owns the keys; the columns find each other by
 * `data-browse-column`, since they are siblings in the shell with no shared
 * React state.
 *
 * Focus lands on the column's selected row, else its first. Focusing a RAC row
 * element moves the collection's focused key with it (its focus handler), so
 * the arrows continue from there; it does not select, so the page stays put.
 * Returns false when there is no such column (the narrow layout, a page with
 * no variables column), so the caller lets the key through.
 */
export function focusBrowseColumn(column: "registers" | "variables"): boolean {
  const root = document.querySelector<HTMLElement>(
    `[data-browse-column="${column}"]`,
  );
  if (!root || root.offsetParent === null) return false;
  const row =
    root.querySelector<HTMLElement>(
      '[role="option"][data-selected], [role="row"][data-selected], [role="row"][aria-current]',
    ) ?? root.querySelector<HTMLElement>('[role="option"], [role="row"]');
  if (!row) return false;
  row.focus();
  return true;
}
