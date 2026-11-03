/** After a confirmed removal, focus the first element that exists among
 * `candidates`, in order. The removed row (and the dialog's trigger with it)
 * is gone, so RAC has nothing to restore to; this runs after React has
 * committed the shorter list. */
export function focusAfterRemoval(
  candidates: readonly (() => HTMLElement | null | undefined)[],
): void {
  requestAnimationFrame(() => {
    for (const candidate of candidates) {
      const target = candidate();
      if (target) {
        target.focus();
        return;
      }
    }
  });
}
