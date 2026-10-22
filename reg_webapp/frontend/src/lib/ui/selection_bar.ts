import { type RefObject, useEffect } from "react";

/**
 * The selection bar of a browse column (DESIGN.md → ColumnList): one 2px accent
 * bar that translates between items at `duration-base`/`ease-out` when the
 * selection moves, instead of a static bar per row.
 *
 * The column renders the bar as an `aria-hidden` sibling of its RAC list inside
 * a positioned scroll box; this places it on the selected row
 * (`[data-selected]`) whenever the box's DOM changes, since RAC renders its rows
 * in its own passes, after the column's. It moves without a transition when the
 * selected row stays the same (a group above it collapsed), on the first
 * placement (nothing animates on page load) and when the selection reappears
 * after none. The rows' CSS drops their own static bar while the box carries
 * `data-selection-bar`.
 */
export function useSelectionBar(
  boxRef: RefObject<HTMLElement | null>,
  barRef: RefObject<HTMLElement | null>,
): void {
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    let last: Element | null = null;
    let frame = 0;

    const place = () => {
      frame = 0;
      const bar = barRef.current;
      if (!bar) return;
      box.dataset.selectionBar = "";
      const row = box.querySelector<HTMLElement>(
        '[role="option"][data-selected], [role="row"][data-selected]',
      );
      if (!row) {
        bar.hidden = true;
        last = null;
        return;
      }
      bar.style.transition = last !== null && last !== row ? "" : "none";
      bar.hidden = false;
      bar.style.transform = `translateY(${row.offsetTop}px)`;
      bar.style.height = `${row.offsetHeight}px`;
      last = row;
    };
    const schedule = () => {
      if (frame === 0) frame = requestAnimationFrame(place);
    };

    const mutations = new MutationObserver(schedule);
    mutations.observe(box, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-selected", "data-expanded"],
    });
    const sizes = new ResizeObserver(schedule);
    sizes.observe(box);
    schedule();
    return () => {
      cancelAnimationFrame(frame);
      mutations.disconnect();
      sizes.disconnect();
    };
  }, [boxRef, barRef]);
}
