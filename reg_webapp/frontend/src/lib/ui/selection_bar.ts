import { type RefObject, useEffect } from "react";

/**
 * The selection bar of a browse column (DESIGN.md → ColumnList): one 2px accent
 * bar that translates between items at `duration-base`/`ease-out` when the
 * selection moves, instead of a static bar per row.
 *
 * RAC renders the list and its rows (in its own render passes, after the
 * column's), so the bar is one `aria-hidden` node appended to the list and
 * placed on the selected row (`[data-selected]`) whenever the column's DOM
 * changes. It moves without a transition when the selected row stays the same
 * (a group above it collapsed), on the first placement (nothing animates on
 * page load) and when the selection reappears after none. The rows' CSS drops
 * their own static bar while the column carries `data-selection-bar`.
 */
export function useSelectionBar(
  columnRef: RefObject<HTMLElement | null>,
  barClass: string,
): void {
  useEffect(() => {
    const column = columnRef.current;
    if (!column) return;
    let last: Element | null = null;
    let frame = 0;

    const place = () => {
      frame = 0;
      const list = column.querySelector<HTMLElement>(
        '[role="listbox"], [role="treegrid"]',
      );
      if (!list) {
        last = null;
        return;
      }
      let bar = list.querySelector<HTMLElement>(`:scope > .${barClass}`);
      if (!bar) {
        bar = document.createElement("span");
        bar.className = barClass;
        bar.setAttribute("aria-hidden", "true");
        list.append(bar);
      }
      column.dataset.selectionBar = "";
      const row = list.querySelector<HTMLElement>("[data-selected]");
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
    mutations.observe(column, {
      subtree: true,
      childList: true,
      attributes: true,
      attributeFilter: ["data-selected", "data-expanded"],
    });
    const sizes = new ResizeObserver(schedule);
    sizes.observe(column);
    schedule();
    return () => {
      cancelAnimationFrame(frame);
      mutations.disconnect();
      sizes.disconnect();
    };
  }, [columnRef, barClass]);
}
