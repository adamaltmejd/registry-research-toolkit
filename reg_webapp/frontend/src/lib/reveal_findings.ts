import { useEffect } from "react";

// The banner's verdict link (#1835) opens /project on its findings. The panel
// may not be mounted yet, or its findings not rendered, so the request waits
// here until the panel can serve it.
let revealPending = false;
const REVEAL_EVENT = "reg:reveal-findings";

/** Bring the first non-empty findings group into view, now if the panel shows
 * one, else as soon as it does. The caller navigates to /project itself. */
export function revealFindings(): void {
  revealPending = true;
  window.dispatchEvent(new Event(REVEAL_EVENT));
}

/** Scroll to the first findings group (`[data-findings]`) in the page and move
 * focus to its heading; false while there is none to show. */
function showFirstFindings(): boolean {
  const heading = document.querySelector<HTMLElement>("[data-findings] h4");
  if (heading == null) return false;
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  heading.focus({ preventScroll: true });
  heading.scrollIntoView({
    behavior: reduce ? "auto" : "smooth",
    block: "start",
  });
  return true;
}

/** The validation panel's side: serve a pending reveal once findings are on the
 * page (on mount, after any render that may have added them) and on a press
 * while the panel is already showing. */
export function useRevealFindings(): void {
  useEffect(() => {
    const serve = () => {
      if (revealPending && showFirstFindings()) revealPending = false;
    };
    serve();
    window.addEventListener(REVEAL_EVENT, serve);
    return () => window.removeEventListener(REVEAL_EVENT, serve);
  });
}
