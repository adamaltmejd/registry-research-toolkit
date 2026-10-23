import { useEffect } from "react";

// The banner's verdict link (#1835) opens /project on its findings. The panel
// may not be mounted yet, and mounts before its findings render, so the
// request waits here until the panel can serve it. It expires: a request the
// page never served (findings gone by the time it rendered) must not take
// focus on a later visit. Not cleared on unmount: StrictMode's dev remount
// would drop it.
const REVEAL_TTL_MS = 3000;
let revealUntil = 0;
const REVEAL_EVENT = "reg:reveal-findings";

/** Bring the first non-empty findings group into view, now if the panel shows
 * one, else as soon as it does. The caller navigates to /project itself. */
export function revealFindings(): void {
  revealUntil = performance.now() + REVEAL_TTL_MS;
  window.dispatchEvent(new Event(REVEAL_EVENT));
}

/** Scroll to the first findings group (`[data-findings]`) in the page and move
 * focus to its heading, if a request is pending and there is one to show. */
function serve(): void {
  if (performance.now() > revealUntil) return;
  const heading = document.querySelector<HTMLElement>("[data-findings] h4");
  if (heading == null) return;
  revealUntil = 0;
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  heading.focus({ preventScroll: true });
  heading.scrollIntoView({
    behavior: reduce ? "auto" : "smooth",
    block: "start",
  });
}

/** The validation panel's side: serve a pending reveal after any render that
 * may have put findings on the page, and on a press while the panel is
 * already showing. */
export function useRevealFindings(): void {
  useEffect(serve);
  useEffect(() => {
    window.addEventListener(REVEAL_EVENT, serve);
    return () => window.removeEventListener(REVEAL_EVENT, serve);
  }, []);
}
