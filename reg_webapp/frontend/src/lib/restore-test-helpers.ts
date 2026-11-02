// Hold the app-owned draft restore open at the persistence boundary, for the cases
// that land something (an edit, a New, a navigation) while a write waits on
// `projectStore.restored`. Browser-only.
import { onTestFinished, vi } from "vitest";
import type { RawDraft } from "./project_data";
import { setPersistence, startDraftLifecycle } from "./project_store";

/** Start the draft lifecycle over a storage read that has not answered yet (a slow
 * IndexedDB open on a cold entry), and return the release: the read answers with
 * no saved draft, so the store's own draft stands. The lifecycle stops at the
 * release, and at the end of the test if the case never got there, so a failed
 * case cannot leave the next one waiting on a restore that never settles. The
 * release unstubs EVERY global, so a caller that stubs `fetch` itself must do it
 * after the release, not before the hold. */
export function holdRestore(): () => void {
  const { promise, resolve } = Promise.withResolvers<RawDraft | null>();
  setPersistence({ save: () => Promise.resolve(), load: () => promise });
  // The lifecycle also validates every draft change against the server: answer
  // that POST green so a long hold never reaches the network.
  const realFetch = globalThis.fetch;
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL, init?: RequestInit) =>
      String(input).includes("/project/validate")
        ? Promise.resolve(
            Response.json({ data: { ok: true, issues: [] }, meta: {} }),
          )
        : realFetch(input, init),
    ),
  );
  const stop = startDraftLifecycle();
  let held = true;
  const release = () => {
    if (!held) {
      return;
    }
    held = false;
    stop();
    vi.unstubAllGlobals();
    resolve(null);
  };
  onTestFinished(release);
  return release;
}
