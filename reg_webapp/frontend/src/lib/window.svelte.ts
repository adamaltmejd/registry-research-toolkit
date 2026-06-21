/**
 * The active "project window" runtime layer — a MODULE-SINGLETON Svelte 5 rune
 * store (mirrors `router.svelte.ts`'s shape: one instance per SPA, reactive
 * `$state`/`$derived` getters). It is the SINGLE read/write path for the active
 * study window (`{from, to}` int years, or `null` = no window → full history);
 * the header year slider and (later, #615) each page's period picker go through
 * it rather than touching the project store or `localStorage` directly.
 *
 * PRECEDENCE (see #611 → Period model). Two backing stores, one source of truth:
 *  - When a project draft is ACTIVE, the window IS `draft.window`. Reading
 *    returns it; setting MUTATES the draft (via `projectStore.updateField`,
 *    which marks it dirty → the store's existing debounced autosave persists it).
 *    The window is durable/exportable BECAUSE it rides on `project_data.json`.
 *  - When there is NO draft (browsing without a project), the window falls back
 *    to `localStorage` so the slider still works and survives a reload.
 *
 * The draft ALWAYS wins while it exists: a draft with no `window` reads as `null`
 * (its own absence), NOT the localStorage value — the active project's state is
 * authoritative, and localStorage is purely the no-project fallback. We do NOT
 * mirror writes into localStorage while a draft is active (the draft is the
 * durable copy; mirroring would resurrect a stale value the next time the store
 * is pristine).
 */

import type { StudyWindow } from "./project_data";
import { projectStore } from "./project_store.svelte";

/** The localStorage key for the no-draft fallback window. Namespaced so it
 * doesn't collide with any future SPA-local key. */
const STORAGE_KEY = "reg_webapp:project_window";

/** Parse a stored window JSON blob into a `{from, to}` int pair, or `null` when
 * absent / malformed / not two finite integers with `to >= from`. Never throws:
 * a corrupt value (hand-edited storage, an older shape) reads as "no window"
 * rather than white-screening a module-init read. */
function parseStored(raw: string | null): StudyWindow | null {
  if (raw === null) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== "object") {
    return null;
  }
  const { from, to } = parsed as { from?: unknown; to?: unknown };
  if (
    !Number.isInteger(from) ||
    !Number.isInteger(to) ||
    (to as number) < (from as number)
  ) {
    return null;
  }
  return { from: from as number, to: to as number };
}

/** Read the no-draft fallback window off `localStorage` (null on any failure —
 * a privacy-mode `localStorage` getter can throw). */
function readStored(): StudyWindow | null {
  try {
    return parseStored(localStorage.getItem(STORAGE_KEY));
  } catch {
    return null;
  }
}

class WindowStore {
  /** The localStorage fallback, held as reactive `$state` so a write updates the
   * getter even while no draft is active. Seeded once from storage at module
   * init; thereafter this rune IS the live fallback value (we write storage AND
   * this together). */
  #fallback = $state<StudyWindow | null>(readStored());

  /** The active window: the draft's `window` when a draft exists (its absence
   * reads as `null`), else the localStorage fallback. A `$derived` so consumers
   * that read `windowStore.value` re-run when EITHER the draft changes
   * (projectStore.draft is reactive) or the fallback is written. */
  readonly value = $derived<StudyWindow | null>(
    projectStore.draft != null
      ? (projectStore.draft.window ?? null)
      : this.#fallback,
  );

  /** The browse-time (no-draft) fallback window, IGNORING any active draft.
   * `value` collapses to `null` once a windowless draft exists, so the
   * draft-creation path (#629 item 3) reads the fallback HERE to seed a fresh
   * draft's `window` — keeping `localStorage` reachable only through this store
   * (never reached into directly from project_store). */
  get fallback(): StudyWindow | null {
    return this.#fallback;
  }

  /** Set (or clear, with `null`) the active window through the single write
   * path. With a draft active it mutates `draft.window` (→ dirty → autosave);
   * with no draft it writes the localStorage fallback. */
  set(next: StudyWindow | null): void {
    if (projectStore.draft != null) {
      // Mutate the draft. `updateField("window", undefined)` omits the key
      // (additive — an unset window serializes as absent), matching the optional
      // `StudyWindow` shape; a value writes it. Reuses the store's dirty +
      // debounced-autosave path — no separate persistence here.
      projectStore.updateField("window", next ?? undefined);
      return;
    }
    this.#fallback = next;
    try {
      if (next === null) {
        localStorage.removeItem(STORAGE_KEY);
      } else {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      }
    } catch {
      // A failed write (privacy mode / quota) leaves the in-memory `#fallback`
      // as the session's source of truth — the slider still works, it just
      // doesn't survive a reload. Not fatal.
    }
  }
}

export const windowStore = new WindowStore();
