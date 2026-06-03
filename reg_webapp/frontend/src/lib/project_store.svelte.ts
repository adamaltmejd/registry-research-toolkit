/**
 * The project-authoring store — a MODULE-SINGLETON Svelte 5 rune store (`.svelte.ts`
 * so the compiler processes the runes). One draft per SPA session; the home/new
 * screen is `draft == null`.
 *
 * This file is the A5.4 SEAM. Three deliberately-real-but-minimal mechanisms are
 * wired here so A5.4 is a drop-in extension, never a refactor:
 *
 *  1. `checkVersionGate` — the version-acceptance function. ACCEPTS the Model A
 *     range (schema_version major 2, reg_meta_version `reg_meta/v1.x.y`), HARD-rejects
 *     v0.x (schema_version 1.x OR reg_meta/v0.x.y → a blocking `{ok:false, reason}`,
 *     A5.4), and is a NEUTRAL no-op (`{ok:true}`) for everything else.
 *  2. `ProjectPersistence` — the autosave interface. c-i ships an in-memory Map
 *     stub (`InMemoryPersistence`) with a DEBOUNCED autosave `$effect` over the
 *     draft + a load-at-init (returns null → no restore). A5.4 swaps the impl for
 *     IndexedDB via `setPersistence`; the `storeSchemaVersion` constant gates a
 *     stored-schema mismatch (an A5.4 reject the in-memory stub never trips).
 *  3. `openError` — the blocking open-error channel. c-i sets it on a parse failure
 *     or a (currently no-op) gate failure; A5.4 reuses it for the v0.x reject.
 *
 * §9.6: NOT a structural validator — the backend is canonical. The store only
 * constructs / opens / immutably edits / serializes the draft and drives the
 * write endpoints via `lib/api.ts`.
 */

import {
  downloadBundle,
  downloadOrderCsv,
  errMessage,
  type ProjectDataBody,
  triggerDownload,
  type ValidationResultModel,
  validateProject,
} from "./api";
import {
  addBinding,
  addSource,
  newProjectData,
  type ProjectData,
  type ProjectSeed,
  removeBinding,
  removeSource,
  type Source,
  serializeProjectData,
  updateBinding,
  updateField,
  updateSource,
} from "./project_data";

/** The autosave store's OWN schema version (distinct from `project_data`'s
 * `schema_version`). Stamped alongside each persisted draft; A5.4's IndexedDB impl
 * hard-rejects a stored draft whose `storeSchemaVersion` differs (§9.7 "IndexedDB
 * schema versioning"). Bumped only when the persisted shape changes. */
export const storeSchemaVersion = 1;

/** The debounce window for the autosave `$effect` (§9.7: ~500ms). */
const AUTOSAVE_DEBOUNCE_MS = 500;

// ── Version gate (THE A5.4 SEAM) ────────────────────────────────────────────

/** The result of `checkVersionGate`. `ok:true` = load is allowed (the accept path,
 * live in c-i); `ok:false` carries a human `reason` for the blocking open-error
 * banner (A5.4 adds the branches that return this). */
export interface VersionGateResult {
  ok: boolean;
  reason?: string;
}

/** Pull the integer MAJOR out of a dotted version string (`"2.0.0"` → 2), or
 * `null` when it has no leading integer. Tolerant of a `reg_meta/vX.Y.Z` prefix
 * (caller strips that first). */
function majorOf(version: string): number | null {
  const match = /^(\d+)\./.exec(version);
  return match ? Number(match[1]) : null;
}

/** Strip the `reg_meta/v` prefix off a `reg_meta_version` to expose the dotted
 * version (`"reg_meta/v1.0.0"` → `"1.0.0"`), or `null` when it isn't that shape. */
function regMetaDotted(regMetaVersion: string): string | null {
  const match = /^reg_meta\/v(\d+\.\d+\.\d+.*)$/.exec(regMetaVersion);
  return match ? match[1] : null;
}

/**
 * THE A5.4 SEAM. Decide whether an opened project_data dict is loadable by version.
 *
 * ACCEPTS the Model A range: `schema_version` major 2 AND `reg_meta_version` of
 * the form `reg_meta/v1.x.y` (major 1). HARD-rejects v0.x (A5.4): a
 * `schema_version` major 1 OR a `reg_meta/v0.x.y` returns `{ok:false, reason}`
 * (§9.7 "Hard reject v0.x files") — no migration, pre-v1 policy. Anything else is
 * a NEUTRAL no-op (`{ok:true}`): it lets unrecognized versions through so the
 * backend remains the canonical authority.
 */
export function checkVersionGate(parsed: ProjectDataBody): VersionGateResult {
  const schemaVersion =
    typeof parsed.schema_version === "string" ? parsed.schema_version : "";
  const regMetaVersion =
    typeof parsed.reg_meta_version === "string" ? parsed.reg_meta_version : "";

  const schemaMajor = majorOf(schemaVersion);
  const dotted = regMetaDotted(regMetaVersion);
  const regMetaMajor = dotted ? majorOf(dotted) : null;

  // v0.x hard-reject (A5.4): pre-Model-A files. No migration — pre-v1 policy (§9.7).
  if (schemaMajor === 1 || regMetaMajor === 0) {
    return {
      ok: false,
      reason:
        "This project predates Model A (v1.0). Please re-author against the current schema.",
    };
  }

  // Accept the Model A range explicitly (the documented happy path).
  if (schemaMajor === 2 && regMetaMajor === 1) {
    return { ok: true };
  }

  // Neutral no-op for everything else in c-i: do not block. The backend is the
  // canonical validator; the SPA's version gate only HARD-rejects v0.x (A5.4).
  return { ok: true };
}

// ── Persistence interface (the A5.4 IndexedDB swap point) ────────────────────

/** The autosave persistence contract. c-i ships `InMemoryPersistence` (a Map
 * stub); A5.4 swaps in an IndexedDB impl via `setPersistence`. `save` carries the
 * `storeSchemaVersion` so A5.4's load can hard-reject a mismatched stored draft. */
export interface ProjectPersistence {
  /** Persist the draft under `key` with the store schema version stamped. */
  save(key: string, draft: ProjectData, schemaVersion: number): Promise<void>;
  /** Restore the most-recently-saved draft, or `null` when none/incompatible.
   * c-i's stub always returns `null` (no restore-on-init). */
  load(): Promise<ProjectData | null>;
}

/** The default in-memory stub: a `Map` keyed by project key. `load` returns
 * `null` (no restore at init) — A5.4's IndexedDB impl actually restores. */
class InMemoryPersistence implements ProjectPersistence {
  private store = new Map<
    string,
    { draft: ProjectData; schemaVersion: number }
  >();

  save(key: string, draft: ProjectData, schemaVersion: number): Promise<void> {
    this.store.set(key, { draft, schemaVersion });
    return Promise.resolve();
  }

  load(): Promise<ProjectData | null> {
    // c-i: no restore-on-init. A5.4's impl reads the most-recent key + gates on
    // `schemaVersion === storeSchemaVersion`.
    return Promise.resolve(null);
  }
}

let persistence: ProjectPersistence = new InMemoryPersistence();

/** Swap the persistence impl (A5.4's IndexedDB drop-in). Called before
 * `initPersistence` so the load-at-init uses the new impl. */
export function setPersistence(impl: ProjectPersistence): void {
  persistence = impl;
}

/** The single autosave key (one draft per session). Exported as the single source
 * of truth so `main.ts` wires the IndexedDB load key from it (A5.4). */
export const AUTOSAVE_KEY = "current";

// ── The store ───────────────────────────────────────────────────────────────

/** The draft, or `null` for the home/new screen. */
let draft = $state<ProjectData | null>(null);

/** The serialized text of the last DOWNLOAD (the §9.7 dirty baseline). Set on
 * open (the opened raw text) and on download (the just-written text); `null` while
 * no draft is loaded. */
let lastDownloaded = $state<string | null>(null);

/** The blocking open-error channel (parse failure / A5.4 version reject). Cleared
 * on a successful open / new / when the user dismisses. */
let openError = $state<string | null>(null);

/** The last `/validate` result (a 200 `{ok, issues}`), or `null` before the first
 * validate / after an edit invalidates it. */
let validation = $state<ValidationResultModel | null>(null);

/** A malformed-REQUEST error from `/validate` / the download endpoints (a true
 * 4xx ApiError message) — distinct from a 200 `ok:false` issue list. */
let requestError = $state<string | null>(null);

/** True while a `/validate` or download POST is in flight (disables the toolbar). */
let busy = $state(false);

/** The dirty flag: the draft has diverged from the last download (§9.7). */
const dirty = $derived(
  draft != null && serializeProjectData(draft) !== lastDownloaded,
);

/** A clean draft that has VALIDATED ok — the gate for the order/bundle downloads.
 * Re-validation is required after any edit (an edit clears `validation`). */
const validatedClean = $derived(validation?.ok === true);

/** Replace the draft, clearing the stale validation (an edit invalidates the last
 * `/validate` result). The mutators below all funnel through here so `dirty` and
 * `validatedClean` recompute on every edit. */
function setDraft(next: ProjectData): void {
  draft = next;
  validation = null;
}

export const projectStore = {
  // ── Reactive reads (getters so consumers stay subscribed) ─────────────────
  get draft() {
    return draft;
  },
  get dirty() {
    return dirty;
  },
  get openError() {
    return openError;
  },
  get validation() {
    return validation;
  },
  get requestError() {
    return requestError;
  },
  get busy() {
    return busy;
  },
  get validatedClean() {
    return validatedClean;
  },

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /** Start a fresh Model A draft (clears any open error + stale validation). The
   * baseline is the new skeleton serialized, so a brand-new project is NOT dirty
   * until edited. */
  newProject(seed: ProjectSeed): void {
    const next = newProjectData(seed);
    draft = next;
    lastDownloaded = serializeProjectData(next);
    validation = null;
    openError = null;
    requestError = null;
  },

  /**
   * Open a `project_data.json` File. Parse → guard non-object → `checkVersionGate`.
   * On accept: load the parsed dict VERBATIM (unmapped keys / namespaced blocks
   * survive) and set the dirty baseline (`lastDownloaded`) to the draft
   * re-serialized through OUR serializer — NOT the file's raw text — so a
   * freshly-opened, unedited draft is CLEAN even when the file's own formatting
   * differs from our pretty-print. On a parse error or a gate failure: set
   * `openError` and do NOT load (the existing draft, if any, is untouched).
   */
  async openFromFile(file: File): Promise<void> {
    const text = await file.text();
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      openError = "Not valid JSON — could not parse the file.";
      return;
    }
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      openError = "project_data.json must be a JSON object at the top level.";
      return;
    }
    const obj = parsed as ProjectDataBody;
    const gate = checkVersionGate(obj);
    if (!gate.ok) {
      openError = gate.reason ?? "This project file cannot be opened.";
      return;
    }
    // Load verbatim. The dirty baseline is the draft re-serialized through OUR
    // serializer (not the file's raw text): that way an unedited open compares
    // equal to itself even when the file's formatting differs from our
    // pretty-print, so a freshly-opened draft is not spuriously dirty.
    draft = obj as ProjectData;
    lastDownloaded = serializeProjectData(obj as ProjectData);
    validation = null;
    openError = null;
    requestError = null;
  },

  /** Dismiss the open-error banner. */
  clearOpenError(): void {
    openError = null;
  },

  /** Dismiss the malformed-request banner. */
  clearRequestError(): void {
    requestError = null;
  },

  /** Download the current draft as `project_data.json` and mark it clean (set the
   * dirty baseline to the just-written text). A no-op when no draft is loaded. */
  downloadProject(): void {
    if (draft == null) {
      return;
    }
    const text = serializeProjectData(draft);
    triggerDownload(
      new Blob([text], { type: "application/json" }),
      "project_data.json",
    );
    lastDownloaded = text;
  },

  // ── Write endpoints ───────────────────────────────────────────────────────

  /** POST the serialized draft to `/validate`, storing the 200 `{ok, issues}`.
   * A true 4xx (malformed request) sets `requestError` instead (NEVER on
   * `ok:false`). Returns the result (null on a request error). */
  async validate(): Promise<ValidationResultModel | null> {
    if (draft == null) {
      return null;
    }
    // Snapshot the draft reference. The name input is NOT disabled during a
    // validate, and the mutators / new / open all REPLACE the whole draft object,
    // so a mid-flight edit makes `draft !== target`. Discard a stale response
    // rather than writing `validation` for a superseded draft — otherwise an
    // edit's `validation = null` (setDraft) gets clobbered by the old result,
    // wrongly flipping `validatedClean` back on and re-enabling the downloads.
    const target = draft;
    busy = true;
    requestError = null;
    try {
      const result = await validateProject(target as ProjectDataBody);
      if (draft !== target) {
        return result;
      }
      validation = result;
      return result;
    } catch (e) {
      if (draft === target) {
        requestError = errMessage(e);
      }
      return null;
    } finally {
      busy = false;
    }
  },

  /** Download the order-export CSV (`/project/order`). A structurally invalid spec
   * is the backend's 422 → `requestError`. */
  async downloadOrder(): Promise<void> {
    if (draft == null) {
      return;
    }
    busy = true;
    requestError = null;
    try {
      await downloadOrderCsv(draft as ProjectDataBody);
    } catch (e) {
      requestError = errMessage(e);
    } finally {
      busy = false;
    }
  },

  /** Download the MONA `.py` bundle (`/bundle`). A build-gate failure is the
   * backend's 422 → `requestError`. */
  async downloadBundleFile(): Promise<void> {
    if (draft == null) {
      return;
    }
    busy = true;
    requestError = null;
    try {
      await downloadBundle(draft as ProjectDataBody);
    } catch (e) {
      requestError = errMessage(e);
    } finally {
      busy = false;
    }
  },

  // ── Immutable mutators (replace the draft so `dirty` recomputes) ──────────
  // Guarded against a null draft so a stray call on the home screen is a no-op.

  updateField<K extends keyof ProjectData>(
    key: K,
    value: ProjectData[K],
  ): void {
    if (draft != null) {
      setDraft(updateField(draft, key, value));
    }
  },
  addSource(): void {
    if (draft != null) {
      setDraft(addSource(draft));
    }
  },
  removeSource(index: number): void {
    if (draft != null) {
      setDraft(removeSource(draft, index));
    }
  },
  updateSource(index: number, patch: Partial<Source>): void {
    if (draft != null) {
      setDraft(updateSource(draft, index, patch));
    }
  },
  addBinding(sourceIndex: number): void {
    if (draft != null) {
      setDraft(addBinding(draft, sourceIndex));
    }
  },
  removeBinding(sourceIndex: number, bindingIndex: number): void {
    if (draft != null) {
      setDraft(removeBinding(draft, sourceIndex, bindingIndex));
    }
  },
  updateBinding(
    sourceIndex: number,
    bindingIndex: number,
    patch: Parameters<typeof updateBinding>[3],
  ): void {
    if (draft != null) {
      setDraft(updateBinding(draft, sourceIndex, bindingIndex, patch));
    }
  },
};

// ── Autosave + load-at-init (the A5.4 persistence wiring) ────────────────────

/**
 * Wire the debounced autosave `$effect` + the load-at-init. MUST be called inside
 * a reactive root (a component init or an `$effect.root`) — it registers an
 * `$effect`. c-i: the autosave writes to the in-memory stub; the load-at-init
 * returns null (no restore). A5.4 swaps in the IndexedDB impl and snapshots the
 * draft at the persistence boundary (a live $state proxy is not
 * structured-cloneable — see the save call below).
 *
 * Returns the (already-pending) load promise so a caller can await the
 * (currently no-op) restore if it wants to. The debounce timer is cleared on
 * teardown so a pending save doesn't fire after unmount.
 */
export function initPersistence(): Promise<void> {
  // Load-at-init: restore the most-recent draft (null in c-i → no restore). Only
  // restore onto an empty store so we never clobber an in-progress new/open.
  const loaded = persistence.load().then((restored) => {
    if (restored != null && draft == null) {
      draft = restored;
      // Do NOT reset lastDownloaded here: a restored autosave draft has NOT been
      // downloaded to the durable project_data.json this session, so it must read
      // as DIRTY (§9.7 unsaved-changes warning). lastDownloaded stays null →
      // dirty=true → the header indicator + beforeunload warning fire. IndexedDB
      // autosave is recovery, not the durable file.
    }
  });

  // Debounced autosave: re-runs whenever `draft` changes (the `$effect` tracks the
  // `serializeProjectData(draft)` read). Debounced ~500ms so a burst of edits
  // writes once. A null draft clears nothing here (the home screen).
  $effect(() => {
    const current = draft;
    if (current == null) {
      return;
    }
    // The `const current = draft` read above already registers the reactive
    // dependency: every mutator replaces the whole draft object (immutable
    // spread), so the `draft` reference changes on each edit. Debounce so a burst
    // of edits writes once.
    const timer = setTimeout(() => {
      // $state.snapshot de-proxies the draft before the persistence boundary: a
      // live Svelte rune proxy is NOT structured-cloneable, so handing it to
      // IndexedDB.put() throws DataCloneError. Persist a plain snapshot.
      void persistence.save(
        AUTOSAVE_KEY,
        $state.snapshot(current),
        storeSchemaVersion,
      );
    }, AUTOSAVE_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  });

  return loaded;
}
