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
 *     or a (currently no-op) gate failure; the v0.x reject reuses it.
 *
 * NOT a structural validator — the backend is canonical (see reg_webapp/DESIGN.md
 * → Pydantic boundary). The store only
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
  type BindingResolution,
  deriveType,
  resolveBindingAt,
  type UnresolvedReason,
  variantSeg,
} from "./catalog";
import { periodFromWire, periodToWire } from "./period";
import {
  addBinding,
  addSource,
  type Binding,
  defaultSourceName,
  isPrefilledSourceName,
  newProjectData,
  type Period,
  type ProjectData,
  type ProjectSeed,
  removeBinding,
  removeSource,
  type Source,
  serializeProjectData,
  uniqueSourceName,
  updateBinding,
  updateField,
  updateSource,
} from "./project_data";

/** The autosave store's OWN schema version (distinct from `project_data`'s
 * `schema_version`). Stamped alongside each persisted draft; the IndexedDB impl
 * hard-rejects a stored draft whose `storeSchemaVersion` differs (see
 * reg_webapp/DESIGN.md → Browser storage + project-file persistence (the SPA
 * store)). Bumped only when the persisted shape changes. */
export const storeSchemaVersion = 1;

/** The debounce window for the autosave `$effect` (~500ms). */
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
 * the form `reg_meta/v1.x.y` (major 1). HARD-rejects v0.x: a
 * `schema_version` major 1 OR a `reg_meta/v0.x.y` returns `{ok:false, reason}`
 * — no migration, pre-v1 policy. Anything else is
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

  // v0.x hard-reject: pre-Model-A files. No migration — pre-v1 policy.
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

// ── Stable client-side ids (issue #200) ──────────────────────────────────────
//
// The `{#each}` blocks over `sources[]` / `bindings[]` need a STABLE key so Svelte
// remounts the correct component instance on a middle-remove (an index key rebinds a
// surviving instance to a shifted item, landing stale per-instance UI — an open
// CatalogPicker, a BindingEditor `picking` flag, a PeriodEditor's seed — on the
// WRONG item; see issue #200).
//
// Stable ids must NEVER enter the serialized draft: `Source`/`Binding` are closed
// objects (`extra="forbid"` in reg_schema/structural.py) — an injected `_uid` would
// trip `unexpected_field` AND leak into the downloaded project_data.json. So the
// store owns a PARALLEL id tree, kept in lockstep with every mutator, that the draft
// (and thus every serialize / validate / order / bundle POST, all of which
// JSON.stringify the draft) never sees. The immutable mutators replace edited
// objects on every edit, so an object-keyed WeakMap wouldn't survive — the store
// owns the id↔position association positionally, patching it alongside each
// structural edit.

/** One source's stable id + its bindings' stable ids (positional mirror of
 * `draft.sources[i]`). */
interface SourceIds {
  id: string;
  bindings: string[];
}

/** The id tree mirroring `draft.sources` 1:1 by position. Rebuilt on a wholesale
 * draft replacement (new / open / restore), patched in lockstep on structural
 * edits. */
let sourceIds = $state<SourceIds[]>([]);

let _idSeq = 0;
/** A fresh, process-unique client id. Opaque — never serialized. */
function nextId(): string {
  _idSeq += 1;
  return `c${_idSeq}`;
}

/** Build a fresh id tree for a wholesale-replaced draft. A malformed (non-array)
 * `sources` / `bindings` — including a `null`/`undefined` source element — yields an
 * empty mirror for that slot, matching the editors' coercion (they render such slots
 * as []), so no keyed instance is created against them. NEVER throws: it runs at a
 * draft-replacement boundary where a throw would abort the update mid-assignment. */
function buildIds(next: ProjectData | null): SourceIds[] {
  const sources =
    next != null && Array.isArray(next.sources) ? next.sources : [];
  return sources.map((s) => ({
    id: nextId(),
    // `s?.bindings`: a null/undefined source element must not throw — yield no
    // binding ids for it (the malformed-yields-empty-mirror contract).
    bindings: Array.isArray(s?.bindings) ? s.bindings.map(() => nextId()) : [],
  }));
}

// ── Derived-binding provenance (B2 — UI audit: stale derived fields) ──────────
//
// A binding's `type` / `display_name` / `representation` are DERIVED-ON-PICK from
// reg_meta at the source's (period, variant) (see CatalogPicker derive-on-pick).
// Two staleness bugs (the UI audit): (1) picking with the period unset left the
// binding silently on the opaque fallback; (2) changing the source period/variant
// AFTER a pick re-derived nothing, so types/representations/display defaults went
// silently stale. The fix re-resolves every binding when its source's
// (period, variant) changes — but must NEVER clobber a value the user edited by
// hand.
//
// The mechanism: a PARALLEL provenance mirror (like the issue-#200 `sourceIds`
// tree — editor-local, NEVER serialized, so project_data.json stays schema-pure
// under `extra="forbid"`). For each binding we remember the LAST-DERIVED snapshot.
// On re-derive we compare the binding's CURRENT field to the last-derived one:
//   - field still equals last-derived  → it's still ours → update it.
//   - field diverged (user edited it)  → KEEP the user value, surface a
//     non-blocking mismatch hint on the binding row.
// A binding never picked/derived (no provenance) is left entirely alone.

/** The display status of a binding's derivation, surfaced on the binding row.
 *  - `null`            — never derived (no marker).
 *  - `derived`         — resolved cleanly at the current (period, variant).
 *  - `unresolved`      — resolution impossible (period unset / no covering state).
 *  - `ambiguous`       — >1 representation now co-exists; the author must re-pick.
 *  - `error`           — the resolve fetch failed (network / 422).
 * `mismatch` rides ALONGSIDE the status: a field the user hand-edited away from
 * the last-derived value would have re-derived differently (non-blocking — the
 * user value is kept; the validator stays the authority). */
export interface BindingDerivation {
  status: "derived" | "unresolved" | "ambiguous" | "error";
  /** Why unresolved (only when `status === "unresolved"`). */
  reason?: UnresolvedReason;
  /** A human note for `ambiguous` / `error` (the resolve message). */
  detail?: string;
  /** The (period, variant) the last derivation ran at — so a no-op re-derive
   * (period unchanged) can skip the fetch, and the mismatch note can name them. */
  variant: string;
  period: string | null;
  /** The values the LAST derivation produced — the clobber-vs-keep baseline. A
   * field still equal to these is still ours to update; a diverged field is the
   * user's and is preserved. `undefined` type means "no derived type yet". */
  derivedType?: string;
  derivedDisplayName?: string | null;
  derivedRepresentation?: string | null;
  /** Set when a hand-edited field would now re-derive differently (advisory). */
  mismatch?: { field: "type" | "display_name"; derived: string } | null;
}

/** The provenance mirror — `[sourceIndex][bindingIndex]`, positional like
 * `sourceIds`. A `null` slot = no derivation yet (no marker). Rebuilt empty on a
 * wholesale draft replacement (a restored/opened draft has no in-session
 * derivation history — its bindings render markerless until the next pick or
 * period change re-derives them, which is honest: we can't claim a value was
 * auto-derived when we didn't derive it). */
let bindingDerivations = $state<(BindingDerivation | null)[][]>([]);

/** Build an empty derivation mirror matching a wholesale-replaced draft's shape. */
function buildDerivations(
  next: ProjectData | null,
): (BindingDerivation | null)[][] {
  const sources =
    next != null && Array.isArray(next.sources) ? next.sources : [];
  return sources.map((s) =>
    Array.isArray(s?.bindings) ? s.bindings.map(() => null) : [],
  );
}

// Per-source re-derivation generation counters. Re-resolution is async; rapid
// period typing fires overlapping re-derive passes, so each pass captures the
// source's generation at start and DISCARDS its writes if a newer pass has since
// started (the same stale-response guard `asyncResource` / the `validate` snapshot
// use). Keyed by the source's STABLE id so it survives index shifts.
const rederiveGen = new Map<string, number>();

/** Invalidate a source's in-flight re-derive pass and drop its gen entry. Any
 * pending applyResolution captured the OLD generation, so bumping past it makes
 * every in-flight write for this source a no-op. Called on a STRUCTURAL mutation
 * (remove source / remove binding) that shifts positional indices the in-flight
 * pass captured; also prevents the gen Map from leaking entries for removed
 * sources. A `null`/empty id (a source with no mirror entry) is ignored. */
function invalidateSourceGen(sourceId: string): void {
  rederiveGen.delete(sourceId);
}

/** The serialized text of the last DOWNLOAD (the dirty baseline). Set on
 * open (the opened raw text) and on download (the just-written text); `null` while
 * no draft is loaded. */
let lastDownloaded = $state<string | null>(null);

/** The blocking open-error channel (parse failure / version reject). Cleared
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

/** The dirty flag: the draft has diverged from the last download. */
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

// ── Catalog → project handoff (C1 — UI audit finding 2) ──────────────────────
//
// The catalog variable page hands a resolved variable-state to the project store
// WITHOUT importing any editor component: `addFromCatalog` is the single store-level
// entry point that (find-or-create source) → (duplicate guard) → (append binding) →
// (derive at the SOURCE's period through the SAME resolveBindingAt + applyPickedBinding
// path the picker uses). Catalog routes are reachable from the module-singleton store,
// so this keeps the browse→author handoff a pure store API.

/** The catalog page's resolved variable-state to add to the project (C1). Carries
 * the full register coordinate + the variable FQID + the chosen representation (the
 * delivery column, when the page resolved one) + the page's resolved period as a
 * wire string (prefills a freshly-created source's period; an existing source keeps
 * its own period). */
export interface CatalogAddPayload {
  /** The 3-seg `provider/register/variant` coordinate (the source register_variant). */
  registerVariant: string;
  /** The bare 3-seg variable FQID (`scb/lisa/kon`). */
  variable: string;
  /** The chosen delivery column when the page resolved a specific representation
   * (the StatesView is at a single state), else null. */
  representation: string | null;
  /** The page's resolved period as a wire string (`"2018"`, `"2010..2020"`, a
   * token), or null when the user hasn't resolved a period — only used to PREFILL a
   * newly-created source. */
  resolvedPeriod: string | null;
}

/** The outcome of an `addFromCatalog` call, for the catalog page's inline feedback.
 *  - `added`         — a binding was appended (to a found or newly-created source).
 *  - `already-present` — the source already had this fqid (+ representation); no-op.
 */
export interface CatalogAddResult {
  status: "added" | "already-present";
  /** True when a source was created (vs. appended to an existing matching source) —
   * lets the confirmation distinguish "added to scb/lisa/v1" from "started a new
   * source". */
  createdSource: boolean;
}

/** Whether a source already carries a binding for this fqid (+ representation) — the
 * duplicate guard, applied at the CONCEPT level (review MAJOR 2). For the same
 * variable, a stored binding decides as follows:
 *  - the PAYLOAD representation is null (a single-column concept, or an unspecified
 *    add) → any existing binding of the variable is a duplicate (the concept is
 *    already in the project; there is no other column to distinguish);
 *  - the STORED representation is null → ALSO a duplicate for ANY payload
 *    representation. A null stored representation means "the only column at the
 *    source's period" — so a page that pinned a column R against a DIFFERENT (page)
 *    period, where the source resolves single-rep, must NOT add a second binding
 *    (the source's resolve collapses R to that single column). This is the desync
 *    the page-pin-vs-source-derive gap would otherwise miss.
 *  - both non-null → exact column match (two genuinely co-existing columns are
 *    distinct extractions and may both live in the source). */
function sourceHasBinding(
  source: Source,
  variable: string,
  representation: string | null,
): boolean {
  const bindings = Array.isArray(source.bindings) ? source.bindings : [];
  return bindings.some((b) => {
    if ((typeof b.variable === "string" ? b.variable : "") !== variable) {
      return false;
    }
    const bRep = typeof b.representation === "string" ? b.representation : null;
    // null on EITHER side means "the only column" — a duplicate regardless of the
    // other side's value; both non-null compares the exact delivery column.
    if (representation == null || bRep == null) {
      return true;
    }
    return bRep === representation;
  });
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

  // ── Stable client-side ids (issue #200 — keys for the editor each-blocks) ──
  // Positional accessors so the `{#each}` blocks key on a STABLE id rather than the
  // array index. A fresh source/binding gets a new id; a remove drops its id; an
  // update leaves the id in place — so a middle-remove remounts the right instance
  // instead of rebinding a survivor's stale UI state to a shifted item. Out-of-range
  // (a malformed spec where the id mirror is empty) falls back to the index so the
  // key is still defined.

  /** The stable id for the source at `index` (falls back to the index string when
   * the mirror has no entry — a malformed non-array `sources`). */
  sourceId(index: number): string {
    return sourceIds[index]?.id ?? `i${index}`;
  },
  /** The stable id for the binding at `[sourceIndex][bindingIndex]` (index fallback
   * when the mirror has no entry). */
  bindingId(sourceIndex: number, bindingIndex: number): string {
    return sourceIds[sourceIndex]?.bindings[bindingIndex] ?? `i${bindingIndex}`;
  },

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  /** Start a fresh Model A draft (clears any open error + stale validation). The
   * baseline is the new skeleton serialized, so a brand-new project is NOT dirty
   * until edited. */
  newProject(seed: ProjectSeed): void {
    const next = newProjectData(seed);
    // Atomic replacement (compute the mirror before mutating store state) — the
    // skeleton is always well-formed here, but this matches openFromFile/restore.
    const ids = buildIds(next);
    draft = next;
    sourceIds = ids;
    bindingDerivations = buildDerivations(next);
    rederiveGen.clear();
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
    // Compute the id mirror BEFORE mutating any store state so the replacement is
    // atomic: a throw here would otherwise leave a malformed draft loaded while
    // lastDownloaded/validation/openError still belong to the previous document
    // (stale validatedClean keeps the order/bundle downloads enabled). buildIds is
    // guarded never to throw, but the atomic order is the durable invariant.
    const opened = obj as ProjectData;
    const ids = buildIds(opened);
    draft = opened;
    sourceIds = ids;
    bindingDerivations = buildDerivations(opened);
    rederiveGen.clear();
    lastDownloaded = serializeProjectData(opened);
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
  // STRUCTURAL edits to `sources`/`bindings` MUST go through the mirror-aware
  // mutators below (addSource/removeSource/addBinding/removeBinding) — they patch
  // the stable-id mirror (`sourceIds`) in lockstep. A structural edit that bypasses
  // them desyncs the mirror and resurrects the wrong-instance bug class issue #200
  // fixes. `updateField` therefore rebuilds the mirror when handed `sources`.

  // ── Derived-binding provenance reads (B2) ─────────────────────────────────

  /** The derivation status for the binding at `[sourceIndex][bindingIndex]`
   * (`null` = never derived → no marker). Drives BindingEditor's unresolved /
   * stale marker. */
  bindingDerivation(
    sourceIndex: number,
    bindingIndex: number,
  ): BindingDerivation | null {
    return bindingDerivations[sourceIndex]?.[bindingIndex] ?? null;
  },

  updateField<K extends keyof ProjectData>(
    key: K,
    value: ProjectData[K],
  ): void {
    if (draft != null) {
      const next = updateField(draft, key, value);
      setDraft(next);
      // `K extends keyof ProjectData` admits `"sources"` (and any string via the
      // open index signature). A wholesale `sources` replacement must rebuild the
      // mirror or it desyncs from the new array — keep them consistent.
      if (key === "sources") {
        sourceIds = buildIds(next);
        bindingDerivations = buildDerivations(next);
        rederiveGen.clear();
      }
    }
  },
  addSource(): void {
    if (draft != null) {
      setDraft(addSource(draft));
      // Keep the id mirror in lockstep: append a fresh source id (no bindings).
      sourceIds = [...sourceIds, { id: nextId(), bindings: [] }];
      bindingDerivations = [...bindingDerivations, []];
    }
  },
  removeSource(index: number): void {
    if (draft != null) {
      // A structural shift desyncs the positional (sourceIndex, bindingIndex) an
      // in-flight rederiveSource captured: an applyResolution dispatched against
      // the OLD layout would land on whatever source now sits at that index.
      // INVALIDATE the removed source's generation (and drop its leaked gen entry)
      // so any in-flight pass for it is discarded; the surviving sources keep their
      // own gen — but applyResolution ALSO re-verifies the stable id before any
      // write (belt-and-suspenders), so a shift that moves a survivor's index is
      // caught there. The removed source's id is read BEFORE the filter below.
      invalidateSourceGen(projectStore.sourceId(index));
      setDraft(removeSource(draft, index));
      sourceIds = sourceIds.filter((_, i) => i !== index);
      bindingDerivations = bindingDerivations.filter((_, i) => i !== index);
    }
  },
  updateSource(index: number, patch: Partial<Source>): void {
    if (draft == null) {
      return;
    }
    // B2: detect a (period, variant)-affecting change BEFORE the mutate so we can
    // re-derive every binding of this source against the NEW coordinate. Both
    // `period` and `register_variant` (which carries the variant seg) feed the
    // resolve, so a change to either invalidates the derived fields.
    const before = draft.sources?.[index];
    // #312: a register_variant change prefills the source name from the register
    // slug (uppercased, uniqueness-suffixed) — every variant-setting path funnels
    // through here (addFromCatalog's source creation, the picker, hand-typing).
    // Prefill-only: applies when the variant ACTUALLY changes (a no-op re-pick
    // must not recompute a suffixed name, e.g. LISA_3 → LISA_2 after a sibling
    // remove freed the slot), the patch doesn't set a name itself, AND the
    // current name is empty or a previous prefill; a user-entered name survives.
    if (
      before &&
      typeof patch.register_variant === "string" &&
      patch.register_variant !== registerVariantOf(before) &&
      patch.name === undefined &&
      isPrefilledSourceName(
        typeof before.name === "string" ? before.name : "",
        registerVariantOf(before),
      )
    ) {
      const base = defaultSourceName(patch.register_variant);
      const sources = Array.isArray(draft.sources) ? draft.sources : [];
      patch = {
        ...patch,
        name: base ? uniqueSourceName(sources, base, index) : "",
      };
    }
    setDraft(updateSource(draft, index, patch));
    const after = draft.sources?.[index];
    if (before && after && resolutionInputsChanged(before, after)) {
      void rederiveSource(index);
    }
  },
  addBinding(sourceIndex: number): void {
    if (draft != null) {
      setDraft(addBinding(draft, sourceIndex));
      sourceIds = sourceIds.map((s, i) =>
        i === sourceIndex ? { ...s, bindings: [...s.bindings, nextId()] } : s,
      );
      bindingDerivations = bindingDerivations.map((d, i) =>
        i === sourceIndex ? [...d, null] : d,
      );
    }
  },
  removeBinding(sourceIndex: number, bindingIndex: number): void {
    if (draft != null) {
      // Removing a binding shifts every later binding's index down by one. An
      // in-flight rederiveSource pass dispatched applyResolution against the OLD
      // binding indices, so a resolution for old binding j+1 would now land on old
      // binding j+2. INVALIDATE this source's generation so that whole in-flight
      // pass is discarded; the per-binding FQID re-check in applyResolution is the
      // second line of defence.
      invalidateSourceGen(projectStore.sourceId(sourceIndex));
      setDraft(removeBinding(draft, sourceIndex, bindingIndex));
      sourceIds = sourceIds.map((s, i) =>
        i === sourceIndex
          ? { ...s, bindings: s.bindings.filter((_, j) => j !== bindingIndex) }
          : s,
      );
      bindingDerivations = bindingDerivations.map((d, i) =>
        i === sourceIndex ? d.filter((_, j) => j !== bindingIndex) : d,
      );
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

  /**
   * Apply a picker's derive-on-pick result to a binding AND record its provenance
   * (B2). This is the pick path's single entry point: it writes the variable +
   * derived type + (default) display name + representation through the immutable
   * mutator, then stamps the last-derived snapshot so a later period/variant change
   * knows which fields are still ours to update vs. user-diverged. `display_name`
   * is only set when the resolve gave a default AND the user hasn't already set one
   * (mirrors the old BindingEditor.onPickVariable contract).
   */
  applyPickedBinding(
    sourceIndex: number,
    bindingIndex: number,
    picked: {
      variable: string;
      type: string;
      displayNameDefault: string | null;
      representation?: string | null;
      // The resolution status from the picker so the row marker is honest even on
      // a pick (e.g. picked with period unset → unresolved/opaque).
      status?: BindingDerivation["status"];
      reason?: UnresolvedReason;
    },
  ): void {
    if (draft == null) {
      return;
    }
    const existing = draft.sources?.[sourceIndex]?.bindings?.[bindingIndex] as
      | Binding
      | undefined;
    const userHasDisplayName = (existing?.display_name ?? "") !== "";
    const patch: Partial<Binding> = {
      variable: picked.variable,
      type: picked.type,
      representation: picked.representation ?? null,
    };
    if (picked.displayNameDefault != null && !userHasDisplayName) {
      patch.display_name = picked.displayNameDefault;
    }
    setDraft(updateBinding(draft, sourceIndex, bindingIndex, patch));

    const source = draft.sources?.[sourceIndex];
    setDerivation(sourceIndex, bindingIndex, {
      status: picked.status ?? "derived",
      reason: picked.reason,
      variant: source ? variantSeg(registerVariantOf(source)) : "",
      period: source ? periodToWire(source.period as Period) : null,
      derivedType: picked.type,
      // The display name we'd CLAIM as derived is the default — but only when we
      // actually wrote it (user hadn't set one). When the user already had a name,
      // the field is theirs from the start, so the derived baseline is that there
      // is no derived display name to later clobber.
      derivedDisplayName:
        picked.displayNameDefault != null && !userHasDisplayName
          ? picked.displayNameDefault
          : null,
      derivedRepresentation: picked.representation ?? null,
      mismatch: null,
    });
  },

  /**
   * Add a catalog variable-state to the project (C1 — catalog→project handoff).
   * The single store-level entry point the catalog page calls (it imports NO editor
   * component). Steps:
   *   1. Ensure a draft exists — implicitly create the untitled project from `seed`
   *      when the store is pristine (same as "New project").
   *   2. Find a source whose `register_variant` matches `payload.registerVariant`;
   *      create one (prefilling its period from `payload.resolvedPeriod`) when none.
   *   3. Duplicate guard: a source already carrying this fqid (+ representation) is a
   *      no-op → `already-present`.
   *   4. Append the binding and write its variable (+ the page's representation)
   *      SYNCHRONOUSLY so the binding is non-empty immediately (a fast second add is
   *      caught by the duplicate guard, which keys on the variable), STAMP a provisional
   *      `unresolved` marker so the row is never a bare type:"" with no cue, THEN derive
   *      the type/display/representation at the SOURCE's (period, variant) by kicking the
   *      SAME guarded `rederiveSource` pass the editor's period-change uses — so the
   *      catalog-add derive participates in the source's rederiveGen + the per-binding
   *      identity re-check, and a later period change (or a remove) correctly SUPERSEDES
   *      a stale in-flight add-derive instead of clobbering. A found source's period may
   *      differ from the page's, so resolving at the source's own period (what
   *      rederiveSource does) is the correct behavior, not trusting the page's
   *      representation blindly. The synchronous return reports the add outcome; the type
   *      fills in once the (guarded) resolve lands.
   */
  addFromCatalog(
    payload: CatalogAddPayload,
    seed: ProjectSeed,
  ): CatalogAddResult {
    // 1. Pristine store → create the untitled project (same path as New project).
    if (draft == null) {
      projectStore.newProject(seed);
    }
    if (draft == null) {
      // newProject always sets the draft; this guards the type-narrowing only.
      return { status: "already-present", createdSource: false };
    }

    const sources = Array.isArray(draft.sources) ? draft.sources : [];
    let sourceIndex = sources.findIndex(
      (s) =>
        (typeof s.register_variant === "string" ? s.register_variant : "") ===
        payload.registerVariant,
    );
    let createdSource = false;

    // 2. No matching source → create one (period prefilled from the page's resolved
    //    period; unset otherwise — PR B's unresolved marker then guides the user).
    if (sourceIndex < 0) {
      projectStore.addSource();
      sourceIndex =
        (Array.isArray(draft.sources) ? draft.sources : []).length - 1;
      projectStore.updateSource(sourceIndex, {
        register_variant: payload.registerVariant,
        period: periodFromWire(payload.resolvedPeriod),
      });
      createdSource = true;
    }

    // 3. Duplicate guard against the (now-resolved) source.
    const source = draft.sources?.[sourceIndex];
    if (
      source &&
      sourceHasBinding(source, payload.variable, payload.representation)
    ) {
      return { status: "already-present", createdSource };
    }

    // 4. Append the binding and write its variable (+ provisional representation)
    //    SYNCHRONOUSLY — the binding must be non-empty before this call returns so a
    //    rapid second add is caught by the duplicate guard (which keys on variable +
    //    representation).
    projectStore.addBinding(sourceIndex);
    const bindingIndex =
      (Array.isArray(draft.sources?.[sourceIndex]?.bindings)
        ? (draft.sources[sourceIndex].bindings as Binding[])
        : []
      ).length - 1;
    projectStore.updateBinding(sourceIndex, bindingIndex, {
      variable: payload.variable,
      representation: payload.representation,
    });
    // Stamp a provisional `unresolved` marker so the row shows an honest cue (not a
    // bare type:"" with nothing) until the guarded derive lands — or, if a structural
    // shift drops that derive before it replaces this, this marker is what the user
    // sees (never a silent type:""). The reason reflects the source's period: unset →
    // "set the period"; set → "resolving / no covering state" (no-states), which the
    // landing derive corrects to the real status.
    const provSource = draft.sources?.[sourceIndex];
    const provPeriod = provSource
      ? periodToWire(provSource.period as Period)
      : null;
    setDerivation(sourceIndex, bindingIndex, {
      status: "unresolved",
      reason: provPeriod ? "no-states" : "period-unset",
      variant: provSource ? variantSeg(registerVariantOf(provSource)) : "",
      period: provPeriod,
      // The provisionally-written representation is OURS (the add wrote it, not the
      // user), so record it as the derived baseline — the guarded derive then clears
      // it when the source's period resolves single-rep (the collapse case) instead
      // of treating it as a user-owned value to preserve (review MAJOR 2).
      derivedRepresentation: payload.representation,
      mismatch: null,
    });
    // Derive through the SAME guarded pass the editor's period-change uses: it bumps
    // the source's rederiveGen and re-checks identity + gen before any write, so a
    // later period change (or a remove) supersedes this in-flight add-derive instead
    // of letting a stale resolution clobber the binding (review MAJOR 1).
    void rederiveSource(sourceIndex);
    return { status: "added", createdSource };
  },
};

// ── Re-derivation engine (B2) ─────────────────────────────────────────────────

/** The source's `register_variant` as a string (coerce non-string to ""). */
function registerVariantOf(source: Source): string {
  return typeof source.register_variant === "string"
    ? source.register_variant
    : "";
}

/** Whether two source revisions differ in a RESOLUTION input (period or the
 * variant seg of register_variant). The register PREFIX change alone doesn't
 * matter for re-derivation here (a prefix change means a different register
 * entirely — the bindings' FQIDs would be wrong, which the validator flags), but
 * the variant seg and the period both feed `resolveBindingAt`. */
function resolutionInputsChanged(before: Source, after: Source): boolean {
  const pBefore = periodToWire(before.period as Period);
  const pAfter = periodToWire(after.period as Period);
  const vBefore = variantSeg(registerVariantOf(before));
  const vAfter = variantSeg(registerVariantOf(after));
  return pBefore !== pAfter || vBefore !== vAfter;
}

/** Write one binding's derivation slot (allocates the source row if absent so a
 * provenance write never throws on a not-yet-mirrored slot). */
function setDerivation(
  sourceIndex: number,
  bindingIndex: number,
  value: BindingDerivation | null,
): void {
  bindingDerivations = bindingDerivations.map((row, i) => {
    if (i !== sourceIndex) {
      return row;
    }
    const next = [...row];
    next[bindingIndex] = value;
    return next;
  });
}

/**
 * Re-resolve EVERY binding of the source at `sourceIndex` against its current
 * (period, variant), updating only the fields the user hasn't hand-edited (B2).
 * Serialized per source by a generation counter (keyed on the source's stable id):
 * a newer call bumps the generation, so an in-flight pass discards its writes once
 * superseded — rapid period typing can't land a stale response over a fresh one.
 */
async function rederiveSource(sourceIndex: number): Promise<void> {
  const sourceId = projectStore.sourceId(sourceIndex);
  const gen = (rederiveGen.get(sourceId) ?? 0) + 1;
  rederiveGen.set(sourceId, gen);

  const source = draft?.sources?.[sourceIndex];
  if (!source || !Array.isArray(source.bindings)) {
    return;
  }
  // The bindings carry full 3-seg variable FQIDs (registerPrefix-scoped at pick
  // time), so the resolve takes the bare variable + the source's (period, variant).
  const variant = variantSeg(registerVariantOf(source));
  const period = periodToWire(source.period as Period);

  // Snapshot the binding list (fqid + index) up front; resolve each in parallel.
  const targets = source.bindings.map((b, j) => ({
    bindingIndex: j,
    variable: typeof b.variable === "string" ? b.variable : "",
  }));

  await Promise.all(
    targets.map(async ({ bindingIndex, variable }) => {
      // A binding with no variable picked yet has nothing to re-derive; leave it
      // markerless.
      if (!variable) {
        return;
      }
      // Capture the binding's stable id alongside the source id: applyResolution
      // re-verifies BOTH (the source still sits at this index with this id, and the
      // binding still sits at this index with this fqid) before any write, so a
      // structural shift that slipped past the gen guard can't mis-attribute.
      const bindingId = projectStore.bindingId(sourceIndex, bindingIndex);
      let result: BindingResolution;
      try {
        // Resolve through the SAME path the picker uses. The fqid is the bare
        // 3-seg variable (registerPrefix-scoped at pick time); resolveBindingAt
        // takes (period, variant).
        result = await resolveBindingAt(variable, period, variant);
      } catch (e) {
        if (rederiveGen.get(sourceId) !== gen) {
          return; // superseded — drop this stale response
        }
        applyResolution(
          { sourceIndex, bindingIndex, sourceId, bindingId, variable },
          period,
          variant,
          { kind: "error", detail: e instanceof Error ? e.message : String(e) },
        );
        return;
      }
      // Stale-response guard: a newer re-derive (or a draft replacement) started
      // while this fetch was in flight → discard.
      if (rederiveGen.get(sourceId) !== gen) {
        return;
      }
      applyResolution(
        { sourceIndex, bindingIndex, sourceId, bindingId, variable },
        period,
        variant,
        result,
      );
    }),
  );
}

/** The captured identity of a re-derive target, threaded from dispatch to
 * applyResolution so the write can be DROPPED if the layout shifted underneath an
 * in-flight resolve (a remove during the fetch window). */
interface RederiveTarget {
  sourceIndex: number;
  bindingIndex: number;
  /** The source's stable client id at dispatch time. */
  sourceId: string;
  /** The binding's stable client id at dispatch time. */
  bindingId: string;
  /** The binding's variable FQID at dispatch time. */
  variable: string;
}

/** Apply one binding's re-resolution to the draft + provenance, honoring the
 * clobber-vs-keep rule (B2.2): a field still equal to the last-derived value is
 * updated to the freshly-derived one; a user-diverged field is KEPT and a
 * non-blocking mismatch hint is recorded. `registerPrefix-scoped` `registerError`
 * is the `error` pseudo-result. */
function applyResolution(
  target: RederiveTarget,
  period: string | null,
  variant: string,
  result: BindingResolution | { kind: "error"; detail: string },
): void {
  if (draft == null) {
    return;
  }
  const { sourceIndex, bindingIndex, sourceId, bindingId, variable } = target;
  // IDENTITY RE-CHECK (the second line of defence behind the gen guard): a remove
  // during the resolve's fetch window can shift the source/binding that now sits at
  // (sourceIndex, bindingIndex). Verify the captured stable ids STILL map to these
  // indices AND the binding still carries the same variable FQID — otherwise this
  // resolution belongs to a binding that moved or was deleted; drop it silently so
  // we never clobber a neighbour or mis-attribute a marker.
  if (
    projectStore.sourceId(sourceIndex) !== sourceId ||
    projectStore.bindingId(sourceIndex, bindingIndex) !== bindingId
  ) {
    return;
  }
  const binding = draft.sources?.[sourceIndex]?.bindings?.[bindingIndex] as
    | Binding
    | undefined;
  if (!binding) {
    return;
  }
  // The binding must still hold the variable we resolved (a re-pick during the
  // fetch could have swapped it under the same stable id).
  if (
    (typeof binding.variable === "string" ? binding.variable : "") !== variable
  ) {
    return;
  }
  const prev = bindingDerivations[sourceIndex]?.[bindingIndex] ?? null;

  if (result.kind === "error") {
    // Keep all values; just mark the row so the user knows the resolve failed.
    setDerivation(sourceIndex, bindingIndex, {
      ...(prev ?? { variant, period }),
      status: "error",
      detail: result.detail,
      variant,
      period,
    });
    return;
  }

  if (result.kind === "unresolved") {
    // Resolution impossible (period unset / no covering state). Do NOT clobber the
    // existing type — the validator is the authority; the row shows WHY (B2.3).
    setDerivation(sourceIndex, bindingIndex, {
      ...(prev ?? {}),
      status: "unresolved",
      reason: result.reason,
      variant,
      period,
      mismatch: null,
    });
    return;
  }

  if (result.kind === "ambiguous") {
    // The concept resolves to >1 co-existing representation. If the binding ALREADY
    // carries a representation that is one of the co-existing columns, the choice is
    // already made — narrow to that column and derive it (the catalog-add page-pin
    // case AND a re-derive of a binding whose chosen column still co-exists). Only
    // when no chosen column matches do we surface the non-blocking "re-pick" marker
    // (the store can't auto-pick; the picker chooser must — B2.4).
    const chosenRep =
      typeof binding.representation === "string"
        ? binding.representation
        : null;
    const chosenState = chosenRep
      ? result.states.find((s) => s.delivery_column_name === chosenRep)
      : undefined;
    if (chosenState) {
      // Re-run the derived-result branch below for the chosen column.
      applyDerivedResult(
        sourceIndex,
        bindingIndex,
        binding,
        prev,
        variant,
        period,
        {
          kind: "derived",
          type: deriveType(chosenState),
          displayNameDefault: chosenState.delivery_column_name ?? chosenRep,
          representation: chosenRep,
        },
      );
      return;
    }
    setDerivation(sourceIndex, bindingIndex, {
      ...(prev ?? {}),
      status: "ambiguous",
      detail: `${result.states.length} delivery columns now co-exist — re-pick a representation.`,
      variant,
      period,
      mismatch: null,
    });
    return;
  }

  // result.kind === "derived": the clobber-vs-keep core.
  applyDerivedResult(
    sourceIndex,
    bindingIndex,
    binding,
    prev,
    variant,
    period,
    result,
  );
}

/** The clobber-vs-keep core for a single-representation `derived` resolution (B2.2),
 * applied to the binding at (sourceIndex, bindingIndex). Extracted so the ambiguous
 * branch can reuse it when the binding's chosen representation still co-exists (the
 * choice is already made → derive that column rather than re-flagging ambiguous).
 * The CALLER owns the gen + identity re-checks (this runs only after they pass). */
function applyDerivedResult(
  sourceIndex: number,
  bindingIndex: number,
  binding: Binding,
  prev: BindingDerivation | null,
  variant: string,
  period: string | null,
  result: {
    kind: "derived";
    type: string;
    displayNameDefault: string | null;
    representation: string | null;
  },
): void {
  if (draft == null) {
    return;
  }
  const currentType = typeof binding.type === "string" ? binding.type : "";
  const currentDisplay =
    typeof binding.display_name === "string" ? binding.display_name : "";
  const patch: Partial<Binding> = {};

  // TYPE: still ours (== last-derived, or never derived & still the opaque
  // fallback) → update; user-diverged → keep + flag mismatch.
  const lastType = prev?.derivedType;
  const typeIsOurs =
    lastType === undefined
      ? currentType === "opaque" || currentType === ""
      : currentType === lastType;
  let typeMismatch: BindingDerivation["mismatch"] = null;
  if (typeIsOurs) {
    if (currentType !== result.type) {
      patch.type = result.type;
    }
  } else if (currentType !== result.type) {
    typeMismatch = { field: "type", derived: result.type };
  }

  // DISPLAY NAME: the derived default only ever auto-fills a blank/own name; a
  // user-set name is never clobbered. A diverged name whose derived default
  // changed is flagged.
  const lastDisplay = prev?.derivedDisplayName ?? null;
  const displayIsOurs =
    lastDisplay === null
      ? currentDisplay === ""
      : currentDisplay === lastDisplay;
  let displayMismatch: BindingDerivation["mismatch"] = null;
  if (result.displayNameDefault != null) {
    if (displayIsOurs) {
      if (currentDisplay !== result.displayNameDefault) {
        patch.display_name = result.displayNameDefault;
      }
    } else if (currentDisplay !== result.displayNameDefault) {
      displayMismatch = {
        field: "display_name",
        derived: result.displayNameDefault,
      };
    }
  }

  // REPRESENTATION: a single-rep derive clears any stale representation we set.
  // (Multi-rep is the `ambiguous` branch above.) Only touch it when WE own it
  // (it equals the last-derived representation), never a user-typed value.
  const lastRep = prev?.derivedRepresentation ?? null;
  const currentRep =
    typeof binding.representation === "string" ? binding.representation : null;
  if (currentRep === lastRep && currentRep !== result.representation) {
    patch.representation = result.representation;
  }

  if (Object.keys(patch).length > 0) {
    setDraft(updateBinding(draft, sourceIndex, bindingIndex, patch));
  }

  // The new last-derived baseline. For an OURS field we just wrote the derived
  // value, so the baseline is the derived value; for a diverged (mismatch) field
  // the baseline stays the derived value too (so a later edit back to it clears
  // the mismatch). The user's value remains in the draft.
  setDerivation(sourceIndex, bindingIndex, {
    status: "derived",
    variant,
    period,
    derivedType: result.type,
    derivedDisplayName: result.displayNameDefault,
    derivedRepresentation: result.representation,
    mismatch: typeMismatch ?? displayMismatch,
  });
}

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
      // Atomic replacement: compute the mirror before assigning `draft` so a throw
      // can't leave a restored draft with a stale/empty mirror inside this `.then()`.
      const ids = buildIds(restored);
      draft = restored;
      sourceIds = ids;
      bindingDerivations = buildDerivations(restored);
      rederiveGen.clear();
      // Do NOT reset lastDownloaded here: a restored autosave draft has NOT been
      // downloaded to the durable project_data.json this session, so it must read
      // as DIRTY (unsaved-changes warning). lastDownloaded stays null →
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
