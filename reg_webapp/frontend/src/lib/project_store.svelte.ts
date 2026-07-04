/**
 * The project-authoring store — a MODULE-SINGLETON Svelte 5 rune store (`.svelte.ts`
 * so the compiler processes the runes). One draft per SPA session; the home/new
 * screen is `draft == null`.
 *
 * This file is the A5.4 SEAM. Three deliberately-real-but-minimal mechanisms are
 * wired here so A5.4 is a drop-in extension, never a refactor:
 *
 *  1. `checkVersionGate` — the version-acceptance function. ACCEPTS Model A
 *     projects by `schema_version` major 2, HARD-rejects schema major 1
 *     (pre-Model-A → a blocking `{ok:false, reason}`, A5.4), and is a NEUTRAL
 *     no-op (`{ok:true}`) for everything else.
 *  2. `ProjectPersistence` — the autosave interface. c-i ships an in-memory Map
 *     stub (`InMemoryPersistence`) with a DEBOUNCED autosave `$effect` over the
 *     draft + a load-at-init (returns null → no restore). A5.4 swaps the impl for
 *     IndexedDB via `setPersistence`; the `storeSchemaVersion` constant gates a
 *     stored-schema mismatch (an A5.4 reject the in-memory stub never trips).
 *  3. `openError` — the blocking open-error channel. c-i sets it on a parse failure
 *     or a schema-version gate failure.
 *
 * NOT a structural validator — the backend is canonical (see reg_webapp/DESIGN.md
 * → Pydantic boundary). The store only
 * constructs / opens / immutably edits / serializes the draft and drives the
 * write endpoints via `lib/api.ts`.
 */

import {
  downloadOrderCsv,
  errMessage,
  type ProjectDataBody,
  triggerDownload,
  type ValidationResultModel,
  validateProject,
} from "./api";
import {
  type BindingResolution,
  bindingFieldsFromResolution,
  resolveBindingAt,
  variantSeg,
} from "./catalog";
import { periodCoverageUnion, periodFromWire, periodToWire } from "./period";
import {
  type Binding,
  defaultSourceName,
  newProjectData,
  type Period,
  type ProjectData,
  type ProjectSeed,
  removeBinding,
  removeSource,
  type Source,
  serializeProjectData,
  uniqueSourceName,
  updateField,
  updateSource,
} from "./project_data";
import { windowStore } from "./window.svelte";

/** The autosave store's OWN schema version (distinct from `project_data`'s
 * `schema_version`). Stamped alongside each persisted draft; the IndexedDB impl
 * hard-rejects a stored draft whose `storeSchemaVersion` differs (see
 * reg_webapp/DESIGN.md → Browser storage + project-file persistence (the SPA
 * store)). Bumped only when the persisted shape changes. */
export const storeSchemaVersion = 1;

/** The debounce window for the autosave `$effect` (~500ms). */
const AUTOSAVE_DEBOUNCE_MS = 500;

/** The debounce window for automatic backend validation after a draft edit. */
const AUTO_VALIDATE_DEBOUNCE_MS = 300;

// ── Version gate (THE A5.4 SEAM) ────────────────────────────────────────────

/** The result of `checkVersionGate`. `ok:true` = load is allowed (the accept path,
 * live in c-i); `ok:false` carries a human `reason` for the blocking open-error
 * banner (A5.4 adds the branches that return this). */
export interface VersionGateResult {
  ok: boolean;
  reason?: string;
}

/** Pull the integer MAJOR out of a dotted version string (`"2.0.0"` → 2), or
 * `null` when it has no leading integer. */
function majorOf(version: string): number | null {
  const match = /^(\d+)\./.exec(version);
  return match ? Number(match[1]) : null;
}

/**
 * THE A5.4 SEAM. Decide whether an opened project_data dict is loadable by version.
 *
 * ACCEPTS Model A projects by `schema_version` major 2. HARD-rejects schema major
 * 1 as pre-Model-A — no migration, pre-v1 policy. The reg_meta package may still
 * be `reg_meta/v0.x.y` while the schema is Model A, so reg_meta major is not a
 * pre-Model-A signal. Anything else is a NEUTRAL no-op (`{ok:true}`): it lets
 * unrecognized versions through so the backend remains the canonical authority.
 */
export function checkVersionGate(parsed: ProjectDataBody): VersionGateResult {
  const schemaVersion =
    typeof parsed.schema_version === "string" ? parsed.schema_version : "";

  const schemaMajor = majorOf(schemaVersion);

  // schema_version 1.x hard-reject: pre-Model-A files. No migration — pre-v1 policy.
  if (schemaMajor === 1) {
    return {
      ok: false,
      reason:
        "This project predates Model A (v1.0). Please re-author against the current schema.",
    };
  }

  // Accept the Model A schema range explicitly (the documented happy path).
  if (schemaMajor === 2) {
    return { ok: true };
  }

  // Neutral no-op for everything else in c-i: do not block. The backend is the
  // canonical validator; the SPA's version gate only HARD-rejects schema 1.x (A5.4).
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
// surviving instance to a shifted item, landing stale per-instance UI — a card's
// expanded/error state seeded from the wrong item — on the WRONG item; see issue
// #200).
//
// Stable ids must NEVER enter the serialized draft: `Source`/`Binding` are closed
// objects (`extra="forbid"` in reg_schema/structural.py) — an injected `_uid` would
// trip `unexpected_field` AND leak into the downloaded project_data.json. So the
// store owns a PARALLEL id tree, kept in lockstep with every mutator, that the draft
// (and thus every serialize / validate / order POST, all of which
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

/** True while a `/validate` POST is in flight. */
let validationBusy = $state(false);

/** True while an automatic validation debounce is waiting to fire. */
let validationScheduled = $state(false);

/** True while an order CSV download POST is in flight. */
let orderBusy = $state(false);

/** Whether a validation request is currently running; non-reactive because the
 * reactive status is `validationBusy`. */
let validationInFlight = false;

/** A draft change or explicit validate call happened while a validation request was
 * already in flight; run one trailing validation once the current request settles. */
let validationQueued = false;

/** Monotonic draft generation used to reject stale validation responses even when a
 * future edit reuses the same object shape. */
let validationGeneration = 0;

/** The dirty flag: the draft has diverged from the last download. */
const dirty = $derived(
  draft != null && serializeProjectData(draft) !== lastDownloaded,
);

/** A clean draft that has VALIDATED ok — the gate for the order CSV download.
 * Re-validation is required after any edit (an edit clears `validation`). */
const validatedClean = $derived(validation?.ok === true);

export type ValidationStatus =
  | "unchecked"
  | "checking"
  | "ok"
  | "warnings"
  | "errors";

const validationStatus = $derived.by<ValidationStatus>(() => {
  if (validationScheduled || validationBusy) {
    return "checking";
  }
  if (validation == null) {
    return "unchecked";
  }
  if (!validation.ok) {
    return "errors";
  }
  return validation.issues.length > 0 ? "warnings" : "ok";
});

const canDownloadOrder = $derived(
  validatedClean && !validationScheduled && !validationBusy && !orderBusy,
);

/** Replace the draft, clearing the stale validation (an edit invalidates the last
 * `/validate` result). The mutators below all funnel through here so `dirty` and
 * `validatedClean` recompute on every edit. */
function setDraft(next: ProjectData): void {
  draft = next;
  validationGeneration += 1;
  validation = null;
}

// ── Catalog → project handoff (C1 — UI audit finding 2) ──────────────────────
//
// The catalog variable page hands a resolved variable-state to the project store
// WITHOUT importing any editor component: `addFromCatalog` is the single store-level
// entry point that (find-or-create source by variant) → (duplicate guard) → (resolve
// ONCE at the SOURCE's period through `resolveBindingAt`) → (append binding with the
// FINAL fields). Catalog routes are reachable from the module-singleton store, so this
// keeps the browse→author handoff a pure store API. Under the #991 data-order model
// every field is written ONCE at pick time — there is no client-side re-derivation
// engine; drift between a source's period and its bindings is the server validator's
// job (see reg_webapp/DESIGN.md → Pydantic boundary).

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
   * (the picker row pins a single column), else null. */
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
  /** The name of the source the binding landed in (or was already in) — the #312
   * prefill on a created source, the existing name otherwise. Drives the page's
   * "added as N sources: …" confirmation (#306 variant-segment split). */
  sourceName: string;
}

/** Whether a binding of `variable` carrying `bRep` matches `wantRep` under the
 * CONCEPT-level rule (review MAJOR 2): a null on EITHER side means "the only column"
 * and matches any value; two non-null representations compare the exact delivery
 * column. The single match leaf shared by the duplicate guard (`sourceHasBinding`)
 * AND `applyStagedDiff`'s removes — so the two never drift on the null-either-side
 * semantics. `bVariable` must equal `variable` for a match.
 *
 * simplify: concept-level null-either-side dedup is NOT period-scoped — a genuinely
 * co-existing column of a variable already stored with `representation: null` can be
 * dropped when the source spans multiple periods (the null-as-only-column shortcut
 * isn't scoped to the resolved period). Correct handling needs resolution-aware
 * coexistence (the staged-add semantics owned by #995/#838); do NOT naively flip this
 * rule — it would break the single-rep page-pin dedup (a page pinned R against a
 * different single-rep period, documented in `sourceHasBinding`). */
function bindingMatches(
  b: Binding,
  variable: string,
  wantRep: string | null,
): boolean {
  if ((typeof b.variable === "string" ? b.variable : "") !== variable) {
    return false;
  }
  const bRep = typeof b.representation === "string" ? b.representation : null;
  // null on EITHER side means "the only column" — a match regardless of the other
  // side's value; both non-null compares the exact delivery column.
  return wantRep == null || bRep == null || bRep === wantRep;
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
  return bindings.some((b) => bindingMatches(b, variable, representation));
}

/** The `register_variant` of a source as a string (coerce non-string to ""). */
function registerVariantOf(source: Source): string {
  return typeof source.register_variant === "string"
    ? source.register_variant
    : "";
}

// ── Staged-diff commit path (#992 → the #995 consumer's ONE atomic mutation) ──
//
// The browse-and-stage flow accumulates a user's picks/removes as a diff, then
// commits them in ONE synchronous store mutation (`applyStagedDiff`) so autosave
// + the stable-id mirror fire/rebuild a single time. Find-or-create keys on
// `register_variant` ALONE (a new disjoint window EXTENDS the source's period to
// the #307 list form rather than minting a second source).

/** A binding to add (already-resolved final fields — the #991 write-once model). */
export interface StagedBinding {
  variable: string;
  type: string;
  display_name?: string | null;
  representation?: string | null;
}

/** Add a binding to a source found-or-created by `registerVariant`, extending the
 * found source's period to cover `period`. */
export interface StagedAdd {
  registerVariant: string;
  period: Period;
  binding: StagedBinding;
}

/** Drop a binding of `variable` from the source at `registerVariant`. A null
 * `representation` matches the variable's binding regardless of stored column
 * (the same null-either-side rule as the duplicate guard, via `bindingMatches`). */
export interface StagedRemove {
  registerVariant: string;
  variable: string;
  representation?: string | null;
}

/** Replace a source's period wholesale. */
export interface StagedPeriodChange {
  registerVariant: string;
  period: Period;
}

/** One atomic batch of staged edits (the #995 consumer's commit payload). Applied
 * in order: removes → adds → period changes. */
export interface StagedDiff {
  adds?: StagedAdd[];
  removes?: StagedRemove[];
  periodChange?: StagedPeriodChange[];
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
    return validationBusy || orderBusy;
  },
  get validationBusy() {
    return validationBusy;
  },
  get orderBusy() {
    return orderBusy;
  },
  get validatedClean() {
    return validatedClean;
  },
  get canDownloadOrder() {
    return canDownloadOrder;
  },
  get validationStatus() {
    return validationStatus;
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
    // #629 item 3: carry the browse-time window into a project created FROM
    // BROWSING (i.e. no draft was open). A window set while browsing lives on the
    // no-draft localStorage fallback; reading it through `windowStore.fallback`
    // (the single read path — no direct localStorage reach) seeds `draft.window`
    // so it isn't silently dropped on create. Guard on `draft === null`: "New"
    // from WITHIN an active project must NOT inherit the fallback — that fallback
    // is the STALE browse-time value (active-draft window writes/clears don't
    // touch it), so seeding it would silently hand the fresh project an old
    // window. From within a draft the new project starts windowless (full
    // history) unless the user sets one. Open/restore keep their OWN window (they
    // bypass newProject). A `null` fallback leaves the key absent.
    const fromBrowsing = draft === null;
    const seedWindow = windowStore.fallback;
    if (fromBrowsing && seedWindow !== null) {
      next.window = seedWindow;
    }
    // Atomic replacement (compute the mirror before mutating store state) — the
    // skeleton is always well-formed here, but this matches openFromFile/restore.
    const ids = buildIds(next);
    draft = next;
    validationGeneration += 1;
    sourceIds = ids;
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
    // (stale validatedClean keeps the order download enabled). buildIds is
    // guarded never to throw, but the atomic order is the durable invariant.
    const opened = obj as ProjectData;
    const ids = buildIds(opened);
    draft = opened;
    validationGeneration += 1;
    sourceIds = ids;
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
    if (validationInFlight) {
      validationQueued = true;
      return null;
    }

    let latestResult: ValidationResultModel | null = null;
    validationInFlight = true;
    validationBusy = true;
    try {
      do {
        validationQueued = false;
        // Snapshot the draft reference + generation. Mutators / new / open all
        // REPLACE the whole draft object and bump the generation, so a mid-flight
        // edit makes this response stale. Discard it rather than writing
        // validation for a superseded draft — otherwise stale green results would
        // re-enable the order CSV download.
        const target: ProjectData | null = draft;
        const targetGeneration = validationGeneration;
        if (target == null) {
          return latestResult;
        }
        requestError = null;
        try {
          const result = await validateProject(target as ProjectDataBody);
          if (draft !== target || validationGeneration !== targetGeneration) {
            latestResult = result;
            continue;
          }
          validation = result;
          latestResult = result;
        } catch (e) {
          if (draft === target && validationGeneration === targetGeneration) {
            validation = null;
            requestError = errMessage(e);
          }
          latestResult = null;
        }
      } while (validationQueued && draft != null);
      return latestResult;
    } finally {
      validationInFlight = false;
      validationBusy = false;
    }
  },

  /** Download the order-export CSV (`/project/order`). A structurally invalid spec
   * is the backend's 422 → `requestError`. */
  async downloadOrder(): Promise<void> {
    if (draft == null) {
      return;
    }
    orderBusy = true;
    requestError = null;
    try {
      await downloadOrderCsv(draft as ProjectDataBody);
    } catch (e) {
      requestError = errMessage(e);
    } finally {
      orderBusy = false;
    }
  },

  // ── Mutators (replace the draft so `dirty` recomputes) ────────────────────
  // Guarded against a null draft so a stray call on the home screen is a no-op.
  // STRUCTURAL edits to `sources`/`bindings` MUST keep the stable-id mirror
  // (`sourceIds`) in lockstep (issue #200) — a bypass desyncs it and resurrects the
  // wrong-instance bug class. `updateField` rebuilds the mirror when handed
  // `sources`; `removeSource`/`removeBinding` patch it positionally; `applyStagedDiff`
  // rebuilds it once for the whole batch.

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
      }
    }
  },
  removeSource(index: number): void {
    if (draft != null) {
      setDraft(removeSource(draft, index));
      sourceIds = sourceIds.filter((_, i) => i !== index);
    }
  },
  removeBinding(sourceIndex: number, bindingIndex: number): void {
    if (draft != null) {
      setDraft(removeBinding(draft, sourceIndex, bindingIndex));
      sourceIds = sourceIds.map((s, i) =>
        i === sourceIndex
          ? { ...s, bindings: s.bindings.filter((_, j) => j !== bindingIndex) }
          : s,
      );
    }
  },

  /**
   * Apply a staged batch of edits in ONE synchronous mutation (#992 → the #995
   * consumer's commit path). Under the #991 data-order model the browse-and-stage
   * flow accumulates the user's picks/removes/period-changes as a diff, then commits
   * them here so autosave + the stable-id mirror fire/rebuild a SINGLE time. A null
   * draft is a no-op (the home screen). The batch is applied in order:
   *   (a) removes  — drop matching bindings (`bindingMatches` null-either-side rule);
   *   (b) adds     — find-or-create the source by `register_variant` ALONE, extend the
   *                  source period to cover the add's period, append the binding
   *                  unless the duplicate guard says it is already present;
   *   (c) periodChanges — replace the matching source's `period` wholesale;
   *   (d) prune    — drop a source only when this batch removed from it AND it is now
   *                  empty. Deferred to LAST (not folded into removes) so a remove+add
   *                  of the SAME register_variant in one batch preserves the source's
   *                  name/period/metadata instead of minting a fresh source.
   * The next draft + rebuilt id mirror are computed BEFORE assigning either, so the
   * replacement is atomic (like open/new) and `dirty`/autosave recompute once.
   */
  applyStagedDiff(diff: StagedDiff): void {
    if (draft == null) {
      return;
    }
    let sources: Source[] = Array.isArray(draft.sources) ? draft.sources : [];

    // (a) removes → drop matching bindings, but do NOT prune emptied sources yet.
    //     A single batch may remove a source's last binding AND re-add one for the
    //     SAME register_variant (e.g. swapping a representation). Pruning inside this
    //     loop would drop the source before (b) runs, so the add would mint a FRESH
    //     newSource — losing the source's user-set name / period / panel-referenced
    //     metadata. Defer the prune to a final scoped step (below) instead. Track the
    //     removed register_variants so the prune only touches sources this batch
    //     actually removed from (an adds-only batch never prunes a pre-existing empty
    //     source; a removed-then-readded source is preserved by its non-zero count).
    const removedVariants = new Set<string>();
    for (const remove of diff.removes ?? []) {
      removedVariants.add(remove.registerVariant);
      sources = sources.map((s) =>
        registerVariantOf(s) === remove.registerVariant
          ? {
              ...s,
              bindings: (Array.isArray(s.bindings) ? s.bindings : []).filter(
                (b) =>
                  !bindingMatches(
                    b,
                    remove.variable,
                    remove.representation ?? null,
                  ),
              ),
            }
          : s,
      );
    }

    // (b) adds → find-or-create by register_variant ALONE + merge period.
    for (const add of diff.adds ?? []) {
      const idx = sources.findIndex(
        (s) => registerVariantOf(s) === add.registerVariant,
      );
      const binding = stagedToBinding(add.binding);
      if (idx < 0) {
        sources = [
          ...sources,
          newSource(add.registerVariant, add.period, sources, [binding]),
        ];
        continue;
      }
      const found = sources[idx];
      const existing = Array.isArray(found.bindings) ? found.bindings : [];
      const isDup = existing.some((b) =>
        bindingMatches(
          b,
          add.binding.variable,
          add.binding.representation ?? null,
        ),
      );
      sources = sources.map((s, i) =>
        i === idx
          ? {
              ...s,
              period: periodCoverageUnion(s.period as Period, add.period),
              bindings: isDup ? existing : [...existing, binding],
            }
          : s,
      );
    }

    // (c) period changes → replace the matching source's period wholesale.
    for (const change of diff.periodChange ?? []) {
      sources = sources.map((s) =>
        registerVariantOf(s) === change.registerVariant
          ? { ...s, period: change.period }
          : s,
      );
    }

    // (d) prune → drop a source ONLY when this batch removed a binding from it AND it
    //     is now empty. Scoping to `removedVariants` means a remove+add of the same
    //     register_variant keeps the source (the add refilled it), while a remove-only
    //     batch that empties a source still prunes it; a pre-existing empty source an
    //     adds-only batch never touched is left alone.
    sources = sources.filter(
      (s) =>
        !removedVariants.has(registerVariantOf(s)) ||
        (Array.isArray(s.bindings) ? s.bindings : []).length > 0,
    );

    // Atomic replacement: compute the next draft + rebuilt mirror BEFORE assigning
    // either (like open/new), so autosave + the id mirror fire/rebuild ONCE.
    const next = { ...draft, sources };
    const ids = buildIds(next);
    setDraft(next);
    sourceIds = ids;
  },

  /**
   * Add a catalog variable-state to the project (C1 — catalog→project handoff).
   * The single store-level entry point the catalog page calls (it imports NO editor
   * component). ASYNC because it resolves the binding's final fields ONCE at pick
   * time (the #991 write-once model — no client re-derivation engine).
   *
   * SERIALIZED at the store (`addChain`): the real body (`realAddFromCatalog`) runs
   * strictly one-at-a-time, so two OVERLAPPING calls (the picker's Add button isn't
   * awaited/disabled — a double-click on a multi-select can re-enter before the first
   * resolve settles) can't interleave. Without serialization, the later call's
   * pre-resolve `setDraft` (create/merge) changes `draft`, which trips the earlier
   * call's `draft !== target` guard → the earlier call returns `already-present` and
   * silently DROPS its binding. Chaining makes a later call's mutations start only
   * after the earlier call has fully committed, so the `draft !== target` guard only
   * ever fires on a genuine open/New replacement (see `realAddFromCatalog`).
   */
  addFromCatalog(
    payload: CatalogAddPayload,
    seed: ProjectSeed,
  ): Promise<CatalogAddResult> {
    // Chain onto the tail so the real body runs after any in-flight add commits. The
    // `.catch` resets the chain so a rejected add can't wedge the queue; the caller
    // still sees the real result/rejection via `run` (a separate promise). Serial
    // find-or-create then sees the prior committed draft (no concurrent `setDraft`).
    const run = addChain.then(() => realAddFromCatalog(payload, seed));
    addChain = run.catch(() => {});
    return run;
  },
};

/** The serialization tail for `addFromCatalog` (module-level so it spans the
 * singleton store). Each call chains its real body onto this so overlapping adds
 * (an un-awaited double-click) run strictly one-at-a-time instead of interleaving
 * their pre-resolve `setDraft` with each other's `draft !== target` guard. */
let addChain: Promise<unknown> = Promise.resolve();

/**
 * The real `addFromCatalog` body (serialized behind `addChain` — see the store
 * method). Steps:
 *   1. Pristine store → create the untitled project from `seed` (same as New).
 *   2. Find-or-create the source by `register_variant` ALONE (#992). On found:
 *      the duplicate guard runs FIRST (step 3) BEFORE any mutation; a non-duplicate
 *      then extends the source period to cover `payload.resolvedPeriod`. On create:
 *      prefill the name (#312) + set the period from the page's resolved period.
 *   3. Duplicate guard (found path only — a fresh source can't hold one): a source
 *      already carrying this fqid (+ representation) is a TRUE no-op →
 *      `already-present` with ZERO mutation (no period merge, no setDraft).
 *   4. Resolve ONCE at the SOURCE's (period, variant) and map the resolution to the
 *      binding's FINAL fields (`bindingFieldsFromResolution`), then append in ONE
 *      mutation.
 * Serialization guarantees no concurrent add changes `draft` mid-flight, so each
 * serial add sees the prior committed draft for find-or-create and the
 * `draft !== target` guard fires only on a genuine open/New replacement.
 */
async function realAddFromCatalog(
  payload: CatalogAddPayload,
  seed: ProjectSeed,
): Promise<CatalogAddResult> {
  // 1. Pristine store → create the untitled project (same path as New project).
  if (draft == null) {
    projectStore.newProject(seed);
  }
  if (draft == null) {
    // newProject always sets the draft; this guards the type-narrowing only.
    return {
      status: "already-present",
      createdSource: false,
      sourceName: "",
    };
  }

  // 2. Find-or-create by register_variant ALONE (#992). A disjoint window extends
  //    the found source's period; a fresh source is prefilled from the page period.
  const sources = Array.isArray(draft.sources) ? draft.sources : [];
  const incomingPeriod = periodFromWire(payload.resolvedPeriod);

  /** The landed source's name (read AFTER the create path's #312 prefill). */
  const nameAt = (index: number): string => {
    const name = draft?.sources?.[index]?.name;
    return typeof name === "string" ? name : "";
  };

  let sourceIndex = sources.findIndex(
    (s) => registerVariantOf(s) === payload.registerVariant,
  );
  let createdSource = false;
  if (sourceIndex < 0) {
    const created = newSource(
      payload.registerVariant,
      incomingPeriod,
      sources,
      [],
    );
    const next = { ...draft, sources: [...sources, created] };
    sourceIndex = sources.length;
    const ids = buildIds(next);
    setDraft(next);
    sourceIds = ids;
    createdSource = true;
  } else {
    // 3. Found: run the duplicate guard FIRST, against the found source, BEFORE
    //    any period merge or setDraft. `already-present` documents a NO-OP, so a
    //    duplicate must not widen the source's period (would pull in extra years)
    //    nor clear `validation` (setDraft would flip `validatedClean` off and
    //    disable the order download) — the UI would report "already there" while
    //    silently mutating. Return with ZERO mutation. A fresh source (create
    //    path) can't hold a duplicate, so the guard only matters here.
    const found = sources[sourceIndex];
    if (sourceHasBinding(found, payload.variable, payload.representation)) {
      return {
        status: "already-present",
        createdSource: false,
        sourceName: nameAt(sourceIndex),
      };
    }
    // Not a duplicate: extend its period to cover the add's window (#992 merge).
    const merged = periodCoverageUnion(found.period as Period, incomingPeriod);
    setDraft(updateSource(draft, sourceIndex, { period: merged }));
  }

  // 4. Resolve ONCE at the SOURCE's (period, variant) and write the FINAL fields.
  //    A found source's period may differ from the page's, so resolving at the
  //    source's own period is correct (not trusting the page's representation
  //    blindly). No marker/engine — the validator flags any residual drift.
  const resolveSource = draft.sources?.[sourceIndex];
  const period = resolveSource
    ? periodToWire(resolveSource.period as Period)
    : null;
  const variant = resolveSource
    ? variantSeg(registerVariantOf(resolveSource))
    : "";
  // Capture the draft reference BEFORE the awaited resolve. new/open/restore all
  // REPLACE the whole `draft` object, so if the user opens a different project or
  // starts a New one DURING this fetch, `draft !== target` afterwards. Appending
  // then (by re-reading `draft` + re-finding by register_variant) would land the
  // binding in the WRONG (newly-opened) project that happens to share the variant.
  // Discard the stale add — a no-op — instead. (The retired re-derivation engine
  // had a generation guard for exactly this; this restores it for the single-pick
  // path. `validate()`'s stale-response guard is the same idea for its POST.)
  const target = draft;
  let resolution: BindingResolution;
  try {
    resolution = await resolveBindingAt(payload.variable, period, variant);
  } catch {
    // A network/422 leaves the binding unresolved (the validator is the
    // authority); the add still lands so the pick isn't silently dropped.
    if (draft !== target) {
      return {
        status: "already-present",
        createdSource: false,
        sourceName: "",
      };
    }
    resolution = { kind: "unresolved", reason: "no-states" };
  }
  if (draft !== target) {
    // The draft was replaced mid-resolve — discard this add entirely.
    return {
      status: "already-present",
      createdSource: false,
      sourceName: "",
    };
  }
  const fields = bindingFieldsFromResolution(
    payload.variable,
    resolution,
    payload.representation,
  );

  // Re-find the source by variant before appending: the await window could have
  // let a concurrent add shift indices (callers await sequentially, but this stays
  // robust). Append the resolved binding in ONE mutation + rebuild the mirror once.
  const landing = Array.isArray(draft.sources) ? draft.sources : [];
  const landIndex = landing.findIndex(
    (s) => registerVariantOf(s) === payload.registerVariant,
  );
  if (landIndex < 0) {
    return { status: "already-present", createdSource, sourceName: "" };
  }
  const nextSources = landing.map((s, i) =>
    i === landIndex
      ? {
          ...s,
          bindings: [...(Array.isArray(s.bindings) ? s.bindings : []), fields],
        }
      : s,
  );
  const next = { ...draft, sources: nextSources };
  setDraft(next);
  sourceIds = buildIds(next);
  return { status: "added", createdSource, sourceName: nameAt(landIndex) };
}

// ── addFromCatalog / applyStagedDiff helpers ─────────────────────────────────

/** A staged/add binding → the stored `Binding` shape, dropping `undefined` optional
 * fields (`display_name`/`representation`) so an unset field never serializes as a
 * literal `undefined` and the closed-object shape stays clean. */
function stagedToBinding(b: StagedBinding): Binding {
  const binding: Binding = { variable: b.variable, type: b.type };
  if (b.display_name !== undefined) {
    binding.display_name = b.display_name;
  }
  if (b.representation !== undefined) {
    binding.representation = b.representation;
  }
  return binding;
}

/** Build a new `Source` for `registerVariant` at `period`, with the #312 name
 * prefill (the register slug uppercased, uniqueness-suffixed against `siblings`).
 * `siblings` is the current source list the new name must be unique among. */
function newSource(
  registerVariant: string,
  period: Period,
  siblings: Source[],
  bindings: Binding[],
): Source {
  const base = defaultSourceName(registerVariant);
  // uniqueSourceName excludes the source at `excludeIndex`; the new source isn't in
  // `siblings` yet, so an out-of-range index excludes nothing.
  const name = base ? uniqueSourceName(siblings, base, siblings.length) : "";
  return { name, register_variant: registerVariant, period, bindings };
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
      validationGeneration += 1;
      sourceIds = ids;
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

  // Auto-validation: every draft replacement schedules a backend validation of the
  // current serialized draft. The backend stays canonical; this only retires the
  // manual "Validate" click from the cart UI.
  $effect(() => {
    const current = draft;
    if (current == null) {
      validationScheduled = false;
      return;
    }
    validationScheduled = true;
    const timer = setTimeout(() => {
      validationScheduled = false;
      void projectStore.validate();
    }, AUTO_VALIDATE_DEBOUNCE_MS);
    return () => {
      clearTimeout(timer);
      validationScheduled = false;
    };
  });

  return loaded;
}
