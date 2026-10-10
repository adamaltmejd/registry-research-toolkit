/**
 * Pure project_data.json draft helpers (NO runes — unit-tested in isolation;
 * `project_data.test.ts`).
 *
 * The draft is the uploaded or edited JSON as is (`RawDraft`): `/validate` must see
 * a malformed upload unchanged to diagnose it, so string panel members, unknown
 * keys and invalid enums survive until the server reports them. Raw retention is a
 * diagnostic path, not a supported extension mechanism. Reads go through the safe
 * accessors below; writes build values typed with the `Project*` types, which are
 * generated from `crates/reg-core/src/project.rs` (`bun run gen:types`). The
 * accepted model is never written back into the draft.
 *
 * This is NOT a structural validator — the server is canonical. These helpers only
 * construct + immutably edit the document the SPA posts to `/api/project/validate`
 * / `/order`.
 */

import type { components } from "./api-types-rust";

type Schemas = components["schemas"];

/** The draft: a JSON object, otherwise unchecked. */
export type RawDraft = Record<string, unknown>;

/** The canonical project document (`reg_core::project::ProjectData`). */
export type ProjectData = Schemas["ProjectData"];
/** One logical extraction (`Source`). */
export type ProjectSource = Schemas["ProjectSource"];
/** One variable to extract (`Binding`). */
export type ProjectBinding = Schemas["ProjectBinding"];
/** `Source.period`: one segment, or a sorted, disjoint list of them. */
export type ProjectSourcePeriod = Schemas["ProjectSourcePeriod"];
/** One contiguous piece of a source period: a year, a token or a `{from, to}`. */
export type ProjectPeriodSegment = Schemas["ProjectPeriodSegment"];
/** The optional study window, in plain int years (`to >= from`). */
export type ProjectStudyWindow = Schemas["ProjectStudyWindow"];

/** A binding the SPA writes: a `ProjectBinding`, except that an add whose column
 * type did not resolve writes `type: ""`, for the server to report, rather than a
 * valid type nobody resolved (`bindingFieldsFromResolution`). */
export type DraftBinding = Omit<ProjectBinding, "type"> & {
  type: ProjectBinding["type"] | "";
};

/** The Model A `schema_version` a NEW draft is seeded with (reg-core 3.0.0). */
export const MODEL_A_SCHEMA_VERSION = "3.0.0";

/** Seed for a new project (from `/api/context`): the canonical reg_meta release
 * tag (derive it from the deployment's bare package version with
 * `regMetaReleaseTag`) + the deployment's steward id. `name` and `sources`
 * start empty. */
export interface ProjectSeed {
  reg_meta_version: string;
  steward: string;
}

/** Construct a fresh Model A skeleton. The version fields are seeded from
 * the deployment context; `schema_version` 2.x is the Model A gate, while
 * `reg_meta_version` records the catalog release used by this deployment. */
export function newProjectData(seed: ProjectSeed): RawDraft {
  return {
    schema_version: MODEL_A_SCHEMA_VERSION,
    steward: seed.steward,
    reg_meta_version: seed.reg_meta_version,
    name: "",
    sources: [],
  };
}

/**
 * Format the deployment's bare reg_meta PACKAGE version
 * (`context.webapp.reg_meta_version`, e.g. `"1.0.0"`) into the canonical
 * project_data release-tag form (`"reg_meta/v1.0.0"`; see crates/DESIGN.md →
 * Versioning and determinism). Empty in → empty out (the context hasn't
 * resolved yet; the seed is corrected on the next New).
 */
export function regMetaReleaseTag(packageVersion: string): string {
  return packageVersion ? `reg_meta/v${packageVersion}` : "";
}

// ── Type guards ─────────────────────────────────────────────────────────────

/** A STRICT plain-object guard: true only for a non-null, non-array object. The
 * shared form of the "is this a JSON object, not `null` and not an array" check that
 * the store's open-file guard and the editors' malformed-slot fallbacks all need
 * (a verbatim-loaded draft can carry a `null`/array where an object is expected). */
export function isPlainObject(
  value: unknown,
): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** A read-side source slot after the SPA's malformed-slot seam has run. A plain
 * object is safe to inspect field-by-field, but not assumed structurally valid;
 * `null` means the original `sources[]` slot was `null`, an array, or another
 * non-object value. The raw draft is still kept verbatim for serialize/validate. */
export type SafeSource = Record<string, unknown> | null;

export function asSafeSource(slot: unknown): SafeSource {
  return isPlainObject(slot) ? slot : null;
}

/** Read-side view of a draft's `sources`. Non-array `sources` renders as empty;
 * malformed array slots stay counted as `null` so `/sources/{i}` addressing and
 * degraded cards line up with backend validation paths. */
export function safeSourceSlots(sources: unknown): SafeSource[] {
  return Array.isArray(sources) ? sources.map(asSafeSource) : [];
}

/** The draft's study window when it is a usable year pair, else null — an opened
 * spec is held verbatim, so `window` can be absent or malformed. The read side's
 * one coercion of it, done once at the `/project` boundary (beside
 * `safeSourceSlots`) and threaded to both readers: the coverage hints and the
 * source card's deviation marker. */
export function safeStudyWindow(window: unknown): ProjectStudyWindow | null {
  const safe = isPlainObject(window) ? window : null;
  return safe != null &&
    Number.isInteger(safe.from) &&
    Number.isInteger(safe.to)
    ? (safe as unknown as ProjectStudyWindow)
    : null;
}

export function safeSourceName(source: unknown): string {
  const safe = asSafeSource(source);
  return typeof safe?.name === "string" ? safe.name : "";
}

export function safeSourceRegisterVariant(source: unknown): string {
  const safe = asSafeSource(source);
  return typeof safe?.register_variant === "string"
    ? safe.register_variant
    : "";
}

export function safeSourcePeriod(source: unknown): ProjectSourcePeriod | null {
  const safe = asSafeSource(source);
  return safe != null && "period" in safe
    ? (safe.period as ProjectSourcePeriod)
    : null;
}

export function safeSourceBindings(source: unknown): ProjectBinding[] {
  const safe = asSafeSource(source);
  return Array.isArray(safe?.bindings)
    ? (safe.bindings as ProjectBinding[])
    : [];
}

export function sourceBindingsMalformed(source: unknown): boolean {
  const safe = asSafeSource(source);
  return (
    safe != null && safe.bindings !== undefined && !Array.isArray(safe.bindings)
  );
}

/** A stable text image of ONE source slot — the complete stored value, every
 * binding field and every unmapped key included. Two images compare equal exactly
 * when the source has not changed, which is what lets an edited source period
 * refuse to land on a source that moved after the researcher looked at it
 * (`projectStore.applySourcePeriodEdit`). Insertion order is preserved by the
 * immutable mutators here, so a re-spread of an unchanged source images the same. */
export function sourceSnapshot(source: unknown): string {
  return JSON.stringify(source ?? null);
}

// ── Immutable top-level edits ───────────────────────────────────────────────
// Every mutator returns a NEW object (shallow clone + replaced slice) so the
// store can swap the `$state` reference and `dirty` recomputes. Unmapped keys on
// the spread survive (the `...draft` carries known panels and raw invalid keys).

/** Replace a top-level scalar field (`name`, `steward`, `reg_meta_version`, …). */
export function updateField<K extends keyof ProjectData>(
  draft: RawDraft,
  key: K,
  value: ProjectData[K],
): RawDraft {
  return { ...draft, [key]: value };
}

// ── Immutable source edits ──────────────────────────────────────────────────

/** Coerce a draft's `sources` to an array. An opened spec may carry a malformed
 * non-array `sources` (kept verbatim for serialize/validate); these mutators match
 * the editors' coercion doctrine (SourceEditor/ProjectEditor render a non-array as
 * []) so a structural edit on such a draft starts from [] rather than spreading a
 * string into char "sources" or throwing on `.map`/`.filter`. The malformed value is
 * thus REPLACED by a well-formed array on the first structural edit (intentional —
 * the user is fixing the draft via the editor). */
function sourcesArray(draft: RawDraft): unknown[] {
  return Array.isArray(draft.sources) ? draft.sources : [];
}

// ── Source-name prefill (#312) ──────────────────────────────────────────────
// Source names are panel-key join handles (reg-core panels join on source
// name), so a prefill must be unique among the draft's sources. The prefill is
// advisory: it only ever replaces an empty name — never a user-entered name; the
// catalog add's create path (`newSource` in the store) is its single caller.

/** Default source name for a register_variant coordinate: the register slug
 * (segment 2 of `provider/register/variant`) uppercased — `scb/lisa/v1` →
 * `"LISA"` (Swedish register stubs are mostly acronyms). `""` when the
 * coordinate has no register segment yet. */
export function defaultSourceName(registerVariant: string): string {
  const slug = registerVariant.split("/")[1] ?? "";
  return slug.toUpperCase();
}

/** `base` if no OTHER source (case-sensitive, as the schema compares) already
 * uses it, else the first free `base_2`, `base_3`, … The source at
 * `excludeIndex` is ignored (it's the one being named). */
export function uniqueSourceName(
  sources: readonly unknown[],
  base: string,
  excludeIndex: number,
): string {
  const taken = new Set(
    sources.filter((_, i) => i !== excludeIndex).map(safeSourceName),
  );
  if (!taken.has(base)) {
    return base;
  }
  let n = 2;
  while (taken.has(`${base}_${n}`)) {
    n += 1;
  }
  return `${base}_${n}`;
}

/** Remove the source at `index` (no-op if out of range). */
export function removeSource(draft: RawDraft, index: number): RawDraft {
  return {
    ...draft,
    sources: sourcesArray(draft).filter((_, i) => i !== index),
  };
}

// ── Immutable binding edits ─────────────────────────────────────────────────

/** Remove the binding at `bindingIndex` from the source at `sourceIndex`. */
export function removeBinding(
  draft: RawDraft,
  sourceIndex: number,
  bindingIndex: number,
): RawDraft {
  return updateSourceBindings(draft, sourceIndex, (bindings) =>
    bindings.filter((_, i) => i !== bindingIndex),
  );
}

/** Internal: apply a transform to one source's `bindings` immutably. Coerces a
 * non-array `sources` / `bindings` to [] (the editors' doctrine) so a structural
 * binding edit never spreads a string or throws on `.map`. */
function updateSourceBindings(
  draft: RawDraft,
  sourceIndex: number,
  fn: (bindings: ProjectBinding[]) => ProjectBinding[],
): RawDraft {
  return {
    ...draft,
    sources: sourcesArray(draft).map((s, i) => {
      const safe = asSafeSource(s);
      if (i !== sourceIndex || safe == null) {
        return s;
      }
      return { ...safe, bindings: fn(safeSourceBindings(safe)) };
    }),
  };
}

/**
 * Serialize a draft to the wire JSON text the SPA posts / downloads. A stable,
 * pretty (2-space) JSON. Raw invalid keys are carried by `JSON.stringify` so an
 * opened malformed file remains diagnosable and is never silently repaired.
 * NOT key-sorted: insertion order is preserved (a re-serialized file is
 * structurally faithful to what was opened), which is also what the `dirty`
 * baseline compares against.
 */
export function serializeProjectData(draft: RawDraft): string {
  return JSON.stringify(draft, null, 2);
}
