/**
 * Pure project_data.json model helpers (NO runes — unit-tested in isolation;
 * `project_data.test.ts`). The SPA's authoring draft is an OPEN object: the
 * FOCUSED authoring scope (A5.3c) edits the top-level fields + `sources[]`
 * (`register_variant`, `period`, `bindings[]`); `panels[]` and steward-namespaced
 * blocks (`reg_monabundle`, `swecov`, …) ROUND-TRIP VERBATIM — they ride on the
 * dict side and are NEVER stripped on open/save (matching the backend raw-dict
 * embed; see reg_monabundle/DESIGN.md → The two halves and `routes/bundle.py`).
 *
 * This is NOT a structural validator — the backend is canonical (see
 * reg_webapp/DESIGN.md → Pydantic boundary). These
 * helpers only construct + immutably edit the shape the SPA posts to
 * `/api/project/validate` / `/order` / `/bundle`. The field names mirror
 * `reg_schema/src/reg_schema/project_data.py` (see reg_schema/DESIGN.md → Two
 * layers: models vs. validator).
 *
 * There is no codegen'd `ProjectData` schema in `api-types` (the write endpoints
 * declare `additionalProperties: true` open-object request bodies, NOT the pinned
 * model — `/validate` must accept malformed specs to diagnose them). So the draft
 * type is a hand-written OPEN shape that keeps unmapped keys.
 */

/** A binding on a source — one variable to include. Only the fields the
 * A5.3c surface touches are named; any other key (`id_subtype`, `date_format`, …)
 * survives via the index signature. */
export interface Binding {
  variable: string;
  type: string;
  display_name?: string | null;
  value_set?: string | null;
  // The delivery column selecting which REPRESENTATION of the concept to extract
  // (set only when the concept resolves to >1 column at the source period — the
  // chooser fills it; the backend semantic check flags an ambiguous binding that
  // omits it). The job the retired `@version` pin once did, keyed on the column.
  representation?: string | null;
  [key: string]: unknown;
}

/** A `Source.period` value: a bare year, a period-token string, the
 * `"_default"` sentinel, or a `{from, to}` range object. Kept loose — the server
 * is the canonical period validator. */
export type Period =
  | number
  | string
  | { from: number | string; to: number | string };

/** A data source / table. Open: panel-referenced or future keys survive. */
export interface Source {
  name: string;
  register_variant: string;
  period: Period;
  bindings: Binding[];
  [key: string]: unknown;
}

/**
 * The top-level project_data.json draft. OPEN: `panels[]`, `reg_monabundle`
 * and any steward-namespaced block ride through the index signature untouched —
 * the A5.3c surface never edits them, but they must round-trip verbatim.
 */
export interface ProjectData {
  schema_version: string;
  steward: string;
  reg_meta_version: string;
  name: string;
  sources: Source[];
  [key: string]: unknown;
}

/** The Model A `schema_version` a NEW draft is seeded with (reg_schema 2.0.0). */
export const MODEL_A_SCHEMA_VERSION = "2.0.0";

/** The 6 ColumnType values (reg_schema `ColumnType` `Literal`). The
 * BindingEditor's type `<select>` + the type-conditional advanced-field gating key
 * off this. Hand-maintained — `ColumnType` isn't on the OpenAPI surface
 * (project_data isn't a response model), so codegen can't supply it; co-located
 * with the `Binding` type it enumerates. */
export const COLUMN_TYPES = [
  "id",
  "categorical",
  "numeric",
  "date",
  "datetime",
  "opaque",
] as const;

/** Seed for a new project (from `/api/context`): the canonical reg_meta release
 * tag (`reg_meta/v1.x.y` — derive it from the deployment's bare package version
 * with `regMetaReleaseTag`) + the deployment's steward id. `name` and `sources`
 * start empty. */
export interface ProjectSeed {
  reg_meta_version: string;
  steward: string;
}

/** Construct a fresh Model A skeleton. The version fields are seeded from
 * the deployment context so a new draft already carries the accepted version
 * range (`schema_version` 2.x + `reg_meta/v1.x.y`). */
export function newProjectData(seed: ProjectSeed): ProjectData {
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
 * project_data release-tag form (`"reg_meta/v1.0.0"`; see reg_meta/DESIGN.md →
 * Release tags and distribution) — Model A files require a v1.x reg_meta release
 * tag. The inverse of the
 * version gate's `regMetaDotted` strip. Empty in → empty out (the context
 * hasn't resolved yet; the seed is corrected on the next New).
 */
export function regMetaReleaseTag(packageVersion: string): string {
  return packageVersion ? `reg_meta/v${packageVersion}` : "";
}

// ── Immutable top-level edits ───────────────────────────────────────────────
// Every mutator returns a NEW object (shallow clone + replaced slice) so the
// store can swap the `$state` reference and `dirty` recomputes. Unmapped keys on
// the spread survive (the `...draft` carries `panels`/`reg_monabundle`/…).

/** Replace a top-level scalar field (`name`, `steward`, `reg_meta_version`, …). */
export function updateField<K extends keyof ProjectData>(
  draft: ProjectData,
  key: K,
  value: ProjectData[K],
): ProjectData {
  return { ...draft, [key]: value };
}

// ── Immutable source edits ──────────────────────────────────────────────────

/** Append an empty source skeleton. */
export function addSource(draft: ProjectData): ProjectData {
  const source: Source = {
    name: "",
    register_variant: "",
    period: "",
    bindings: [],
  };
  return { ...draft, sources: [...draft.sources, source] };
}

/** Remove the source at `index` (no-op if out of range). */
export function removeSource(draft: ProjectData, index: number): ProjectData {
  return { ...draft, sources: draft.sources.filter((_, i) => i !== index) };
}

/** Patch the source at `index` with `patch` (shallow merge — preserves the
 * source's unmapped keys + its `bindings`). */
export function updateSource(
  draft: ProjectData,
  index: number,
  patch: Partial<Source>,
): ProjectData {
  return {
    ...draft,
    sources: draft.sources.map((s, i) =>
      i === index ? { ...s, ...patch } : s,
    ),
  };
}

// ── Immutable binding edits ─────────────────────────────────────────────────

/** Append an empty binding to the source at `sourceIndex`. */
export function addBinding(
  draft: ProjectData,
  sourceIndex: number,
): ProjectData {
  const binding: Binding = { variable: "", type: "" };
  return updateSourceBindings(draft, sourceIndex, (bindings) => [
    ...bindings,
    binding,
  ]);
}

/** Remove the binding at `bindingIndex` from the source at `sourceIndex`. */
export function removeBinding(
  draft: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
): ProjectData {
  return updateSourceBindings(draft, sourceIndex, (bindings) =>
    bindings.filter((_, i) => i !== bindingIndex),
  );
}

/** Patch the binding at `bindingIndex` (shallow merge — preserves its unmapped
 * keys). */
export function updateBinding(
  draft: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
  patch: Partial<Binding>,
): ProjectData {
  return updateSourceBindings(draft, sourceIndex, (bindings) =>
    bindings.map((b, i) => (i === bindingIndex ? { ...b, ...patch } : b)),
  );
}

/** Internal: apply a transform to one source's `bindings` immutably. */
function updateSourceBindings(
  draft: ProjectData,
  sourceIndex: number,
  fn: (bindings: Binding[]) => Binding[],
): ProjectData {
  return {
    ...draft,
    sources: draft.sources.map((s, i) =>
      i === sourceIndex ? { ...s, bindings: fn(s.bindings) } : s,
    ),
  };
}

/**
 * Serialize a draft to the wire JSON text the SPA posts / downloads. A stable,
 * pretty (2-space) JSON — unmapped keys (`panels`, `reg_monabundle`, …) are
 * carried by `JSON.stringify` over the open object, so steward blocks round-trip
 * verbatim. NOT key-sorted: insertion order is preserved (a re-serialized file is
 * structurally faithful to what was opened), which is also what the `dirty`
 * baseline compares against.
 */
export function serializeProjectData(draft: ProjectData): string {
  return JSON.stringify(draft, null, 2);
}
