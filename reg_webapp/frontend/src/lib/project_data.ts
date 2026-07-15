/**
 * Pure project_data.json model helpers (NO runes — unit-tested in isolation;
 * `project_data.test.ts`). The canonical ProjectData root is CLOSED. The SPA's
 * in-memory draft remains deliberately raw enough to hold a malformed upload:
 * FOCUSED authoring scope (A5.3c) edits the top-level fields + `sources[]`
 * (`register_variant`, `period`, `bindings[]`); known `panels[]` and invalid
 * unknown root keys ROUND-TRIP VERBATIM until backend validation reports them.
 * Raw retention is a diagnostic path, not a supported extension mechanism.
 *
 * This is NOT a structural validator — the backend is canonical (see
 * reg_webapp/DESIGN.md → Pydantic boundary). These
 * helpers only construct + immutably edit the shape the SPA posts to
 * `/api/project/validate` / `/order`. The field names mirror
 * `reg_schema/src/reg_schema/project_data.py` (see reg_schema/DESIGN.md → Two
 * layers: models vs. validator).
 *
 * OpenAPI codegen documents the closed canonical request model, but the SPA cannot
 * use that strict type for its in-memory draft because `/validate` must also accept
 * malformed specs to diagnose them. The hand-written draft type therefore keeps an
 * index signature solely to retain invalid uploaded keys until diagnostics are
 * produced.
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

/** One contiguous piece of a `Source.period`: a bare year, a period-token
 * string, or a `{from, to}` range object. */
export type PeriodSegment =
  | number
  | string
  | { from: number | string; to: number | string };

/** A `Source.period` value: a segment, the `"_default"` sentinel (rides the
 * string arm), or a LIST of segments — an interrupted series (#307; the
 * backend enforces non-empty, sorted ascending, non-overlapping). Kept loose —
 * the server is the canonical period validator. */
export type Period = PeriodSegment | PeriodSegment[];

/** The optional global study window (the "project window", #611 → Period model).
 * A plain year-int pair matching reg_schema's `StudyWindow` wire shape (#613:
 * `{from, to}` int years, `to >= from`). NOT the full `Period` grammar — the
 * window is year-granular by design; per-page deviation keeps the rich grammar.
 * Absent = full history (backward-compatible — existing specs serialize
 * unchanged). project_data isn't a response model, so this isn't codegen'd into
 * `api-types` — it's hand-authored here alongside the `ProjectData` shape. */
export interface StudyWindow {
  from: number;
  to: number;
}

/** A data source / table. Open: panel-referenced or future keys survive. */
export interface Source {
  name: string;
  register_variant: string;
  period: Period;
  bindings: Binding[];
  [key: string]: unknown;
}

/**
 * The top-level project_data.json draft. Canonically closed; the index signature
 * exists only so an invalid uploaded key is not normalized away before validation.
 */
export interface ProjectData {
  schema_version: string;
  steward: string;
  reg_meta_version: string;
  name: string;
  sources: Source[];
  // The optional global study window (#611). Additive: omitted when unset (the
  // serializer drops `undefined` keys), so a project with no window round-trips
  // and validates unchanged.
  window?: StudyWindow;
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
 * Release tags and distribution). Empty in → empty out (the context hasn't
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

export function safeSourcePeriod(source: unknown): Period | null {
  const safe = asSafeSource(source);
  return safe != null && "period" in safe ? (safe.period as Period) : null;
}

export function safeSourceBindings(source: unknown): Binding[] {
  const safe = asSafeSource(source);
  return Array.isArray(safe?.bindings) ? (safe.bindings as Binding[]) : [];
}

export function sourceBindingsMalformed(source: unknown): boolean {
  const safe = asSafeSource(source);
  return (
    safe != null && safe.bindings !== undefined && !Array.isArray(safe.bindings)
  );
}

// ── Immutable top-level edits ───────────────────────────────────────────────
// Every mutator returns a NEW object (shallow clone + replaced slice) so the
// store can swap the `$state` reference and `dirty` recomputes. Unmapped keys on
// the spread survive (the `...draft` carries known panels and raw invalid keys).

/** Replace a top-level scalar field (`name`, `steward`, `reg_meta_version`, …). */
export function updateField<K extends keyof ProjectData>(
  draft: ProjectData,
  key: K,
  value: ProjectData[K],
): ProjectData {
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
function sourcesArray(draft: ProjectData): Source[] {
  return Array.isArray(draft.sources) ? draft.sources : [];
}

// ── Source-name prefill (#312) ──────────────────────────────────────────────
// Source names are panel-key join handles (reg_schema panels join on source
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
export function removeSource(draft: ProjectData, index: number): ProjectData {
  return {
    ...draft,
    sources: sourcesArray(draft).filter((_, i) => i !== index),
  };
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
    sources: sourcesArray(draft).map((s, i) => {
      if (i !== index || asSafeSource(s) == null) {
        return s;
      }
      return { ...s, ...patch };
    }),
  };
}

// ── Immutable binding edits ─────────────────────────────────────────────────

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

/** Internal: apply a transform to one source's `bindings` immutably. Coerces a
 * non-array `sources` / `bindings` to [] (the editors' doctrine) so a structural
 * binding edit never spreads a string or throws on `.map`. */
function updateSourceBindings(
  draft: ProjectData,
  sourceIndex: number,
  fn: (bindings: Binding[]) => Binding[],
): ProjectData {
  return {
    ...draft,
    sources: sourcesArray(draft).map((s, i) => {
      if (i !== sourceIndex || asSafeSource(s) == null) {
        return s;
      }
      return { ...s, bindings: fn(safeSourceBindings(s)) };
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
export function serializeProjectData(draft: ProjectData): string {
  return JSON.stringify(draft, null, 2);
}
