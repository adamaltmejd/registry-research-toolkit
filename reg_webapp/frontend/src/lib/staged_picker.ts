import {
  addWindowBounds,
  type BindingResolution,
  bindingFieldsFromResolution,
  type PickerRepresentation,
  type PickerVariantSegment,
  pickerRowVariantFamily,
  resolveBindingAt,
  rowAddPeriod,
  rowCoversColumn,
  windowsAddPeriod,
  windowsOverlapWindow,
} from "./catalog";
import {
  boundedPeriodSegments,
  isStructurallyValidPeriodWire,
  type PeriodBounds,
  periodCoverageUnion,
  periodFromWire,
  periodLabel,
  periodToWire,
  periodWindowRelation,
  yearWindowLabel,
} from "./period";
import {
  isPlainObject,
  type Period,
  type ProjectData,
  regMetaReleaseTag,
  type StudyWindow,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  safeSourceSlots,
} from "./project_data";
import {
  projectStore,
  type StagedAdd,
  type StagedRemove,
} from "./project_store.svelte";

export interface StagedPickerBand {
  key: string;
  registerPrefix: string;
  rows: PickerRepresentation[];
}

export interface PickerCommittedRow {
  key: string;
  registerVariant: string;
  variable: string;
  representation: string | null;
  sourceName: string;
  sourcePeriod: Period;
  removals?: StagedRemove[];
}

export interface PickerAddPeriod {
  registerVariant: string;
  period: Period;
}

export interface PickerSourcePeriod {
  registerVariant: string;
  period: Period;
}

export interface PickerCommitScope {
  period?: string | null | undefined;
  window?: [number, number] | null;
}

type CommitVariantSegment = Pick<PickerVariantSegment, "variant" | "windows">;

export function rowRegisterVariantForVariant(
  band: StagedPickerBand,
  variant: string,
): string {
  return `${band.registerPrefix}/${variant}`;
}

export function pickerRowKey(
  band: StagedPickerBand,
  row: PickerRepresentation,
): string {
  return [
    `${band.registerPrefix}/${pickerRowVariantFamily(row)}`,
    band.key,
    row.representation ?? row.column,
  ].join("::");
}

function sourcePeriod(source: unknown): Period {
  return safeSourcePeriod(source) ?? "";
}

function bindingVariable(binding: unknown): string {
  return isPlainObject(binding) && typeof binding.variable === "string"
    ? binding.variable
    : "";
}

function bindingRepresentation(binding: unknown): string | null {
  return isPlainObject(binding) && typeof binding.representation === "string"
    ? binding.representation
    : null;
}

function rowWindowBounds(row: PickerRepresentation): PeriodBounds[] {
  if (
    row.period_scope === "year_independent" ||
    row.from === null ||
    row.to === null
  )
    return [];
  return (
    row.windows.length > 0 ? row.windows : [{ from: row.from, to: row.to }]
  ).map((window) => ({
    from: window.from,
    to: window.to,
  }));
}

/** Do these delivery windows reach a COMMITTED source's period? The period side is
 * the #307 comma-union a source can carry, so every segment of it is tried. Exported
 * because the register list asks it of a delivery column's own eras (Y-104): its
 * rows are the variable's — a #902 rename chain folds into ONE row spanning the
 * whole chain — so only the NAME's windows can say whether the source period
 * reached the years that name was delivered in. */
export function windowsOverlapPeriod(
  windows: readonly { from: string; to: string }[],
  period: Period,
): boolean {
  const segments = boundedPeriodSegments(period);
  if (!segments) {
    return false;
  }
  return segments.some((segment) =>
    windowsOverlapWindow(windows, segment.bounds),
  );
}

function rowOverlapsPeriod(row: PickerRepresentation, period: Period): boolean {
  if (row.period_scope === "year_independent") return period === "_default";
  return windowsOverlapPeriod(rowWindowBounds(row), period);
}

/** The concrete `register_variant` segments a folded picker row spans (#376): its
 * `variantSegments` when folded, else the single-segment fallback on `row.variant`
 * (the unfolded HEAD — see the per-concrete-segment invariant in catalog.ts). The
 * ONE place the row → concrete-segment fan-out is derived. */
function rowVariantSegments(row: PickerRepresentation): CommitVariantSegment[] {
  return row.variantSegments && row.variantSegments.length > 0
    ? row.variantSegments
    : [{ variant: row.variant, windows: row.windows }];
}

/** The concrete segments a row commits under `scope`, plus the scope machinery each
 * consumer needs. Shared by `rowRelevantSegments` (staging-match) and `rowAddSegments`
 * (Apply fan-out) so the two can't drift on WHICH segments a period-scoped add touches
 * (the #376 whack-a-mole seam). A single-segment (unfolded) row is always fully
 * relevant; a folded family narrows to the segments whose delivery windows overlap the
 * active add window, falling back to ALL segments when none do (an explicitly-selected
 * out-of-window row is never silently dropped: its staging match still finds it, and
 * its Apply is refused by name rather than skipped). */
function relevantSegments(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): {
  segments: CommitVariantSegment[];
  addWindow: { from: string; to: string } | null;
  folded: boolean;
} {
  const addWindow = addWindowBounds(scope.period, scope.window ?? null);
  const segments = rowVariantSegments(row);
  if (segments.length === 1) {
    return { segments, addWindow, folded: false };
  }
  const overlapping = segments.filter((segment) =>
    windowsOverlapWindow(segment.windows, addWindow),
  );
  return {
    segments: overlapping.length > 0 ? overlapping : segments,
    addWindow,
    folded: true,
  };
}

/** Whether a picker row has a delivery era inside `scope`'s add window — the gate
 * the register list (Y-83) applies before a tick is even offered, since the study
 * window is the only period it has. A subject page instead keeps every row
 * selectable and lets `applyStagedPicks` refuse the Add (`outside-scope`): a row
 * the window clips to nothing has no period to commit, and inventing one from the
 * row's own span is what the common-study-window decision rules out. Reads the add
 * window through the SAME `addWindowBounds` derivation `relevantSegments` does (#678:
 * a sub-annual `?period` wins over the year window), so a gate and the commit it
 * gates can never disagree about which window a row was judged against. With
 * no window nothing is clipped, every row passes, and `finalAddPeriodWires` is the one
 * that asks for a period. */
export function rowDeliversInScope(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): boolean {
  if (row.period_scope === "year_independent")
    return (
      scope.period === "_default" ||
      (scope.period == null && scope.window == null)
    );
  return windowsOverlapWindow(
    row.windows,
    addWindowBounds(scope.period, scope.window ?? null),
  );
}

function rowRelevantSegments(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): CommitVariantSegment[] {
  return relevantSegments(row, scope).segments;
}

/** One concrete `register_variant` an Apply must stage for a (folded or plain) picker
 * row, with its scope-clipped add period. `outsideScope` marks a dated segment the
 * active add window clips to NOTHING — its `periodWire` is then null, and the Apply
 * is refused rather than given a period nobody asked for. */
export interface RowAddSegment {
  variant: string;
  registerVariant: string;
  periodWire: string | null;
  outsideScope: boolean;
}

/** The per-concrete-segment Apply plan for a picker row (#376): ONE source per concrete
 * `register_variant` the row's active scope touches, each with its own era-clipped wire
 * period. The single home for the picker-row → staged-add fan-out, consumed by every
 * view's `stagedAddCandidates` so the per-concrete-segment invariant (catalog.ts) is
 * enforced once, not re-derived per view.
 *   - An UNFOLDED row stages its one variant with `rowAddPeriod` (the whole-row
 *     windows clipped to the add window).
 *   - A FOLDED family stages each relevant concrete segment with its OWN delivery
 *     windows clipped to the add window (era-precise, so a partial-family add can't
 *     leak coverage into the other era).
 * Either way the clip keeps EVERY disjoint era the window reaches — the full
 * available intersection the common study window defaults an add to — and a
 * segment it reaches none of is `outsideScope` (`relevantSegments` still hands
 * back a family's every segment then, so the refusal can name the row). */
export function rowAddSegments(
  band: StagedPickerBand,
  row: PickerRepresentation,
  scope: PickerCommitScope,
): RowAddSegment[] {
  const { segments, addWindow, folded } = relevantSegments(row, scope);
  return segments.map((segment) => {
    const periodWire = folded
      ? windowsAddPeriod(segment.windows, addWindow)
      : rowAddPeriod(row, addWindow);
    return {
      variant: segment.variant,
      registerVariant: rowRegisterVariantForVariant(band, segment.variant),
      periodWire,
      outsideScope:
        addWindow !== null &&
        row.period_scope !== "year_independent" &&
        periodWire === null,
    };
  });
}

function rowMatchesBinding(
  binding: unknown,
  row: PickerRepresentation,
  sourcePeriod: Period,
): boolean {
  const representation = bindingRepresentation(binding);
  if (representation !== null) {
    return rowCoversColumn(row, representation);
  }
  return rowOverlapsPeriod(row, sourcePeriod);
}

export function committedPickerRows(
  draft: ProjectData | null,
  bands: readonly StagedPickerBand[],
  scope: PickerCommitScope = {},
): Map<string, PickerCommittedRow> {
  const committed = new Map<string, PickerCommittedRow>();
  const sources = safeSourceSlots(draft?.sources);
  if (sources.length === 0) {
    // Nothing is committed anywhere, so no row can be — and the ordinary
    // catalog-browsing state (no draft) should not pay for a whole register list
    // of key + segment work to discover that.
    return committed;
  }
  for (const band of bands) {
    for (const row of band.rows) {
      const rowKey = pickerRowKey(band, row);
      const segments = rowRelevantSegments(row, scope);
      const matched: PickerCommittedRow[] = [];
      for (const segment of segments) {
        const registerVariant = rowRegisterVariantForVariant(
          band,
          segment.variant,
        );
        const source = sources.find(
          (s) => safeSourceRegisterVariant(s) === registerVariant,
        );
        if (!source) {
          continue;
        }
        const period = sourcePeriod(source);
        const binding = safeSourceBindings(source).find(
          (b) =>
            bindingVariable(b) === band.key &&
            rowMatchesBinding(b, row, period),
        );
        if (!binding) {
          continue;
        }
        matched.push({
          key: rowKey,
          registerVariant,
          variable: band.key,
          representation: bindingRepresentation(binding),
          sourceName: safeSourceName(source),
          sourcePeriod: period,
        });
      }
      if (matched.length !== segments.length) {
        continue;
      }
      const first = matched[0];
      if (!first) {
        continue;
      }
      committed.set(rowKey, {
        ...first,
        removals:
          matched.length > 1
            ? matched.map((match) => ({
                registerVariant: match.registerVariant,
                variable: match.variable,
                representation: match.representation,
              }))
            : undefined,
      });
    }
  }
  return committed;
}

/** How a picker's "In project" marker reads against the common study window — the
 * picker half of "highlight every divergence" (the `/project` source card is the
 * other). A source whose period is the window's own years says nothing more; one
 * that differs says so; one with no years inside the window says it is outside, at
 * error tone, because that is what blocks the order. The label stays short — it is a
 * one-line tag inside a picker row, down to 375px — and `detail` names both periods
 * for assistive tech (visually hidden: the window is on screen in the rail and the
 * source period on the `/project` card); null when there is none. */
export interface CommittedMarker {
  tone: "info" | "error";
  glyph: string;
  label: string;
  detail: string | null;
}

export function committedMarker(
  sourcePeriod: Period,
  studyWindow: StudyWindow | null,
): CommittedMarker {
  const relation = periodWindowRelation(sourcePeriod, studyWindow);
  const period = periodLabel(sourcePeriod);
  if (relation === null || relation === "same" || studyWindow === null) {
    return { tone: "info", glyph: "i", label: "In project", detail: null };
  }
  const window = yearWindowLabel(studyWindow);
  if (relation === "disjoint") {
    return {
      tone: "error",
      glyph: "✕",
      label: "In project, outside study window",
      detail: `(source period ${period}; study window ${window})`,
    };
  }
  return {
    tone: "info",
    glyph: "i",
    label: "In project, years differ",
    detail: `(source period ${period}; study window ${window})`,
  };
}

export function sourcePeriodsFromDraft(
  draft: ProjectData | null,
): PickerSourcePeriod[] {
  const out: PickerSourcePeriod[] = [];
  const sources = safeSourceSlots(draft?.sources);
  for (const source of sources) {
    const registerVariant = safeSourceRegisterVariant(source);
    if (registerVariant) {
      out.push({ registerVariant, period: sourcePeriod(source) });
    }
  }
  return out;
}

export function stagedRemoveForCommitted(
  committed: PickerCommittedRow,
): StagedRemove[] {
  return committed.removals && committed.removals.length > 0
    ? committed.removals
    : [
        {
          registerVariant: committed.registerVariant,
          variable: committed.variable,
          representation: committed.representation,
        },
      ];
}

export function nullBindingCommittedRowKeys(
  committed: Iterable<PickerCommittedRow>,
  target: PickerCommittedRow,
): string[] {
  if (target.representation !== null) {
    return [target.key];
  }
  const keys: string[] = [];
  for (const row of committed) {
    if (
      row.representation === null &&
      row.registerVariant === target.registerVariant &&
      row.variable === target.variable &&
      periodToWire(row.sourcePeriod) === periodToWire(target.sourcePeriod)
    ) {
      keys.push(row.key);
    }
  }
  return keys.length > 0 ? keys : [target.key];
}

/** The nudge a leaf/group page shows when `finalAddPeriodWires` refuses a staged
 * add (Y-77): named both ways out — set the study window in the rail, or apply a
 * period on this page — as ONE shared string, so the two pages can't drift. */
export const ADD_PERIOD_REQUIRED_MESSAGE =
  "Apply a period before adding — set the study window in the rail, or press Apply under Period above, then select and add again.";

/** The same refusal on a page that carries NO Period control of its own — the
 * register list (Y-83), where the study window is the only way out. Same gate,
 * same opening words; it just doesn't name a control this page hasn't got. */
export const ADD_WINDOW_REQUIRED_MESSAGE =
  "Apply a period before adding — set the study window in the rail, then add again.";

/** The wire period each staged add resolves its binding at and commits under, in
 * `adds` order — or null when ANY of them has no valid dated or explicit year-independent period. A picker row
 * with an open-ended delivery window, picked with neither a `?period` nor a project
 * window to clip it to, resolves no period at all: committing it would author
 * `period: ""` and a `type: ""` the resolve cannot derive, which only the backend
 * validator would catch. All-or-nothing so one such row can't half-apply a batch —
 * the caller keeps the draft unchanged and asks for a period instead
 * (`ADD_PERIOD_REQUIRED_MESSAGE`). */
export function finalAddPeriodWires(
  existing: Iterable<PickerSourcePeriod>,
  adds: readonly PickerAddPeriod[],
): string[] | null {
  const sources = [...existing];
  const scopes = new Map(
    sources.map((source) => [
      source.registerVariant,
      periodToWire(source.period),
    ]),
  );
  for (const add of adds) {
    const incoming = periodToWire(add.period);
    const current = scopes.get(add.registerVariant);
    if (
      incoming &&
      current &&
      (incoming === "_default") !== (current === "_default")
    )
      return null;
    if (incoming) scopes.set(add.registerVariant, incoming);
  }
  const periods = finalSourcePeriodsForStagedAdds(sources, adds);
  const wires: string[] = [];
  for (const add of adds) {
    const wire = periodToWire(periods.get(add.registerVariant) ?? add.period);
    if (
      wire === "_default" &&
      (add.registerVariant.split("/").length !== 3 ||
        add.registerVariant.split("/")[2] === "_default")
    )
      return null;
    if (wire === null || !isStructurallyValidPeriodWire(wire)) {
      return null;
    }
    wires.push(wire);
  }
  return wires;
}

export function finalSourcePeriodsForStagedAdds(
  existing: Iterable<PickerSourcePeriod>,
  adds: readonly PickerAddPeriod[],
): Map<string, Period> {
  const periods = new Map<string, Period>();
  for (const source of existing) {
    if (!periods.has(source.registerVariant)) {
      periods.set(source.registerVariant, source.period);
    }
  }
  for (const add of adds) {
    const current = periods.get(add.registerVariant);
    periods.set(
      add.registerVariant,
      current === undefined
        ? add.period
        : periodCoverageUnion(current, add.period),
    );
  }
  return periods;
}

// ── The staged add → resolve → commit stack (Y-83) ───────────────────────────
// ONE home for what the binding leaf, the concept group and the register page all
// do with a set of picked rows: fan each row out to its concrete register
// variants, resolve every add's binding fields at the period it commits under, and
// write the whole batch through `projectStore.applyStagedDiff`. It was three
// copies of the same forty lines; the hosts now own only their own `$state`
// (the confirmation / refusal lines) and their scope.

/** One picked row as a host hands it over: the band it belongs to and the row. The
 * fields are the ones staging reads, so every host's richer band/row types
 * (`PickerBand`, the register page's own bands) satisfy it structurally. */
export interface StagedPick {
  band: StagedPickerBand;
  row: PickerRepresentation;
}

/** The batch one Apply commits: rows to add, committed rows to remove. */
export interface StagedApplyPayload {
  adds: readonly StagedPick[];
  removes: readonly { committed: PickerCommittedRow }[];
}

/** The deployment seed a pristine store needs to mint the project the picks land
 * in (C1: threaded from `/api/context` through the page). */
export interface StagedApplySeed {
  regMetaVersion: string;
  steward: string;
}

/** What an Apply did, for the host's inline confirmation — or `null` when the
 * batch was empty (nothing to confirm). */
export interface StagedApplyOutcome {
  added: number;
  removed: number;
}

/** A staged diff in words — "+2 columns · -1 column", only the parts that are
 * non-zero, "" for an empty diff. ONE formatter so the picker footer (what an
 * Apply WILL do) and a page's confirmation (what it DID) can never phrase the
 * same two counts differently. */
export function stagedDiffSummary(counts: StagedApplyOutcome): string {
  const parts: string[] = [];
  if (counts.added > 0) {
    parts.push(`+${counts.added} ${counts.added === 1 ? "column" : "columns"}`);
  }
  if (counts.removed > 0) {
    parts.push(
      `-${counts.removed} ${counts.removed === 1 ? "column" : "columns"}`,
    );
  }
  return parts.join(" · ");
}

/** The ways an Apply can end:
 *  - `applied` — the diff is committed (the host clears its staging);
 *  - `period-required` — refused BEFORE any mutation because an add resolved no
 *    finite period (the host shows `ADD_PERIOD_REQUIRED_MESSAGE` and keeps the
 *    staging, so an Apply that authored nothing never looks like one that did);
 *  - `outside-scope` — refused BEFORE any mutation because a picked column has no
 *    years inside the active add window (the common study window, or the page's
 *    own period): there is no overlap to persist, and the batch is not half-applied.
 *    `columns` names them for `outsideScopeMessage`;
 *  - `outside-study-window` — refused BEFORE any mutation because a page's own
 *    `?period` resolved a picked column to years wholly outside the project's
 *    study window: the add would author a source the window already blocks;
 *  - `abandoned` — the page was left, or the draft replaced, while the picks were
 *    in flight; nothing was written. */
export type StagedApplyResult =
  | { kind: "applied"; outcome: StagedApplyOutcome | null }
  | { kind: "period-required" }
  | { kind: "outside-scope"; columns: string[] }
  | {
      kind: "outside-study-window";
      columns: string[];
      studyWindow: StudyWindow;
    }
  | { kind: "abandoned" };

/** "Kon", "Kon and Sni", "Kon, Sni and Lan" — the columns a refusal names. */
function columnList(columns: readonly string[]): string {
  return columns.length <= 1
    ? (columns[0] ?? "This column")
    : `${columns.slice(0, -1).join(", ")} and ${columns.at(-1)}`;
}

/** The refusal an `outside-scope` Apply shows in the picker: which columns, the
 * window they miss, and the two ways out — untick them, or move the period that
 * excludes them. `scope` is the one the Apply ran under, so the copy names the
 * page's own period when that is what clipped them, else the study window. */
export function outsideScopeMessage(
  columns: readonly string[],
  scope: PickerCommitScope,
): string {
  const names = columnList(columns);
  const verb = columns.length > 1 ? "have" : "has";
  if (scope.period) {
    return `Not added: ${names} ${verb} no years inside the selected period ${periodLabel(periodFromWire(scope.period)) ?? scope.period}. Untick ${columns.length > 1 ? "them" : "it"}, or change the period above.`;
  }
  const window = scope.window
    ? ` ${yearWindowLabel({ from: scope.window[0], to: scope.window[1] })}`
    : "";
  return `Not added: ${names} ${verb} no years inside the study window${window}. Untick ${columns.length > 1 ? "them" : "it"}, or widen the study window in the rail.`;
}

/** The refusal line a host shows for an Apply that authored nothing, or null when
 * there is none to show (applied, or abandoned). `periodRequired` is the host's own
 * wording for that gate — it names the controls THAT page has. */
export function stagedApplyRefusal(
  result: StagedApplyResult,
  scope: PickerCommitScope,
  periodRequired: string,
): string | null {
  if (result.kind === "period-required") {
    return periodRequired;
  }
  if (result.kind === "outside-scope") {
    return outsideScopeMessage(result.columns, scope);
  }
  if (result.kind === "outside-study-window") {
    const many = result.columns.length > 1;
    return `Not added: under the selected period, ${columnList(result.columns)} ${many ? "have" : "has"} no years inside the study window ${yearWindowLabel(result.studyWindow)}. Choose a period that overlaps the study window, or widen the study window in the rail.`;
  }
  return null;
}

/** One staged add, resolved down to a concrete `register_variant` + period. */
export interface StagedAddCandidate {
  pick: StagedPick;
  variant: string;
  registerVariant: string;
  periodWire: string | null;
  period: Period;
  outsideScope: boolean;
}

/** Fan ONE picked row out to a staged add per concrete `register_variant` its
 * active scope touches (#376) — the per-concrete-segment invariant lives in
 * `rowAddSegments`, never re-derived per host (see catalog.ts's
 * `PickerRepresentation` seam note). */
export function stagedAddCandidates(
  pick: StagedPick,
  scope: PickerCommitScope,
): StagedAddCandidate[] {
  return rowAddSegments(pick.band, pick.row, scope).map((segment) => ({
    pick,
    variant: segment.variant,
    registerVariant: segment.registerVariant,
    periodWire: segment.periodWire,
    period: periodFromWire(segment.periodWire),
    outsideScope: segment.outsideScope,
  }));
}

/** Resolve ONE candidate's binding fields at the period it commits under. A failed
 * resolve is `unresolved`, which authors `type: ""` for the backend validator to
 * flag rather than a synthesized-valid binding.
 * simplify: one GET per add. A researcher picks a few dozen columns per action, so
 * the batch is tens of parallel requests; give the catalog a bulk resolve if a
 * single action ever stages hundreds. */
async function stagedAdd(
  candidate: StagedAddCandidate,
  resolvePeriodWire: string,
): Promise<StagedAdd> {
  const { band, row } = candidate.pick;
  let resolution: BindingResolution;
  try {
    resolution = await resolveBindingAt(
      band.key,
      resolvePeriodWire,
      candidate.variant,
    );
  } catch {
    resolution = { kind: "unresolved" as const, reason: "no-states" as const };
  }
  return {
    registerVariant: candidate.registerVariant,
    period: candidate.period,
    binding: bindingFieldsFromResolution(
      band.key,
      resolution,
      row.representation,
      { pinRepresentation: row.pinRepresentation === true },
    ),
  };
}

/** Commit a staged batch through ONE synchronous store mutation. `scope` is the
 * host's active (period, window) — the same one its `committedPickerRows` reads,
 * so what an add commits under is what the page showed. `cancelled` reports that the
 * batch no longer describes that page — the host is gone (its `$effect` teardown), or
 * a control the host left usable has moved `scope` out from under it. It is asked at
 * BOTH of the awaits below (the restore gate, then the binding resolves), because a
 * pick that waits is a pick the researcher can walk away from mid-wait, and neither
 * wait may end in a commit into a page they have left behind.
 *
 * The replacement guard below starts HERE, when this call does. A host that reads
 * anything asynchronously between the press and this call has a window this guard
 * cannot see, and must capture `projectStore.replacementGeneration` at the PRESS and
 * report it through `cancelled` — as `CatalogNodeView.addSelected` does, whose guard
 * also covers the route and the study window, which this one cannot see at all. */
export async function applyStagedPicks(
  payload: StagedApplyPayload,
  ctx: {
    scope: PickerCommitScope;
    /** The project's common study window, when one is set. A page's own
     * `?period` can override the add window (`scope`), so it is judged here too:
     * an add wholly outside it would author a source that blocks the order. */
    studyWindow?: StudyWindow | null;
    seed: StagedApplySeed;
    cancelled: () => boolean;
  },
): Promise<StagedApplyResult> {
  if (payload.adds.length === 0 && payload.removes.length === 0) {
    return { kind: "applied", outcome: null };
  }
  // The draft lifecycle is application-owned and its restore is ASYNCHRONOUS: on a
  // cold entry at a catalog route the store is still empty while IndexedDB is read.
  // Wait for it to settle, or this Add mints a SECOND project over the saved one.
  // The wait is unbounded, so the pick stays bound to the project it was staged
  // against: a New/Open (or leaving the page) while it is pending means the
  // researcher moved on, and these rows are not a pick against the replacement.
  const stagedAgainst = projectStore.replacementGeneration;
  await projectStore.restored;
  if (ctx.cancelled() || projectStore.replacementGeneration !== stagedAgainst) {
    return { kind: "abandoned" };
  }
  // Every add commits under a FINITE period — the one it also resolves its binding
  // metadata at. A row delivered open-ended, picked with neither a `?period` nor a
  // project window to clip it to, has none: applying it would author `period: ""`
  // and an underivable `type: ""` onto a draft that autosaves before /project is
  // ever opened. Refuse BEFORE any mutation (a null draft is not even minted).
  const candidates = payload.adds.flatMap((pick) =>
    stagedAddCandidates(pick, ctx.scope),
  );
  // A column with NO years inside the add window has no overlap to persist: the
  // common study window blocks it rather than inventing a period (reg_webapp/
  // DESIGN.md → "Common study window"), and the whole batch with it, so nothing
  // is half-applied.
  const outside = [
    ...new Set(
      candidates
        .filter((candidate) => candidate.outsideScope)
        .map((candidate) => candidate.pick.row.column),
    ),
  ];
  if (outside.length > 0) {
    return { kind: "outside-scope", columns: outside };
  }
  // The add window may be the page's own `?period`, not the study window. An add
  // it resolves wholly outside the study window is the disjoint source §12 blocks —
  // refused here, naming the window, rather than "applied" into a blocked project.
  const studyWindow = ctx.studyWindow ?? null;
  if (studyWindow !== null) {
    const disjoint = [
      ...new Set(
        candidates
          .filter(
            (candidate) =>
              periodWindowRelation(candidate.period, studyWindow) ===
              "disjoint",
          )
          .map((candidate) => candidate.pick.row.column),
      ),
    ];
    if (disjoint.length > 0) {
      return { kind: "outside-study-window", columns: disjoint, studyWindow };
    }
  }
  const addPeriods = finalAddPeriodWires(
    sourcePeriodsFromDraft(projectStore.draft),
    candidates,
  );
  if (addPeriods === null) {
    return { kind: "period-required" };
  }
  const target = projectStore.draft;
  const adds = await Promise.all(
    candidates.map((candidate, i) => stagedAdd(candidate, addPeriods[i])),
  );
  // One resolve GET per add, so the host can go — or the scope it staged under can
  // move — WHILE they are in flight, which the gate above ran too early to see: the
  // draft is untouched by either, so its identity alone would let the batch through.
  // The mint waits until after this gate, so an abandoned batch cannot leave a fresh
  // empty project as its only trace.
  if (ctx.cancelled() || projectStore.draft !== target) {
    return { kind: "abandoned" };
  }
  if (projectStore.draft === null && payload.adds.length > 0) {
    projectStore.newProject({
      reg_meta_version: regMetaReleaseTag(ctx.seed.regMetaVersion),
      steward: ctx.seed.steward,
    });
  }
  const removes = payload.removes.flatMap((r) =>
    stagedRemoveForCommitted(r.committed),
  );
  projectStore.applyStagedDiff({ adds, removes });
  return {
    kind: "applied",
    outcome: {
      added: payload.adds.length,
      removed: removes.length,
    },
  };
}
