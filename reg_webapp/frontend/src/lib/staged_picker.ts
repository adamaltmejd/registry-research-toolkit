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
  periodToWire,
} from "./period";
import {
  isPlainObject,
  type Period,
  type ProjectData,
  regMetaReleaseTag,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  safeSourceSlots,
} from "./project_data";
import {
  projectStore,
  type StagedAdd,
  type StagedPeriodChange,
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

function boundsOverlap(a: PeriodBounds, b: PeriodBounds): boolean {
  return a.from <= b.to && b.from <= a.to;
}

function rowWindowBounds(row: PickerRepresentation): PeriodBounds[] {
  return (row.windows.length > 0 ? row.windows : [row]).map((window) => ({
    from: window.from,
    to: window.to,
  }));
}

function rowOverlapsPeriod(row: PickerRepresentation, period: Period): boolean {
  const segments = boundedPeriodSegments(period);
  if (!segments) {
    return false;
  }
  const windows = rowWindowBounds(row);
  return segments.some((segment) =>
    windows.some((rowWindow) => boundsOverlap(segment.bounds, rowWindow)),
  );
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
 * out-of-window row is never silently dropped). */
function relevantSegments(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): {
  segments: CommitVariantSegment[];
  addWindow: { from: string; to: string } | null;
  clipped: boolean;
  folded: boolean;
} {
  const addWindow = addWindowBounds(scope.period, scope.window ?? null);
  const segments = rowVariantSegments(row);
  if (segments.length === 1) {
    return { segments, addWindow, clipped: true, folded: false };
  }
  const overlapping = segments.filter((segment) =>
    windowsOverlapWindow(segment.windows, addWindow),
  );
  const clipped = overlapping.length > 0;
  return {
    segments: clipped ? overlapping : segments,
    addWindow,
    clipped,
    folded: true,
  };
}

/** Whether a picker row has a delivery era inside `scope`'s add window — the gate a
 * host must apply when that window IS the only period it can commit under.
 * `rowAddSegments` deliberately FALLS BACK to a row's whole span where the window
 * clips it to nothing, so a subject page's explicitly-picked dimmed row still commits
 * something; that page has a Period control to say what. The register list (Y-83) has
 * none — the study window is the only period there is — so inheriting that fallback
 * would author years the researcher never asked for, and it refuses the row instead.
 * Reads the add window through the SAME `addWindowBounds` derivation `relevantSegments`
 * does (#678: a sub-annual `?period` wins over the year window), so a gate and the
 * commit it gates can never disagree about which window a row was judged against. With
 * no window nothing is clipped, every row passes, and `finalAddPeriodWires` is the one
 * that asks for a period. */
export function rowDeliversInScope(
  row: PickerRepresentation,
  scope: PickerCommitScope,
): boolean {
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
 * row, with its scope-clipped add period. */
export interface RowAddSegment {
  variant: string;
  registerVariant: string;
  periodWire: string | null;
}

/** The per-concrete-segment Apply plan for a picker row (#376): ONE source per concrete
 * `register_variant` the row's active scope touches, each with its own era-clipped wire
 * period. The single home for the picker-row → staged-add fan-out, consumed by every
 * view's `stagedAddCandidates` so the per-concrete-segment invariant (catalog.ts) is
 * enforced once, not re-derived per view.
 *   - An UNFOLDED row stages its one variant with `rowAddPeriod` (the whole-row window,
 *     fallback allowed so an out-of-window add still commits the row's own span).
 *   - A FOLDED family stages each relevant concrete segment with its OWN delivery
 *     windows clipped to the add window (no fallback: a family segment's period is
 *     era-precise so a partial-family add can't leak coverage into the other era). */
export function rowAddSegments(
  band: StagedPickerBand,
  row: PickerRepresentation,
  scope: PickerCommitScope,
): RowAddSegment[] {
  const { segments, addWindow, clipped, folded } = relevantSegments(row, scope);
  return segments.map((segment) => ({
    variant: segment.variant,
    registerVariant: rowRegisterVariantForVariant(band, segment.variant),
    periodWire: folded
      ? windowsAddPeriod(segment.windows, clipped ? addWindow : null, false)
      : rowAddPeriod(row, addWindow),
  }));
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

export function periodChangesWithStagedAdds(
  changes: readonly StagedPeriodChange[],
  adds: readonly PickerAddPeriod[],
): StagedPeriodChange[] {
  const addPeriods = new Map<string, Period>();
  for (const add of adds) {
    const current = addPeriods.get(add.registerVariant);
    addPeriods.set(
      add.registerVariant,
      current === undefined
        ? add.period
        : periodCoverageUnion(current, add.period),
    );
  }
  return changes.map((change) => {
    const addPeriod = addPeriods.get(change.registerVariant);
    if (addPeriod === undefined) {
      return change;
    }
    return {
      ...change,
      period: periodCoverageUnion(change.period, addPeriod),
    };
  });
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
 * `adds` order — or null when ANY of them has no valid FINITE period. A picker row
 * with an open-ended delivery window, picked with neither a `?period` nor a project
 * window to clip it to, resolves no period at all: committing it would author
 * `period: ""` and a `type: ""` the resolve cannot derive, which only the backend
 * validator would catch. All-or-nothing so one such row can't half-apply a batch —
 * the caller keeps the draft unchanged and asks for a period instead
 * (`ADD_PERIOD_REQUIRED_MESSAGE`). */
export function finalAddPeriodWires(
  existing: Iterable<PickerSourcePeriod>,
  changes: readonly StagedPeriodChange[],
  adds: readonly PickerAddPeriod[],
): string[] | null {
  const periods = finalSourcePeriodsForStagedAdds(existing, changes, adds);
  const wires: string[] = [];
  for (const add of adds) {
    const wire = periodToWire(periods.get(add.registerVariant) ?? add.period);
    if (wire === null || !isStructurallyValidPeriodWire(wire)) {
      return null;
    }
    wires.push(wire);
  }
  return wires;
}

export function finalSourcePeriodsForStagedAdds(
  existing: Iterable<PickerSourcePeriod>,
  changes: readonly StagedPeriodChange[],
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
  for (const change of periodChangesWithStagedAdds(changes, adds)) {
    periods.set(change.registerVariant, change.period);
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

/** The batch one Apply commits: rows to add, committed rows to remove, and
 * source-period changes to fold in. */
export interface StagedApplyPayload {
  adds: readonly StagedPick[];
  removes: readonly { committed: PickerCommittedRow }[];
  periodChanges: readonly StagedPeriodChange[];
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
  periodChanged: number;
}

/** A staged diff in words — "+2 columns · -1 column · 1 period change", only the
 * parts that are non-zero, "" for an empty diff. ONE formatter so the picker footer
 * (what an Apply WILL do) and a page's confirmation (what it DID) can never phrase
 * the same three counts differently. */
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
  if (counts.periodChanged > 0) {
    parts.push(
      `${counts.periodChanged} ${counts.periodChanged === 1 ? "period change" : "period changes"}`,
    );
  }
  return parts.join(" · ");
}

/** The three ways an Apply can end:
 *  - `applied` — the diff is committed (the host clears its staging);
 *  - `period-required` — refused BEFORE any mutation because an add resolved no
 *    finite period (the host shows `ADD_PERIOD_REQUIRED_MESSAGE` and keeps the
 *    staging, so an Apply that authored nothing never looks like one that did);
 *  - `abandoned` — the page was left, or the draft replaced, while the picks were
 *    in flight; nothing was written. */
export type StagedApplyResult =
  | { kind: "applied"; outcome: StagedApplyOutcome | null }
  | { kind: "period-required" }
  | { kind: "abandoned" };

/** One staged add, resolved down to a concrete `register_variant` + period. */
export interface StagedAddCandidate {
  pick: StagedPick;
  variant: string;
  registerVariant: string;
  periodWire: string | null;
  period: Period;
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
 * anything asynchronously between the press and this call (the register list re-reads
 * each ticked variable's delivery eras) has a window this guard cannot see, and must
 * capture `projectStore.replacementGeneration` at the PRESS and abandon its own batch
 * if it moved — as `CatalogNodeView.addSelected` does. */
export async function applyStagedPicks(
  payload: StagedApplyPayload,
  ctx: {
    scope: PickerCommitScope;
    seed: StagedApplySeed;
    cancelled: () => boolean;
  },
): Promise<StagedApplyResult> {
  if (
    payload.adds.length === 0 &&
    payload.removes.length === 0 &&
    payload.periodChanges.length === 0
  ) {
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
  const addPeriods = finalAddPeriodWires(
    sourcePeriodsFromDraft(projectStore.draft),
    payload.periodChanges,
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
  projectStore.applyStagedDiff({
    adds,
    removes,
    periodChange: periodChangesWithStagedAdds(
      payload.periodChanges,
      candidates,
    ),
  });
  return {
    kind: "applied",
    outcome: {
      added: payload.adds.length,
      removed: removes.length,
      periodChanged: payload.periodChanges.length,
    },
  };
}
