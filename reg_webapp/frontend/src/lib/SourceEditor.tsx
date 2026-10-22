import { useEffect, useId, useRef, useState } from "react";
import { BindingEditor } from "./BindingEditor";
import { fqidSegments, sourceCardHeading } from "./catalog";
import { sourceNames } from "./catalog_names.svelte";
import {
  normalizePeriodRows,
  periodWindowRelation,
  resolveYearEntry,
  yearSegmentsFromWire,
  yearWindowLabel,
} from "./period";
import {
  type ProjectSourcePeriod,
  type ProjectStudyWindow,
  type SafeSource,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  sourceBindingsMalformed,
  sourceSnapshot,
} from "./project_data";
import { projectStore, type SourcePeriodEditTarget } from "./project_store";
import { sourcePeriodFromWire, sourcePeriodToWire } from "./reg_core";
import ScopedDataWarnings from "./ScopedDataWarnings.svelte";
import styles from "./SourceEditor.module.css";
import { SvelteIsland } from "./SvelteIsland";
import { useRune } from "./svelte_bridge.svelte";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Tag } from "./ui/Tag";
import type { KeyValueRow } from "./ui/types";
import {
  issuesUnderPointer,
  jsonPointer,
  sourceAnchorId,
  type ValidationIssue,
} from "./validation";

// The source card of the #991 data-order cart: the cart SHOWS what has been picked
// (a register and the columns taken from it) and supports delete + navigate-out —
// a wrong variable, variant or representation is fixed by picking again in the
// catalog, never by editing the row. The ONE exception is this source's PERIOD
// (Y-81): it is a field of the source rather than of any pick, and this card is the
// only surface that shows a source whole — its full coordinate and every binding it
// carries — which is what a source-wide period rewrite has to be looked at against.
// The header rolls up all errors under `/sources/{i}` as a badge (fixes are reached
// via the ValidationPanel's catalog link). See reg_webapp/DESIGN.md.

interface Props {
  sourceIndex: number;
  source: SafeSource;
  issues: readonly ValidationIssue[];
  /** Whether this deployment serves more than one provider, so the card names the
   * provider that owns the register: a bare register name can stand for two
   * registers there. A deployment fact, read once at the app root. */
  providerQualified: boolean;
  /** The project's own study window, or null when none is set — what this
   * source's period is MARKED against when the two differ. An authoring seed,
   * never an inheritance (crates/reg-core/DESIGN.md). */
  studyWindow: ProjectStudyWindow | null;
}

/** One period row's two fields, as TEXT — kept as typed rather than parsed, so a
 * refused entry stays on screen as it was typed instead of being rewritten. */
interface YearRow {
  from: string;
  to: string;
}

/** A refusal about one or more rows: the message, and which row(s)/field(s) it
 * marks — a within-row grammar problem highlights its own field, a cross-row
 * overlap both rows whole. */
interface RowsProblem {
  problem: string;
  rows: { index: number; from: boolean; to: boolean }[];
}

const BLANK_ROW: YearRow = { from: "", to: "" };

export function SourceEditor({
  sourceIndex,
  source,
  issues,
  providerQualified,
  studyWindow,
}: Props) {
  // Instance-scoped ids, so every card's period fields keep real `<label for>`
  // pairs on a page that renders one card per source.
  const uid = useId();

  const rolledUp = issuesUnderPointer(
    [...issues],
    jsonPointer(["sources", sourceIndex]),
  );
  const errorCount = rolledUp.filter((i) => i.level === "error").length;
  const registerVariant = safeSourceRegisterVariant(source);

  // The card is HEADED by the REGISTER this source delivers from and qualified by
  // the VARIANT it extracts, in the catalog's own words (cached per register);
  // until they land the raw coordinate heads the card. The source's `name` is a
  // generated join key, so it titles nothing; it stays a detail row below.
  const names = useRune(sourceNames, registerVariant);
  const sourceMalformed = source === null;
  const heading = sourceMalformed
    ? "(malformed source)"
    : sourceCardHeading(names.register, registerVariant) || "(no register)";
  const variantName = names.variant;
  // The provider OWNS the register: an attribute, said only where the deployment
  // serves more than one, as a labelled row.
  const providerName = providerQualified ? names.provider : null;
  // A heading fallen back to the raw coordinate is an identifier: mono, and printed
  // once on the card.
  const titleIsCoordinate = heading === registerVariant;
  // The concrete variant the source extracts, for the columns' own resolve.
  const variantSlug = fqidSegments(registerVariant)[2] ?? "";

  // Defensive: a malformed opened spec may carry `bindings` as a non-array.
  const bindings = safeSourceBindings(source);
  const bindingsMalformed = sourceBindingsMalformed(source);

  // The panel/order join key (reg_meta `OrderEntry.source`): the one thing that
  // still differs between two sources an imported spec put on ONE register_variant.
  const sourceName = safeSourceName(source) || "(unnamed source)";

  const period = safeSourcePeriod(source) as ProjectSourcePeriod;
  // The stored period as its WIRE string (list-period aware); null when the source
  // carries none or an unshapeable one.
  const periodWire = sourcePeriodToWire(period);

  const metaRows: KeyValueRow[] = [
    ...(providerName ? [{ label: "Provider", value: providerName }] : []),
    ...(titleIsCoordinate
      ? []
      : [{ label: "Register variant", value: registerVariant, mono: true }]),
    { label: "Source name", value: sourceName, mono: true },
  ];

  // ── The source's period (Y-81, list segments Y-101) ───────────────────────
  //
  // Authored in the catalog's own period vocabulary: a YEAR RANGE per row, one row
  // for a plain period and one per segment of a #307 comma list. A TOKEN period
  // (`HT2018`) is a vocabulary nothing here authors; it is shown as it stands.

  /** The stored period's segments as year windows — null for a token/mixed period
   * as well as for an unset one. */
  const storedSegments =
    periodWire === null ? null : yearSegmentsFromWire(periodWire);
  const storedRows: YearRow[] =
    storedSegments === null
      ? [BLANK_ROW]
      : storedSegments.map((w) => ({ from: String(w.from), to: String(w.to) }));
  /** Whether the rows can author THIS source's period. */
  const yearsEditable = periodWire === null || storedSegments !== null;

  /** The rows as EDITED, or null while they mirror the stored segments. */
  const [rows, setRows] = useState<YearRow[] | null>(null);
  /** Committed (blur or Apply): a half-typed year must not announce a refusal on
   * every keystroke. */
  const [entryCommitted, setEntryCommitted] = useState(false);
  /** The last Apply was refused because the SOURCE moved under the edit. */
  const [writeRefused, setWriteRefused] = useState(false);
  /** The windows a landed write set, until the next keystroke retires them. */
  const [appliedWindows, setAppliedWindows] = useState<
    ProjectStudyWindow[] | null
  >(null);
  /** The last Apply had nothing to write. */
  const [unchangedNotice, setUnchangedNotice] = useState(false);
  /** An Apply is in flight — from the press that found something to write until
   * the store's write returns, i.e. the restore-gate wait. The fields, Add, Remove
   * and Apply freeze for it, so a second press can't queue a write of its own. */
  const [applying, setApplying] = useState(false);
  const applyingNow = useRef(false);
  /** The source as it stood when this edit began, which the store re-checks the
   * write against. Nothing renders from it. */
  const editedFrom = useRef<SourcePeriodEditTarget | null>(null);
  /** So an Apply still waiting on the restore gate is abandoned when the card
   * goes away, rather than written behind the researcher's back. */
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const displayRows = rows ?? storedRows;
  const rowResolutions = displayRows.map((row) =>
    rows === null ? null : resolveYearEntry(row.from, row.to),
  );

  let rowProblem: RowsProblem | null = null;
  for (let i = 0; i < rowResolutions.length; i++) {
    const r = rowResolutions[i];
    if (r !== null && "problem" in r) {
      rowProblem = { problem: r.problem, rows: [{ index: i, ...r.at }] };
      break;
    }
  }
  const rowWindows: ProjectStudyWindow[] | null =
    rows === null || rowProblem !== null
      ? null
      : rowResolutions.map((r) => (r as { years: ProjectStudyWindow }).years);
  const normalized =
    rowWindows === null ? null : normalizePeriodRows(rowWindows);
  /** Two rows sharing a year: coalescing would erase which row was meant. */
  const overlapProblem: RowsProblem | null =
    rowWindows === null || normalized === null || "period" in normalized
      ? null
      : {
          problem: `${yearWindowLabel(rowWindows[normalized.a])} and ${yearWindowLabel(rowWindows[normalized.b])} overlap — a year can only be in one segment.`,
          rows: [
            { index: normalized.a, from: true, to: true },
            { index: normalized.b, from: true, to: true },
          ],
        };
  const entryProblem = rowProblem ?? overlapProblem;
  const entryRefusal = entryCommitted ? entryProblem : null;
  const problemId = `${uid}-problem`;
  const rowInvalid = (index: number, side: "from" | "to") =>
    entryRefusal?.rows.some((r) => r.index === index && r[side]) ?? false;

  /** The wire the rows would write, or null when they name no valid list / no
   * change from what is stored. */
  let proposedWire: string | null = null;
  if (normalized !== null && "period" in normalized) {
    const wire = sourcePeriodToWire(normalized.period);
    proposedWire = wire === periodWire ? null : wire;
  }
  const appliedLabel =
    appliedWindows === null
      ? null
      : `Period set to ${appliedWindows.map(yearWindowLabel).join(", ")}.`;
  /** How this period sits against the study window — what the card MARKS. */
  const windowRelation = periodWindowRelation(period, studyWindow);

  /** The edit starts HERE: capture the source as it is, so a write onto a source
   * that has moved since is refused. */
  function beginEdit(): void {
    editedFrom.current = {
      sourceName: safeSourceName(source),
      registerVariant,
      snapshot: sourceSnapshot(source),
      replacementGeneration: projectStore.replacementGeneration,
    };
    setWriteRefused(false);
    setAppliedWindows(null);
    setUnchangedNotice(false);
  }

  function editRows(change: (base: YearRow[]) => YearRow[]): void {
    if (rows === null) {
      beginEdit();
    }
    setRows(change(rows ?? storedRows));
  }

  /** Commit the edited period: ONE period-only diff through the store's guarded
   * `applySourcePeriodEdit`. Either way the fields re-arm on the source as it NOW
   * stands. */
  async function applyPeriod(): Promise<void> {
    if (applyingNow.current) {
      // Held on the SAME gate as an earlier press: nothing new to contribute.
      return;
    }
    // Committing FIRST is what makes a refused Apply say why.
    setEntryCommitted(true);
    const target = editedFrom.current;
    const wire = proposedWire;
    if (target === null || wire === null) {
      // Nothing to write: the refusal explains why, or the rows name exactly the
      // stored period — say so rather than look like nothing happened.
      setUnchangedNotice(entryProblem === null);
      setWriteRefused(false);
      setAppliedWindows(null);
      return;
    }
    setUnchangedNotice(false);
    applyingNow.current = true;
    setApplying(true);
    try {
      const written = yearSegmentsFromWire(wire);
      // The draft lifecycle's restore is ASYNCHRONOUS, so this waits for it
      // exactly as the catalog's Add path does: the store's re-check is only
      // worth anything against the SETTLED draft. A card gone by then abandons
      // the write.
      await projectStore.restored;
      if (!mounted.current) {
        return;
      }
      const refused = !projectStore.applySourcePeriodEdit({
        ...target,
        period: sourcePeriodFromWire(wire),
      });
      setWriteRefused(refused);
      setAppliedWindows(refused ? null : written);
      setRows(null);
      setEntryCommitted(false);
      editedFrom.current = null;
    } finally {
      applyingNow.current = false;
      if (mounted.current) {
        setApplying(false);
      }
    }
  }

  // The removal question and the controls' accessible names are SENTENCES naming
  // this source in words. A malformed slot, or a source with no coordinate, says
  // "this source", and the buttons keep their visible text as their whole name.
  const identified = !sourceMalformed && registerVariant !== "";
  const columnCount = `${bindings.length} column${bindings.length === 1 ? "" : "s"}`;
  // The generated join key first, since it is the one thing still unique between
  // two sources on the SAME register_variant; the heading rides along.
  const identifiedName = `${sourceName} (${heading}${variantName ? `, ${variantName}` : ""})`;
  const removeQuestion = identified
    ? `Remove the source ${identifiedName} and its ${columnCount}?`
    : `Remove this source and its ${columnCount}?`;

  // Removing a source takes its whole column list with it, so it ASKS first.
  // Removing ONE column doesn't: that's a single row the researcher points at.
  const [removeOpen, setRemoveOpen] = useState(false);

  // A fresh object per render is fine: the island assigns it onto the live
  // instance, and an unchanged value moves nothing there.
  const warningProps = {
    fqid: fqidSegments(registerVariant).slice(0, 2).join("/"),
    period: periodWire,
    registerOnly: true,
    framed: false,
  };

  const statusClass = applying
    ? "info"
    : entryRefusal !== null
      ? "refused"
      : appliedLabel !== null
        ? "applied"
        : unchangedNotice
          ? "info"
          : "";

  return (
    // `id` is the click-to-locate anchor the ValidationPanel scrolls to.
    <section
      className={styles.source}
      id={sourceAnchorId(sourceIndex)}
      aria-label={`Source ${sourceIndex + 1}`}
    >
      <header className={styles.head}>
        {/* The title is COMPOSED: the register heads the card and the variant that
            names the population sits under it. `aria-busy` while the names are in
            flight. */}
        <div
          className={styles.title}
          aria-busy={names.loading ? "true" : undefined}
        >
          <h3 className={titleIsCoordinate ? styles.mono : undefined}>
            {heading}
            {errorCount > 0 && (
              <Tag tone="error" glyph="✕">
                {errorCount} error{errorCount === 1 ? "" : "s"}
              </Tag>
            )}
          </h3>
          {variantName && <p className={styles.variant}>{variantName}</p>}
        </div>
        <Button
          variant="default"
          size="sm"
          aria-label={
            identified ? `Remove source ${identifiedName}` : undefined
          }
          onPress={() => setRemoveOpen(true)}
        >
          Remove source
        </Button>
      </header>

      <ConfirmDialog
        isOpen={removeOpen}
        onOpenChange={setRemoveOpen}
        title={removeQuestion}
        actions={
          <>
            <Button onPress={() => setRemoveOpen(false)}>Cancel</Button>
            <Button
              variant="danger"
              onPress={() => {
                setRemoveOpen(false);
                projectStore.removeSource(sourceIndex);
              }}
            >
              Remove source
            </Button>
          </>
        }
      >
        The source leaves this project with every column it carries. Pick them
        again in the catalog to bring them back.
      </ConfirmDialog>

      {sourceMalformed ? (
        // A null/non-object slot: a degraded card that still occupies its index
        // so validation addressing lines up.
        <p className="error" role="alert">
          <span aria-hidden="true">✕</span> This source entry is malformed — fix
          via re-download or hand-edit.
        </p>
      ) : (
        <>
          <KeyValue rows={metaRows} />

          {/* The PERIOD: the one field the cart edits (Y-81), one From/To row per
              segment (Y-101). */}
          <div className={styles.period}>
            <span className="micro-label">Period</span>
            {yearsEditable ? (
              // A form, so Enter in a year field applies the period.
              <form
                className={styles.entry}
                onSubmit={(event) => {
                  event.preventDefault();
                  void applyPeriod();
                }}
              >
                <fieldset
                  className={styles.yearRows}
                  aria-label={
                    identified ? `Period for ${identifiedName}` : "Period"
                  }
                >
                  {displayRows.map((row, i) => (
                    // Rows are positional: an edit names a row by its place.
                    // oxlint-disable-next-line react/no-array-index-key
                    <div key={i} className={styles.yearRow}>
                      <label
                        className="micro-label"
                        htmlFor={`${uid}-from-${i}`}
                      >
                        From
                      </label>
                      <input
                        id={`${uid}-from-${i}`}
                        className="year"
                        type="text"
                        inputMode="numeric"
                        autoComplete="off"
                        value={row.from}
                        placeholder="yyyy"
                        readOnly={applying}
                        aria-invalid={rowInvalid(i, "from")}
                        aria-describedby={
                          entryRefusal === null ? undefined : problemId
                        }
                        onChange={(event) => {
                          const from = event.currentTarget.value;
                          editRows((base) =>
                            base.map((r, j) => (j === i ? { ...r, from } : r)),
                          );
                        }}
                        onBlur={() => {
                          if (rows !== null) setEntryCommitted(true);
                        }}
                      />
                      <label className="micro-label" htmlFor={`${uid}-to-${i}`}>
                        To
                      </label>
                      <input
                        id={`${uid}-to-${i}`}
                        className="year"
                        type="text"
                        inputMode="numeric"
                        autoComplete="off"
                        value={row.to}
                        placeholder="yyyy"
                        readOnly={applying}
                        aria-invalid={rowInvalid(i, "to")}
                        aria-describedby={
                          entryRefusal === null ? undefined : problemId
                        }
                        onChange={(event) => {
                          const to = event.currentTarget.value;
                          editRows((base) =>
                            base.map((r, j) => (j === i ? { ...r, to } : r)),
                          );
                        }}
                        onBlur={() => {
                          if (rows !== null) setEntryCommitted(true);
                        }}
                      />
                      {displayRows.length > 1 && (
                        <Button
                          size="sm"
                          isInert={applying}
                          onPress={() =>
                            editRows((base) => base.filter((_, j) => j !== i))
                          }
                        >
                          Remove
                        </Button>
                      )}
                    </div>
                  ))}
                </fieldset>
                <div className={styles.actions}>
                  <Button
                    size="sm"
                    isInert={applying}
                    onPress={() => editRows((base) => [...base, BLANK_ROW])}
                  >
                    Add years
                  </Button>
                  {/* Named per source. NEVER truly disabled: a refused entry
                      explains itself on click, and an Apply in flight is inert
                      (the handler's own guard) without leaving the tab order. */}
                  <Button
                    type="submit"
                    size="sm"
                    isInert={applying}
                    aria-label={
                      identified
                        ? `Apply period for ${identifiedName}`
                        : undefined
                    }
                  >
                    Apply period
                  </Button>
                </div>
              </form>
            ) : (
              // A token period: the year fields cannot express it.
              <p className={styles.fixed}>
                <span className={styles.wire}>{periodWire}</span>
                <span className={styles.fixedNote}>
                  The From and To fields can't express this period — change it
                  in <code>project_data.json</code> and open the file again.
                </span>
              </p>
            )}

            {/* Rendered ALWAYS, so a refusal lands in a live region that already
                existed. It carries the CONFIRMATION too. */}
            <output
              className={`problem ${styles.status} ${statusClass}`}
              id={problemId}
            >
              {applying ? (
                <>
                  <span aria-hidden="true">i</span> Waiting for the project to
                  load…
                </>
              ) : entryRefusal !== null ? (
                <>
                  <span aria-hidden="true">✕</span> {entryRefusal.problem}
                </>
              ) : appliedLabel !== null ? (
                <>
                  <span aria-hidden="true">✓</span> {appliedLabel}
                </>
              ) : unchangedNotice ? (
                <>
                  <span aria-hidden="true">i</span> Period unchanged.
                </>
              ) : null}
            </output>

            {writeRefused && (
              <p className={styles.stale} role="alert">
                <span aria-hidden="true">▲</span> This source changed while you
                were editing, so its period was not changed. The years now show
                what the source holds — enter them again and apply.
              </p>
            )}

            {studyWindow && windowRelation === "differs" ? (
              // The window is an authoring SEED, so this MARKS the divergence
              // rather than warning about it.
              <p className={styles.deviation}>
                Differs from study window {yearWindowLabel(studyWindow)}
              </p>
            ) : studyWindow && windowRelation === "disjoint" ? (
              // The one divergence that blocks the order (reg_webapp/DESIGN.md →
              // "Common study window").
              <p className={`problem refused ${styles.status}`}>
                <span aria-hidden="true">✕</span> No years inside the study
                window {yearWindowLabel(studyWindow)}, so the order is blocked.
                Change the period to overlap the window, or remove the source.
              </p>
            ) : null}
          </div>

          {/* The cart says COLUMNS: a binding is one delivery column of the
              order. */}
          <div className={styles.bindings}>
            <h4>Columns ({bindings.length})</h4>
            {bindingsMalformed ? (
              <p className="error" role="alert">
                <span aria-hidden="true">✕</span> This source's{" "}
                <code>bindings</code> are malformed — fix via re-download or
                hand-edit.
              </p>
            ) : bindings.length === 0 ? (
              <EmptyState title="No columns yet. Browse the catalog to add columns from this register." />
            ) : (
              <>
                <SvelteIsland
                  component={ScopedDataWarnings}
                  props={warningProps}
                />
                <ul className={styles.bindingList}>
                  {bindings.map((binding, j) => (
                    // Keyed by the store-owned STABLE id (issue #200), so a middle
                    // remove remounts the right row.
                    <li key={projectStore.bindingId(sourceIndex, j)}>
                      <BindingEditor
                        sourceIndex={sourceIndex}
                        bindingIndex={j}
                        binding={binding}
                        variant={variantSlug}
                        period={periodWire}
                      />
                    </li>
                  ))}
                </ul>
              </>
            )}
          </div>
        </>
      )}
    </section>
  );
}
