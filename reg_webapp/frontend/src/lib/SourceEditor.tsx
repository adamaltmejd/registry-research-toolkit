import { type ReactNode, useEffect, useId, useRef, useState } from "react";
import {
  catalogHref,
  fqidSegments,
  registerPrefixOf,
  sourceCardHeading,
} from "./catalog";
import { registerRead, sourceNames, UNASKED } from "./catalog_names";
import { focusAfterRemoval } from "./focus_after_removal";
import { LaneGraph, type LaneModel, type LaneSpanModel } from "./LaneGraph";
import { PeriodThumbs } from "./PeriodThumbs";
import {
  intersectCoverageWindow,
  periodLabel,
  sameYearWindow,
  uncoveredYears,
  type WindowRelation,
  yearSegmentsFromWire,
  yearWindowLabel,
  yearWindowToWire,
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
import { router } from "./router";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import styles from "./SourceEditor.module.css";
import {
  bindingSpans,
  familyYears,
  memberNote,
  sourceFamily,
  yearsHull,
} from "./source_timeline";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { StatusRow } from "./ui/StatusRow";
import { Tag } from "./ui/Tag";
import type { KeyValueRow } from "./ui/types";
import { useCatalogRead } from "./use_store_read";
import {
  issuesUnderPointer,
  jsonPointer,
  PROJECT_HEADING_ID,
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
  /** How this source's period sits against the study window, judged inside its
   * columns' availability (`sourceWindowRelation`) — what the card MARKS.
   * `pending` while that availability is still being read. */
  windowRelation: WindowRelation | "pending" | null;
  /** The page's one axis, shared by every source card (DESIGN.md → Time). */
  axis: [number, number];
  /** The catalog vintage: where a delivery still running ends. */
  vintageYear: number;
}

/** Where focus lands after a source is removed: the next source's heading,
 * else the previous one's, else the project heading. */
function focusAfterSourceRemoval(index: number): void {
  const heading = (i: number) =>
    document.getElementById(sourceAnchorId(i))?.querySelector("h3");
  focusAfterRemoval([
    () => heading(index),
    () => (index > 0 ? heading(index - 1) : null),
    () => document.getElementById(PROJECT_HEADING_ID),
  ]);
}

/** After one column leaves a source, focus moves to the lane remove action
 * that took its place, else the one before it, else the source's heading:
 * never to `<body>`. */
function focusAfterColumnRemoval(
  sourceIndex: number,
  bindingIndex: number,
): void {
  const card = () => document.getElementById(sourceAnchorId(sourceIndex));
  const action = (i: number) =>
    card()?.querySelectorAll<HTMLElement>("[data-lane-remove] button")[i];
  focusAfterRemoval([
    () => action(bindingIndex),
    () => (bindingIndex > 0 ? action(bindingIndex - 1) : null),
    () => card()?.querySelector("h3"),
  ]);
}

const readUnasked = () => UNASKED;

export function SourceEditor({
  sourceIndex,
  source,
  issues,
  providerQualified,
  studyWindow,
  windowRelation,
  axis,
  vintageYear,
}: Props) {
  const periodLabelId = useId();

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
  const names = useCatalogRead(sourceNames, registerVariant);
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
  // A dated source reaches its variant's whole succession family (#1840), so its
  // columns resolve over the members (from the register read the heading already
  // makes); a `_default` source names its variant exactly.
  const registerFqid =
    fqidSegments(registerVariant).length === 3
      ? registerPrefixOf(registerVariant)
      : "";
  const register = useCatalogRead(
    registerFqid ? registerRead : readUnasked,
    registerFqid,
  );
  const familyVariants = sourceFamily(
    register.value ?? null,
    variantSlug,
    periodWire,
  );

  const metaRows: KeyValueRow[] = [
    ...(providerName ? [{ label: "Provider", value: providerName }] : []),
    ...(titleIsCoordinate
      ? []
      : [{ label: "Register variant", value: registerVariant, mono: true }]),
    { label: "Source name", value: sourceName, mono: true },
  ];

  // ── The source's period (Y-81; #1838) ─────────────────────────────────────
  //
  // Drawn as the source timeline (DESIGN.md → Time): the family's coverage row
  // with the period as the window band, dragged by its thumbs, and one lane per
  // column. A TOKEN period (`HT2018`, `_default`) is a vocabulary the thumbs do
  // not author; it is shown as it stands.

  /** The stored period's year segments — null for a token/mixed period as well
   * as for an unset one. A comma list (`2015..2017,2019..2020`) is several. */
  const storedSegments =
    periodWire === null ? null : yearSegmentsFromWire(periodWire);
  const storedHull: ProjectStudyWindow | null =
    storedSegments === null
      ? null
      : yearsHull(storedSegments.flatMap((w) => [w.from, w.to]));
  /** Whether the thumbs can author THIS source's period. */
  const yearsEditable = periodWire === null || storedSegments !== null;

  const familyDelivered = register.value
    ? familyYears(register.value, familyVariants, vintageYear)
    : [];
  const covered = yearsHull(familyDelivered);
  const members = register.value
    ? memberNote(register.value, familyVariants, vintageYear)
    : null;

  /** The thumbs mid-drag (and through the write that follows); null at rest. */
  const [dragging, setDragging] = useState<ProjectStudyWindow | null>(null);
  /** The column a lane's Remove asks about. It outlives the dialog's close,
   * so the closing dialog keeps its words. */
  const [removingColumn, setRemovingColumn] = useState({
    index: -1,
    label: "",
  });
  const [columnRemoveOpen, setColumnRemoveOpen] = useState(false);
  /** The last write was refused because the SOURCE moved under the edit. */
  const [writeRefused, setWriteRefused] = useState(false);
  /** A write is in flight — from the rest that found something to write until
   * the store's write returns, i.e. the restore-gate wait. A ref, not a
   * disabled slider: disabling the thumb a key press just moved would drop its
   * focus on every step. */
  const applying = useRef(false);
  /** The source as it stood when this drag began, which the store re-checks the
   * write against. Nothing renders from it. */
  const editedFrom = useRef<SourcePeriodEditTarget | null>(null);
  /** So a write still waiting on the restore gate is abandoned when the card
   * goes away, rather than written behind the researcher's back. */
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  // Where the thumbs rest: the stored period (a list at its outer years), else
  // the study window inside the family's coverage, else that coverage.
  const resting =
    storedHull ??
    intersectCoverageWindow(
      covered,
      studyWindow,
      axis[0],
      covered?.to ?? axis[1],
    );
  const selection = dragging ?? resting;
  const band = dragging ?? storedHull;
  const bounds = covered ?? { from: axis[0], to: axis[1] };

  /** The drag starts HERE: capture the source as it is, so a write onto a
   * source that has moved since is refused. */
  function beginEdit(): void {
    editedFrom.current = {
      sourceName: safeSourceName(source),
      registerVariant,
      snapshot: sourceSnapshot(source),
      replacementGeneration: projectStore.replacementGeneration,
    };
    setWriteRefused(false);
  }

  /** The thumbs came to rest: ONE period-only diff through the store's guarded
   * `applySourcePeriodEdit`, replacing the period with the span they frame. */
  async function applyPeriod(at: ProjectStudyWindow): Promise<void> {
    if (applying.current) {
      // A rest while an earlier write waits on the restore gate is dropped,
      // not queued: its edit snapshot predates that write, so the store would
      // refuse it. The thumbs then return to the period that landed, so what
      // shows is what is saved. Only reachable before the draft has restored.
      return;
    }
    const target = editedFrom.current;
    // A rest where the thumbs started writes nothing: a drag away and back
    // must not collapse a comma list into its outer years.
    if (target === null || sameYearWindow(at, resting)) {
      editedFrom.current = null;
      setDragging(null);
      return;
    }
    applying.current = true;
    try {
      // The draft lifecycle's restore is ASYNCHRONOUS, so this waits for it
      // exactly as the catalog's Add path does: the store's re-check is only
      // worth anything against the SETTLED draft. A card gone by then abandons
      // the write.
      await projectStore.restored;
      if (!mounted.current) {
        return;
      }
      setWriteRefused(
        !projectStore.applySourcePeriodEdit({
          ...target,
          period: sourcePeriodFromWire(yearWindowToWire(at)),
        }),
      );
    } finally {
      applying.current = false;
      editedFrom.current = null;
      if (mounted.current) {
        setDragging(null);
      }
    }
  }

  // The years the period requests, a comma list segment by segment, so a
  // column delivered only in a list's gap reads as outside it.
  const periodRuns: ProjectStudyWindow[] = dragging
    ? [dragging]
    : (storedSegments ?? []);

  // The columns as lanes under the coverage row, one per binding: each column a
  // span of the years the family delivers it.
  const lanes: LaneModel[] = [];
  const spans: LaneSpanModel[] = [];
  bindings.forEach((binding, j) => {
    const id = projectStore.bindingId(sourceIndex, j);
    const variable =
      typeof binding.variable === "string" ? binding.variable : "";
    const variableName =
      register.value?.children
        .find((child) => child.fqid === variable)
        ?.name?.trim() || variable;
    const columns = register.value
      ? bindingSpans(register.value, binding, familyVariants, vintageYear)
      : [];
    // What the remove action calls this column: the name the file gives it,
    // else the delivered one, else its FQID. A variable the catalog does not
    // know has no delivered name, and must still be removable (it blocks the
    // order, and nothing on its catalog page can untick it).
    const fileColumn =
      (typeof binding.display_name === "string" && binding.display_name) ||
      (typeof binding.representation === "string" && binding.representation);
    const columnLabel =
      fileColumn || columns[0]?.column || variable || "this column";
    lanes.push({
      id,
      name: variableName || "(no variable)",
      href: variable ? catalogHref(variable) : null,
      // Removes this binding from THIS source only: a same-family source that
      // carries the column keeps it. Never disabled: a register read that
      // failed must not trap a column in the project.
      action: (
        <span data-lane-remove="">
          <Button
            variant="ghost"
            aria-label={`Remove ${columnLabel} from ${sourceName}`}
            onPress={() => {
              setRemovingColumn({ index: j, label: columnLabel });
              setColumnRemoveOpen(true);
            }}
          >
            Remove
          </Button>
        </span>
      ),
    });
    for (const column of columns) {
      const years = yearWindowLabel(column);
      const outside =
        periodRuns.length > 0 &&
        !periodRuns.some((r) => column.to >= r.from && column.from <= r.to);
      spans.push({
        id: `${id}:${column.column}`,
        lane: id,
        name: column.column,
        from: column.from,
        to: column.to,
        segments: column.runs.map((r) => ({ name: null, ...r })),
        years,
        state: outside ? "out" : "default",
        selectable: false,
        accessibleName: `${column.column}, ${variableName}, delivered ${column.runs.map(yearWindowLabel).join(", ")}${outside ? ", outside the period" : ""}`,
      });
    }
  });

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

  // Removing a source takes all its columns with it, so it ASKS first.
  const [removeOpen, setRemoveOpen] = useState(false);

  // A fresh object per render is fine: the island assigns it onto the live
  // instance, and an unchanged value moves nothing there.
  const warningProps = {
    fqid: fqidSegments(registerVariant).slice(0, 2).join("/"),
    period: periodWire,
    registerOnly: true,
    framed: false,
  };

  /** The period's thumbs over the coverage row, on the drawing's axis. */
  function thumbs(drawn: [number, number]): ReactNode {
    return (
      <PeriodThumbs
        axis={drawn}
        selection={selection}
        bounds={bounds}
        label={
          identified ? `Period for ${identifiedName} (years)` : "Period (years)"
        }
        // Until the family's coverage lands there is nothing to bound them by.
        isDisabled={register.loading}
        onMove={(at) => {
          if (sameYearWindow(at, selection)) return;
          if (editedFrom.current === null) beginEdit();
          setDragging(at);
        }}
        onRest={(at) => void applyPeriod(at)}
      />
    );
  }

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
          {/* A focus target (not a tab stop) for focusAfterSourceRemoval. */}
          <h3
            className={titleIsCoordinate ? styles.mono : undefined}
            tabIndex={-1}
          >
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
                focusAfterSourceRemoval(sourceIndex);
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

      <ConfirmDialog
        isOpen={columnRemoveOpen}
        onOpenChange={setColumnRemoveOpen}
        title={`Remove the column ${removingColumn.label} from ${sourceName}?`}
        actions={
          <>
            <Button onPress={() => setColumnRemoveOpen(false)}>Cancel</Button>
            <Button
              variant="danger"
              onPress={() => {
                const { index } = removingColumn;
                setColumnRemoveOpen(false);
                projectStore.removeBinding(sourceIndex, index);
                focusAfterColumnRemoval(sourceIndex, index);
              }}
            >
              Remove column
            </Button>
          </>
        }
      >
        The column leaves this source only. Pick it again in the catalog to
        bring it back.
      </ConfirmDialog>

      {sourceMalformed ? (
        // A null/non-object slot: a degraded card that still occupies its index
        // so validation addressing lines up.
        <StatusRow tone="error" live="alert">
          This source entry is malformed. Download the project again, or fix it
          in <code>project_data.json</code>.
        </StatusRow>
      ) : (
        <>
          <KeyValue rows={metaRows} />

          {/* The PERIOD: the one field the cart edits (Y-81), on the source
              timeline (#1838). */}
          <section className={styles.period} aria-labelledby={periodLabelId}>
            <div className={styles.periodHead}>
              <h4 id={periodLabelId} className="micro-label">
                Period
              </h4>
              <span
                className={styles.periodYears}
                // Announces each period that lands; the thumbs say the years
                // while they move.
                aria-live="polite"
                title={members ?? undefined}
                aria-busy={register.loading ? "true" : undefined}
              >
                {periodLabel(period) ?? "No period"}
                {members && (
                  <span className="visually-hidden">. {members}</span>
                )}
              </span>
            </div>
            {yearsEditable ? (
              <LaneGraph
                label={identified ? `Columns of ${identifiedName}` : "Columns"}
                lanes={lanes}
                spans={spans}
                edges={[]}
                axis={axis}
                window={band && [band.from, band.to]}
                coverage={{
                  covered,
                  years: familyDelivered,
                  outside: band ? uncoveredYears(band, covered) : [],
                  bandName: band ? "Source period" : null,
                }}
                overlay={thumbs}
                onToggle={() => {}}
              />
            ) : (
              // A token period: the thumbs cannot express it.
              <p className={styles.fixed}>
                <span className={styles.wire}>{periodWire}</span>
                <span className={styles.fixedNote}>
                  The year thumbs can't express this period — change it in{" "}
                  <code>project_data.json</code> and open the file again.
                </span>
              </p>
            )}
            {storedSegments !== null && storedSegments.length > 1 && (
              <p className={styles.fixedNote}>
                Moving a thumb replaces these years with one span.
              </p>
            )}

            {writeRefused && (
              <StatusRow tone="warn" live="alert">
                This source changed while you were editing, so its period was
                not changed. The thumbs now show what the source holds — move
                them again.
              </StatusRow>
            )}

            {studyWindow && windowRelation === "differs" ? (
              <StatusRow tone="warn">
                Differs from study window {yearWindowLabel(studyWindow)}
              </StatusRow>
            ) : studyWindow && windowRelation === "disjoint" ? (
              // The one divergence that blocks the order (reg_webapp/DESIGN.md →
              // "Common study window").
              <StatusRow tone="error">
                No years inside the study window {yearWindowLabel(studyWindow)},
                so the order is blocked. Move the thumbs to overlap the window,
                or remove the source.
              </StatusRow>
            ) : null}
          </section>

          {/* The columns are the timeline's lanes: no list repeats them
              (maintainer ruling 2026-10-23). */}
          {bindingsMalformed ? (
            <StatusRow tone="error" live="alert">
              This source's <code>bindings</code> are malformed. Download the
              project again, or fix them in <code>project_data.json</code>.
            </StatusRow>
          ) : bindings.length === 0 ? (
            <EmptyState
              title="No columns yet"
              description="Browse the catalog to add columns from this register."
              action={
                <Button onPress={() => router.navigate("/catalog")}>
                  Browse registers
                </Button>
              }
            />
          ) : (
            <ScopedDataWarnings {...warningProps} />
          )}
        </>
      )}
    </section>
  );
}
