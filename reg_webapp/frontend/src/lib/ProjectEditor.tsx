import { useEffect, useMemo, useRef } from "react";
import {
  Button as AriaButton,
  Label,
  ListBox,
  ListBoxItem,
  Popover,
  Select,
  SelectValue,
  Text,
} from "react-aria-components";
import { fqidSegments, registerPrefixOf } from "./catalog";
import { catalogNames, registerRead, UNASKED } from "./catalog_names";
import styles from "./ProjectEditor.module.css";
import { dataAxis, yearSegmentsFromWire } from "./period";
import {
  type ProjectData,
  type ProjectStudyWindow,
  type RawDraft,
  regMetaReleaseTag,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  safeSourceSlots,
  safeStudyWindow,
} from "./project_data";
import { projectStore } from "./project_store";
import { sourcePeriodToWire } from "./reg_core";
import { router } from "./router";
import { SourceEditor } from "./SourceEditor";
import { StudyWindowSummary } from "./StudyWindowSummary";
import { familyYears, sourceFamily, yearsHull } from "./source_timeline";
import { sourceWindowRelation } from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useProject } from "./use_project";
import { useCatalogRead } from "./use_store_read";
import { ValidationPanel } from "./ValidationPanel";
import {
  PROJECT_HEADING_ID,
  projectFindings,
  windowDisjointFindings,
} from "./validation";
import type { WindowBounds } from "./window_bounds";

// The /project page — a READ-ONLY data-order CART (#991/#993), not an editor.
// Under #991 the project IS the cart: it SHOWS what the researcher picked while
// browsing (sources + their columns), and adding/changing data always happens in
// the catalog browser. So this page is browse-only authoring: view the picked
// sources/columns, delete a source/column, edit the project NAME and each source's
// period, and Open/Download the project_data.json + Download order. It is:
//  - the home/new screen (draft == null): New / Open buttons,
//  - the loaded-draft view: the editable name, a dirty indicator, the toolbar, the
//    open-error banner, the sources/columns list, the ValidationPanel and a
//    provenance footer (steward / reg_meta / schema version, read-only).
// Validation runs automatically against the current draft, so the order download
// is gated on the current backend result instead of a manual Validate click.

interface Props {
  regMetaVersion: string;
  steward: string;
  /** The study window a New project starts with (`defaultStudyWindow`). */
  defaultWindow: ProjectStudyWindow;
  /** The catalog's span and vintage (`windowBounds`): the sources' shared axis
   * falls back to the span, and a delivery still running ends at the vintage. */
  bounds: WindowBounds;
  /** The steward's order hand-off (`steward.json` → `order_instructions`): how a
   * researcher sends the project in. Null where the deployment takes no orders. */
  orderInstructions: string | null;
  /** Whether this deployment serves more than one provider, so a source card's
   * title carries its provider. Read once at the app root. */
  providerQualified: boolean;
}

/** Each source's period against the study window, judged inside its columns'
 * availability (`sourceWindowRelation`), for `useCatalogRead`: the register
 * reads are the ones every source card already makes for its name, so this
 * issues no request of its own and re-runs when one lands. */
function windowRelationsOf(draft: RawDraft | null) {
  if (draft == null) {
    return [];
  }
  const window = safeStudyWindow(draft.window);
  return safeSourceSlots(draft.sources).map((source) => {
    const rv = safeSourceRegisterVariant(source);
    const read =
      fqidSegments(rv).length === 3
        ? registerRead(registerPrefixOf(rv))
        : UNASKED;
    return sourceWindowRelation(
      source,
      window,
      read.value ?? null,
      read.loading,
    );
  });
}

/** The years every source card draws, for the page's one axis (DESIGN.md →
 * Time): each source's family coverage and its period's outer years. Read from
 * the same register reads as `windowRelationsOf`. */
function sourceYearsOf({
  draft,
  vintageYear,
}: {
  draft: RawDraft | null;
  vintageYear: number;
}): number[] {
  if (draft == null) {
    return [];
  }
  return safeSourceSlots(draft.sources).flatMap((source) => {
    const rv = safeSourceRegisterVariant(source);
    const wire = sourcePeriodToWire(safeSourcePeriod(source));
    const segments = yearSegmentsFromWire(wire);
    const drawn = segments?.flatMap((w) => [w.from, w.to]) ?? [];
    const [, , variant] = fqidSegments(rv);
    if (!variant) {
      return drawn;
    }
    const register = registerRead(registerPrefixOf(rv)).value;
    if (!register) {
      return drawn;
    }
    const hull = yearsHull(
      familyYears(register, sourceFamily(register, variant, wire), vintageYear),
    );
    return hull === null ? drawn : [...drawn, hull.from, hull.to];
  });
}

/** The visible reason a blank name closes the order download. */
const ORDER_NAME_REASON_ID = "order-name-reason";

const nameOf = (draft: RawDraft): string =>
  typeof draft.name === "string" ? draft.name : "";

type ExtractFormat = NonNullable<ProjectData["extract_format"]>;

/** The extract formats, Stata first: the contract's default when the draft sets none. */
const EXTRACT_FORMATS: { id: ExtractFormat; label: string }[] = [
  { id: "dta", label: "Stata (.dta)" },
  { id: "parquet", label: "Parquet (.parquet)" },
  { id: "csv", label: "CSV (.csv)" },
];

const formatOf = (draft: RawDraft): ExtractFormat =>
  EXTRACT_FORMATS.find((f) => f.id === draft.extract_format)?.id ?? "dta";

/** The project's `extract_format`: the one file format the steward's extract
 * writes every ordered file in. A pick is written explicitly, `dta` included. */
function ExtractFormatField({ value }: { value: ExtractFormat }) {
  return (
    <Select
      className={styles.field}
      value={value}
      onChange={(key) => {
        if (key != null) {
          projectStore.updateField("extract_format", key as ExtractFormat);
        }
      }}
    >
      <Label>File format</Label>
      <AriaButton className={styles.selectButton}>
        <SelectValue />
        <TreeChevron />
      </AriaButton>
      <Text slot="description" className={`muted ${styles.hint}`}>
        Every file of the order is written in this format.
      </Text>
      <Popover
        offset={4}
        style={{ zIndex: "var(--z-popover)" }}
        className={({ isEntering, isExiting }) =>
          `${styles.selectPopover} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
        }
      >
        <ListBox items={EXTRACT_FORMATS} className={styles.selectList}>
          {(format) => (
            <ListBoxItem id={format.id} className={styles.selectItem}>
              {format.label}
            </ListBoxItem>
          )}
        </ListBox>
      </Popover>
    </Select>
  );
}

export function ProjectEditor({
  regMetaVersion,
  steward,
  defaultWindow,
  bounds,
  orderInstructions,
  providerQualified,
}: Props) {
  const draft = useProject((s) => s.draft);
  const dirty = useProject((s) => s.dirty);
  const openError = useProject((s) => s.openError);
  const replacementPending = useProject((s) => s.replacementPending);
  const validation = useProject((s) => s.validation);
  const status = useProject((s) => s.validationStatus);
  const requestError = useProject((s) => s.requestError);
  const requestErrorSource = useProject((s) => s.requestErrorSource);
  const orderFindings = useProject((s) => s.orderFindings);
  const canDownloadOrder = useProject((s) => s.canDownloadOrder);
  const windowBlocked = useProject((s) => s.windowBlocked);
  const nameBlocked = useProject((s) => s.nameBlocked);
  const orderBusy = useProject((s) => s.orderBusy);
  const windowRelations = useCatalogRead(windowRelationsOf, draft);
  // Read-side safe source slots preserve array positions for degraded cards while
  // the draft itself stays verbatim for serialize/validate. The window is coerced
  // once here and threaded to every reader.
  const sources = safeSourceSlots(draft?.sources);
  const studyWindow = draft == null ? null : safeStudyWindow(draft.window);
  const sourceYears = useCatalogRead(
    sourceYearsOf,
    useMemo(
      () => ({ draft, vintageYear: bounds.vintageYear }),
      [draft, bounds.vintageYear],
    ),
  );
  // One axis for every source card, so their periods line up (Q9).
  const axis = dataAxis(sourceYears, studyWindow, [bounds.min, bounds.max]);

  // Every finding once, located; each renders on its card and the panel links
  // to it (#1906).
  const findings = projectFindings({
    issues: validation?.issues ?? [],
    orderFindings,
    windowFindings: windowDisjointFindings(studyWindow, sources),
    sources,
  });
  // The coordinates the findings print, said in the catalog's words. Keyed by
  // their text, so the read is rebuilt only when the set changes.
  const coordinateKey = [
    ...new Set(
      findings.flatMap((f) => [
        ...f.fqids,
        ...(f.location?.catalogFqid ? [f.location.catalogFqid] : []),
      ]),
    ),
  ].join("\n");
  const coordinates = useMemo(
    () => (coordinateKey ? coordinateKey.split("\n") : []),
    [coordinateKey],
  );
  const names = useCatalogRead(catalogNames, coordinates);
  const catalogName = (fqid: string) => names[fqid] ?? null;

  // A hidden <input type=file> driven by the toolbar "Open" button.
  const fileInput = useRef<HTMLInputElement>(null);
  const nameInput = useRef<HTMLInputElement>(null);
  // Reading a picked file is asynchronous, and anything the researcher does
  // meanwhile SUPERSEDES that read: a New or a newer Open makes it the wrong
  // answer, and this page's teardown leaves nobody to answer it.
  const readGeneration = useRef(0);

  // This page is the only surface that asks the replacement question, so drop any
  // unanswered one with it: left standing, a catalog pick made meanwhile commits
  // normally and coming back would re-raise a stale question whose confirm
  // destroys the newly picked work.
  useEffect(
    () => () => {
      readGeneration.current += 1;
      projectStore.cancelReplacement();
    },
    [],
  );

  // New and Open are the app's only deliberate replacements of a loaded project,
  // and both go through the store's replacement policy: with a dirty draft it holds
  // the incoming project and this page asks; with a clean one it replaces at once.
  function onNew(): void {
    readGeneration.current += 1;
    projectStore.requestNewProject({
      reg_meta_version: regMetaReleaseTag(regMetaVersion),
      steward,
      window: defaultWindow,
    });
  }

  async function onFilePicked(input: HTMLInputElement): Promise<void> {
    const file = input.files?.[0];
    input.value = ""; // allow re-picking the same file
    if (!file) {
      return;
    }
    const generation = ++readGeneration.current;
    const text = await file.text();
    if (generation !== readGeneration.current) {
      return;
    }
    // Parse BEFORE asking to replace anything: a file that can't be opened raises
    // the open-error banner, never the replacement question.
    const parsed = projectStore.parseProjectText(text);
    if (parsed != null) {
      projectStore.requestOpenProject(parsed);
    }
  }

  const openPicker = () => fileInput.current?.click();

  return (
    <article className={styles.editor}>
      <input
        ref={fileInput}
        type="file"
        accept="application/json,.json"
        hidden
        onChange={(event) => void onFilePicked(event.currentTarget)}
      />

      {/* The store owns the question, so every dismissal routes back through it. */}
      <ConfirmDialog
        isOpen={replacementPending}
        onOpenChange={(open) => {
          if (!open) {
            projectStore.cancelReplacement();
          }
        }}
        title="Replace the current project?"
        actions={
          <>
            <Button onPress={() => projectStore.cancelReplacement()}>
              Cancel
            </Button>
            <Button
              onPress={() => {
                // Keep the durable copy, then carry out the held replacement.
                projectStore.downloadProject();
                projectStore.confirmReplacement();
              }}
            >
              Download project_data.json
            </Button>
            <Button
              variant="danger"
              onPress={() => projectStore.confirmReplacement()}
            >
              Replace without downloading
            </Button>
          </>
        }
      >
        {(draft && nameOf(draft)) || "Untitled project"} has unsaved changes
        since its last download. Your browser keeps one recovery copy, and the
        replacement overwrites it.
      </ConfirmDialog>

      {openError && (
        <StatusRow
          tone="error"
          live="alert"
          className={styles.openError}
          action={
            <Button
              variant="ghost"
              onPress={() => projectStore.clearOpenError()}
            >
              Dismiss
            </Button>
          }
        >
          {openError}
        </StatusRow>
      )}

      {draft == null ? (
        <>
          <h1 className={styles.title}>Author a project</h1>
          <p className="muted">
            Start a new <code>project_data.json</code> or open an existing one.
            Projects live in your browser; download the file to keep the durable
            copy.
          </p>
          <div className={styles.toolbar}>
            <Button variant="primary" onPress={onNew}>
              New project
            </Button>
            <Button onPress={openPicker}>Open project_data.json…</Button>
          </div>
        </>
      ) : (
        <>
          <header className={styles.head}>
            <h1 id={PROJECT_HEADING_ID} className={styles.title} tabIndex={-1}>
              {nameOf(draft) || "Untitled project"}
              {dirty && (
                <span
                  className={styles.dirty}
                  title="Unsaved changes since last download"
                >
                  unsaved
                </span>
              )}
            </h1>
          </header>

          <div className={styles.toolbar}>
            <Button onPress={onNew}>New</Button>
            <Button onPress={openPicker}>Open…</Button>
            <Button onPress={() => projectStore.downloadProject()}>
              Download project_data.json
            </Button>
            {/* Gated behind the CURRENT automatic validation: the backend rejects
                anything that is not an order with a 422. A materializer block can
                still land here, in the panel's request-error slot. */}
            <Button
              variant="primary"
              isInert={!canDownloadOrder}
              aria-describedby={nameBlocked ? ORDER_NAME_REASON_ID : undefined}
              title={
                canDownloadOrder
                  ? "Download the order manifest"
                  : nameBlocked
                    ? "Blocked — the project has no name; see below"
                    : windowBlocked
                      ? "Blocked — a source has no years inside the study window; see below"
                      : "Not available yet — see the validation results below"
              }
              onPress={() => void projectStore.downloadOrder()}
            >
              {orderBusy ? "Downloading…" : "Download order"}
            </Button>
          </div>
          {/* The order manifest carries the name as its provenance, so a blank
              one closes the download (the server refuses it as
              `project_unnamed`). Said visibly, beside the button it explains. */}
          {nameBlocked && (
            <StatusRow
              tone="info"
              className={styles.orderReason}
              action={
                <Button
                  variant="ghost"
                  onPress={() => nameInput.current?.focus()}
                >
                  Set name
                </Button>
              }
            >
              <span id={ORDER_NAME_REASON_ID}>
                Name the project to download the order.
              </span>
            </StatusRow>
          )}
          {/* The two downloads hand over DIFFERENT artifacts, said visibly. */}
          <p className="muted">
            <code>project_data.json</code> is the editable project draft; the
            order file is the order manifest generated from it, named after the
            project and the date.
          </p>
          {/* The steward's own words on how to hand the project in: the next
              step, so plain body text. */}
          {orderInstructions && <p>{orderInstructions}</p>}

          {/* `name` and `extract_format` are the editable top-level fields on
              this page. */}
          <label className={styles.field}>
            <span>Name</span>
            <input
              ref={nameInput}
              type="text"
              value={nameOf(draft)}
              placeholder="Project name"
              onChange={(event) =>
                projectStore.updateField("name", event.currentTarget.value)
              }
            />
          </label>
          <ExtractFormatField value={formatOf(draft)} />

          {/* The sources/columns cart, keyed by the store-owned STABLE client id
              (issue #200), never the index. Adding data happens in the catalog,
              so there is no "Add source" here. */}
          <section aria-label="Sources">
            {/* No counter (DESIGN.md → No counters): the cards are the count. */}
            <Panel title="Sources">
              {sources.length === 0 ? (
                <EmptyState
                  title="No sources yet"
                  description="Browse the catalog to add data to your project."
                  action={
                    <Button onPress={() => router.navigate("/catalog")}>
                      Browse registers
                    </Button>
                  }
                />
              ) : (
                <>
                  <StudyWindowSummary
                    sources={sources}
                    studyWindow={studyWindow}
                    relations={windowRelations}
                  />
                  <div className={styles.sourceList}>
                    {sources.map((source, i) => (
                      <SourceEditor
                        key={projectStore.sourceId(i)}
                        sourceIndex={i}
                        source={source}
                        findings={findings.filter(
                          (f) => f.location?.sourceIndex === i,
                        )}
                        nameOf={catalogName}
                        providerQualified={providerQualified}
                        studyWindow={studyWindow}
                        windowRelation={windowRelations[i] ?? null}
                        axis={axis}
                        vintageYear={bounds.vintageYear}
                      />
                    ))}
                  </div>
                </>
              )}
            </Panel>
          </section>

          <ValidationPanel
            result={validation}
            status={status}
            requestError={requestError}
            requestErrorSource={requestErrorSource}
            findings={findings}
            sources={sources}
            nameOf={catalogName}
            onRetry={() => void projectStore.validate()}
            onRetryOrder={() => void projectStore.downloadOrder()}
          />

          {/* The file's provenance, at the foot of the page: read-only
              deployment-seed stamps, coerced to strings so a malformed opened
              spec still renders. */}
          <footer className={styles.provenance}>
            <h3 className="micro-label">Project provenance</h3>
            <KeyValue
              rows={[
                {
                  label: "Steward",
                  value: String(draft.steward ?? ""),
                  mono: true,
                },
                {
                  label: "reg_meta version",
                  value: String(draft.reg_meta_version ?? ""),
                  mono: true,
                },
                {
                  label: "Schema version",
                  value: String(draft.schema_version ?? ""),
                  mono: true,
                },
              ]}
            />
          </footer>
        </>
      )}
    </article>
  );
}
