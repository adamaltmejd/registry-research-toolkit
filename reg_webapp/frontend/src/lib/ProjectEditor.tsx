import { useRef, useState } from "react";
import { FileTrigger, Input, Label, TextField } from "react-aria-components";
import {
  type CatalogSegment,
  type Context,
  errMessage,
  stewardConfig,
} from "./api";
import { registerRead, tableSegmentsReads } from "./catalog_names";
import { DeliverySummary } from "./DeliverySummary";
import { stewardDatabases } from "./databases";
import { IssueLink, LocatedIssue } from "./Finding";
import { focusAfterRemoval } from "./focus_after_removal";
import styles from "./ProjectEditor.module.css";
import { dataAxis, sourcePeriodYearWindows } from "./period";
import type { ProjectData } from "./project_data";
import { type ProjectSnapshot, projectStore } from "./project_store";
import { SourceCard } from "./SourceCard";
import { tableYears } from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { Select } from "./ui/Select";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogNames } from "./use_catalog_names";
import {
  DATABASE_CODES,
  type ProjectCheck,
  projectVerdict,
  useProject,
  VERDICT_WORDS,
  type Verdict,
} from "./use_project";
import { useCatalogRead, useDistinct } from "./use_store_read";
import { issueId, type ValidationIssue } from "./validation";
import { createProject, windowStore } from "./window";
import { windowBounds } from "./window_bounds";

export function ProjectEditor({
  context,
  validation,
}: {
  context: Context | null;
  validation: ProjectCheck;
}) {
  const project = useProject();
  const [fileError, setFileError] = useState<string | null>(null);
  const [pending, setPending] = useState<{
    action: () => void;
    discard?: boolean;
  } | null>(null);
  const raw = project.raw;
  const draft = project.draft;
  // While a check is in flight the page keeps the previous one, marked busy.
  const busy = validation.loading && validation.previous !== null;
  const shown: ProjectCheck = busy
    ? { ...validation, ...validation.previous, loading: false }
    : validation;
  const config = stewardConfig(context);
  const instructions = config?.presentation.order_instructions ?? null;
  // A steward with several databases, each with its own person key: an order
  // draws from one, so the project names it (`linkage.database`). With one
  // database there is nothing to choose and nothing is shown.
  const databases = stewardDatabases(context);
  const database = draft?.linkage?.database ?? null;
  // The client gate, from `/api/context` with no round trip: the server's
  // check may stop before it reaches the database. An id the steward does
  // not have counts as no choice (the server names it `database_unknown`).
  const needsDatabase =
    draft !== null &&
    databases !== null &&
    !databases.some((space) => space.id === database);
  // A saved window outside this deployment's catalog span is clamped into it at
  // open, so the exported project never keeps decades the catalog cannot hold (#1037).
  const open = (text: string) => {
    projectStore.open(text);
    const bounds = windowBounds(context);
    if (bounds.enforced && projectStore.draft)
      windowStore.clampTo(bounds.min, bounds.max);
  };
  const replace = (action: () => void) => {
    if (raw === null) action();
    else setPending({ action });
  };
  const openFile = (
    <FileTrigger
      acceptedFileTypes={["application/json", ".json"]}
      onSelect={(files) => {
        const file = files?.[0];
        if (!file) return;
        replace(() => {
          void file.text().then(
            (text) => {
              open(text);
              setFileError(null);
            },
            (error: unknown) =>
              setFileError(
                `Could not read the file: ${errMessage(error)}. Choose it again.`,
              ),
          );
        });
      }}
    >
      <Button onPress={() => {}}>Open a project file…</Button>
    </FileTrigger>
  );
  return (
    <article className={styles.editor}>
      <h1 className={styles.title}>Project</h1>
      {project.storageError && (
        <StatusRow tone="error" live="alert">
          {project.storageError}
        </StatusRow>
      )}
      {fileError && (
        <StatusRow tone="error" live="alert">
          {fileError}
        </StatusRow>
      )}
      {raw === null ? (
        <EmptyState
          title="No project yet"
          description="Create one, or open a project_data.json."
          action={
            <div className={styles.toolbar}>
              <Button
                isInert={!context}
                onPress={() => {
                  if (context) createProject(context);
                }}
              >
                Create a project
              </Button>
              {openFile}
            </div>
          }
        />
      ) : (
        <>
          {draft && (
            <TextField
              className={styles.field}
              value={draft.name}
              onChange={(name) => projectStore.updateField("name", name)}
            >
              {/* The shared form-field label, as `ui/Select` draws it (#2341). */}
              <Label className="micro-label">Project name</Label>
              <Input />
            </TextField>
          )}
          {draft && databases && (
            <div className={styles.field}>
              <Select
                label="Database"
                placeholder="Choose a database"
                value={needsDatabase ? null : database}
                options={databases}
                onChange={(id) =>
                  projectStore.updateField("linkage", { database: id })
                }
              />
            </div>
          )}
          <div className={styles.toolbar}>
            <Button
              isInert={needsDatabase}
              aria-describedby={needsDatabase ? DATABASE_REASON : undefined}
              onPress={() => {
                if (!needsDatabase) projectStore.download();
              }}
            >
              Download project_data.json
            </Button>
            {openFile}
            <Button
              variant="ghost"
              onPress={() =>
                setPending({
                  action: () => projectStore.discard(),
                  discard: true,
                })
              }
            >
              Discard
            </Button>
          </div>
          {instructions && (
            <p className={styles.instructions}>{instructions}</p>
          )}
          <Check
            project={project}
            validation={shown}
            busy={busy}
            databaseShown={draft !== null && databases !== null}
            needsDatabase={needsDatabase}
          />
          {draft && (
            <Sources
              context={context}
              draft={draft}
              issues={shown.data?.issues ?? []}
              raw={raw}
            />
          )}
          {draft &&
            shown.data?.order &&
            ["ready", "warnings"].includes(
              projectVerdict(project, shown, databases !== null),
            ) && <DeliverySummary order={shown.data.order} busy={busy} />}
        </>
      )}
      <ConfirmDialog
        isOpen={pending !== null}
        onOpenChange={(open) => {
          if (!open) setPending(null);
        }}
        title={
          pending?.discard
            ? "Discard the working project?"
            : "Replace the working project?"
        }
        actions={
          <>
            <Button onPress={() => setPending(null)}>Cancel</Button>
            <Button
              variant="danger"
              onPress={() => {
                pending?.action();
                setPending(null);
              }}
            >
              {pending?.discard ? "Discard project" : "Replace project"}
            </Button>
          </>
        }
      >
        Download project_data.json first if you want to keep the current
        project.{" "}
        {pending?.discard
          ? "Discarding it removes the browser's saved working copy."
          : "Replacing it changes the browser's saved working copy."}
      </ConfirmDialog>
    </article>
  );
}

/** The row that says why Download refuses, which the button points at. */
const DATABASE_REASON = "project-database-reason";

const count = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** The verdict in the banner's words, then the findings no table card holds in
 * full and a plain link to each one a card holds. The banner's verdict link
 * lands here (`#project-checks`). */
function Check({
  project,
  validation,
  busy,
  databaseShown,
  needsDatabase,
}: {
  project: ProjectSnapshot;
  validation: ProjectCheck;
  busy: boolean;
  /** The Database select is on the page (several databases, a readable draft). */
  databaseShown: boolean;
  needsDatabase: boolean;
}) {
  const checked = projectVerdict(project, validation, databaseShown);
  // A database not yet chosen: with the select on the page, the info row above
  // is the whole answer (a choice to make, not an error, #2345); once one is
  // chosen, the previous check still says it until the next arrives, so the
  // page says Checking. A chosen database the server still refuses, or no
  // select (no `/api/context`), lists the finding as a problem.
  const verdict: Verdict =
    checked !== "database"
      ? checked
      : needsDatabase
        ? "database"
        : busy
          ? "checking"
          : "blocked";
  const structural = project.draft === null;
  // While the select and the row above the verdict say what `database_required`
  // and `database_unknown` say, in the page's words, neither finding is listed
  // nor counted again; so too while the check after a choice runs, since the
  // previous check, kept on screen, still carries it.
  const hideDatabase = databaseShown && (needsDatabase || busy);
  const issues: ValidationIssue[] = (
    structural ? project.issues : (validation.data?.issues ?? [])
  ).filter((issue) => !hideDatabase || !DATABASE_CODES.includes(issue.code));
  const onCard = (issue: ValidationIssue) =>
    !structural && issue.path.startsWith("/sources/");
  // A note on a card (level info) needs no link here: it changes no verdict.
  const listed = issues.filter(
    (issue) => !onCard(issue) || issue.level !== "info",
  );
  const problems = issues.filter((issue) => issue.level === "error").length;
  const warnings = issues.filter((issue) => issue.level === "warning").length;
  // An empty project has no verdict, but Download still needs its reason.
  if (verdict === "empty" && !needsDatabase) return null;
  // A ready project with nothing listed shows nothing here (#2417): the
  // section stays for its live region but is no named landmark and takes no
  // space in the editor's column.
  const silent = verdict === "ready" && !needsDatabase && listed.length === 0;
  return (
    <section
      id="project-checks"
      tabIndex={-1}
      aria-label={silent ? undefined : "Check"}
      aria-busy={busy || verdict === "checking" || undefined}
      className={`${styles.check}${silent ? ` ${styles.silent}` : ""}`}
    >
      {needsDatabase && (
        <StatusRow tone="info" id={DATABASE_REASON}>
          Choose the database your data comes from.
        </StatusRow>
      )}
      {verdict === "empty" ? null : verdict === "checking" ? (
        // The verdict word, plain as in the banner: no glyph, no tone.
        <p className={styles.checking}>{VERDICT_WORDS.checking}</p>
      ) : validation.error && !structural ? (
        <StatusRow
          tone="error"
          live="alert"
          action={<Button onPress={validation.retry}>Retry</Button>}
        >
          The check did not run ({validation.error}). Retry, or reload the page.
        </StatusRow>
      ) : verdict === "database" ? null : verdict === "ready" ? (
        // A ready project shows no verdict (#2417): the row stays as an empty
        // live region, so a later verdict lands in a region the page had.
        <StatusRow tone={null} live="status" />
      ) : (
        <StatusRow
          tone={verdict === "warnings" ? "warn" : "error"}
          live="status"
        >
          {verdict === "warnings"
            ? `${VERDICT_WORDS[verdict]}: ${count(warnings, "warning")}. Review ${warnings === 1 ? "it" : "them"} before you download.`
            : structural
              ? `${VERDICT_WORDS[verdict]}: this file is not a project_data.json the site can read. Fix the ${count(problems, "problem")} below and open it again.`
              : problems === 0
                ? `${VERDICT_WORDS[verdict]}.`
                : `${VERDICT_WORDS[verdict]}: ${count(problems, "problem")}. Fix ${problems === 1 ? "it" : "them"} below.`}
        </StatusRow>
      )}
      {listed.length > 0 && !["checking", "empty"].includes(verdict) && (
        <ul className={styles.issues} aria-label="Problems and warnings">
          {listed.map((issue) =>
            onCard(issue) ? (
              <IssueLink key={issueId(issue)} issue={issue} raw={project.raw} />
            ) : (
              <LocatedIssue
                key={issueId(issue)}
                issue={issue}
                raw={project.raw}
              />
            ),
          )}
        </ul>
      )}
    </section>
  );
}

const readRegisters = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((ref) => [ref, registerRead(ref)]));
const columnKey = (table: string, variable: string) =>
  JSON.stringify([table, variable]);
/** Every source's columns' rows in its table, keyed by `columnKey`. */
const readSegments = (columns: readonly string[]) => {
  const byTable = new Map<string, string[]>();
  for (const column of columns) {
    const [table, variable] = JSON.parse(column) as [string, string];
    byTable.set(table, [...(byTable.get(table) ?? []), variable]);
  }
  return Object.fromEntries(
    [...byTable].flatMap(([table, variables]) =>
      Object.entries(tableSegmentsReads(table, variables)).map(
        ([variable, read]) => [columnKey(table, variable), read],
      ),
    ),
  );
};

function Sources({
  draft,
  issues,
  raw,
  context,
}: {
  draft: ProjectData;
  issues: ValidationIssue[];
  raw: string | null;
  context: Context | null;
}) {
  const heading = useRef<HTMLHeadingElement>(null);
  const registers = useCatalogRead(
    readRegisters,
    useDistinct(
      draft.sources.map((source) =>
        source.table.split("/").slice(0, 2).join("/"),
      ),
    ),
  );
  const variables = useDistinct(
    draft.sources.flatMap((source) =>
      source.bindings.map((binding) => binding.variable),
    ),
  );
  const segmentReads = useCatalogRead(
    readSegments,
    useDistinct(
      draft.sources.flatMap((source) =>
        source.bindings.map((binding) =>
          columnKey(source.table, binding.variable),
        ),
      ),
    ),
  );
  // By variable, over every table it is a column of: each card keeps its own
  // table's rows.
  const segments = new Map<string, CatalogSegment[]>();
  for (const read of Object.values(segmentReads)) {
    for (const row of read.value ?? [])
      segments.set(row.variable, [...(segments.get(row.variable) ?? []), row]);
  }
  const columnRead = (table: string, variable: string) =>
    segmentReads[columnKey(table, variable)];
  const names = useCatalogNames([
    ...draft.sources.map((source) => source.table),
    ...variables,
    ...[...segments.values()].flatMap((items) =>
      items.flatMap((segment) =>
        segment.classification ? [segment.classification] : [],
      ),
    ),
    ...draft.sources.flatMap((source) =>
      source.bindings.flatMap((binding) =>
        binding.classification ? [binding.classification] : [],
      ),
    ),
  ]);
  const catalog = windowBounds(context);
  const bounds: [number, number] = [catalog.min, catalog.max];
  const tables = Object.values(registers).flatMap(
    (read) => read.value?.tables ?? [],
  );
  const axis = dataAxis(
    [
      ...tables
        .filter((table) =>
          draft.sources.some((source) => source.table === table.fqid),
        )
        .flatMap((table) => {
          const years = tableYears(table, bounds);
          return years ? [years.from, years.to] : [];
        }),
      ...draft.sources.flatMap(
        (source) =>
          sourcePeriodYearWindows(source.period)?.flatMap(({ from, to }) => [
            from,
            to,
          ]) ?? [],
      ),
    ],
    draft.window ?? null,
    bounds,
  );
  return (
    <section aria-labelledby="project-tables" className={styles.sources}>
      <h2
        id="project-tables"
        ref={heading}
        tabIndex={-1}
        className={styles.sourcesHeading}
      >
        Tables
      </h2>
      {draft.sources.length === 0 ? (
        <EmptyState
          title="No tables in this project"
          description="Browse the registers and add columns from a variable's page."
        />
      ) : (
        draft.sources.map((source, sourceIndex) => {
          const registerOf = (table: string) =>
            table.split("/").slice(0, 2).join("/");
          const register = registers[registerOf(source.table)];
          return (
            <SourceCard
              key={source.name}
              draft={draft}
              sourceIndex={sourceIndex}
              issues={issues}
              raw={raw}
              names={names}
              segments={segments}
              table={tables.find((table) => table.fqid === source.table)}
              sharedHeading={draft.sources.some(
                (other) =>
                  other.table !== source.table &&
                  registerOf(other.table) === registerOf(source.table),
              )}
              axis={axis}
              bounds={bounds}
              loading={
                !!register?.loading ||
                source.bindings.some(
                  (binding) =>
                    columnRead(source.table, binding.variable)?.loading,
                )
              }
              loadError={
                (source.bindings.some((binding) => {
                  const read = columnRead(source.table, binding.variable);
                  return read && !read.loading && read.value === null;
                })
                  ? "Column years are unavailable"
                  : null) ??
                (register && !register.loading && !register.value
                  ? "Table availability is unavailable"
                  : null)
              }
              onRemoved={() => focusAfterRemoval([() => heading.current])}
            />
          );
        })
      )}
    </section>
  );
}
