import { useRef, useState } from "react";
import { FileTrigger, Input, Label, TextField } from "react-aria-components";
import { type Context, errMessage, stewardConfig } from "./api";
import { registerRead, segmentsRead } from "./catalog_names";
import { DeliverySummary } from "./DeliverySummary";
import { IssueLink, LocatedIssue } from "./Finding";
import { focusAfterRemoval } from "./focus_after_removal";
import styles from "./ProjectEditor.module.css";
import { dataAxis } from "./period";
import type { ProjectData } from "./project_data";
import { type ProjectSnapshot, projectStore } from "./project_store";
import { sourcePeriodYears } from "./reg_core";
import { SourceCard } from "./SourceCard";
import { tableYears } from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogNames } from "./use_catalog_names";
import {
  type ProjectCheck,
  projectVerdict,
  useProject,
  VERDICT_WORDS,
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
  const instructions =
    stewardConfig(context)?.presentation.order_instructions ?? null;
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
              <Label>Project name</Label>
              <Input />
            </TextField>
          )}
          <div className={styles.toolbar}>
            <Button onPress={() => projectStore.download()}>
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
          <Check project={project} validation={validation} />
          {draft && (
            <Sources
              context={context}
              draft={draft}
              issues={validation.data?.issues ?? []}
              raw={raw}
            />
          )}
          {draft &&
            validation.data?.order &&
            // A check in flight keeps the last order: show none until it lands.
            ["ready", "warnings"].includes(
              projectVerdict(project, validation),
            ) && <DeliverySummary order={validation.data.order} />}
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

const count = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

/** The verdict in the banner's words, then the findings no table card holds in
 * full and a plain link to each one a card holds. The banner's verdict link
 * lands here (`#project-checks`). */
function Check({
  project,
  validation,
}: {
  project: ProjectSnapshot;
  validation: ProjectCheck;
}) {
  const verdict = projectVerdict(project, validation);
  const words = VERDICT_WORDS[verdict];
  const structural = project.draft === null;
  const issues: ValidationIssue[] = structural
    ? project.issues
    : (validation.data?.issues ?? []);
  const onCard = (issue: ValidationIssue) =>
    !structural && issue.path.startsWith("/sources/");
  // A note on a card (level info) needs no link here: it changes no verdict.
  const listed = issues.filter(
    (issue) => !onCard(issue) || issue.level !== "info",
  );
  const problems = issues.filter((issue) => issue.level === "error").length;
  const warnings = issues.filter((issue) => issue.level === "warning").length;
  if (verdict === "empty") return null;
  return (
    <section
      id="project-checks"
      tabIndex={-1}
      aria-label="Check"
      className={styles.check}
    >
      {verdict === "checking" ? (
        <div aria-busy="true">
          <Skeleton />
        </div>
      ) : validation.error && !structural ? (
        <StatusRow
          tone="error"
          live="alert"
          action={<Button onPress={validation.retry}>Retry</Button>}
        >
          The check did not run ({validation.error}). Retry, or reload the page.
        </StatusRow>
      ) : (
        <StatusRow
          tone={
            verdict === "ready"
              ? "ok"
              : verdict === "warnings"
                ? "warn"
                : "error"
          }
          live="status"
        >
          {verdict === "ready"
            ? `${words}.`
            : verdict === "warnings"
              ? `${words}: ${count(warnings, "warning")}. Review ${warnings === 1 ? "it" : "them"} before you order.`
              : structural
                ? `${words}: this file is not a project_data.json the site can read. Fix the ${count(problems, "problem")} below and open it again.`
                : problems === 0
                  ? `${words}.`
                  : `${words}: ${count(problems, "problem")}. Fix ${problems === 1 ? "it" : "them"} below.`}
        </StatusRow>
      )}
      {listed.length > 0 && verdict !== "checking" && (
        <ul className={styles.issues} aria-label="Check findings">
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
const readSegments = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((ref) => [ref, segmentsRead(ref)]));

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
  const segmentReads = useCatalogRead(readSegments, variables);
  const segments = new Map(
    Object.entries(segmentReads).flatMap(([ref, read]) =>
      read.value ? [[ref, read.value] as const] : [],
    ),
  );
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
        (source) => sourcePeriodYears(source.period)?.flat() ?? [],
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
          const register =
            registers[source.table.split("/").slice(0, 2).join("/")];
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
              axis={axis}
              bounds={bounds}
              loading={
                !!register?.loading ||
                source.bindings.some(
                  (binding) => segmentReads[binding.variable]?.loading,
                )
              }
              loadError={
                (source.bindings.some((binding) => {
                  const read = segmentReads[binding.variable];
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
