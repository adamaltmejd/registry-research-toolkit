import { useRef, useState } from "react";
import {
  FileTrigger,
  Input,
  Label,
  TextArea,
  TextField,
} from "react-aria-components";
import { type Context, errMessage } from "./api";
import { registerRead, segmentsRead } from "./catalog_names";
import { focusAfterRemoval } from "./focus_after_removal";
import { OrderView } from "./OrderView";
import styles from "./ProjectEditor.module.css";
import { dataAxis } from "./period";
import type { ProjectData } from "./project_data";
import { projectStore } from "./project_store";
import { sourcePeriodYears } from "./reg_core";
import { SourceCard } from "./SourceCard";
import { tableYears } from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogNames } from "./use_catalog_names";
import { type ProjectValidation, useProject } from "./use_project";
import { useQueryParam } from "./use_router";
import { useCatalogRead, useDistinct } from "./use_store_read";
import { ValidationPanel } from "./ValidationPanel";
import type { ValidationIssue } from "./validation";
import { createProject, windowStore } from "./window";
import { windowBounds } from "./window_bounds";

export function ProjectEditor({
  context,
  validation,
}: {
  context: Context | null;
  validation: ProjectValidation;
}) {
  const project = useProject();
  const tab = useQueryParam("tab");
  const [pasted, setPasted] = useState("");
  const [fileError, setFileError] = useState<string | null>(null);
  const [pending, setPending] = useState<{
    action: () => void;
    discard?: boolean;
  } | null>(null);
  const raw = project.raw;
  const draft = project.draft;
  // A saved window outside this deployment's catalog span is clamped into it at
  // open, so the exported project never keeps decades the catalog cannot hold (#1037).
  const open = (text: string, fromFile = false) => {
    projectStore.open(text, fromFile);
    const bounds = windowBounds(context);
    if (bounds.enforced && projectStore.draft)
      windowStore.clampTo(bounds.min, bounds.max);
  };
  const replace = (action: () => void) => {
    if (raw === null) action();
    else setPending({ action });
  };
  return (
    <article className={styles.editor}>
      <h1 className={styles.title}>
        {tab === "order" && draft ? "Order" : "Project"}
      </h1>
      <div className={styles.toolbar}>
        <Button
          isInert={!context}
          onPress={() => {
            if (context) replace(() => createProject(context));
          }}
        >
          Create a project
        </Button>
        {raw !== null && (
          <>
            <Button onPress={() => projectStore.download()}>
              Download project_data.json
            </Button>
            <Button
              variant="ghost"
              onPress={() =>
                setPending({
                  action: () => projectStore.discard(),
                  discard: true,
                })
              }
            >
              Discard project
            </Button>
          </>
        )}
      </div>
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
          title="No project is open"
          description="Create a project, or open project_data.json to check it and review its order."
        />
      ) : (
        <>
          <nav
            aria-label="Project pages"
            className={`${styles.toolbar} ${styles.pages}`}
          >
            <a
              className="link"
              href="/project"
              aria-current={tab !== "order" ? "page" : undefined}
            >
              Project
            </a>
            {draft && (
              <a
                className="link"
                href="/project?tab=order"
                aria-current={tab === "order" ? "page" : undefined}
              >
                Order
              </a>
            )}
          </nav>
          {!draft ? (
            <>
              <section
                id="project-checks"
                tabIndex={-1}
                className={styles.checks}
              >
                <ValidationPanel
                  title="Draft check"
                  validMessage="Project structure is valid."
                  result={{ ok: false, issues: project.issues }}
                  raw={raw}
                />
              </section>
            </>
          ) : tab === "order" ? (
            <OrderView draft={draft} />
          ) : (
            <>
              <TextField
                className={styles.field}
                value={draft.name}
                onChange={(name) => projectStore.updateField("name", name)}
              >
                <Label>Project name</Label>
                <Input />
              </TextField>
              <Sources
                context={context}
                draft={draft}
                issues={validation.data?.issues ?? []}
                raw={raw}
              />
              <section
                id="project-checks"
                tabIndex={-1}
                className={styles.checks}
              >
                <ValidationPanel
                  title="Draft check"
                  validMessage="Project structure is valid."
                  result={{ ok: true, issues: [] }}
                  raw={raw}
                />
                <div className={styles.toolbar}>
                  <Button onPress={validation.refresh}>Check catalog</Button>
                </div>
                <ValidationPanel
                  title="Catalog check"
                  validMessage="Project is valid against this catalog."
                  summarizeLocated
                  result={validation.data}
                  raw={raw}
                  loading={validation.loading}
                  error={validation.error}
                />
              </section>
            </>
          )}
        </>
      )}
      {(!draft || tab !== "order") && (
        <Panel title="Open a project">
          <div className={`${styles.toolbar} ${styles.openActions}`}>
            <FileTrigger
              acceptedFileTypes={["application/json", ".json"]}
              onSelect={(files) => {
                const file = files?.[0];
                if (!file) return;
                replace(() => {
                  void file.text().then(
                    (text) => {
                      open(text, true);
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
              <Button onPress={() => {}}>Open project file</Button>
            </FileTrigger>
          </div>
          <TextField
            className={styles.field}
            value={pasted}
            onChange={setPasted}
          >
            <Label>Project JSON</Label>
            <TextArea rows={4} spellCheck={false} />
          </TextField>
          <Button
            isInert={!pasted.trim()}
            onPress={() => {
              if (pasted.trim()) replace(() => open(pasted));
            }}
          >
            Open project
          </Button>
        </Panel>
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
        Download project_data.json first if you want to keep the current draft.{" "}
        {pending?.discard
          ? "Discarding it removes the browser's saved working copy."
          : "Replacing it changes the browser's saved working copy."}
      </ConfirmDialog>
    </article>
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
    <section aria-label="Project sources" className={styles.sources}>
      <h2 ref={heading} tabIndex={-1} className={styles.sourcesHeading}>
        Sources
      </h2>
      {draft.sources.length === 0 ? (
        <EmptyState
          title="No sources in this project"
          description="Browse the catalog to add columns."
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
                  ? "Column coverage is unavailable"
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
