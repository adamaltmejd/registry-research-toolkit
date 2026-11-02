import { useState } from "react";
import { Input, Label, TextArea, TextField } from "react-aria-components";
import { type Context, errMessage, validateProject } from "./api";
import { catalogHref } from "./catalog";
import { OrderView } from "./OrderView";
import styles from "./ProjectEditor.module.css";
import type { ProjectData } from "./project_data";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store";
import { sourcePeriodToWire } from "./reg_core";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogNames } from "./use_catalog_names";
import { useProject } from "./use_project";
import { useQueryParam } from "./use_router";
import { LocatedIssue, ValidationPanel } from "./ValidationPanel";
import { issueId, issuePlace, type ValidationIssue } from "./validation";

export function ProjectEditor({ context }: { context: Context | null }) {
  const project = useProject();
  const tab = useQueryParam("tab");
  const [pasted, setPasted] = useState("");
  const [fileError, setFileError] = useState<string | null>(null);
  const [pending, setPending] = useState<{
    action: () => void;
    discard?: boolean;
  } | null>(null);
  const [checkRun, setCheckRun] = useState(0);
  const raw = project.raw;
  const draft = project.draft;
  const validation = useAsyncResource(
    draft ? `${raw}:${checkRun}` : null,
    async (signal) => {
      // Debounce the catalog boundary while typing; WASM has already checked this edit.
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 350);
        signal.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            reject(signal.reason);
          },
          { once: true },
        );
      });
      if (!draft)
        throw new Error("Open a supported project before checking the catalog");
      return validateProject(draft, { signal });
    },
  );
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
            if (context)
              replace(() =>
                projectStore.newProject({
                  reg_meta_version: regMetaReleaseTag(context.reg_meta_version),
                }),
              );
          }}
        >
          Create a new project
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
          <nav aria-label="Project pages" className={styles.toolbar}>
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
              <StatusRow tone="error">
                This file cannot be opened as a current project. Download it as
                it is, or discard it and open a supported project_data.json.
              </StatusRow>
              <ValidationPanel
                title="Draft check"
                result={{ ok: false, issues: project.issues }}
                raw={raw}
              />
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
                draft={draft}
                issues={validation.data?.issues ?? []}
                raw={raw}
              />
              <ValidationPanel
                title="Draft check"
                result={{ ok: true, issues: [] }}
                raw={raw}
              />
              <Button onPress={() => setCheckRun((run) => run + 1)}>
                Check catalog
              </Button>
              <ValidationPanel
                title="Catalog check"
                summarizeLocated
                result={validation.data}
                raw={raw}
                loading={validation.loading}
                error={validation.error}
              />
            </>
          )}
        </>
      )}
      <Panel title="Open a project">
        <label className={styles.field}>
          <span>Open project file</span>
          <input
            type="file"
            accept="application/json,.json"
            onChange={(event) => {
              const file = event.target.files?.[0];
              event.target.value = "";
              if (!file) return;
              const action = async () => {
                try {
                  projectStore.open(await file.text());
                  setFileError(null);
                } catch (error) {
                  setFileError(
                    `Could not read the file: ${errMessage(error)}. Choose it again.`,
                  );
                }
              };
              // An explicit file selection opens the selected document. The old draft
              // remains recoverable by Cancel when replacing it.
              replace(() => {
                void action();
              });
            }}
          />
        </label>
        <TextField className={styles.field} value={pasted} onChange={setPasted}>
          <Label>Project JSON</Label>
          <TextArea rows={4} spellCheck={false} />
        </TextField>
        <Button
          isInert={!pasted.trim()}
          onPress={() => {
            if (pasted.trim()) replace(() => projectStore.open(pasted));
          }}
        >
          Open project
        </Button>
      </Panel>
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

function Sources({
  draft,
  issues,
  raw,
}: {
  draft: ProjectData;
  issues: ValidationIssue[];
  raw: string | null;
}) {
  const names = useCatalogNames(
    draft.sources.flatMap((source) => [
      source.table,
      ...source.bindings.map((binding) => binding.variable),
    ]),
  );
  if (draft.sources.length === 0)
    return (
      <EmptyState
        title="No sources in this project"
        description="Open a project_data.json with sources to review its order."
      />
    );
  return (
    <section aria-label="Project sources" className={styles.sources}>
      {draft.sources.map((source, sourceIndex) => (
        <Panel key={source.name} title={source.name}>
          <ul className={styles.plainList}>
            {issues
              .filter((issue) => {
                const place = issuePlace(issue, raw);
                return (
                  place.source === source.name &&
                  !issue.path.startsWith(`/sources/${sourceIndex}/bindings/`)
                );
              })
              .map((issue) => (
                <LocatedIssue key={issueId(issue)} issue={issue} raw={raw} />
              ))}
          </ul>
          <p className={styles.sourceMeta}>
            <a className="link" href={catalogHref(source.table)}>
              {names.get(source.table) ?? "Unknown table"}
            </a>{" "}
            <span>
              Period: {sourcePeriodToWire(source.period) ?? "Not stated"}
            </span>
          </p>
          <ul className={styles.plainList}>
            {source.bindings.map((binding, bindingIndex) => {
              const bindingPath = `/sources/${sourceIndex}/bindings/${bindingIndex}`;
              return (
                <li key={bindingPath} className={styles.binding}>
                  <a className="link" href={catalogHref(binding.variable)}>
                    {names.get(binding.variable) ?? "Unknown variable"}
                  </a>
                  {binding.display_name && (
                    <span>
                      Output name: <code>{binding.display_name}</code>
                    </span>
                  )}
                  <ul className={styles.plainList}>
                    {issues
                      .filter((issue) => {
                        const place = issuePlace(issue, raw);
                        return (
                          place.source === source.name &&
                          (issue.path === bindingPath ||
                            issue.path.startsWith(`${bindingPath}/`))
                        );
                      })
                      .map((issue) => (
                        <LocatedIssue
                          key={issueId(issue)}
                          issue={issue}
                          raw={raw}
                        />
                      ))}
                  </ul>
                </li>
              );
            })}
          </ul>
        </Panel>
      ))}
    </section>
  );
}
