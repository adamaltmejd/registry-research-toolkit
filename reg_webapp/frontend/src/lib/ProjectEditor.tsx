import { useState } from "react";
import {
  FileTrigger,
  Input,
  Label,
  TextArea,
  TextField,
} from "react-aria-components";
import { type Context, errMessage } from "./api";
import { catalogHref } from "./catalog";
import { sourcePeriodLabel } from "./catalog_period";
import { OrderView } from "./OrderView";
import styles from "./ProjectEditor.module.css";
import type { ProjectData } from "./project_data";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogName } from "./use_catalog_names";
import { type ProjectValidation, useProject } from "./use_project";
import { useQueryParam } from "./use_router";
import { LocatedIssue, ValidationPanel } from "./ValidationPanel";
import { issueId, issuePlace, type ValidationIssue } from "./validation";

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
      {tab !== "order" && (
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
                      projectStore.open(text, true);
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
              if (pasted.trim()) replace(() => projectStore.open(pasted));
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

function Sources({
  draft,
  issues,
  raw,
}: {
  draft: ProjectData;
  issues: ValidationIssue[];
  raw: string | null;
}) {
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
            <CatalogNameLink fqid={source.table} kind="Table" />{" "}
            <span>
              Period:{" "}
              {source.period === "_default" ? (
                "Unbounded"
              ) : (
                <code>{sourcePeriodLabel(source.period)}</code>
              )}
            </span>
          </p>
          <ul className={styles.plainList}>
            {source.bindings.map((binding, bindingIndex) => {
              const bindingPath = `/sources/${sourceIndex}/bindings/${bindingIndex}`;
              return (
                <li key={bindingPath} className={styles.binding}>
                  <CatalogNameLink fqid={binding.variable} kind="Variable" />
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

function CatalogNameLink({ fqid, kind }: { fqid: string; kind: string }) {
  const name = useCatalogName(fqid);
  if (name.loading) return null;
  return (
    <a className="link" href={catalogHref(fqid)}>
      {name.value ?? `${kind} name unavailable`}
    </a>
  );
}
