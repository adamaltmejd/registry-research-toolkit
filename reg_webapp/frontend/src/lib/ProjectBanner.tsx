import { Link } from "react-aria-components";
import type { Context } from "./api";
import styles from "./ProjectBanner.module.css";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store";
import { router } from "./router";
import { Button } from "./ui/Button";
import { type ProjectValidation, useProject } from "./use_project";

const VERDICTS = {
  checking: { label: "Checking", glyph: "", findings: false },
  ok: { label: "Valid", glyph: "✓", findings: false },
  warnings: { label: "Warnings", glyph: "▲", findings: true },
  errors: { label: "Errors", glyph: "✕", findings: true },
  blocked: { label: "Order blocked", glyph: "✕", findings: true },
};

export function ProjectBanner({
  context,
  validation,
}: {
  context: Context | null;
  validation: ProjectValidation;
}) {
  const { raw, draft, dirty, orderBlocked } = useProject();
  if (raw === null)
    return (
      <div className={styles.empty}>
        <Button
          isInert={!context}
          onPress={() => {
            if (!context) return;
            projectStore.newProject({
              reg_meta_version: regMetaReleaseTag(context.reg_meta_version),
            });
            router.navigate("/project");
          }}
        >
          Create a project
        </Button>
      </div>
    );
  const status = !draft
    ? "errors"
    : orderBlocked
      ? "blocked"
      : validation.loading
        ? "checking"
        : validation.error || !validation.data?.ok
          ? "errors"
          : validation.data.issues.some((issue) => issue.level === "warning")
            ? "warnings"
            : "ok";
  const verdict = VERDICTS[status];
  const sources = draft?.sources.length ?? 0;
  const columns =
    draft?.sources.reduce(
      (total, source) => total + source.bindings.length,
      0,
    ) ?? 0;
  const words = (
    <>
      <span aria-hidden="true">
        {verdict.glyph}
        {verdict.glyph && " "}
      </span>
      {verdict.label}
    </>
  );
  return (
    <section className={styles.banner} aria-label="Project">
      <div className={styles.summary}>
        <div className={styles.titleRow}>
          <Link className={`link ${styles.name}`} href="/project">
            {draft?.name || "Review project file"}
          </Link>
          {dirty && <span className={styles.unsaved}>unsaved</span>}
        </div>
        <div className={styles.meta}>
          {draft && (
            <span>
              <span className={styles.counts}>{sources}</span>{" "}
              {sources === 1 ? "source" : "sources"},{" "}
              <span className={styles.counts}>{columns}</span>{" "}
              {columns === 1 ? "column" : "columns"}
            </span>
          )}
          <span className={styles.status} data-status={status}>
            {Object.values(VERDICTS).map((value) => (
              <span
                key={value.label}
                className={styles.sizer}
                aria-hidden="true"
              >
                {value.glyph} {value.label}
              </span>
            ))}
            {verdict.findings ? (
              <Link
                className={`link ${styles.verdict}`}
                href="/project#project-checks"
                onPress={() => {
                  requestAnimationFrame(() =>
                    document.getElementById("project-checks")?.focus(),
                  );
                }}
              >
                {words}
              </Link>
            ) : (
              <span className={styles.verdict}>{words}</span>
            )}
          </span>
        </div>
      </div>
      {draft?.window && (
        <span className={styles.window}>
          Study window{" "}
          <span className={styles.readout}>
            {draft.window.from}–{draft.window.to}
          </span>
        </span>
      )}
    </section>
  );
}
