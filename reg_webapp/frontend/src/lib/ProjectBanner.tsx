import { useRef, useState } from "react";
import {
  Label,
  Link,
  Slider,
  SliderFill,
  SliderOutput,
  SliderThumb,
  SliderTrack,
} from "react-aria-components";
import type { Context } from "./api";
import styles from "./ProjectBanner.module.css";
import { clampYearWindow } from "./period";
import { type ProjectStudyWindow, regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store";
import { router } from "./router";
import { Button } from "./ui/Button";
import band from "./ui/WindowBand.module.css";
import { type ProjectValidation, useProject } from "./use_project";
import { windowStore } from "./window";
import {
  defaultStudyWindow,
  type WindowBounds,
  windowBounds,
} from "./window_bounds";

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
            const bounds = windowBounds(context);
            projectStore.newProject({
              reg_meta_version: regMetaReleaseTag(context.reg_meta_version),
              window: clampYearWindow(
                windowStore.value ?? defaultStudyWindow(bounds),
                bounds.min,
                bounds.max,
              ),
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
      {draft && context && (
        <StudyWindow
          active={draft.window ?? null}
          bounds={windowBounds(context)}
        />
      )}
    </section>
  );
}

const YEAR_FORMAT: Intl.NumberFormatOptions = { useGrouping: false };

function StudyWindow({
  active,
  bounds,
}: {
  active: ProjectStudyWindow | null;
  bounds: WindowBounds;
}) {
  const [dragging, setDragging] = useState<number[] | null>(null);
  const startThumb = useRef<HTMLDivElement>(null);
  const value = dragging ?? [
    active?.from ?? bounds.min,
    active?.to ?? bounds.max,
  ];
  return (
    <Slider
      className={styles.window}
      value={value}
      minValue={bounds.min}
      // A disabled one-year catalog still needs a non-zero drawing scale.
      maxValue={Math.max(bounds.max, bounds.min + 1)}
      isDisabled={bounds.min === bounds.max}
      formatOptions={YEAR_FORMAT}
      onChange={setDragging}
      onChangeEnd={([from, to]) => {
        setDragging(null);
        projectStore.updateField(
          "window",
          clampYearWindow({ from, to }, bounds.min, bounds.max),
        );
      }}
    >
      <div className={styles.windowHead}>
        <Label className="micro-label">Study window</Label>
        <SliderOutput className={styles.readout}>
          {({ state }) =>
            active === null && dragging === null
              ? "Not set"
              : `${state.getThumbValueLabel(0)}–${state.getThumbValueLabel(1)}`
          }
        </SliderOutput>
        {active !== null && (
          <span className={styles.clear}>
            <Button
              variant="ghost"
              onPress={() => {
                setDragging(null);
                projectStore.updateField("window", undefined);
                startThumb.current?.querySelector("input")?.focus();
              }}
            >
              Clear
            </Button>
          </span>
        )}
      </div>
      <SliderTrack className={styles.track}>
        {(active !== null || dragging !== null) && (
          <SliderFill className={`${band.band} ${styles.fill}`} />
        )}
        <SliderThumb
          ref={startThumb}
          index={0}
          aria-label="From year"
          className={`${band.thumb} ${styles.thumb}`}
        />
        <SliderThumb
          index={1}
          aria-label="To year"
          className={`${band.thumb} ${styles.thumb}`}
        />
      </SliderTrack>
    </Slider>
  );
}
