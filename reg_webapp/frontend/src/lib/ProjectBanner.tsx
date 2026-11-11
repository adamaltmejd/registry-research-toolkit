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
import { stewardDatabases } from "./databases";
import styles from "./ProjectBanner.module.css";
import { clampYearWindow } from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { projectStore } from "./project_store";
import { router } from "./router";
import { Button } from "./ui/Button";
import band from "./ui/WindowBand.module.css";
import {
  type ProjectCheck,
  projectVerdict,
  useProject,
  VERDICT_WORDS,
  type Verdict,
} from "./use_project";
import { useRoute } from "./use_router";
import { createProject } from "./window";
import { type WindowBounds, windowBounds } from "./window_bounds";

// A ready project shows no verdict and no slot in the box (#2417); /project
// still says it.
const GLYPHS: Record<Exclude<Verdict, "ready">, string> = {
  checking: "",
  empty: "",
  warnings: "▲",
  problems: "✕",
  blocked: "✕",
  database: "",
  unchecked: "",
};

/** Verdicts that /project explains (with findings, or with Retry for a check
 * that did not run): the banner links to them. */
const EXPLAINED: ReadonlySet<Verdict> = new Set([
  "warnings",
  "problems",
  "blocked",
  "database",
  "unchecked",
]);

export function ProjectBanner({
  context,
  validation,
}: {
  context: Context | null;
  validation: ProjectCheck;
}) {
  const snapshot = useProject();
  const route = useRoute();
  const { raw, draft, dirty } = snapshot;
  // /project offers its own "Create a project" with "Open a project file": the
  // box stays, hidden, so the top bar keeps its height there.
  const hidden = route.name === "project";
  if (raw === null)
    return (
      <div
        className={styles.empty}
        data-hidden={hidden || undefined}
        aria-hidden={hidden || undefined}
      >
        <Button
          isInert={!context || hidden}
          onPress={() => {
            if (!context) return;
            createProject(context);
            router.navigate("/project");
          }}
        >
          Create a project
        </Button>
      </div>
    );
  const status = projectVerdict(
    snapshot,
    validation,
    stewardDatabases(context) !== null,
  );
  const sources = draft?.sources.length ?? 0;
  const columns =
    draft?.sources.reduce(
      (total, source) => total + source.bindings.length,
      0,
    ) ?? 0;
  const words = status !== "ready" && (
    <>
      <span aria-hidden="true">
        {GLYPHS[status]}
        {GLYPHS[status] && " "}
      </span>
      {VERDICT_WORDS[status]}
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
              {sources === 1 ? "table" : "tables"},{" "}
              <span className={styles.counts}>{columns}</span>{" "}
              {columns === 1 ? "column" : "columns"}
            </span>
          )}
          {words && (
            <span className={styles.status} data-status={status}>
              {(Object.keys(GLYPHS) as (keyof typeof GLYPHS)[]).map((value) => (
                <span key={value} className={styles.sizer} aria-hidden="true">
                  {GLYPHS[value]} {VERDICT_WORDS[value]}
                </span>
              ))}
              {EXPLAINED.has(status) ? (
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
          )}
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
        <Label className="micro-label">Study years</Label>
        <SliderOutput className={styles.readout}>
          {({ state }) =>
            active === null && dragging === null ? (
              <span className={styles.unset}>Not set</span>
            ) : (
              `${state.getThumbValueLabel(0)}–${state.getThumbValueLabel(1)}`
            )
          }
        </SliderOutput>
        {/* Hidden, not removed, while there is nothing to clear: the banner
            keeps the button's height and does not grow when it appears. */}
        <span
          className={styles.clear}
          data-hidden={active === null || undefined}
        >
          <Button
            variant="ghost"
            isInert={active === null}
            onPress={() => {
              setDragging(null);
              projectStore.updateField("window", undefined);
              startThumb.current?.querySelector("input")?.focus();
            }}
          >
            Clear
          </Button>
        </span>
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
