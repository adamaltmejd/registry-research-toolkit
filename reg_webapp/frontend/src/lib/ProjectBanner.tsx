import { useState } from "react";
import {
  Label,
  Link,
  Slider,
  SliderFill,
  SliderOutput,
  SliderThumb,
  SliderTrack,
} from "react-aria-components";
import styles from "./ProjectBanner.module.css";
import {
  regMetaReleaseTag,
  safeSourceBindings,
  safeSourceSlots,
} from "./project_data";
import { projectStore, type ValidationStatus } from "./project_store";
import { router } from "./router";
import { Button } from "./ui/Button";
import { useStudyWindow } from "./use_project";
import { useProjectRead } from "./use_store_read";
import { windowStore } from "./window";
import type { WindowBounds } from "./window_bounds";

/** The banner's verdict: the draft's validation, except that a source the study
 * window has moved off blocks the ORDER whatever validation said — the panel's
 * own "Order blocked" wording, so the banner never reads "Warnings" or "Draft
 * valid" beside a closed order download. A failing or in-flight validation still
 * wins: it is the more specific thing to fix first. */
type BannerStatus = ValidationStatus | "blocked";

// The verdict, glyph first (DESIGN.md → ProjectBanner): each verdict leads with
// its glyph from the status set, so none is told apart by hue alone. Unchecked
// and in-flight are not verdicts: plain muted "Checking", no glyph. "Valid"
// speaks for the draft; a window-blocked order still reads "Order blocked".
const STATUS: Record<BannerStatus, { label: string; glyph: string | null }> = {
  unchecked: { label: "Checking", glyph: null },
  checking: { label: "Checking", glyph: null },
  ok: { label: "Valid", glyph: "✓" },
  warnings: { label: "Warnings", glyph: "▲" },
  errors: { label: "Errors", glyph: "✕" },
  blocked: { label: "Order blocked", glyph: "✕" },
};

interface ProjectSummary {
  open: boolean;
  name: string;
  sources: number;
  columns: number;
  status: BannerStatus;
  dirty: boolean;
}

function readProject(): ProjectSummary {
  const draft = projectStore.draft;
  const sources = safeSourceSlots(draft?.sources);
  const status = projectStore.validationStatus;
  const name = typeof draft?.name === "string" ? draft.name.trim() : "";
  return {
    open: draft != null,
    name: name || "Untitled project",
    sources: sources.length,
    columns: sources.reduce((n, s) => n + safeSourceBindings(s).length, 0),
    status:
      projectStore.windowBlocked && (status === "ok" || status === "warnings")
        ? "blocked"
        : status,
    dirty: projectStore.dirty,
  };
}

function plural(count: number, one: string, many: string): string {
  return `${count} ${count === 1 ? one : many}`;
}

// Years are identifiers, not quantities: "2018", never "2,018".
const YEAR_FORMAT: Intl.NumberFormatOptions = { useGrouping: false };

/**
 * The project's presence in the top bar (DESIGN.md → Components → ProjectBanner):
 * name, source and column counts, the verdict, the save state and the study
 * window, which is the period picker. Until a project is started it is an
 * empty box holding one "Create a project" button, which starts one and opens
 * it (maintainer ruling 2026-10-22).
 */
export function ProjectBanner({
  bounds,
  seed,
}: {
  bounds: WindowBounds;
  /** The deployment a new project is seeded from; empty until /api/context
   * resolves. */
  seed: { regMetaVersion: string; steward: string };
}) {
  const project = useProjectRead(readProject, undefined);
  if (!project.open) {
    const ready = seed.regMetaVersion !== "" && seed.steward !== "";
    return (
      <div className={styles.empty}>
        <Button
          isInert={!ready}
          onPress={() => {
            if (!ready) return;
            // No project is open, so the store replaces nothing and asks
            // nothing: the new one is open at once.
            projectStore.requestNewProject({
              reg_meta_version: regMetaReleaseTag(seed.regMetaVersion),
              steward: seed.steward,
            });
            router.navigate("/project");
          }}
        >
          Create a project
        </Button>
      </div>
    );
  }
  const status = STATUS[project.status];
  return (
    <section className={styles.banner} aria-label="Project">
      <div className={styles.summary}>
        <div className={styles.titleRow}>
          <Link href="/project" className={styles.name}>
            {project.name}
          </Link>
          {project.dirty && (
            <span
              className={styles.unsaved}
              title="Unsaved changes since the last download"
            >
              unsaved
            </span>
          )}
        </div>
        <div className={styles.meta}>
          <span className={styles.counts}>
            {plural(project.sources, "source", "sources")},{" "}
            {plural(project.columns, "column", "columns")}
          </span>
          <span className={styles.status} data-status={project.status}>
            {status.glyph !== null && (
              <span aria-hidden="true">{status.glyph} </span>
            )}
            {status.label}
          </span>
        </div>
      </div>
      <StudyWindow bounds={bounds} />
    </section>
  );
}

/**
 * The study window: a two-thumb year slider over the catalog's bounds. The
 * thumbs move live while dragged; the window store is written once, on release
 * or on each committed key press, so a drag is one draft edit and one autosave.
 * Unset, the thumbs rest at the full span and the readout says so.
 */
function StudyWindow({ bounds }: { bounds: WindowBounds }) {
  const active = useStudyWindow();
  // The thumbs mid-drag; null when they rest on the stored window.
  const [dragging, setDragging] = useState<number[] | null>(null);
  const value = dragging ?? [
    active?.from ?? bounds.min,
    active?.to ?? bounds.max,
  ];
  return (
    <Slider
      className={styles.window}
      value={value}
      // The viewed catalog's first and last year (DESIGN.md → Time): a stored
      // window outside them is clamped into them by the shell, never widens
      // the track.
      minValue={bounds.min}
      maxValue={bounds.max}
      formatOptions={YEAR_FORMAT}
      onChange={setDragging}
      onChangeEnd={([from, to]) => {
        setDragging(null);
        windowStore.set({ from, to });
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
            <Button variant="ghost" onPress={() => windowStore.set(null)}>
              Clear
            </Button>
          </span>
        )}
      </div>
      <SliderTrack className={styles.track}>
        <SliderFill className={styles.fill} />
        <SliderThumb
          index={0}
          aria-label="Start year"
          className={styles.thumb}
        />
        <SliderThumb index={1} aria-label="End year" className={styles.thumb} />
      </SliderTrack>
    </Slider>
  );
}
