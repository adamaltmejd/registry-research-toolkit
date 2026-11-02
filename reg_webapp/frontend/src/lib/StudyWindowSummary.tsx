import { useEffect, useRef, useState } from "react";
import { getShow } from "./api";
import { periodLabel, type WindowRelation, yearWindowLabel } from "./period";
import {
  type ProjectStudyWindow,
  type SafeSource,
  safeSourceSlots,
} from "./project_data";
import { projectStore } from "./project_store";
import styles from "./StudyWindowSummary.module.css";
import {
  type OverlapPlan,
  overlapRegisters,
  planWindowOverlap,
} from "./study_window";
import { Button } from "./ui/Button";
import { ConfirmDialog } from "./ui/ConfirmDialog";
import { StatusRow } from "./ui/StatusRow";

// The project's common study window, at the head of the /project sources list
// (reg_webapp/DESIGN.md → "Common study window"): which window the sources are
// judged against, how many of them diverge from it, and the ONE explicit action
// that rewrites source periods to it — "Apply window overlap". A window edit in
// the banner never rewrites a period; this does, and asks first, because it
// replaces periods the researcher may have set by hand and nothing puts them back.

interface Props {
  sources: readonly SafeSource[];
  /** The draft's coerced window (`safeStudyWindow`), null when none is set. */
  studyWindow: ProjectStudyWindow | null;
  /** Each source's relation to the window, by index (`sourceWindowRelation`):
   * judged inside its columns' availability, `pending` while that is read. */
  relations: readonly (WindowRelation | "pending" | null)[];
}

/** The verdict of the last press, for the status line. */
type Outcome =
  | { kind: "applied"; count: number; plan: OverlapPlan }
  | { kind: "unchanged"; plan: OverlapPlan }
  | { kind: "unreadable" }
  | { kind: "stale" };

function sourcesCount(n: number): string {
  return `${n} source${n === 1 ? "" : "s"}`;
}

function windowKeyOf(window: ProjectStudyWindow | null): string {
  return window === null ? "" : `${window.from}..${window.to}`;
}

/** The sources the plan left alone, and why — their columns are delivered in no
 * year of the window, or the catalog lists no delivery for one of their columns —
 * and the ways out. One spelling for the status row and the dialog. */
function missNote(plan: OverlapPlan, window: ProjectStudyWindow): string {
  const sentences: string[] = [];
  const noOverlap = plan.misses.filter((m) => m.reason === "no-overlap");
  const unknown = plan.misses.filter((m) => m.reason === "unknown-column");
  if (noOverlap.length > 0) {
    const one = noOverlap.length === 1;
    sentences.push(
      `${noOverlap.map((m) => m.sourceName).join(", ")} ${one ? "keeps its period" : "keep their periods"}: ${one ? "its" : "their"} columns are not delivered in any year of ${yearWindowLabel(window)}. Remove ${one ? "the source" : "them"} or widen the study window.`,
    );
  }
  if (unknown.length > 0) {
    const one = unknown.length === 1;
    sentences.push(
      `${unknown.map((m) => m.sourceName).join(", ")} ${one ? "keeps its period" : "keep their periods"}: the catalog lists no delivery for one of ${one ? "its" : "their"} columns, so the overlap can't be worked out. Set the period on the source card.`,
    );
  }
  return sentences.join(" ");
}

/** The divergence summary in words, null when there is nothing dated to judge. */
function divergence(differing: number, disjoint: number): string {
  if (differing === 0 && disjoint === 0) {
    return "Every source matches the window where its columns are delivered.";
  }
  const parts: string[] = [];
  if (differing > 0) {
    parts.push(
      `${sourcesCount(differing)} ${differing === 1 ? "differs" : "differ"} from it`,
    );
  }
  if (disjoint > 0) {
    parts.push(
      `${sourcesCount(disjoint)} ${disjoint === 1 ? "has" : "have"} no years inside it`,
    );
  }
  return `${parts.join("; ")}.`;
}

export function StudyWindowSummary({ sources, studyWindow, relations }: Props) {
  const differing = relations.filter((r) => r === "differs").length;
  const disjoint = relations.filter((r) => r === "disjoint").length;
  /** Dated sources the window can be compared with — what the action can touch. */
  const dated = relations.filter((r) => r !== null).length;
  /** A source still being judged holds the verdict back, so it never flips. */
  const judging = relations.includes("pending");
  const windowKey = windowKeyOf(studyWindow);
  const contentKey = (slots: readonly SafeSource[]) =>
    `${windowKey}|${JSON.stringify(slots)}`;

  const [checking, setChecking] = useState(false);
  // The verdict describes one press under one window; a window MOVE retires it.
  // Keyed on the years, not the object: an applied rewrite replaces the draft
  // (and the window object) without moving the window, and must keep its verdict.
  const [outcome, setOutcome] = useState<{
    windowKey: string;
    value: Outcome;
  } | null>(null);
  const shown = outcome?.windowKey === windowKey ? outcome.value : null;
  // The plan waiting on the researcher's answer, and the draft it was made for.
  const [pending, setPending] = useState<OverlapPlan | null>(null);
  const pendingFor = useRef<unknown>(null);
  // The sources and window the last plan found NOTHING more to change in. While the
  // project still matches it, the action is frozen: a second press could only
  // report "no period changed". Keyed on the content, so any edit lifts it.
  const [settledKey, setSettledKey] = useState<string | null>(null);
  const nothingToApply =
    settledKey !== null && settledKey === contentKey(sources);

  // So a read still out when the page goes away is dropped, not shown.
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const report = (value: Outcome) => setOutcome({ windowKey, value });

  /** Read the availability of every dated source's register, then plan. The
   * draft may move while the reads are out (an edit, a New, a window drag): the
   * plan is then for a project that no longer exists, so it is dropped. */
  async function prepare(): Promise<void> {
    if (checking || nothingToApply || studyWindow === null) {
      return;
    }
    const window = studyWindow;
    const target = projectStore.draft;
    setChecking(true);
    setOutcome(null);
    try {
      const registers = overlapRegisters(sources, window);
      let reads: Awaited<ReturnType<typeof getShow>>[];
      try {
        reads = await Promise.all(registers.map((r) => getShow(r)));
      } catch {
        if (mounted.current) {
          report({ kind: "unreadable" });
        }
        return;
      }
      if (!mounted.current) {
        return;
      }
      if (projectStore.draft !== target) {
        report({ kind: "stale" });
        return;
      }
      const nodes = new Map(
        registers.map((register, i) => {
          const node = reads[i];
          return [
            register,
            node.kind === "register" ? node : { children: [], variants: [] },
          ];
        }),
      );
      const plan = planWindowOverlap(sources, window, nodes);
      if (plan.changes.length === 0) {
        report({ kind: "unchanged", plan });
        setSettledKey(contentKey(sources));
        return;
      }
      pendingFor.current = target;
      setPending(plan);
    } finally {
      if (mounted.current) {
        setChecking(false);
      }
    }
  }

  function confirm(): void {
    const plan = pending;
    setPending(null);
    if (plan === null) {
      return;
    }
    // The dialog is modal, but the draft is not only edited from this page: a
    // restore or another tab's Open can still replace it.
    if (projectStore.draft !== pendingFor.current) {
      report({ kind: "stale" });
      return;
    }
    projectStore.applyStagedDiff({
      periodChange: plan.changes.map((c) => ({
        sourceName: c.sourceName,
        registerVariant: c.registerVariant,
        period: c.to,
      })),
    });
    report({ kind: "applied", count: plan.changes.length, plan });
    // The rewrite just landed, so the project now holds the plan's result: read
    // it back through the same coercion the page hands this component.
    setSettledKey(contentKey(safeSourceSlots(projectStore.draft?.sources)));
  }

  return (
    <div className={styles.studyWindow}>
      {studyWindow === null ? (
        <p className={styles.muted}>
          No study window is set. Set one in the project banner; columns you add
          then default to the years they are delivered inside it.
        </p>
      ) : (
        <>
          <div className={styles.windowLine}>
            <p className={styles.windowText}>
              <span className={styles.windowName}>
                Study window{" "}
                <span className={styles.years}>
                  {yearWindowLabel(studyWindow)}
                </span>
              </span>
              {dated > 0 && !judging && (
                <span className={styles.summary}>
                  {divergence(differing, disjoint)}
                </span>
              )}
            </p>
            {differing + disjoint > 0 && (
              // Never natively disabled (focus would drop to the page top, and
              // the dialog returns focus HERE): inert while it checks, and once
              // the last plan left nothing to change.
              <Button
                isInert={checking || nothingToApply}
                onPress={() => void prepare()}
              >
                {checking ? "Checking availability…" : "Apply window overlap"}
              </Button>
            )}
          </div>
          {/* Always rendered, so the verdict lands in a live region that already
              exists. */}
          <StatusRow
            live="status"
            tone={
              shown?.kind === "applied"
                ? "ok"
                : shown?.kind === "unchanged"
                  ? "info"
                  : shown
                    ? "warn"
                    : null
            }
          >
            {shown?.kind === "applied"
              ? `Applied the window overlap to ${sourcesCount(shown.count)}. ${missNote(shown.plan, studyWindow)}`
              : shown?.kind === "unchanged"
                ? `No period changed: every source already holds its overlap with the window. ${missNote(shown.plan, studyWindow)}`
                : shown?.kind === "unreadable"
                  ? "Couldn't read which years the registers deliver, so no period changed. Try again."
                  : shown?.kind === "stale"
                    ? "The project changed while availability was loading, so no period changed. Apply again."
                    : null}
          </StatusRow>
        </>
      )}

      <ConfirmDialog
        isOpen={pending !== null}
        onOpenChange={(open) => {
          if (!open) {
            setPending(null);
          }
        }}
        title={`Apply window overlap to ${sourcesCount(pending?.changes.length ?? 0)}?`}
        actions={
          <>
            <Button onPress={() => setPending(null)}>Cancel</Button>
            <Button variant="primary" onPress={confirm}>
              Apply window overlap
            </Button>
          </>
        }
      >
        {pending && studyWindow && (
          <>
            <p className={styles.dialogLead}>
              Each source below takes the years its columns are delivered in
              inside {yearWindowLabel(studyWindow)}, replacing its current
              period.
            </p>
            <ul className={styles.changes}>
              {pending.changes.map((change) => (
                <li key={change.sourceName}>
                  <span className={styles.mono}>{change.sourceName}</span>
                  <span>
                    <span className={styles.mono}>
                      {periodLabel(change.from)}
                    </span>{" "}
                    to{" "}
                    <span className={styles.mono}>
                      {periodLabel(change.to)}
                    </span>
                  </span>
                </li>
              ))}
            </ul>
            {pending.misses.length > 0 && (
              <p className={styles.dialogLead}>
                {missNote(pending, studyWindow)}
              </p>
            )}
          </>
        )}
      </ConfirmDialog>
    </div>
  );
}
