<script lang="ts">
import { getCatalogNode, isCatalogNode } from "./api";
import { unmountedFlag } from "./async.svelte";
import { periodLabel, periodWindowRelation, yearWindowLabel } from "./period";
import {
  type SafeSource,
  type StudyWindow,
  safeSourcePeriod,
} from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  type OverlapPlan,
  overlapRegisters,
  planWindowOverlap,
} from "./study_window";
import { Button, ConfirmDialog } from "./ui";

// The project's common study window, at the head of the /project sources list
// (reg_webapp/DESIGN.md → "Common study window"): which window the sources are
// judged against, how many of them diverge from it, and the ONE explicit action
// that rewrites source periods to it — "Apply window overlap". A
// window edit in the rail never rewrites a period; this does, and asks first,
// because it replaces periods the researcher may have set by hand and nothing
// puts them back.
const {
  sources,
  studyWindow,
}: {
  sources: readonly SafeSource[];
  /** The draft's coerced window (`safeStudyWindow`), null when none is set. */
  studyWindow: StudyWindow | null;
} = $props();

const relations = $derived(
  sources.map((s) => periodWindowRelation(safeSourcePeriod(s), studyWindow)),
);
const differing = $derived(relations.filter((r) => r === "differs").length);
const disjoint = $derived(relations.filter((r) => r === "disjoint").length);
/** Dated sources the window can be compared with — what the action can touch. */
const dated = $derived(relations.filter((r) => r !== null).length);

function sourcesCount(n: number): string {
  return `${n} source${n === 1 ? "" : "s"}`;
}

/** The divergence summary in words, null when there is nothing dated to judge. */
const summary = $derived.by((): string | null => {
  if (dated === 0) {
    return null;
  }
  if (differing === 0 && disjoint === 0) {
    return "Every source covers exactly these years.";
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
});

/** The verdict of the last press, for the status line. */
type Outcome =
  | { kind: "applied"; count: number; plan: OverlapPlan }
  | { kind: "unchanged"; plan: OverlapPlan }
  | { kind: "unreadable" }
  | { kind: "stale" };

let checking = $state(false);
let outcome = $state<Outcome | null>(null);
/** The plan waiting on the researcher's answer, and the draft it was made for. */
let pending = $state<OverlapPlan | null>(null);
let pendingFor: unknown = null;
const unmounted = unmountedFlag();

// The verdict describes one press under one window; a window MOVE retires it. Keyed
// on the years, not the object: an applied rewrite replaces the draft, and with it
// the window object, without moving the window — which must not erase the verdict
// that rewrite just earned.
const windowKey = $derived(
  studyWindow === null ? "" : `${studyWindow.from}..${studyWindow.to}`,
);
$effect(() => {
  void windowKey;
  outcome = null;
});

/** Read the availability of every dated source's register, then plan. The draft
 * may move while the reads are out (an edit, a New, a window drag): the plan is
 * then for a project that no longer exists, so it is dropped rather than shown. */
async function prepare(): Promise<void> {
  if (checking || studyWindow === null) {
    return;
  }
  const window = studyWindow;
  const target = projectStore.draft;
  checking = true;
  outcome = null;
  try {
    const registers = overlapRegisters(sources, window);
    let reads: Awaited<ReturnType<typeof getCatalogNode>>[];
    try {
      reads = await Promise.all(registers.map((r) => getCatalogNode(r)));
    } catch {
      if (!unmounted()) {
        outcome = { kind: "unreadable" };
      }
      return;
    }
    if (unmounted()) {
      return;
    }
    if (projectStore.draft !== target) {
      outcome = { kind: "stale" };
      return;
    }
    const children = new Map(
      registers.map((register, i) => {
        const node = reads[i];
        return [
          register,
          isCatalogNode(node) && node.kind === "register"
            ? node.children.filter((c) => c.kind === "binding")
            : [],
        ];
      }),
    );
    const plan = planWindowOverlap(sources, window, children);
    if (plan.changes.length === 0) {
      outcome = { kind: "unchanged", plan };
      return;
    }
    pendingFor = target;
    pending = plan;
  } finally {
    checking = false;
  }
}

function confirm(): void {
  const plan = pending;
  pending = null;
  if (plan === null) {
    return;
  }
  // The dialog is modal, but the draft is not only edited from this page: a
  // restore, an autosave-driven reload or another tab's Open can still replace it.
  if (projectStore.draft !== pendingFor) {
    outcome = { kind: "stale" };
    return;
  }
  projectStore.applyStagedDiff({
    periodChange: plan.changes.map((c) => ({
      sourceName: c.sourceName,
      registerVariant: c.registerVariant,
      period: c.to,
    })),
  });
  outcome = { kind: "applied", count: plan.changes.length, plan };
}

/** The sources the plan left alone, and why: their columns are delivered in no year
 * of the window, so there is no overlap to apply — and the two ways out. One
 * spelling for the status row and the dialog. */
function missNote(plan: OverlapPlan, window: StudyWindow): string {
  if (plan.misses.length === 0) {
    return "";
  }
  const names = plan.misses.map((m) => m.sourceName).join(", ");
  const one = plan.misses.length === 1;
  return `${names} ${one ? "keeps its period" : "keep their periods"}: ${one ? "its" : "their"} columns are not delivered in any year of ${yearWindowLabel(window)}. Remove ${one ? "the source" : "them"} or widen the study window.`;
}
</script>

<div class="study-window">
  {#if studyWindow === null}
    <p class="muted">
      No study window is set. Set one in the rail; columns you add then default to
      the years they are delivered inside it.
    </p>
  {:else}
    <div class="window-line">
      <p class="window-text">
        <span>Study window <span class="years">{yearWindowLabel(studyWindow)}</span></span>
        {#if summary}<span class="summary">{summary}</span>{/if}
      </p>
      {#if differing + disjoint > 0}
        <!-- Never natively disabled while it checks (a disabled button drops keyboard
             focus to the page top): `aria-disabled` freezes it and `prepare` ignores
             a second press. -->
        <Button
          size="sm"
          aria-disabled={checking}
          aria-busy={checking ? "true" : undefined}
          onclick={() => void prepare()}
        >
          {checking ? "Checking availability…" : "Apply window overlap"}
        </Button>
      {/if}
    </div>

    <!-- Always rendered, so the verdict lands in a live region that already exists. -->
    <p
      class="outcome"
      class:ok={outcome?.kind === "applied"}
      class:info={outcome?.kind === "unchanged"}
      class:warn={outcome?.kind === "unreadable" || outcome?.kind === "stale"}
      role="status"
    >
      {#if outcome?.kind === "applied"}
        <span aria-hidden="true">✓</span>
        Applied the window overlap to {sourcesCount(outcome.count)}.
        {missNote(outcome.plan, studyWindow)}
      {:else if outcome?.kind === "unchanged"}
        <span aria-hidden="true">i</span>
        No period changed: every source already holds its overlap with the window.
        {missNote(outcome.plan, studyWindow)}
      {:else if outcome?.kind === "unreadable"}
        <span aria-hidden="true">▲</span>
        Couldn't read which years the registers deliver, so no period changed. Try again.
      {:else if outcome?.kind === "stale"}
        <span aria-hidden="true">▲</span>
        The project changed while availability was loading, so no period changed. Apply
        again.
      {/if}
    </p>
  {/if}
</div>

<ConfirmDialog
  open={pending !== null}
  onOpenChange={(open) => {
    if (!open) {
      pending = null;
    }
  }}
  title="Apply window overlap to {sourcesCount(pending?.changes.length ?? 0)}?"
>
  {#snippet description()}
    {#if pending && studyWindow}
      <p class="dialog-lead">
        Each source below takes the years its columns are delivered in inside
        {yearWindowLabel(studyWindow)}, replacing its current period.
      </p>
      <ul class="changes">
        {#each pending.changes as change (change.sourceName)}
          <li>
            <span class="mono">{change.sourceName}</span>
            <span>
              <span class="mono">{periodLabel(change.from)}</span> to
              <span class="mono">{periodLabel(change.to)}</span>
            </span>
          </li>
        {/each}
      </ul>
      {#if pending.misses.length > 0}
        <p class="dialog-lead">{missNote(pending, studyWindow)}</p>
      {/if}
    {/if}
  {/snippet}
  {#snippet actions()}
    <Button
      onclick={() => {
        pending = null;
      }}
    >
      Cancel
    </Button>
    <Button variant="primary" onclick={confirm}>Apply window overlap</Button>
  {/snippet}
</ConfirmDialog>

<style>
  .study-window {
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
    margin-bottom: var(--space-4);
  }
  .muted {
    margin: 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* The window and its action on one line, the action dropping under the text when
     the line runs out (375px). */
  .window-line {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-2) var(--space-3);
  }
  .window-text {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-1) var(--space-3);
    margin: 0;
    min-width: 0;
  }
  .window-text > span:first-child {
    font-weight: 600;
  }
  /* Years are identifiers (DESIGN.md → Typography). */
  .years,
  .mono {
    font-family: var(--font-mono);
    font-size: var(--text-mono);
  }
  .summary {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* The verdict row: no fill and no height while it says nothing; the status tint,
     foreground and a glyph first once it does (DESIGN.md → Banners and status rows). */
  .outcome {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    margin: 0;
    font-size: var(--text-sm);
  }
  .outcome.ok,
  .outcome.info,
  .outcome.warn {
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius-sm);
  }
  .outcome.ok {
    background: var(--ok-bg);
    color: var(--ok);
  }
  .outcome.info {
    background: var(--info-bg);
    color: var(--info);
  }
  .outcome.warn {
    background: var(--warn-bg);
    color: var(--warn);
  }
  .dialog-lead {
    margin: 0 0 var(--space-2);
  }
  .changes {
    list-style: none;
    margin: 0 0 var(--space-2);
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
  }
  .changes li {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-1) var(--space-3);
    overflow-wrap: anywhere;
  }
</style>
