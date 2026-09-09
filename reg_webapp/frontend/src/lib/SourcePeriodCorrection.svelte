<script lang="ts">
import { tick } from "svelte";
import { variantDisplayLabel } from "./catalog";
import { isStructurallyValidPeriodWire, periodFromWire } from "./period";
import {
  projectStore,
  type SourcePeriodReviewTarget,
} from "./project_store.svelte";
import type { SourcePeriodTarget } from "./staged_picker";
import {
  Button,
  type Column,
  DataTable,
  KeyValue,
  type KeyValueRow,
} from "./ui";

// The catalog-side SOURCE-PERIOD CORRECTION (Y-15): the explicit, reviewed way to
// fix ONE existing named source's requested period without deleting and rebuilding
// it. Ordinary browsing — changing years, filtering rows, following a `?period`
// link — stages nothing; only this action does, and the picker keeps its
// deliberate `periodChanges = []` guard (a partial leaf/group cannot infer a
// SOURCE-WIDE rewrite from the columns it happens to show).
//
// The target is a NAMED source, not a variant: a draft may carry two differently
// named sources on one register variant, and a source period applies to every
// binding on its source. So the closed list names each source, and the review shows
// the source's name, its full coordinate, its current period and EVERY binding it
// carries — including bindings this leaf/group does not show — before anything is
// written.
//
// This component owns the review — its validity, its staleness and its commit through
// the store's guarded `applySourcePeriodReview`; the host supplies the sources its page
// covers and reports the outcome. The review carries the COMPLETE source value as
// reviewed plus the draft generation it belonged to, so a source that moves under an
// open review (its period, its bindings, or the whole project) invalidates it instead
// of being written blind.

let {
  targets,
  disabled = false,
  onreviewchange,
  onapplied,
}: {
  /** The draft's sources on this page's register variants, in draft order. */
  targets: SourcePeriodTarget[];
  /** True while the picker holds staged rows: a correction is its own deliberate
   * diff, so it cannot be started on top of pending add/remove staging (whose
   * period union would otherwise reach the same source). The staged picks are not
   * discarded — the researcher applies or resets them first. */
  disabled?: boolean;
  /** Whether a review is open, so the host can lock the picker's staging for as
   * long as one is (the other half of the same mutual exclusion). */
  onreviewchange?: (open: boolean) => void;
  /** A correction landed — the host reports it the way it reports an Apply. Not
   * called when the store refuses one, which leaves the review open and stale. */
  onapplied?: () => void;
} = $props();

const uid = $props.id();

/** Flipped by the `$effect` teardown when this component goes away, so a correction
 * still waiting on the restore gate is abandoned rather than written into a draft
 * from a page the researcher has left (the same idiom the host views use for a
 * staged Apply). This effect reads nothing, so it never re-runs. */
let unmounted = false;
$effect(() => () => {
  unmounted = true;
});

/** The section element, for the focus handoff below. */
let sectionEl = $state<HTMLElement | null>(null);
/** The period entry, focused when a review opens. */
let periodInput = $state<HTMLInputElement | null>(null);

/** The source under review, as it was WHEN THE REVIEW OPENED — never re-read from
 * the draft, so the snapshot the apply is checked against is the value the
 * researcher actually looked at. Raw: a plain captured value, not a reactive tree. */
let reviewed = $state.raw<SourcePeriodTarget | null>(null);
/** `replacementGeneration` when the review opened — a New/Open since then means
 * this review belongs to a project that is no longer loaded. */
let reviewedGeneration = 0;
/** The proposed period, as typed. */
let proposed = $state("");
/** True while the host's apply is in flight — one submission per review. */
let applying = $state(false);

/** A source's display name — the empty name only a malformed draft carries still has
 * to read as something, in a label and in a table cell alike (`SourceEditor` names it
 * the same way). */
function displayName(sourceName: string): string {
  return sourceName || "(unnamed source)";
}

/** The accessible name of a source's "Change source period" button — shared by the
 * button and by the focus handoff that finds it again when a review closes. */
function triggerLabel(sourceName: string): string {
  return `Change source period for ${displayName(sourceName)}`;
}

/** The open review's identity, as the store re-checks it. Null when none is open. */
const reviewTarget = $derived.by((): SourcePeriodReviewTarget | null => {
  const open = reviewed;
  return open === null
    ? null
    : {
        sourceName: open.sourceName,
        registerVariant: open.registerVariant,
        snapshot: open.snapshot,
        replacementGeneration: reviewedGeneration,
      };
});

/** The review no longer describes the draft: the source was removed or renamed, its
 * period / bindings / any other field changed, or the whole project was replaced.
 * Asked of THE DRAFT through the same predicate the write refuses on — never of
 * `targets`, which narrows as the researcher browses (a source that scrolls off this
 * page has not moved). Only the NOTICE depends on this; the write re-checks for
 * itself, after the restore gate. */
const stale = $derived(
  reviewTarget !== null &&
    !projectStore.sourcePeriodReviewCurrent(reviewTarget),
);

const proposedWire = $derived(proposed.trim());
/** The existing structural period grammar decides — the same gate the catalog's own
 * period entry uses. Empty text, `_default` and an unsorted list all fail it. */
const proposedValid = $derived(isStructurallyValidPeriodWire(proposedWire));
const changed = $derived(
  reviewed !== null && proposedWire !== reviewed.periodWire,
);
const canApply = $derived(!stale && !applying && proposedValid && changed);
const problem = $derived.by(() => {
  if (proposedValid) {
    return null;
  }
  // ONE sentence for every way the structural check refuses, because `2020,2019`
  // fails the sorted/non-overlapping rule while looking exactly like the grammar —
  // "doesn't look like a period" would tell the researcher to type what they typed.
  return proposedWire === ""
    ? "Enter a period — a source keeps a requested period."
    : `${proposedWire} isn't a usable period. Use a year (2014), a range (2012..2014), or a comma-separated list in ascending order with no overlaps (2012..2014,2016).`;
});

/** How a value is SET: `mono` marks a machine value, and is false for prose — prose
 * set in mono reads as a code (frontend/DESIGN.md → Typography). */
interface Display {
  text: string;
  mono: boolean;
}

/** A stored period as displayed; the fallback for a source with no (or an
 * unshapeable) period is prose. */
function periodDisplay(wire: string): Display {
  return wire === ""
    ? { text: "(no period)", mono: false }
    : { text: wire, mono: true };
}

/** A register variant as displayed: #376's variant-FAMILY labels are prose, so only
 * the raw coordinate is set as one. */
function variantDisplay(registerVariant: string): Display {
  const text = variantDisplayLabel(registerVariant);
  return { text, mono: text === registerVariant };
}

/** The reviewed source's current period, as displayed. */
const currentPeriod = $derived(periodDisplay(reviewed?.periodWire ?? ""));

const reviewRows = $derived.by((): KeyValueRow[] => {
  if (reviewed === null) {
    return [];
  }
  const variant = variantDisplay(reviewed.registerVariant);
  return [
    { label: "Register variant", value: variant.text, mono: variant.mono },
    {
      label: "Current period",
      value: currentPeriod.text,
      mono: currentPeriod.mono,
    },
  ];
});

/** One row of the sources table. `target` is the reviewed value the row's action
 * opens from — the display strings are derived from it, never re-derived later. */
type SourceRow = {
  name: string;
  variant: Display;
  period: Display;
  columns: number;
  target: SourcePeriodTarget;
};

const sourceColumns: Column<SourceRow>[] = [
  { key: "name", label: "Source" },
  { key: "variant", label: "Register variant" },
  { key: "period", label: "Period" },
  { key: "columns", label: "Columns", numeric: true },
  { key: "target", label: "Action" },
];

const sourceRows = $derived(
  targets.map(
    (target): SourceRow => ({
      name: displayName(target.sourceName),
      variant: variantDisplay(target.registerVariant),
      period: periodDisplay(target.periodWire),
      columns: target.bindings.length,
      target,
    }),
  ),
);

const rowId = (row: SourceRow): string =>
  `${row.target.sourceName} ${row.target.registerVariant}`;

async function openReview(target: SourcePeriodTarget): Promise<void> {
  reviewed = target;
  reviewedGeneration = projectStore.replacementGeneration;
  proposed = target.periodWire;
  onreviewchange?.(true);
  // Land in the field the action promised to change, with the current period
  // selected so typing replaces it.
  await tick();
  periodInput?.focus();
  periodInput?.select();
}

/** Close the review and hand focus back to the row that opened it — the Reset/Apply
 * button holding focus is unmounted by the close, which would otherwise drop a
 * keyboard user at the top of the document. The trigger is matched by its accessible
 * name (unique, since draft source names are), so no per-row element ref has to be
 * threaded through the table's cell snippet. A source that was deleted under the
 * review has no row to return to; focus then simply stays put. */
async function closeReview(): Promise<void> {
  const closed = reviewed;
  reviewed = null;
  proposed = "";
  onreviewchange?.(false);
  if (closed === null) {
    return;
  }
  await tick();
  const label = CSS.escape(triggerLabel(closed.sourceName));
  sectionEl
    ?.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`)
    ?.focus();
}

/** Commit the reviewed correction: ONE period-only diff, through the store's guarded
 * `applySourcePeriodReview`. The draft lifecycle is application-owned and its restore
 * is ASYNCHRONOUS, so this waits for it exactly as the catalog's Add path does — and
 * the store re-checks the review against the settled draft immediately before it
 * mutates, so a source that moved inside that window is refused, not overwritten. A
 * refusal leaves the review open (and, by then, visibly stale). */
async function apply(): Promise<void> {
  const target = reviewTarget;
  if (!canApply || target === null) {
    return;
  }
  applying = true;
  try {
    await projectStore.restored;
    const applied =
      !unmounted &&
      projectStore.applySourcePeriodReview({
        ...target,
        period: periodFromWire(proposedWire),
      });
    if (applied) {
      onapplied?.();
      await closeReview();
    }
  } finally {
    applying = false;
  }
}
</script>

{#if sourceRows.length > 0 || reviewed !== null}
  <section
    class="source-periods"
    aria-labelledby="{uid}-heading"
    bind:this={sectionEl}
  >
    <!-- The scope is the PAGE, not the project: these are the draft's sources on the
         register variants this leaf/group covers. -->
    <h3 class="micro-label" id="{uid}-heading">Project sources on this page</h3>

    {#if reviewed !== null}
      <!-- The REVIEW: what this correction would rewrite, before it is written. It
           keeps the NAME of the action that opened it, so the flow doesn't rename
           itself mid-way, and that heading is the group's accessible name. The
           bindings come from the source AS REVIEWED, not from the page, so a column
           this leaf never shows is still reviewed. It sits beside the table rather
           than inside it because a source deleted under an open review has no row
           left to be stale in. -->
      <div class="review" role="group" aria-labelledby="{uid}-review-heading">
        <h4 class="review-heading" id="{uid}-review-heading">
          Change source period: {displayName(reviewed.sourceName)}
        </h4>

        <KeyValue rows={reviewRows} />

        <!-- A form, so Enter in the field commits the correction — the sibling
             `PeriodPicker` right above applies its years the same way, and a review
             the keyboard can only finish by tabbing to a button is not finished. -->
        <form
          onsubmit={(event) => {
            event.preventDefault();
            void apply();
          }}
        >
          <div class="entry">
            <label class="micro-label" for="{uid}-period">New period</label>
            <input
              id="{uid}-period"
              class="period"
              type="text"
              autocomplete="off"
              spellcheck="false"
              readonly={applying}
              aria-invalid={problem !== null}
              aria-describedby={problem === null ? undefined : `${uid}-problem`}
              bind:this={periodInput}
              bind:value={proposed}
            />
          </div>

          <!-- Rendered ALWAYS, so a refusal lands in a live region that already
               existed rather than one that appears with it (matching the period
               picker's own refusal line). -->
          <p
            class="problem"
            class:refused={problem !== null}
            id="{uid}-problem"
            role="status"
          >
            {#if problem !== null}
              <span aria-hidden="true">✕</span>
              {problem}
            {/if}
          </p>

          {#if stale}
            <p class="stale" role="alert">
              <span aria-hidden="true">▲</span>
              {reviewed.sourceName || "This source"} changed since you opened this review
              — reset and review it again.
            </p>
          {/if}

          <!-- The other always-present live region: what the Apply would do, or why it
               is inert although the text IS a period. Silent while the text is invalid
               (the refusal line above speaks) or the review is stale (the alert does). -->
          <p class="proposal" role="status">
            {#if !stale && proposedValid}
              {#if changed}
                Applying changes {displayName(reviewed.sourceName)} from
                <span class:mono={currentPeriod.mono}>{currentPeriod.text}</span>
                to <span class="mono">{proposedWire}</span> for every column below.
              {:else}
                That is already this source's period — enter a different one to apply.
              {/if}
            {/if}
          </p>

          <div class="affected">
            <p class="micro-label" id="{uid}-affected">
              Columns on this source ({reviewed.bindings.length})
            </p>
            <ul aria-labelledby="{uid}-affected">
              {#each reviewed.bindings as binding, i (i)}
                <li>
                  <span class="mono">{binding.variable}</span>
                  {#if binding.representation}
                    <span class="mono">{binding.representation}</span>
                  {/if}
                </li>
              {/each}
            </ul>
          </div>

          <div class="actions">
            <Button
              type="submit"
              size="sm"
              disabled={!canApply}
              aria-label={`Apply source period for ${displayName(reviewed.sourceName)}`}
            >
              {applying ? "Applying..." : "Apply source period"}
            </Button>
            <Button
              size="sm"
              variant="ghost"
              disabled={applying}
              onclick={closeReview}
            >
              Reset
            </Button>
          </div>
        </form>
      </div>
    {/if}

    <!-- Browsing narrows what the page covers, so an open review can outlive every
         row: show the table only when there is one. -->
    {#if sourceRows.length > 0}
      <DataTable columns={sourceColumns} rows={sourceRows} getRowId={rowId}>
        {#snippet cell(row, column)}
          {#if column.key === "name"}
            {row.name}
          {:else if column.key === "variant"}
            <span class:mono={row.variant.mono}>{row.variant.text}</span>
          {:else if column.key === "period"}
            <span class:mono={row.period.mono}>{row.period.text}</span>
          {:else if column.key === "columns"}
            {row.columns}
          {:else}
            <Button
              size="sm"
              disabled={disabled || reviewed !== null}
              aria-label={triggerLabel(row.target.sourceName)}
              onclick={() => openReview(row.target)}
            >
              Change source period
            </Button>
          {/if}
        {/snippet}
      </DataTable>
    {/if}

    {#if disabled}
      <p class="blocked" role="status">
        Apply or reset the staged columns above before changing a source period.
      </p>
    {/if}
  </section>
{/if}
<style>
  /* The flat hairline card the sibling catalog controls use (`PeriodPicker`'s
     `.period-picker`, `RepresentationPicker`'s `.rep-picker`) — same margin and
     inset, so the three read as one column of controls. */
  .source-periods {
    margin: 1.25rem 0;
    padding: var(--space-3) 0.9rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    background: var(--surface);
  }
  h3 {
    margin: 0 0 var(--space-2);
  }
  /* The table is the card's last element: its final row rule would otherwise read
     as a stray line above the card's own padding. */
  .source-periods :global(tbody tr:last-child td) {
    border-bottom: none;
  }
  .review {
    margin-bottom: var(--space-3);
    padding-bottom: var(--space-3);
    border-bottom: 1px solid var(--border);
  }
  .review-heading {
    margin: 0 0 var(--space-2);
    font-size: var(--text-sm);
    font-weight: var(--heading-weight);
    color: var(--text);
  }
  .mono {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
  }
  .entry {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-2);
    margin-top: var(--space-3);
  }
  .period {
    box-sizing: border-box;
    width: 12rem;
    max-width: 100%;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    /* A period is a machine identifier (frontend/DESIGN.md → Typography). */
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    font-variant-numeric: tabular-nums;
  }
  .period:focus-visible {
    outline: none;
    border-color: var(--accent);
    box-shadow: var(--focus-ring);
  }
  .period[aria-invalid="true"] {
    border-color: var(--err);
  }
  /* Status ROWS: the status tint as fill, the status foreground as text, the glyph
     first (frontend/DESIGN.md → Banners and status rows). The refusal line keeps no
     fill and no height while it says nothing. */
  .problem {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    margin: var(--space-2) 0 0;
    color: var(--err);
    font-size: var(--text-sm);
  }
  .problem.refused {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--err-border);
    border-radius: var(--radius-sm);
    background: var(--err-bg);
  }
  .stale {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    margin: var(--space-2) 0 0;
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius-sm);
    background: var(--warn-bg);
    color: var(--warn);
    font-size: var(--text-sm);
  }
  .proposal {
    margin: var(--space-2) 0 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .proposal .mono {
    color: var(--text);
  }
  .affected {
    margin-top: var(--space-3);
  }
  .affected p {
    margin: 0 0 var(--space-1);
  }
  .affected ul {
    display: grid;
    gap: var(--space-1);
    margin: 0;
    padding: 0;
    list-style: none;
  }
  .affected li {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
  }
  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    margin-top: var(--space-3);
  }
  .blocked {
    margin: var(--space-2) 0 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
</style>
