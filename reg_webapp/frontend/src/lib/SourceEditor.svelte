<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import { fqidSegments, sourceCardHeading } from "./catalog";
import { sourceNames } from "./catalog_names.svelte";
import {
  grammarYear,
  periodFromWire,
  periodToWire,
  sameYearWindow,
  yearWindowFromWire,
  yearWindowToWire,
} from "./period";
import {
  type Period,
  type SafeSource,
  type StudyWindow,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  sourceBindingsMalformed,
  sourceSnapshot,
} from "./project_data";
import {
  projectStore,
  type SourcePeriodEditTarget,
} from "./project_store.svelte";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  KeyValue,
  type KeyValueRow,
  Tag,
} from "./ui";
import {
  issuesUnderPointer,
  jsonPointer,
  sourceAnchorId,
  type ValidationIssue,
} from "./validation";

// The source card of the #991 data-order cart: the cart SHOWS what has been picked
// (a register and the columns taken from it) and supports delete + navigate-out —
// a wrong variable, variant or representation is fixed by picking again in the
// catalog, never by editing the row. The ONE exception is this source's PERIOD
// (Y-81): it is a field of the source rather than of any pick, and this card is the
// only surface that shows a source whole — its full coordinate and every binding it
// carries — which is what a source-wide period rewrite has to be looked at against.
// The header still rolls up all errors under `/sources/{i}` as a badge (fixes are
// reached via the ValidationPanel's catalog link). See reg_webapp/DESIGN.md.
const { sourceIndex, source, issues, providerQualified, studyWindow } = $props<{
  sourceIndex: number;
  source: SafeSource;
  issues: ValidationIssue[];
  /** Whether this deployment serves more than one provider, so the card names
   * the provider that owns the register above its heading: a bare register name
   * can stand for two registers there. A deployment fact, read once in
   * `App.svelte` and threaded down like `steward` — no route fetches it. */
  providerQualified: boolean;
  /** The project's own study window, or null when none is set — what this
   * source's period is MARKED against when the two differ. An authoring seed,
   * never an inheritance (reg_schema/DESIGN.md): each source keeps its own
   * concrete period, so divergence is shown rather than hidden. */
  studyWindow: StudyWindow | null;
}>();

/** Instance-scoped ids, so every card's period fields keep real `<label for>`
 * pairs on a page that renders one card per source. */
const uid = $props.id();

const sourcePtr = $derived(jsonPointer(["sources", sourceIndex]));
const rolledUp = $derived(issuesUnderPointer(issues, sourcePtr));
const errorCount = $derived(rolledUp.filter((i) => i.level === "error").length);

const registerVariant = $derived(safeSourceRegisterVariant(source));

// The card is HEADED by the REGISTER this source delivers from and qualified by the
// concrete VARIANT it extracts, in the catalog's own words — what the researcher
// picked, spelled as every other route spells it. The names live only in the
// catalog, so they are READ from it (cached per register); until they land the raw
// coordinate heads the card. The source's `name` is a generated join key (`LISA`,
// `LISA_2`, `LISA_3` for three variants of one register), so it titles nothing; it
// stays visible as a detail row below because panels join on it.
const names = $derived(sourceNames(registerVariant));
const sourceMalformed = $derived(source === null);
const heading = $derived(
  sourceMalformed
    ? "(malformed source)"
    : sourceCardHeading(names.register, registerVariant) || "(no register)",
);
// The variant that names the POPULATION this source extracts: the one qualifier
// that belongs in the title, under the heading, because it is what the researcher
// picked alongside the register (the variant browser's own heading-plus-meta
// shape). Absent until the whole name has landed — `sourceNames` is
// all-or-nothing — so it cannot appear beside a coordinate heading.
const variantName = $derived(names.variant);
// The provider that OWNS the register is a different kind of thing: an attribute
// of the register, not part of what was picked, and only worth saying where the
// deployment serves more than one (elsewhere it is the same word on every card).
// So it goes where this card puts its other named attributes — a labelled metadata
// row — rather than as a second unlabelled line no reader could tell from the
// variant.
const providerName = $derived(providerQualified ? names.provider : null);
// The heading has fallen back to the raw coordinate: while the names are in flight,
// and wherever they cannot be read at all. It is then a machine identifier standing
// in for a name, so it takes the machine face and prints only once on the card.
const titleIsCoordinate = $derived(heading === registerVariant);
// The concrete variant the source extracts, for the columns' own catalog resolve:
// a column's default name is the one delivered at THIS variant and period.
const variantSlug = $derived(fqidSegments(registerVariant)[2] ?? "");

// Defensive: a malformed opened spec may carry `bindings` as a non-array. Show an
// inline note instead of the list (full-replace-with-guards, maintainer decision)
// rather than crashing — the draft stays verbatim for serialize/validate.
const bindings = $derived(safeSourceBindings(source));
const bindingsMalformed = $derived(sourceBindingsMalformed(source));

// The panel/order join key (reg_meta `OrderEntry.source`) — the one thing that
// still differs between two sources an imported spec put on the SAME
// register_variant (an import routinely does; the catalog's own add path can't
// author it, since it finds-or-creates by variant). Shared by the detail row below
// and every per-source control's accessible name.
const sourceName = $derived(safeSourceName(source) || "(unnamed source)");

// The stored period as its WIRE string (list-period aware — `periodToWire` already
// joins list segments); null when the source carries none or an unshapeable one.
const periodWire = $derived(periodToWire(safeSourcePeriod(source) as Period));

// The coordinate rows, rendered through the shared KeyValue primitive
// (#804) — same metadata-row styling ProjectEditor uses. The provider heads them
// where the deployment has more than one: it is a word, not an identifier, so it
// takes no mono, and the label is what tells it from the variant in the title. The
// register_variant is a machine FQID coordinate (mono): the source extracts that
// CONCRETE variant, so it stays on the card even once the heading names it in
// words — but only then, because a heading that has fallen back to the coordinate
// would print it twice.
// `name` is the panel/order join key (reg_meta `OrderEntry.source`) — a machine
// identifier, so mono.
const metaRows = $derived([
  ...(providerName ? [{ label: "Provider", value: providerName }] : []),
  ...(titleIsCoordinate
    ? []
    : [{ label: "Register variant", value: registerVariant, mono: true }]),
  {
    label: "Source name",
    value: sourceName,
    mono: true,
  },
] satisfies KeyValueRow[]);

// ── The source's period (Y-81) ───────────────────────────────────────────────
//
// Authored in the catalog's own period vocabulary: a YEAR RANGE, the one grammar
// the catalog's `PeriodPicker` authors, written back through the same wire shaping
// a pick uses (`periodFromWire(yearWindowToWire(…))` — a bare year when the two
// bounds meet, the `{from, to}` object otherwise). A period the year fields cannot
// express — a token like `HT2018`, or the #307 comma list two disjoint picks merge
// into — is shown as it stands and never silently rewritten into a span, exactly as
// the catalog's picker leaves one alone.

/** The stored period as a year window, or null when it is absent, a token or a
 * segment list. */
const storedYears = $derived(yearWindowFromWire(periodWire));
/** Whether the year fields can author THIS source's period. A source with no period
 * yet is authored by them too; it is what a source is missing, not another grammar. */
const yearsEditable = $derived(periodWire === null || storedYears !== null);

/** The two fields as TYPED, or null while they mirror the stored period. Kept as
 * text so a refused entry stays on screen as it was typed rather than being
 * rewritten into some other range. */
let entry = $state<{ from: string; to: string } | null>(null);
/** Whether the entry has been committed (blur or Apply) — a half-typed year must
 * not announce a refusal on every keystroke. */
let entryCommitted = $state(false);
/** The last Apply was refused because the SOURCE moved under the edit — a different
 * thing from `entryRefusal`, which is the years themselves being unusable. */
let writeRefused = $state(false);
/** The period a landed write set, until the next keystroke retires it. A form that
 * says nothing on success leaves the researcher to infer it from a greyed button —
 * and where the new period neither crosses the study window nor changes a finding,
 * there is nothing else on the card that moves. */
let appliedYears = $state<StudyWindow | null>(null);
/** The last Apply had nothing to write — the entry named exactly the stored period,
 * or nothing was ever typed. A different thing from `entryRefusal`: the years ARE
 * usable, there is just no change to make. */
let unchangedNotice = $state(false);
/** An Apply is in flight — from the press that found something to write until the
 * store's write returns. The whole span is the restore-gate wait below (the write
 * itself is synchronous), so this doubles as "waiting for the draft to settle". The
 * two year fields and the Apply button freeze for it: a second press while one is
 * held on the gate must not queue a write of its own — the first press's write may
 * already have moved the draft by the time a queued second one runs, raising a
 * staleness refusal for a write that did land (a1/a3). */
let applying = $state(false);
/** The source as it stood when this edit began — the value the researcher was
 * looking at, which the store re-checks the write against. Plain, not `$state`:
 * nothing renders from it. */
let editedFrom: SourcePeriodEditTarget | null = null;
/** Flipped by the `$effect` teardown when this card goes away, so an Apply still
 * waiting on the restore gate in `applyPeriod` is abandoned rather than written
 * behind the researcher's back (the same idiom the catalog views use for a staged
 * Apply). This effect reads nothing, so it never re-runs. */
let unmounted = false;
$effect(() => () => {
  unmounted = true;
});

const fromText = $derived(
  entry?.from ?? (storedYears ? String(storedYears.from) : ""),
);
const toText = $derived(
  entry?.to ?? (storedYears ? String(storedYears.to) : ""),
);

/** The entry resolved: the year window it names, or why it names none — with the
 * field(s) that refusal is about, so the hairline marks the year at fault rather
 * than both. Null while the fields still mirror the stored period. */
const entryResolution = $derived.by<
  | { years: StudyWindow }
  | { problem: string; at: { from: boolean; to: boolean } }
  | null
>(() => {
  if (entry === null) {
    return null;
  }
  // `grammarYear` is the wire's OWN year rule (19xx/20xx), the same one the
  // catalog's exact-year fields and `periodFromWire` are written against — so a
  // year these fields accept is a year the period wire can carry.
  const from = grammarYear(entry.from);
  const to = grammarYear(entry.to);
  const at = { from: from === null, to: to === null };
  if (from === null || to === null) {
    // Word for word the catalog picker's own refusal (`PeriodPicker`), naming the
    // century range instead of an example year: a cart card has no coverage band to
    // draw an exemplar from.
    const rule = "a four-digit year, 1900 to 2099.";
    if (at.from && at.to) {
      return { problem: `From and To must each be ${rule}`, at };
    }
    return { problem: `${at.from ? "From" : "To"} must be ${rule}`, at };
  }
  if (from > to) {
    // The pair, not either year on its own.
    return {
      problem: `From ${from} is after To ${to} — enter From at or before To.`,
      at: { from: true, to: true },
    };
  }
  return { years: { from, to } };
});

const entryProblem = $derived(
  entryResolution !== null && "problem" in entryResolution
    ? entryResolution
    : null,
);
/** The refusal to show, once the entry has been committed. */
const entryRefusal = $derived(entryCommitted ? entryProblem : null);
/** The refusal line is only described-by while it actually says something. */
const problemId = $derived(
  entryRefusal === null ? undefined : `${uid}-problem`,
);
/** The wire the entry would write, or null when it names no window / no change. */
const proposedWire = $derived.by(() => {
  if (entryResolution === null || !("years" in entryResolution)) {
    return null;
  }
  const wire = yearWindowToWire(entryResolution.years);
  return wire === periodWire ? null : wire;
});
/** The confirmation a landed write leaves in the refusal's own region — one line
 * that is either explaining a refusal or reporting a write, never both. */
const appliedLabel = $derived(
  appliedYears === null
    ? null
    : appliedYears.from === appliedYears.to
      ? `Period set to ${appliedYears.from}.`
      : `Period set to ${appliedYears.from}–${appliedYears.to}.`,
);

/** The study window this source's period is MARKED against, or null when there is
 * nothing to mark — the whole point of a per-source period is that it MAY differ,
 * so the card says when it does instead of flagging it. A source with no period at
 * all differs from nothing; that it has none is the validator's finding, not this
 * marker's. Compared through `sameYearWindow`, the shared user-deviation predicate,
 * so a period is judged by the span it covers rather than by how the wire spells it
 * (a stored `{from: 2020, to: 2020}` is the same span as a window of 2020). */
const deviation = $derived(
  studyWindow !== null &&
    periodWire !== null &&
    !sameYearWindow(storedYears, studyWindow)
    ? studyWindow
    : null,
);

function editYear(side: "from" | "to", value: string): void {
  if (entry === null) {
    // The edit starts HERE: capture the source as it is, so a write onto a source
    // that has moved since — a column removed from this very card, a project
    // replaced — is refused rather than landing on a value nobody looked at.
    editedFrom = {
      sourceName: safeSourceName(source),
      registerVariant,
      snapshot: sourceSnapshot(source),
      replacementGeneration: projectStore.replacementGeneration,
    };
    writeRefused = false;
    appliedYears = null;
    unchangedNotice = false;
  }
  // The buffer starts as what the fields were SHOWING — the stored period on the
  // first keystroke, the previous entry after that. `fromText`/`toText` already say
  // which, so the seed is not spelled a second time here.
  const base = entry ?? { from: fromText, to: toText };
  entry = side === "from" ? { ...base, from: value } : { ...base, to: value };
}

/** Commit the edited period: ONE period-only diff through the store's guarded
 * `applySourcePeriodEdit`, never unioned with anything else. Either way the fields
 * are re-armed on the source as it NOW stands — after a refusal that is the whole
 * point, since the source moved and the next Apply has to be made against a value
 * the researcher can see, so the alert says the years were reset. */
async function applyPeriod(): Promise<void> {
  if (applying) {
    // Held on the SAME gate as an earlier press: the fields are already frozen on
    // what that press captured, so this one has nothing new to contribute.
    return;
  }
  // Committing FIRST is what makes a refused Apply say why: years that name no
  // window leave `proposedWire` null, so the write below is skipped and the
  // refusal line renders instead.
  entryCommitted = true;
  const target = editedFrom;
  const wire = proposedWire;
  if (target === null || wire === null) {
    // Nothing to write: either `entryProblem` already explains why (rendered via
    // `entryRefusal` below), or the entry names exactly the stored period (typed
    // back to it, or never touched at all) — say so rather than leaving Apply
    // looking like it silently did nothing. Retiring the other two verdicts here
    // too: this press is what the status line now reports, not whatever an
    // earlier one left behind.
    unchangedNotice = entryProblem === null;
    writeRefused = false;
    appliedYears = null;
    return;
  }
  unchangedNotice = false;
  applying = true;
  try {
    const written = yearWindowFromWire(wire);
    // The draft lifecycle is application-owned and its restore is ASYNCHRONOUS, so
    // this waits for it exactly as the catalog's Add path does: the store re-checks
    // the edit against the draft it finds, and that check is only worth anything
    // once the draft it reads is the SETTLED one. The wait is unbounded, so a card
    // gone by the time it returns — the researcher left /project — abandons the
    // write rather than landing it on a page nobody is looking at. `applying` keeps
    // the fields read-only for the whole span, so nothing typed here can revise
    // `wire`, and a second press can't queue behind this one.
    await projectStore.restored;
    if (unmounted) {
      return;
    }
    writeRefused = !projectStore.applySourcePeriodEdit({
      ...target,
      period: periodFromWire(wire),
    });
    appliedYears = writeRefused ? null : written;
    entry = null;
    entryCommitted = false;
    editedFrom = null;
  } finally {
    applying = false;
  }
}

// The removal question and the delete button's accessible name are SENTENCES, and
// they name this source in words rather than reciting the heading and the line
// under it: a heading is layout, copy is prose. A malformed slot, or a source
// carrying no coordinate at all, has no words to be named by — the question then
// says "this source", which the card around it already places, and the button
// keeps its visible text as its whole name.
const identified = $derived(!sourceMalformed && registerVariant !== "");
const columnCount = $derived(
  `${bindings.length} column${bindings.length === 1 ? "" : "s"}`,
);
// How a per-source control NAMES this source: the generated join key
// (`sourceName`) first, since it is the one thing still unique between two sources
// an imported spec put on the SAME register_variant — the heading and variant a
// control used to lean on alone are then identical between them (a2). The
// heading (+ variant, when named) still rides along for a reader who does not
// recognise the generated name on its own. Spelled ONCE, because every per-source
// control interpolates it and two spellings would let one card be named two ways.
const identifiedName = $derived(
  `${sourceName} (${heading}${variantName ? `, ${variantName}` : ""})`,
);
const removeQuestion = $derived(
  identified
    ? `Remove the source ${identifiedName} and its ${columnCount}?`
    : `Remove this source and its ${columnCount}?`,
);
const removeLabel = $derived(
  identified ? `Remove source ${identifiedName}` : undefined,
);
const applyPeriodLabel = $derived(
  identified ? `Apply period for ${identifiedName}` : undefined,
);
/** The year pair's accessible name. Every card contributes a field labelled "From"
 * and one labelled "To"; naming the GROUP per source tells them apart in a flat
 * form-field list without touching the visible labels the fields are named by. */
const periodGroupLabel = $derived(
  identified ? `Period for ${identifiedName}` : "Period",
);

// Removing a source takes its whole column list with it, and nothing in the cart
// puts them back (re-picking happens in the catalog), so it ASKS first — naming the
// register and how many columns go. Removing ONE column doesn't: that's a single
// row the researcher is pointing at. The shared ConfirmDialog carries the modal
// semantics, as it does for the project-replacement question in ProjectEditor.
let removeOpen = $state(false);
function confirmRemove(): void {
  removeOpen = false;
  projectStore.removeSource(sourceIndex);
}
</script>

<!-- `id` is the click-to-locate anchor the ValidationPanel scrolls to (matched via
     `sourceAnchorId`). `.locate-flash` (toggled by the panel on the element) briefly
     highlights the card. -->
<section
  class="source"
  id={sourceAnchorId(sourceIndex)}
  aria-label="Source {sourceIndex + 1}"
>
  <header class="source-head">
    <!-- The title is COMPOSED, not joined: the register HEADS the card and the
         variant that names the population sits under it — two elements rather than
         one dot-strung line (frontend/DESIGN.md rules out middle-dot meta strings).
         They wrap independently at 375px and reach a screen reader as two things.
         `aria-busy` covers the whole block while the catalog names are in flight:
         the heading is showing the coordinate as a stand-in and the variant line is
         not there yet, and the screenshot driver waits on it (see
         frontend/DESIGN.md → loading surfaces). -->
    <div class="source-title" aria-busy={names.loading ? "true" : undefined}>
      <h3 class:mono={titleIsCoordinate}>
        {heading}
        {#if errorCount > 0}
          <!-- Status badge: cool error tone + ✕ glyph (aria-hidden); the count text
               carries the meaning for assistive tech (DESIGN.md accent-vs-status). -->
          <Tag tone="error">
            {#snippet glyph()}✕{/snippet}
            {errorCount} error{errorCount === 1 ? "" : "s"}
          </Tag>
        {/if}
      </h3>
      {#if variantName}
        <p class="source-variant">{variantName}</p>
      {/if}
    </div>
    <!-- Per-source accessible name so a screen-reader controls list disambiguates
         the delete buttons (visible text kept as the label prefix — label-in-name).
         It is a sentence, not the heading block read back: see `removeLabel`. -->
    <Button
      variant="danger"
      size="sm"
      aria-label={removeLabel}
      onclick={() => {
        removeOpen = true;
      }}
    >
      Remove source
    </Button>
  </header>

  <ConfirmDialog
    bind:open={removeOpen}
    title={removeQuestion}
  >
    {#snippet description()}
      The source leaves this project with every column it carries. Pick them again
      in the catalog to bring them back.
    {/snippet}
    {#snippet actions()}
      <Button
        variant="default"
        onclick={() => {
          removeOpen = false;
        }}
      >
        Cancel
      </Button>
      <Button variant="danger" onclick={confirmRemove}>Remove source</Button>
    {/snippet}
  </ConfirmDialog>

  {#if sourceMalformed}
    <!-- A null/non-object slot: render a degraded card (not a crash) that still
         occupies its index so validation addressing lines up; the malformed value
         stays verbatim on the draft for serialize/validate. -->
    <p class="error" role="alert">
      This source entry is malformed — fix via re-download or hand-edit.
    </p>
  {:else}
    <KeyValue rows={metaRows} />

    <!-- The PERIOD: the one field the cart edits (Y-81). A year range, in the
         catalog's own vocabulary — two exact-year fields and one Apply, the same
         entry the catalog's period card carries beside its slider. There is no
         slider here: a cart card knows no data-coverage track to draw one against,
         and the years are what the researcher already has in mind. -->
    <div class="source-period">
      <span class="micro-label">Period</span>
      {#if yearsEditable}
        <!-- A form, so Enter in either field applies the period — the catalog's
             period card commits its years the same way, and a change the keyboard
             can only finish by tabbing to a button is not finished. -->
        <form
          class="period-entry"
          onsubmit={(event) => {
            event.preventDefault();
            void applyPeriod();
          }}
        >
          <div class="years" role="group" aria-label={periodGroupLabel}>
            <label class="micro-label" for="{uid}-from">From</label>
            <input
              id="{uid}-from"
              class="year"
              type="text"
              inputmode="numeric"
              autocomplete="off"
              value={fromText}
              placeholder="yyyy"
              readonly={applying}
              aria-invalid={entryRefusal?.at.from === true}
              aria-describedby={problemId}
              oninput={(event) => editYear("from", event.currentTarget.value)}
              onchange={() => {
                entryCommitted = true;
              }}
            />
            <label class="micro-label" for="{uid}-to">To</label>
            <input
              id="{uid}-to"
              class="year"
              type="text"
              inputmode="numeric"
              autocomplete="off"
              value={toText}
              placeholder="yyyy"
              readonly={applying}
              aria-invalid={entryRefusal?.at.to === true}
              aria-describedby={problemId}
              oninput={(event) => editYear("to", event.currentTarget.value)}
              onchange={() => {
                entryCommitted = true;
              }}
            />
          </div>
          <!-- Named per source, so a screen-reader controls list tells one card's
               Apply from the next's — the same disambiguation the Remove button
               takes, and by the same words.

               NEVER truly disabled, as the catalog's own period card commits its
               years: clicking a refused entry explains the refusal instead of
               leaving a dead button and no reason, and a button that disables
               itself blurs the keyboard that pressed it back to the top of the
               page. `aria-disabled` (paired with the year fields' `readonly`)
               freezes the control for an Apply already in flight — announced and
               styled as unavailable, but never pulled out of the tab order — and a
               press while it holds is a no-op (`applyPeriod`'s own guard). An Apply
               with nothing to do says so instead of doing nothing silently. -->
          <Button
            type="submit"
            size="sm"
            aria-label={applyPeriodLabel}
            aria-disabled={applying}
          >
            Apply period
          </Button>
        </form>
      {:else}
        <!-- A token period, or the #307 comma list two disjoint picks merge into:
             the year fields cannot express it, and collapsing it into a span would
             order years nobody asked for. It stands as it is. -->
        <p class="period-fixed">
          <span class="mono">{periodWire}</span>
          <span class="fixed-note">
            The From and To fields can't express this period — change it in
            <code>project_data.json</code> and open the file again.
          </span>
        </p>
      {/if}

      <!-- Rendered ALWAYS, so a refusal lands in a live region that already existed
           rather than one that appears with it (matching the catalog period card's
           own refusal line). It carries the CONFIRMATION too: a write and the
           refusal it fixes are the same line changing, not a second one appearing.
           An Apply in flight wins over whatever this line said before it — a fresh
           press supersedes a stale verdict — and an unchanged entry gets its own
           quiet word rather than the silence a no-op used to leave. -->
      <p
        class="problem"
        class:refused={!applying && entryRefusal !== null}
        class:applied={!applying && entryRefusal === null && appliedLabel !== null}
        class:info={applying ||
          (entryRefusal === null && appliedLabel === null && unchangedNotice)}
        id="{uid}-problem"
        role="status"
      >
        {#if applying}
          <span aria-hidden="true">i</span>
          Waiting for the project to load…
        {:else if entryRefusal !== null}
          <span aria-hidden="true">✕</span>
          {entryRefusal.problem}
        {:else if appliedLabel !== null}
          <span aria-hidden="true">✓</span>
          {appliedLabel}
        {:else if unchangedNotice}
          <span aria-hidden="true">i</span>
          Period unchanged.
        {/if}
      </p>

      {#if writeRefused}
        <p class="stale" role="alert">
          <span aria-hidden="true">▲</span>
          This source changed while you were editing, so its period was not changed.
          The years now show what the source holds — enter them again and apply.
        </p>
      {/if}

      {#if deviation}
        <!-- The window is an authoring SEED, not an inheritance (reg_schema/DESIGN.md):
             a source may deliberately cover more or less. So this MARKS the
             divergence rather than warning about it — whether the study window is
             actually left uncovered is a validation finding, and it has one. -->
        <p class="deviation">
          Differs from study window {deviation.from}–{deviation.to}
        </p>
      {/if}
    </div>

    <!-- The cart says COLUMNS: a binding is one delivery column of the order, and
         that is the word the rail and the researcher already use. The mono
         `bindings` below is deliberately NOT renamed — it is the project_data.json
         key someone hand-editing the file has to find. -->
    <div class="bindings" aria-label="Columns">
      <h4>Columns ({bindings.length})</h4>

      {#if bindingsMalformed}
        <p class="error" role="alert">
          This source's <code>bindings</code> are malformed — fix via re-download or hand-edit.
        </p>
      {:else if bindings.length === 0}
        <EmptyState
          title="No columns yet. Browse the catalog to add columns from this register."
        />
      {:else}
        <ul class="binding-list">
          <!-- Keyed by the store-owned STABLE client id (issue #200), not the index,
               so a middle binding remove remounts the correct BindingEditor instance.
               The id lives only in the store, never in the draft. -->
          {#each bindings as binding, j (projectStore.bindingId(sourceIndex, j))}
            <li>
              <!-- The source's own (variant, period) travels with the row: a
                   column's default name is the one delivered THERE, so the row
                   resolves it at this source's coordinate, not the variable's
                   whole history. -->
              <BindingEditor
                sourceIndex={sourceIndex}
                bindingIndex={j}
                binding={binding}
                variant={variantSlug}
                period={periodWire}
              />
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  {/if}
</section>

<style>
  .source {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: var(--space-4);
    margin-bottom: var(--space-4);
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
    scroll-margin-top: var(--space-4);
  }
  /* Briefly highlights a card when the findings panel locates it. `:global` because
     the class is toggled imperatively on the DOM node by ValidationPanel, not bound
     here (Svelte would otherwise prune the unused selector). */
  :global(.locate-flash) {
    animation: locate-flash 1.6s ease-out;
  }
  @keyframes locate-flash {
    0%,
    25% {
      box-shadow: 0 0 0 2px var(--accent);
    }
    100% {
      box-shadow: 0 0 0 2px transparent;
    }
  }
  .source-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
  }
  /* The title block: the register heading with its variant line stacked under it on
     the spacing rhythm. The heading stays FIRST, so `.source-head`'s baseline
     alignment still puts the Remove button on the heading's own line. As a flex
     child of `.source-head` this block defaults to `min-width: auto`, so a long
     unbroken register name would refuse to shrink and overflow the card on mobile;
     `min-width: 0` lets it shrink (#1110). */
  .source-title {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    min-width: 0;
  }
  .source-head h3 {
    margin: 0;
    font-weight: var(--heading-weight);
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    /* `min-width: 0` + `overflow-wrap: anywhere` (inherited by the name text run,
       which itself becomes an anonymous flex item here) lower the text's min-content
       contribution so a long unbroken register name breaks within the heading
       instead of clipping. The error Tag sits in its own flex item, so the name
       absorbs the shrink and the badge is not squeezed (#1110). */
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* The variant that names the POPULATION: a qualifier of the heading, not the
     card's subject — muted at the small size, the treatment the variant browser
     gives a variant's own metadata line under its name. It is a word, so it never
     takes the machine face, even under a heading that has fallen back to one. */
  .source-variant {
    margin: 0;
    font-size: var(--text-sm);
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  /* A title that has fallen back to the raw `register_variant` is an identifier, not
     a name: mono, like every other FQID in the app, so a reader can tell the two
     apart at a glance (DESIGN.md → Typography). */
  .source-head h3.mono {
    font-family: var(--font-mono);
  }
  /* The error badge inside that heading is COPY — "3 errors" — so it keeps the UI
     face even where the heading has fallen back to the coordinate. `Tag` declares no
     face of its own (frontend/DESIGN.md binds the primitive to mono), so a mono-faced
     context sets the UI face on its own usage, as the register list does. */
  .source-head h3.mono :global(.tag) {
    font-family: var(--font-ui);
  }
  /* The period control: the same entry the catalog's period card carries, on the
     card's own spacing rhythm rather than in a second bordered box (cards inside
     cards, DESIGN.md), and on the SAME two columns `KeyValue` lays the rows above it
     on, so the card keeps ONE term column — the label sits where Provider does, the
     control where its value does. The control is compound (an entry row, then
     whatever it has to say), so everything after the label stacks in the value
     column. */
  .source-period {
    display: grid;
    grid-template-columns: minmax(8rem, max-content) 1fr;
    gap: var(--space-1) var(--space-3);
  }
  .source-period > :not(.micro-label) {
    grid-column: 2;
  }
  /* Narrow: a value column 8rem in cannot hold two year fields and an Apply without
     clipping them, so the label goes back above a full-width control — at the shell's
     own collapse breakpoint. */
  @media (max-width: 48rem) {
    .source-period {
      display: flex;
      flex-direction: column;
      gap: var(--space-1);
    }
  }
  .period-entry {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-2) var(--space-3);
  }
  .years {
    display: flex;
    align-items: center;
    gap: var(--space-2);
  }
  .years label {
    white-space: nowrap;
  }
  /* The exact-year field, as the catalog's own period entry sets it. */
  .year {
    box-sizing: border-box;
    width: 5rem;
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--border-strong);
    border-radius: var(--radius-sm);
    background: var(--surface);
    color: var(--text);
    /* A year is a machine identifier (DESIGN.md → Typography): mono, tabular. */
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    font-variant-numeric: tabular-nums;
  }
  .year:focus-visible {
    outline: none;
    border-color: var(--accent);
    box-shadow: var(--focus-ring);
  }
  .year[aria-invalid="true"] {
    border-color: var(--err);
  }
  /* An Apply already in flight: frozen (`readonly`, not `disabled` — a read-only
     field stays in the tab order), dimmed the way the Apply button's own
     `aria-disabled` is (Button.svelte). */
  .year:read-only {
    opacity: 0.6;
  }
  /* A period the year fields cannot author: the wire itself (a machine value, so
     mono) with the reason beside it, at the muted metadata weight. */
  .period-fixed {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-1) var(--space-2);
    margin: 0;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .period-fixed .mono {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
  }
  .fixed-note {
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  /* Small, but NOT muted: this is a standing fact about the source the researcher
     asked to see, not metadata to skim past (DESIGN.md → Colors). */
  .deviation {
    margin: 0;
    color: var(--text);
    font-size: var(--text-sm);
  }
  /* Status ROWS: the status tint as fill, the status foreground as text, the glyph
     first (frontend/DESIGN.md → Banners and status rows). The refusal line keeps no
     fill and no height while it says nothing. */
  .problem {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    margin: 0;
    color: var(--err);
    font-size: var(--text-sm);
  }
  .problem.refused {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--err-border);
    border-radius: var(--radius-sm);
    background: var(--err-bg);
  }
  /* The same row, reporting rather than refusing — the cool OK roles, as
     ValidationPanel's clean verdict wears them. */
  .problem.applied {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--ok);
    border-radius: var(--radius-sm);
    background: var(--ok-bg);
    color: var(--ok);
  }
  /* Neither a refusal nor a change: the write is still waiting on the restore
     gate, or the entry already names the stored period. Cool info, not ok or err —
     nothing failed and nothing happened. */
  .problem.info {
    padding: var(--space-1) var(--space-2);
    border: 1px solid var(--info);
    border-radius: var(--radius-sm);
    background: var(--info-bg);
    color: var(--info);
  }
  .stale {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    margin: 0;
    padding: var(--space-1) var(--space-2);
    border-radius: var(--radius-sm);
    background: var(--warn-bg);
    color: var(--warn);
    font-size: var(--text-sm);
  }
  .bindings h4 {
    margin: 0 0 var(--space-2);
    font-weight: var(--heading-weight);
  }
  .binding-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
</style>
