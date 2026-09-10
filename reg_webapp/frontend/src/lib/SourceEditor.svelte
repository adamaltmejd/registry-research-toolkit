<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import { fqidSegments, sourceCardHeading } from "./catalog";
import { sourceNames } from "./catalog_names.svelte";
import { periodToWire } from "./period";
import {
  type Period,
  type SafeSource,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
  sourceBindingsMalformed,
} from "./project_data";
import { projectStore } from "./project_store.svelte";
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

// READ-ONLY source card in the #991 data-order cart: the cart SHOWS what has been
// picked (a register and the columns taken from it) and supports delete +
// navigate-out only — adding or changing data happens in the catalog browser, not
// here. So this card DISPLAYS the register it delivers from, its
// register_variant / period / name, and offers "Remove source"; it carries no
// inputs, pickers, or PeriodEditor. The header still rolls up all errors under
// `/sources/{i}` as a badge (fixes are reached via the ValidationPanel's catalog
// link). See reg_webapp/DESIGN.md and issue #991.
const { sourceIndex, source, issues, providerQualified } = $props<{
  sourceIndex: number;
  source: SafeSource;
  issues: ValidationIssue[];
  /** Whether this deployment serves more than one provider, so the card names
   * the provider that owns the register above its heading: a bare register name
   * can stand for two registers there. A deployment fact, read once in
   * `App.svelte` and threaded down like `steward` — no route fetches it. */
  providerQualified: boolean;
}>();

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

// The period as a read-only display string (list-period aware — `periodToWire`
// already joins list segments); null → the "(no period)" fallback.
const periodDisplay = $derived(
  periodToWire(safeSourcePeriod(source) as Period),
);

// The read-only coordinate rows, rendered through the shared KeyValue primitive
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
  { label: "Period", value: periodDisplay ?? "(no period)" },
  {
    label: "Source name",
    value: safeSourceName(source) || "(unnamed source)",
    mono: true,
  },
] satisfies KeyValueRow[]);

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
const removeQuestion = $derived(
  identified
    ? `Remove the ${heading} source${variantName ? ` (${variantName})` : ""} and its ${columnCount}?`
    : `Remove this source and its ${columnCount}?`,
);
// Two sources on the SAME register share a heading, and two in one succession
// family differ by a few words of frame — so the concrete coordinate rides along
// in the button's name, the one thing that always differs. Not where the heading
// already IS that coordinate.
const removeLabel = $derived(
  identified
    ? `Remove source ${heading}${variantName ? `, ${variantName}` : ""}${
        titleIsCoordinate ? "" : ` (${registerVariant})`
      }`
    : undefined,
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
                period={periodDisplay}
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
