<script lang="ts">
import BindingEditor from "./BindingEditor.svelte";
import { fqidSegments, sourceCardTitle } from "./catalog";
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
  /** Whether this deployment serves more than one provider, so the card's title
   * carries the provider too ("Statistiska Centralbyrån · LISA · Individer, 16 år
   * och äldre"). A deployment fact, read once in `App.svelte` and threaded down
   * like `steward` — no route fetches it. */
  providerQualified: boolean;
}>();

const sourcePtr = $derived(jsonPointer(["sources", sourceIndex]));
const rolledUp = $derived(issuesUnderPointer(issues, sourcePtr));
const errorCount = $derived(rolledUp.filter((i) => i.level === "error").length);

const registerVariant = $derived(safeSourceRegisterVariant(source));

// The card's TITLE is the REGISTER this source delivers from and the concrete
// VARIANT it extracts, in the catalog's own words ("LISA · Individer, 16 år och
// äldre") — what the researcher picked, spelled as every other route spells it.
// The names live only in the catalog, so they are READ from it (cached per
// register); until they land the raw coordinate is the title. The source's `name`
// is a generated join key (`LISA`, `LISA_2`, `LISA_3` for three variants of one
// register), so it titles nothing; it stays visible as a detail row below because
// panels join on it.
const names = $derived(sourceNames(registerVariant));
const sourceMalformed = $derived(source === null);
const sourceTitle = $derived(
  sourceMalformed
    ? "(malformed source)"
    : sourceCardTitle(
        {
          provider: providerQualified ? names.provider : null,
          register: names.register,
          variant: names.variant,
        },
        registerVariant,
      ) || "(no register)",
);
// The title has fallen back to the raw coordinate: while the names are in flight,
// and wherever they cannot be read at all. It is then a machine identifier standing
// in for a name, so it takes the machine face and prints only once on the card.
const titleIsCoordinate = $derived(sourceTitle === registerVariant);
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
// (#804) — same metadata-row styling ProjectEditor uses. The register_variant is a
// machine FQID coordinate (mono): the source extracts that CONCRETE variant, so it
// stays on the card even once the title names it in words — but only then, because
// a title that has fallen back to the coordinate would otherwise print it twice.
// `name` is the panel/order join key (reg_meta `OrderEntry.source`) — a machine
// identifier, so mono.
const metaRows = $derived([
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
    <!-- `aria-busy` while the catalog names are in flight: the title is showing the
         coordinate as a stand-in, and the screenshot driver waits on it (see
         frontend/DESIGN.md → loading surfaces). -->
    <h3
      class:mono={titleIsCoordinate}
      aria-busy={names.loading ? "true" : undefined}
    >
      {sourceTitle}
      {#if errorCount > 0}
        <!-- Status badge: cool error tone + ✕ glyph (aria-hidden); the count text
             carries the meaning for assistive tech (DESIGN.md accent-vs-status). -->
        <Tag tone="error">
          {#snippet glyph()}✕{/snippet}
          {errorCount} error{errorCount === 1 ? "" : "s"}
        </Tag>
      {/if}
    </h3>
    <!-- Per-source accessible name so a screen-reader controls list disambiguates
         the delete buttons (visible text kept as the label prefix — label-in-name).
         Two sources on the SAME register share a title, so the concrete coordinate
         — what actually differs between them — rides along in the name, unless the
         title already IS that coordinate. -->
    <Button
      variant="danger"
      size="sm"
      aria-label={`Remove source ${sourceTitle}${registerVariant && !titleIsCoordinate ? ` (${registerVariant})` : ""}`}
      onclick={() => {
        removeOpen = true;
      }}
    >
      Remove source
    </Button>
  </header>

  <ConfirmDialog
    bind:open={removeOpen}
    title={`Remove ${sourceTitle} and its ${bindings.length} column${bindings.length === 1 ? "" : "s"}?`}
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
  .source-head h3 {
    margin: 0;
    font-weight: var(--heading-weight);
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    /* As a flex child of `.source-head` the h3 defaults to `min-width: auto`, so a
       long unbroken source name would refuse to shrink and overflow the card on
       mobile. `min-width: 0` lets it shrink; `overflow-wrap: anywhere` (inherited by
       the name text run, which itself becomes an anonymous flex item here) lowers
       the text's min-content contribution so it breaks within the heading instead of
       clipping. The error Tag sits in its own flex item, so the name absorbs the
       shrink and the badge is not squeezed (#1110). */
    min-width: 0;
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
