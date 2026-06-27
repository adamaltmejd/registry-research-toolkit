<script lang="ts">
import {
  type PickerRepresentation,
  pickerLabeling,
  representationInWindow,
} from "./catalog";

// The direct representation picker (#678 redesign): a variable's representations
// listed as selectable rows; the user picks several and commits them with one
// "Add N to project". REPLACES the auto-planning `choose-variant` selector +
// post-click rep chooser. Thin + presentational: the parent (BindingLeafView)
// owns the data (enumerates `rows` from `node.states`) and the store wiring (the
// `onadd` callback) — this component owns ONLY the selection state + layout.
//
// STRUCTURED as a reusable BAND (header + its rows) so a future group view can
// stack several bands; the binding leaf renders exactly ONE.

let {
  name,
  registerPrefix,
  rows,
  window,
  isSensitive = false,
  isIdentifier = false,
  canAdd,
  onadd,
}: {
  /** The variable's display name (rendered ONCE in the band header). */
  name: string;
  /** The variable's `provider/register` prefix — the band's quiet context. */
  registerPrefix: string;
  /** The representation rows (one per distinct variant+column over the FULL
   * state history; the period window only DIMS, never filters). */
  rows: PickerRepresentation[];
  /** The active period window as an inclusive year pair, or null (no narrowing
   * → no row dims). Rows whose span doesn't overlap it render dimmed. */
  window: [number, number] | null;
  isSensitive?: boolean;
  isIdentifier?: boolean;
  /** Whether the Add action is permitted (the deployment seed is ready). When
   * false the button stays disabled regardless of selection. */
  canAdd: boolean;
  /** Commit the selected rows. The parent maps each to an `addFromCatalog` call
   * and renders the confirmation; this just hands back the picked rows. */
  onadd: (selected: PickerRepresentation[]) => void;
} = $props();

// Adaptive labeling (#678 1b): hoist constant dimensions to the band header and
// show only the varying ones per row. Keyed lookup so each row reads its label
// projection by its (unchanged) selection key.
const labeling = $derived(pickerLabeling(rows));
const labelByKey = $derived(new Map(labeling.rows.map((r) => [r.key, r])));

// Selection by row key. Reset when the row set changes underneath (a different
// variable / a re-enumeration) so a stale key can never commit a vanished row.
let selectedKeys = $state(new Set<string>());
const rowKeys = $derived(rows.map((r) => r.key).join(""));
$effect(() => {
  void rowKeys;
  selectedKeys = new Set<string>();
});

function toggle(key: string): void {
  // Reassign (not mutate) so the `$state` Set is reactive.
  const next = new Set(selectedKeys);
  if (next.has(key)) {
    next.delete(key);
  } else {
    next.add(key);
  }
  selectedKeys = next;
}

const selected = $derived(rows.filter((r) => selectedKeys.has(r.key)));
const selectedCount = $derived(selected.length);

function commit(): void {
  if (selectedCount === 0 || !canAdd) {
    return;
  }
  onadd(selected);
}
</script>

<div class="rep-band">
  <div class="band-header">
    <div class="band-title">
      <span class="var-name">{name}</span>
      <code class="register-prefix">{registerPrefix}</code>
      {#if isIdentifier}
        <span class="badge" title="Identifier">id</span>
      {/if}
      {#if isSensitive}
        <span class="badge sensitive" title="Sensitive">sensitive</span>
      {/if}
    </div>
    <!-- #678 1b: the dimensions shared by every row, hoisted out of the rows and
         rendered once as quiet context (so the rows show only what varies). -->
    {#if labeling.headerContext.length > 0}
      <p class="band-context">{labeling.headerContext.join(" · ")}</p>
    {/if}
  </div>

  <ul class="rep-rows">
    {#each rows as row (row.key)}
      {@const inWindow = representationInWindow(row, window)}
      {@const label = labelByKey.get(row.key)}
      <li>
        <button
          type="button"
          class="rep-row"
          role="checkbox"
          aria-checked={selectedKeys.has(row.key)}
          class:selected={selectedKeys.has(row.key)}
          class:dimmed={!inWindow}
          onclick={() => toggle(row.key)}
        >
          <span class="check" aria-hidden="true"></span>
          <span class="row-main">
            {#if label?.primary.mono}
              <code class="primary mono">{label.primary.text}</code>
            {:else}
              <span class="primary">{label?.primary.text}</span>
            {/if}
            {#if label && label.qualifiers.length > 0}
              <span class="sub">{label.qualifiers.join(" · ")}</span>
            {/if}
          </span>
          {#if label?.period}
            <span class="period">{label.period}</span>
          {/if}
        </button>
      </li>
    {/each}
  </ul>

  <div class="band-footer">
    <span class="count" role="status">
      {selectedCount}
      {selectedCount === 1 ? "representation" : "representations"} selected
    </span>
    <button
      type="button"
      class="add-to-project"
      disabled={selectedCount === 0 || !canAdd}
      onclick={commit}
    >
      Add to project
    </button>
  </div>
</div>

<style>
  /* One variable = one band: the header names the variable ONCE, then its
     representation rows. Structured as a self-contained block so a future group
     view stacks several bands. */
  .rep-band {
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
    margin: 0.75rem 0;
    overflow: hidden;
  }
  .band-header {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    padding: 0.6rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  .band-title {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem 0.6rem;
  }
  .var-name {
    font-weight: 600;
  }
  /* #678 1b: the hoisted constant dimensions — quiet context under the name. */
  .band-context {
    margin: 0;
    font-size: 0.8rem;
    color: var(--text-muted);
  }
  .register-prefix {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: var(--text-muted);
  }
  .badge {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 0.1rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-muted);
  }
  .badge.sensitive {
    border-color: var(--accent);
    color: var(--accent);
  }

  .rep-rows {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .rep-rows li + li {
    border-top: 1px solid var(--border);
  }

  /* A representation row: a click-anywhere checkbox. The whole row is the
     toggle target (real <button> + role=checkbox for keyboard/AT). */
  .rep-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    width: 100%;
    padding: 0.5rem 0.75rem;
    font: inherit;
    text-align: left;
    background: transparent;
    border: none;
    border-left: 3px solid transparent;
    cursor: pointer;
  }
  .rep-row:hover {
    background: var(--accent-bg);
  }
  .rep-row.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  /* Out-of-window rows are de-emphasised but stay selectable. */
  .rep-row.dimmed {
    opacity: 0.45;
  }
  .rep-row.dimmed:hover {
    opacity: 0.7;
  }

  .check {
    flex: 0 0 auto;
    width: 1rem;
    height: 1rem;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: var(--surface);
  }
  .rep-row.selected .check {
    border-color: var(--accent);
    background: var(--accent);
    /* A check mark drawn with a rotated box border — no asset, theme-following. */
    background-image: linear-gradient(
        45deg,
        transparent 46%,
        var(--accent-ink) 46%,
        var(--accent-ink) 54%,
        transparent 54%
      ),
      linear-gradient(
        -45deg,
        transparent 60%,
        var(--accent-ink) 60%,
        var(--accent-ink) 68%,
        transparent 68%
      );
  }

  .row-main {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  /* The prominent row label — the first VARYING dimension. Mono when it is the
     delivery column; normal weight when it is the population / value set. */
  .primary {
    font-size: 0.9rem;
    overflow-wrap: anywhere;
  }
  .primary.mono {
    font-family: var(--font-mono);
  }
  .sub {
    font-size: 0.8rem;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .period {
    flex: 0 0 auto;
    font-size: 0.8rem;
    color: var(--text-muted);
    text-align: right;
    white-space: nowrap;
  }

  .band-footer {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    padding: 0.6rem 0.75rem;
    border-top: 1px solid var(--border);
  }
  .count {
    font-size: 0.85rem;
    color: var(--text-muted);
  }
  .add-to-project {
    font: inherit;
    font-size: 0.9rem;
    padding: 0.35rem 0.9rem;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--accent-bg);
    color: var(--accent-ink);
    cursor: pointer;
  }
  .add-to-project:hover:enabled {
    background: var(--surface);
  }
  .add-to-project:disabled {
    opacity: 0.5;
    cursor: default;
  }
</style>
