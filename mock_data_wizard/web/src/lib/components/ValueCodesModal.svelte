<script lang="ts">
  import { onMount } from "svelte";

  import { getColumnValues, type ColumnValuesResponse } from "../api";
  import Modal from "./Modal.svelte";

  interface Props {
    register: string | null;
    column: string;
    onClose: () => void;
  }

  let { register, column, onClose }: Props = $props();

  // Loading + result state. The fetch is on-demand (per CLAUDE.md
  // request: "fetching them on click from regmeta, not prefetching"), so
  // the modal opens immediately and replaces "Loading…" with the result
  // once the network round-trip completes. Errors render inline rather
  // than as a toast — the popover is throwaway, so co-locating the
  // failure with the empty list saves the user a glance away.
  type LoadState =
    | { kind: "loading" }
    | { kind: "ok"; data: ColumnValuesResponse }
    | { kind: "error"; message: string };
  // `null` = render the column's default (most-common) classification.
  // Set by clicking a chip in the picker; passed back to the server on
  // re-fetch so the popup honors the pick.
  let pickedClassification: string | null = $state(null);
  let loadState: LoadState = $state({ kind: "loading" });

  onMount(() => {
    void load();
  });

  async function load(): Promise<void> {
    loadState = { kind: "loading" };
    try {
      const data = await getColumnValues({
        register,
        column,
        picked_classification: pickedClassification,
      });
      loadState = { kind: "ok", data };
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      loadState = { kind: "error", message };
    }
  }

  function pickClassification(short_name: string): void {
    if (pickedClassification === short_name) return;
    pickedClassification = short_name;
    void load();
  }
</script>

<Modal headingId="value-codes-heading" {onClose}>
  <header>
    <div class="heading-stack">
      <span class="meta-line">
        value codes
        {#if register}· <span class="register-name">{register}</span>{/if}
      </span>
      <h3 id="value-codes-heading" class="mono">{column}</h3>
      {#if loadState.kind === "ok" && loadState.data.kind !== "none"}
        <span class="kind-tag kind-{loadState.data.kind}" title={loadState.data.kind}>
          {loadState.data.kind === "classification"
            ? `classification · ${loadState.data.title}`
            : `value codes · ${loadState.data.codes.length}`}
        </span>
      {/if}
    </div>
    <button type="button" class="close" aria-label="Close" onclick={onClose}>
      ×
    </button>
  </header>

  {#if loadState.kind === "loading"}
    <p class="status">Loading…</p>
  {:else if loadState.kind === "error"}
    <p class="status error">
      Could not load values: {loadState.message}
      <button type="button" class="retry" onclick={() => void load()}>
        Retry
      </button>
    </p>
  {:else if loadState.data.kind === "none"}
    <p class="status muted">
      regmeta has no value codes for <code>{column}</code>{register
        ? ` under ${register}`
        : ""}.
    </p>
  {:else}
    {#if loadState.data.note}
      <p class="variance-note variance-{loadState.data.tier ?? '1'}">
        {loadState.data.note}
      </p>
    {/if}
    {#if loadState.data.classifications.length > 1}
      {@const picked =
        loadState.data.picked_classification ?? loadState.data.classifications[0]}
      <div class="classification-picker">
        <span class="picker-label">Classification:</span>
        <ul class="picker-chips">
          {#each loadState.data.classifications as sn (sn)}
            <li>
              <button
                type="button"
                class="chip"
                class:active={sn === picked}
                aria-pressed={sn === picked}
                onclick={() => pickClassification(sn)}
              >
                {sn}
              </button>
            </li>
          {/each}
        </ul>
      </div>
    {/if}
    {#if loadState.data.description}
      <p class="description">{loadState.data.description}</p>
    {/if}
    <div class="codes-wrap">
      <table class="codes">
        <colgroup>
          <col class="col-code" />
          <col class="col-label" />
        </colgroup>
        <thead>
          <tr>
            <th>Code</th>
            <th>Label</th>
          </tr>
        </thead>
        <tbody>
          {#each loadState.data.codes as c (c.code)}
            <tr>
              <td class="mono">{c.code}</td>
              <td>{c.label ?? ""}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}

  <footer>
    <button type="button" onclick={onClose}>Close</button>
  </footer>
</Modal>

<style>
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .meta-line {
    color: #777;
    font-size: 0.82rem;
  }
  .register-name {
    color: #1a3b80;
    font-weight: 500;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
    word-break: break-word;
  }
  .mono {
    font-family: ui-monospace, monospace;
  }
  .kind-tag {
    display: inline-block;
    align-self: flex-start;
    padding: 0.05rem 0.4rem;
    border-radius: 3px;
    font-size: 0.78rem;
    background: #f0e8fa;
    color: #5d2b8c;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
    flex: 0 0 auto;
    padding: 0;
    line-height: 1;
  }
  .status {
    margin: 0;
    color: #555;
  }
  .status.muted {
    color: #888;
  }
  .status.error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    color: #882020;
    padding: 0.5rem 0.75rem;
    border-radius: 4px;
  }
  .retry {
    margin-left: 0.5rem;
    border: 1px solid currentColor;
    background: transparent;
    color: inherit;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    cursor: pointer;
    font: inherit;
  }
  /* Variance notes (issue #64). Tier 2 = code set differs across years
     but labels are stable. Tier 3a = same code, different labels (the
     dangerous one). Tier 3b = different classifications (paired with
     the picker). Color escalates from amber to red so the eye lands on
     3a / 3b without reading the text first. */
  .variance-note {
    margin: 0 0 0.4rem;
    padding: 0.35rem 0.6rem;
    border-radius: 4px;
    font-size: 0.85rem;
    line-height: 1.35;
    border: 1px solid #e8c184;
    background: #fff7e6;
    color: #7a4a00;
  }
  .variance-3a {
    border-color: #e0a0a0;
    background: #fde8e8;
    color: #882020;
  }
  .variance-3b {
    border-color: #e8c184;
    background: #fff7e6;
    color: #7a4a00;
  }
  .classification-picker {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin: 0.2rem 0 0.5rem;
    flex-wrap: wrap;
  }
  .picker-label {
    color: #555;
    font-size: 0.85rem;
  }
  .picker-chips {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }
  .chip {
    padding: 0.1rem 0.55rem;
    border: 1px solid #c8d3ec;
    border-radius: 3px;
    background: #eef2fb;
    color: #1a3b80;
    font: inherit;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
    cursor: pointer;
  }
  .chip:hover {
    background: #e0e7f7;
  }
  .chip.active {
    background: #1a3b80;
    color: #fff;
    border-color: #1a3b80;
  }
  .chip:focus-visible {
    outline: 2px solid #1a3b80;
    outline-offset: 1px;
  }
  .description {
    margin: 0;
    color: #555;
    font-size: 0.9rem;
  }
  /* Code list often runs to hundreds of rows for big classifications
     (SUN, SSYK, …). Cap height + scroll inside so the modal stays at a
     usable size and the Close button remains reachable. */
  .codes-wrap {
    max-height: 22rem;
    overflow: auto;
    border: 1px solid #e1e1e1;
    border-radius: 4px;
  }
  .codes {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
    table-layout: fixed;
  }
  .codes th,
  .codes td {
    text-align: left;
    padding: 0.25rem 0.5rem;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: top;
    overflow-wrap: anywhere;
  }
  .codes th {
    position: sticky;
    top: 0;
    background: #f7f7f9;
    color: #555;
    font-weight: 500;
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .col-code {
    width: 8rem;
  }
  footer {
    display: flex;
    justify-content: flex-end;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
</style>
