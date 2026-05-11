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
  let state: LoadState = $state({ kind: "loading" });

  onMount(() => {
    void load();
  });

  async function load(): Promise<void> {
    state = { kind: "loading" };
    try {
      const data = await getColumnValues({ register, column });
      state = { kind: "ok", data };
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : String(exc);
      state = { kind: "error", message };
    }
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
      {#if state.kind === "ok" && state.data.kind !== "none"}
        <span class="kind-tag kind-{state.data.kind}" title={state.data.kind}>
          {state.data.kind === "classification"
            ? `classification · ${state.data.title}`
            : `value codes · ${state.data.codes.length}`}
        </span>
      {/if}
    </div>
    <button type="button" class="close" aria-label="Close" onclick={onClose}>
      ×
    </button>
  </header>

  {#if state.kind === "loading"}
    <p class="status">Loading…</p>
  {:else if state.kind === "error"}
    <p class="status error">
      Could not load values: {state.message}
      <button type="button" class="retry" onclick={() => void load()}>
        Retry
      </button>
    </p>
  {:else if state.data.kind === "none"}
    <p class="status muted">
      regmeta has no value codes for <code>{column}</code>{register
        ? ` under ${register}`
        : ""}.
    </p>
  {:else}
    {#if state.data.description}
      <p class="description">{state.data.description}</p>
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
          {#each state.data.codes as c (c.code)}
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
