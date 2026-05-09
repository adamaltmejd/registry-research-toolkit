<script lang="ts">
  import { onMount } from "svelte";

  import GroupCard from "./lib/components/GroupCard.svelte";
  import { store } from "./lib/store.svelte";

  onMount(() => {
    void store.load();
  });

  let snapshot = $derived(store.snapshot);
  let warnings = $derived(snapshot?.warnings ?? []);

  // Header count: peel off the synthetic `noreg-…` "unassigned" group so
  // "11 groups" doesn't read as "11 registers" when one of them is in
  // fact unmatched. Keep an explicit sub-count when any are unassigned.
  let registerCount = $derived(
    (snapshot?.groups ?? []).filter((g) => g.register_id !== null).length,
  );
  let unassignedCount = $derived(
    (snapshot?.groups ?? []).filter((g) => g.register_id === null).length,
  );

  let totalPanels = $derived(snapshot?.config.panels.length ?? 0);
</script>

<header class="app-header">
  <h1>mock_data_wizard</h1>
  {#if snapshot}
    <p class="meta">
      contract <code>{snapshot.config.contract_version}</code>
      · {registerCount} register{registerCount === 1 ? "" : "s"}
      {#if unassignedCount > 0}
        + {unassignedCount} unassigned
      {/if}
      {#if totalPanels > 0}
        · {totalPanels} panel{totalPanels === 1 ? "" : "s"}
      {/if}
      · snapshot
      <code
        class="dim"
        title={`snapshot_version ${snapshot.snapshot_version}\ndiscover_hash ${snapshot.config.discover_hash ?? "—"}`}
        >{snapshot.snapshot_version.slice(0, 12)}…</code
      >
    </p>
  {/if}
</header>

{#if store.loadState.kind === "error"}
  <div class="banner banner-error">
    <strong>Error loading state.</strong>
    {store.loadState.message}
    <button onclick={() => store.load()}>Retry</button>
  </div>
{:else if store.loadState.kind === "uninitialised"}
  <section class="empty-state">
    <h2>This project hasn't been configured yet</h2>
    <p>
      Initialise from <code>mock_data_discovery.json</code> to apply the
      auto-classifier (id-name → regmeta classification → categorical
      heuristics → SQL type). You'll be able to review and override every
      column before extract.
    </p>
    <button
      class="primary"
      onclick={() => store.init()}
      disabled={store.busy}
    >
      {store.busy ? "Initialising…" : "Initialise project"}
    </button>
    <p class="muted">
      No discover file? Run the discover step on MONA first; the
      <code>mdw_runner.py</code> bundle writes
      <code>mock_data_discovery.json</code> next to itself.
    </p>
  </section>
{:else if store.loadState.kind === "loading" || snapshot === null}
  <p class="loading">Loading snapshot…</p>
{:else}
  {#each warnings as warn (warn.code)}
    <div class="banner banner-warning">
      <strong>{warn.code}</strong>
      <span>{warn.message}</span>
    </div>
  {/each}

  <main>
    {#each snapshot.groups as group (group.group_id)}
      <GroupCard {group} />
    {/each}
  </main>
{/if}

<aside class="toasts" aria-live="polite">
  {#each store.toasts as toast (toast.id)}
    <div class="toast toast-{toast.level}">
      <span>{toast.message}</span>
      <button
        class="dismiss"
        aria-label="Dismiss"
        onclick={() => store.dismissToast(toast.id)}>×</button
      >
    </div>
  {/each}
</aside>

<style>
  :global(body) {
    margin: 0;
    background: #f7f7f9;
    color: #222;
    font-family: system-ui, sans-serif;
    line-height: 1.45;
  }
  .app-header {
    background: #1656c0;
    color: #fff;
    padding: 1rem 2rem;
  }
  .app-header h1 {
    margin: 0;
    font-size: 1.25rem;
    letter-spacing: 0.02em;
  }
  .app-header .meta {
    margin: 0.25rem 0 0;
    color: #cfdaef;
    font-size: 0.9rem;
  }
  .app-header code {
    background: rgba(255, 255, 255, 0.12);
    padding: 0.05rem 0.3rem;
    border-radius: 3px;
  }
  .app-header code.dim {
    color: #b0bcdb;
    cursor: help;
  }
  main {
    max-width: 72rem;
    margin: 1.25rem auto;
    padding: 0 1rem;
  }
  .loading {
    text-align: center;
    padding: 2rem;
    color: #777;
  }
  .empty-state {
    max-width: 36rem;
    margin: 3rem auto 1rem;
    background: #fff;
    border: 1px solid #e1e1e1;
    border-radius: 6px;
    padding: 1.5rem 1.75rem;
    text-align: center;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
  }
  .empty-state h2 {
    margin: 0 0 0.5rem;
    font-size: 1.1rem;
  }
  .empty-state p {
    margin: 0.4rem 0;
    color: #444;
    font-size: 0.95rem;
  }
  .empty-state .muted {
    color: #888;
    font-size: 0.85rem;
    margin-top: 1rem;
  }
  .empty-state code {
    background: #f0f0f4;
    padding: 0.05rem 0.3rem;
    border-radius: 3px;
    font-size: 0.85em;
  }
  .empty-state button {
    margin-top: 0.75rem;
    padding: 0.5rem 1rem;
    border: 1px solid #1656c0;
    background: #1656c0;
    color: #fff;
    border-radius: 4px;
    cursor: pointer;
    font: inherit;
  }
  .empty-state button:disabled {
    opacity: 0.6;
    cursor: progress;
  }
  .banner {
    max-width: 72rem;
    margin: 0.5rem auto 0;
    padding: 0.6rem 1rem;
    border-radius: 4px;
    display: flex;
    gap: 0.75rem;
    align-items: center;
    font-size: 0.92rem;
  }
  .banner-warning {
    background: #fff8e1;
    border: 1px solid #f0c14b;
    color: #5b4a14;
  }
  .banner-error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    color: #882020;
  }
  .banner button {
    margin-left: auto;
    padding: 0.25rem 0.7rem;
    border: 1px solid currentColor;
    background: transparent;
    color: inherit;
    cursor: pointer;
    border-radius: 3px;
  }
  .toasts {
    position: fixed;
    bottom: 1rem;
    right: 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    z-index: 200;
    width: min(24rem, 90vw);
  }
  .toast {
    display: flex;
    align-items: flex-start;
    gap: 0.5rem;
    padding: 0.6rem 0.75rem;
    border-radius: 4px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
    font-size: 0.9rem;
  }
  .toast-info {
    background: #e8f1fa;
    color: #114a85;
  }
  .toast-warning {
    background: #fff8e1;
    color: #5b4a14;
  }
  .toast-error {
    background: #fde8e8;
    color: #882020;
  }
  .dismiss {
    margin-left: auto;
    background: transparent;
    border: 0;
    color: inherit;
    cursor: pointer;
    font-size: 1.1rem;
    line-height: 1;
  }
</style>
