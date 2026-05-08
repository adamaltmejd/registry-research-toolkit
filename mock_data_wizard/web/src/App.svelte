<script lang="ts">
  import { onMount } from "svelte";

  import GroupCard from "./lib/components/GroupCard.svelte";
  import { store } from "./lib/store.svelte";

  onMount(() => {
    void store.load();
  });

  let snapshot = $derived(store.snapshot);
  let warnings = $derived(snapshot?.warnings ?? []);
</script>

<header class="app-header">
  <h1>mock_data_wizard</h1>
  {#if snapshot}
    <p class="meta">
      contract <code>{snapshot.config.contract_version}</code>
      · {snapshot.groups.length} group{snapshot.groups.length === 1 ? "" : "s"}
      · snapshot
      <code class="dim">{snapshot.snapshot_version.slice(0, 12)}…</code>
    </p>
  {/if}
</header>

{#if store.loadError}
  <div class="banner banner-error">
    <strong>Error loading state.</strong>
    {store.loadError}
    <button onclick={() => store.load()}>Retry</button>
  </div>
{:else if snapshot === null}
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
