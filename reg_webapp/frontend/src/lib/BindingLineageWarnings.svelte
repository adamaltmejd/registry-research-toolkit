<script lang="ts">
import { Collapsible } from "bits-ui";
import { getBindingLineageWarnings } from "./api";
import { asyncResource } from "./async.svelte";

let { fqidPath }: { fqidPath: string } = $props();

const warnings = asyncResource(() => getBindingLineageWarnings(fqidPath));
const rows = $derived(warnings.data?.lineage_warnings ?? []);
const visible = $derived(!!warnings.error || rows.length > 0);
let open = $state(false);

$effect(() => {
  if (warnings.error) {
    open = true;
  }
});
</script>

{#if visible}
  <Collapsible.Root class="lineage-warnings" bind:open>
    <Collapsible.Trigger class="warning-trigger">
      Lineage warnings
      {#if rows.length > 0}
        <span class="count">{rows.length}</span>
      {/if}
    </Collapsible.Trigger>
    <Collapsible.Content>
      {#if warnings.error}
        <p class="error" role="alert">
          Failed to load lineage warnings: {warnings.error}
        </p>
      {:else}
        <ul class="warnings">
          {#each rows as warning (warning.consumer_state_id + ":" + warning.warning_kind)}
            <li>
              <code>{warning.warning_kind}</code>
              <span>{warning.message}</span>
            </li>
          {/each}
        </ul>
      {/if}
    </Collapsible.Content>
  </Collapsible.Root>
{/if}

<style>
  :global(.lineage-warnings) {
    margin-top: 0.75rem;
    color: var(--muted);
    font-size: var(--text-sm, 0.9rem);
  }
  :global(.warning-trigger) {
    appearance: none;
    border: 0;
    background: transparent;
    color: inherit;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    font: inherit;
    padding: 0;
    text-align: left;
  }
  .count {
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--muted);
    font-size: 0.8em;
    line-height: 1;
    padding: 0.15rem 0.4rem;
  }
  .warnings {
    list-style: none;
    padding: 0;
    margin: 0.5rem 0 0;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .warnings li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
  }
  .warnings code {
    font-size: 0.85em;
  }
</style>
