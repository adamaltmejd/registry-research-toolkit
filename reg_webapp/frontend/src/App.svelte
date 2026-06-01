<script lang="ts">
import { onMount } from "svelte";
import type { components } from "./lib/api-types";

type Context = components["schemas"]["ContextResponse"];

let context = $state<Context | null>(null);
let error = $state<string | null>(null);

onMount(async () => {
  try {
    const resp = await fetch("/api/context");
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    context = (await resp.json()) as Context;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
});
</script>

<main>
  {#if error}
    <p class="error">Failed to load context: {error}</p>
  {:else if context}
    <h1>{context.steward.long_name}</h1>
    <p class="subtitle">{context.steward.name} ({context.steward.id})</p>
    <dl>
      <dt>reg_meta schema</dt>
      <dd>{context.reg_meta.schema_version}</dd>
      <dt>reg_meta imported</dt>
      <dd>{context.reg_meta.import_date}</dd>
      <dt>webapp version</dt>
      <dd>{context.webapp.version}</dd>
      <dt>reg_meta version</dt>
      <dd>{context.webapp.reg_meta_version}</dd>
    </dl>
  {:else}
    <p>Loading…</p>
  {/if}
</main>

<style>
  main {
    font-family: system-ui, sans-serif;
    max-width: 40rem;
    margin: 2rem auto;
    padding: 0 1rem;
  }
  .subtitle {
    color: #555;
  }
  .error {
    color: #b00;
  }
  dl {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.25rem 1rem;
  }
  dt {
    font-weight: 600;
  }
</style>
