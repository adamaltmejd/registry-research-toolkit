<script lang="ts">
import { getDataWarnings } from "./api";
import { asyncResource } from "./async.svelte";
import DataWarnings from "./DataWarnings.svelte";
import { Skeleton } from "./ui";

let {
  fqid,
  period = null,
  variant = null,
  representation = null,
  registerOnly = false,
  framed = true,
}: {
  fqid: string;
  period?: string | null;
  variant?: string | null;
  representation?: string | null;
  registerOnly?: boolean;
  framed?: boolean;
} = $props();
const resource = asyncResource(() =>
  fqid
    ? getDataWarnings(fqid, { period, variant, representation })
    : Promise.resolve([]),
);
const warnings = $derived(
  (resource.data ?? []).filter((warning) =>
    registerOnly ? !warning.variable_fqid : !!warning.variable_fqid,
  ),
);
</script>

{#if resource.loading}
  <div aria-busy="true"><Skeleton /></div>
{:else if resource.error}
  <p class="error" role="alert"><span aria-hidden="true">✕</span> Could not load data warnings: {resource.error}. Check the catalog connection.</p>
{:else}
  <DataWarnings {warnings} {framed} title={registerOnly ? "Unassigned register data warnings" : "Data warnings"} />
{/if}

<style>
  .error { color: var(--err); background: var(--err-bg); padding: var(--space-3); overflow-wrap: anywhere; }
</style>
