<script lang="ts">
  import { getWarnings } from "./api";
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
    collapsed = false,
  }: {
    fqid: string;
    period?: string | null;
    variant?: string | null;
    representation?: string | null;
    registerOnly?: boolean;
    framed?: boolean;
    /** Fold the list into one counted disclosure. The variable and register pages
     * pass it with `registerOnly` (#1300, #1708): unassigned warnings name no
     * variable (ownership was not established, so none is inferred), and a register
     * can carry hundreds, so they read as one line instead of an expanded list. */
    collapsed?: boolean;
  } = $props();
  const resource = asyncResource(() =>
    fqid
      ? getWarnings(fqid, {
          period,
          variant,
          representation,
          unassigned_only: registerOnly,
        })
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
  <p class="error" role="alert">
    <span aria-hidden="true">✕</span> Could not load data warnings: {resource.error}.
    Check the catalog connection.
  </p>
{:else if collapsed}
  {#if warnings.length > 0}
    <details class="collapsed-warnings">
      <summary>
        {warnings.length} data {warnings.length === 1 ? "warning" : "warnings"} not
        assigned to a variable
      </summary>
      <p class="muted">
        The register records these limitations without naming the variable they
        concern, so they may apply to any of its variables.
      </p>
      <DataWarnings {warnings} title={null} />
    </details>
  {/if}
{:else}
  <DataWarnings
    {warnings}
    {framed}
    title={registerOnly ? "Unassigned register data warnings" : "Data warnings"}
  />
{/if}

<style>
  .collapsed-warnings summary {
    cursor: pointer;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .collapsed-warnings p {
    margin: var(--space-2) 0 var(--space-3);
    font-size: var(--text-sm);
  }
  .error {
    color: var(--err);
    background: var(--err-bg);
    padding: var(--space-3);
    overflow-wrap: anywhere;
  }
</style>
