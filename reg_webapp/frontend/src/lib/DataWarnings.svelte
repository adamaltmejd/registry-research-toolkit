<script lang="ts">
import type { components } from "./api-types";
import { formatWindow } from "./catalog";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { Panel, Tag } from "./ui";

let {
  warnings,
  title = "Data warnings",
  framed = true,
}: {
  warnings: components["schemas"]["DataWarning"][];
  title?: string;
  framed?: boolean;
} = $props();
const headingId = $props.id();

function unavailableLabel(field: string): string {
  if (field === "variable") return "verified variable identity";
  if (field === "code_membership" || field === "unbound_value_membership")
    return "verified response codes";
  if (field.startsWith("curation/")) return "unsupported delivery declaration";
  if (field.endsWith("conditional_sensitivity"))
    return "conditional sensitivity flag";
  if (field.endsWith("sensitivity")) return "sensitivity flag";
  if (field.endsWith("identifier")) return "identifier flag";
  if (field.endsWith("value_set")) return "response codes";
  if (field.endsWith("classification")) return "verified classification link";
  if (field.endsWith("data_type")) return "storage type";
  if (field.endsWith("delivery_column_name")) return "delivery column";
  return field.replace(/^state\./, "").replaceAll("_", " ");
}
</script>

{#snippet content()}
    <ul class="warnings">
      {#each warnings as warning (warning.warning_id)}
        <li>
          <Tag tone={warning.severity === "error" ? "error" : "warn"}>
            {#snippet glyph()}▲{/snippet}
            {warning.severity === "error" ? "Error" : "Warning"}
          </Tag>
          <p>{warning.summary}</p>
          {#if warning.delivery_column_name || warning.variant || warning.valid_from || warning.valid_to}
            <p class="scope">
              {#if warning.delivery_column_name}<code>{warning.delivery_column_name}</code>{/if}
              {#if warning.variant}<span>Variant <code>{warning.variant}</code></span>{/if}
              {#if warning.valid_from || warning.valid_to}
                <span>{warning.valid_from && warning.valid_to ? formatWindow(warning.valid_from, warning.valid_to) : warning.valid_from ? `From ${warning.valid_from}` : `Through ${warning.valid_to}`}</span>
              {/if}
            </p>
          {/if}
          {#if warning.withheld_output.length > 0}
            <p class="scope">Unavailable metadata: {[...new Set(warning.withheld_output.map(unavailableLabel))].join(", ")}</p>
          {/if}
          {#if warning.detail !== warning.summary}
            <TechnicalDetails><p>{warning.detail}</p></TechnicalDetails>
          {/if}
        </li>
      {/each}
    </ul>
{/snippet}

{#if warnings.length > 0}
  {#if framed}
    <Panel {title}>{@render content()}</Panel>
  {:else}
    <section class="inline-warnings" aria-labelledby={headingId}>
      <h3 id={headingId}>{title}</h3>
      {@render content()}
    </section>
  {/if}
{/if}

<style>
  .inline-warnings { display: flex; flex-direction: column; gap: var(--space-3); }
  h3 { margin: 0; font-size: var(--text-sm); font-weight: var(--heading-weight); }
  .warnings {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
    list-style: none;
    margin: 0;
    padding: 0;
  }
  li {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: var(--space-2);
    min-width: 0;
  }
  p { margin: 0; max-width: 80ch; overflow-wrap: anywhere; }
  .scope {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  code { font-family: var(--font-mono); }
</style>
