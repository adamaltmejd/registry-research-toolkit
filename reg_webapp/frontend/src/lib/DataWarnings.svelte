<script lang="ts">
import type { DataWarningModel } from "./api";
import { formatWindow } from "./catalog";
import TechnicalDetails from "./TechnicalDetails.svelte";
import { Panel, Tag } from "./ui";

let {
  warnings,
  title = "Data warnings",
  framed = true,
}: {
  warnings: DataWarningModel[];
  /** Null renders the bare list, for a caller that supplies its own label. */
  title?: string | null;
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

// A withheld variable is named by its FQID. It was not published, so there is no
// catalog page to link to: show the identifier. (`curation/` paths are labelled
// by `unavailableLabel`.)
function unavailableItems(
  withheld: readonly string[],
): { label: string; fqid: boolean }[] {
  const items = new Map<string, { label: string; fqid: boolean }>();
  for (const field of withheld) {
    const fqid =
      !field.startsWith("curation/") && /^[^/:]+(\/[^/:]+)+$/.test(field);
    const label = fqid ? field : unavailableLabel(field);
    items.set(label, { label, fqid });
  }
  return [...items.values()];
}

// The API returns warnings in `warning_id` (hash) order; a reader scans by period.
// An open start sorts first and an open end last. `sort` is stable, so warnings
// with the same window keep the API order.
const ordered = $derived(
  [...warnings].sort(
    (a, b) =>
      compareBound(a.valid_from, b.valid_from, -1) ||
      compareBound(a.valid_to, b.valid_to, 1),
  ),
);
function compareBound(
  a: string | null | undefined,
  b: string | null | undefined,
  open: -1 | 1,
): number {
  if (a === b) return 0;
  if (a == null) return open;
  if (b == null) return -open;
  return a < b ? -1 : 1;
}
</script>

{#snippet content()}
    <ul class="warnings">
      {#each ordered as warning (warning.warning_id)}
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
            <p class="scope">
              <span>Unavailable metadata:
                {#each unavailableItems(warning.withheld_output) as item, i (item.label)}{#if i > 0}, {/if}{#if item.fqid}variable <code>{item.label}</code>{:else}{item.label}{/if}{/each}</span>
            </p>
          {/if}
          {#if warning.detail !== warning.summary}
            <TechnicalDetails><p>{warning.detail}</p></TechnicalDetails>
          {/if}
        </li>
      {/each}
    </ul>
{/snippet}

{#if warnings.length > 0}
  {#if title === null}
    {@render content()}
  {:else if framed}
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
