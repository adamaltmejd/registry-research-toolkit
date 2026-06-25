<script lang="ts">
import { Collapsible } from "bits-ui";
import type { Snippet } from "svelte";

// #638 PR4 (density pass): the shared "Technical details" disclosure. Each subject
// page (variable / variable state / concept group) demotes its backend/structural
// fields — type/length, delivery column, is_identifier/is_sensitive, the group's
// internal source tag — behind this collapsed disclosure so they stay AVAILABLE
// without crowding the user-facing fields above. One component keeps the summary
// label + the disclosure styling consistent across all three call sites.
//
// The caller passes the demoted rows as `children` (typically a `<dl class="meta">`);
// callers omit this component entirely when there is nothing to demote.
let { children }: { children: Snippet } = $props();
</script>

<Collapsible.Root class="tech-details">
  <Collapsible.Trigger class="tech-trigger">
    Technical details
  </Collapsible.Trigger>
  <Collapsible.Content>
  {@render children()}
  </Collapsible.Content>
</Collapsible.Root>

<style>
  :global(.tech-details) {
    margin: 0.5rem 0;
  }
  :global(.tech-trigger) {
    appearance: none;
    border: 0;
    background: transparent;
    cursor: pointer;
    color: var(--muted);
    font-size: 0.9em;
    font: inherit;
    padding: 0;
    text-align: left;
  }
</style>
