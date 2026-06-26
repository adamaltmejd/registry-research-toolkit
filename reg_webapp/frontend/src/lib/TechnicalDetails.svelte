<script lang="ts">
import type { Snippet } from "svelte";

// #638 PR4 (density pass): the shared "Technical details" disclosure. Each subject
// page (variable / variable state / concept group) demotes its backend/structural
// fields — type/length, delivery column, is_identifier/is_sensitive, the group's
// internal source tag — behind this collapsed <details> so they stay AVAILABLE
// without crowding the user-facing fields above. One component keeps the summary
// label + the disclosure styling consistent across all three call sites. Matches
// the app's plain-<details> disclosure idiom (ClassificationLineagePanels /
// SearchView's concept-group fold): no chrome, a muted summary, default collapsed.
//
// The caller passes the demoted rows as `children` (typically a `<dl class="meta">`);
// callers omit this component entirely when there is nothing to demote.
let { children }: { children: Snippet } = $props();
</script>

<details class="tech-details">
  <summary>Technical details</summary>
  {@render children()}
</details>

<style>
  .tech-details {
    margin: 0.5rem 0;
  }
  .tech-details summary {
    cursor: pointer;
    color: var(--text-muted);
    font-size: 0.9em;
  }
</style>
