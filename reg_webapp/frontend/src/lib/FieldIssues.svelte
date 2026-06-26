<script lang="ts">
import { codeLabel, type ValidationIssue } from "./validation";

// The single inline-highlight primitive every editor field mounts. Presentational
// ONLY: the parent has ALREADY filtered the issue list to this field's pointer
// (via `issuesForPointer(validation.issues, ptr)`), so this just renders them. The
// backend is the canonical validator (see reg_webapp/DESIGN.md → Pydantic
// boundary) — these echo the LAST /validate click
// and vanish on the next edit (the store nulls `validation` on every mutation).
//
// Color follows ValidationPanel's convention via the status roles
// (--err / --warn / --info). An unknown/unset level degrades to the info color
// (forward-compat) — never the brand accent (DESIGN.md accent-vs-status).
const { issues } = $props<{ issues: ValidationIssue[] }>();
</script>

{#if issues.length > 0}
  <ul class="field-issues" role="alert">
    {#each issues as issue, i (`${issue.code}|${i}`)}
      <li class="issue {issue.level}">
        <span class="label">{codeLabel(issue.code)}</span>
        <span class="message">{issue.message}</span>
      </li>
    {/each}
  </ul>
{/if}

<style>
  .field-issues {
    list-style: none;
    padding: 0;
    margin: 0.25rem 0 0;
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .issue {
    font-size: var(--text-sm);
    line-height: 1.3;
    padding-left: var(--space-2);
    /* Default/unknown level degrades to --info, NOT the brand accent
       (accent-as-status would violate DESIGN.md accent-vs-status). */
    border-left: 2px solid var(--info);
  }
  .issue.error {
    border-left-color: var(--err);
  }
  .issue.warning {
    border-left-color: var(--warn);
  }
  .issue.info {
    border-left-color: var(--info);
  }
  .label {
    font-weight: 600;
  }
  .issue.error .label {
    color: var(--err);
  }
  .issue.warning .label {
    color: var(--warn);
  }
  .message {
    color: var(--text-muted);
  }
</style>
