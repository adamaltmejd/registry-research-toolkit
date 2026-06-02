<script lang="ts">
import { codeLabel, type ValidationIssue } from "./validation";

// The single inline-highlight primitive every editor field mounts. Presentational
// ONLY: the parent has ALREADY filtered the issue list to this field's pointer
// (via `issuesForPointer(validation.issues, ptr)`), so this just renders them. The
// backend is the canonical validator (§9.6) — these echo the LAST /validate click
// and vanish on the next edit (the store nulls `validation` on every mutation).
//
// Color follows ValidationPanel's convention: error #b00020 / warning #d97706 /
// info --accent. An unknown level degrades to the info color (forward-compat).
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
    font-size: 0.8rem;
    line-height: 1.3;
    padding-left: 0.5rem;
    border-left: 2px solid var(--accent);
  }
  .issue.error {
    border-left-color: #b00020;
  }
  .issue.warning {
    border-left-color: #d97706;
  }
  .issue.info {
    border-left-color: var(--accent);
  }
  .label {
    font-weight: 600;
  }
  .issue.error .label {
    color: #b00020;
  }
  .issue.warning .label {
    color: #d97706;
  }
  .message {
    color: var(--muted);
  }
</style>
