<script lang="ts">
import type { ValidationResultModel } from "./api";
import { codeLabel, type ValidationIssue } from "./validation";

// The validation echo (see reg_schema/DESIGN.md → Structural rules and issue
// codes). Renders the `/validate` result's issue list grouped
// by level (error / warning / info), the ok/not-ok summary, and — distinct from a
// 200 `ok:false` issue list — a malformed-REQUEST banner (a true 4xx ApiError).
// Inline field-highlighting against the draft is c-ii; c-i groups by level.
const { result, requestError } = $props<{
  result: ValidationResultModel | null;
  requestError: string | null;
}>();

const LEVELS = ["error", "warning", "info"] as const;
type Level = (typeof LEVELS)[number];

// Group the issues by level (stable order: errors first). `$derived.by` so the
// grouping recomputes when `result` changes.
const grouped = $derived.by<Record<Level, ValidationIssue[]>>(() => {
  const out: Record<Level, ValidationIssue[]> = {
    error: [],
    warning: [],
    info: [],
  };
  for (const issue of result?.issues ?? []) {
    // An unknown level (forward-compat) buckets under `info` so it still shows.
    const level: Level = LEVELS.includes(issue.level as Level)
      ? (issue.level as Level)
      : "info";
    out[level].push(issue);
  }
  return out;
});

const LEVEL_LABEL: Record<Level, string> = {
  error: "Errors",
  warning: "Warnings",
  info: "Info",
};
</script>

<section class="validation" aria-label="Validation results">
  {#if requestError}
    <!-- A malformed REQUEST (true 4xx) — distinct from the 200 ok:false list. -->
    <p class="banner request-error" role="alert">
      <strong>Request failed:</strong>
      {requestError}
    </p>
  {/if}

  {#if result == null}
    <p class="muted">Not yet validated. Run <strong>Validate</strong> to check this project against the backend.</p>
  {:else}
    <p class="summary {result.ok ? 'ok' : 'fail'}" role="status">
      {#if result.ok}
        Valid — no errors.
        {#if result.issues.length > 0}
          ({result.issues.length} non-blocking {result.issues.length === 1 ? "note" : "notes"}.)
        {/if}
      {:else}
        Not valid — {grouped.error.length}
        {grouped.error.length === 1 ? "error" : "errors"}.
      {/if}
    </p>

    {#each LEVELS as level (level)}
      {#if grouped[level].length > 0}
        <div class="group {level}">
          <h4>{LEVEL_LABEL[level]} ({grouped[level].length})</h4>
          <ul>
            {#each grouped[level] as issue, i (`${issue.code}|${issue.path}|${i}`)}
              <li>
                <div class="issue-head">
                  <code class="code">{issue.code}</code>
                  <span class="label">{codeLabel(issue.code)}</span>
                </div>
                <p class="message">{issue.message}</p>
                {#if issue.path}
                  <code class="path">{issue.path}</code>
                {:else}
                  <span class="path muted">(whole document)</span>
                {/if}
              </li>
            {/each}
          </ul>
        </div>
      {/if}
    {/each}
  {/if}
</section>

<style>
  .validation {
    margin-top: 1.5rem;
  }
  .banner {
    padding: 0.75rem 1rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }
  .request-error {
    background: var(--banner-error-bg);
    border: 1px solid var(--banner-error-border);
  }
  .summary {
    font-weight: 600;
    padding: 0.5rem 0.75rem;
    border-radius: 4px;
  }
  .summary.ok {
    background: #f0fdf4;
    border: 1px solid #86efac;
  }
  .summary.fail {
    background: var(--banner-error-bg);
    border: 1px solid var(--banner-error-border);
  }
  .group {
    margin-top: 1rem;
  }
  .group h4 {
    margin: 0 0 0.5rem;
  }
  .group ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .group li {
    padding: 0.5rem 0.75rem;
    border-left: 3px solid var(--border);
    border-radius: 0 4px 4px 0;
    background: #fafafa;
  }
  .group.error li {
    border-left-color: var(--level-error);
  }
  .group.warning li {
    border-left-color: var(--level-warning);
  }
  .group.info li {
    border-left-color: var(--level-info);
  }
  .issue-head {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .code {
    font-size: 0.8rem;
    color: var(--muted);
  }
  .label {
    font-weight: 600;
  }
  .message {
    margin: 0.25rem 0;
  }
  .path {
    font-size: 0.8rem;
    color: var(--muted);
  }
</style>
