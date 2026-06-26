<script lang="ts">
import type { ValidationResultModel } from "./api";
import { codeLabel, findingLocation, type ValidationIssue } from "./validation";

// The validation echo (see reg_schema/DESIGN.md → Structural rules and issue
// codes). Renders the `/validate` result's issue list grouped
// by level (error / warning / info), the ok/not-ok summary, and — distinct from a
// 200 `ok:false` issue list — a malformed-REQUEST banner (a true 4xx ApiError).
// Inline field-highlighting against the draft is c-ii; c-i groups by level.
//
// Each finding LEADS with the human title (`codeLabel`), DEMOTES the raw `code` to
// a small muted chip (still the stable identifier people cite), and — instead of
// leaking the raw JSON pointer — renders a click-to-LOCATE label
// ("Source 'lisa_main' → binding scb/lisa/adeldag", via `findingLocation`) that
// scrolls to and briefly flashes the relevant source/binding card. `sources` is the
// draft's (possibly malformed) source list, used only to resolve those labels.
const { result, requestError, sources } = $props<{
  result: ValidationResultModel | null;
  requestError: string | null;
  sources: readonly { name?: unknown; bindings?: unknown }[];
}>();

// Scroll the located card into view and flash it. Imperative (not a binding):
// findings live in this panel, cards live in sibling components, and the flash is a
// one-shot visual cue — restarting the CSS animation on re-click needs a reflow
// between class removal and re-add.
function locate(anchorId: string): void {
  const el = document.getElementById(anchorId);
  if (el == null) {
    return;
  }
  el.scrollIntoView({ behavior: "smooth", block: "center" });
  el.classList.remove("locate-flash");
  void el.offsetWidth; // force reflow so the animation re-triggers on re-click
  el.classList.add("locate-flash");
}

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
              {@const loc = findingLocation(issue.path, sources)}
              <li>
                <div class="issue-head">
                  <span class="label">{codeLabel(issue.code)}</span>
                  <!-- The raw code stays visible but DEMOTED — it's the stable id
                       people cite in issues, not the heading. -->
                  <code class="code">{issue.code}</code>
                </div>
                <p class="message">{issue.message}</p>
                {#if loc}
                  <!-- Click-to-locate: scrolls to + flashes the source/binding card
                       instead of leaking the raw JSON pointer. -->
                  <button type="button" class="locate" onclick={() => locate(loc.anchorId)}>
                    {loc.label}
                  </button>
                {:else if issue.path}
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
    margin-top: var(--space-4);
  }
  .banner {
    padding: var(--space-3) var(--space-4);
    border-radius: var(--radius-sm);
    margin-bottom: var(--space-4);
  }
  .request-error {
    background: var(--err-bg);
    border: 1px solid var(--red-border);
  }
  .summary {
    font-weight: 600;
    padding: var(--space-2) var(--space-3);
    border-radius: var(--radius-sm);
  }
  /* Status fills use the cooler status roles, never the brand accent
     (DESIGN.md accent-vs-status). */
  .summary.ok {
    background: var(--ok-bg);
    border: 1px solid var(--ok);
    color: var(--ok);
  }
  .summary.fail {
    background: var(--err-bg);
    border: 1px solid var(--red-border);
    color: var(--err);
  }
  .group {
    margin-top: var(--space-4);
  }
  /* Tracked uppercase eyebrow — the micro-label hierarchy device the design
     system uses for section/table headers. */
  .group h4 {
    margin: 0 0 var(--space-2);
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
  }
  .group ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: var(--space-2);
  }
  .group li {
    padding: var(--space-2) var(--space-3);
    border-left: 3px solid var(--border);
    border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
    background: var(--surface-sunken);
  }
  .group.error li {
    border-left-color: var(--err);
  }
  .group.warning li {
    border-left-color: var(--warn);
  }
  .group.info li {
    border-left-color: var(--info);
  }
  .issue-head {
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    flex-wrap: wrap;
  }
  .code {
    font-family: var(--font-mono);
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  .label {
    font-weight: 600;
  }
  .message {
    margin: var(--space-1) 0;
  }
  .path {
    font-size: var(--text-sm);
    color: var(--text-muted);
  }
  /* The raw JSON pointer is a machine identifier → mono; the "(whole document)"
     fallback is prose (a <span>) and stays in the UI face. */
  code.path {
    font-family: var(--font-mono);
  }
  /* The locate trigger is interactive chrome → brand accent is correct here
     (links/click-to-locate), not a status color. */
  .locate {
    font: inherit;
    font-size: var(--text-sm);
    padding: 0;
    border: none;
    background: none;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
  }
  .locate:hover {
    text-decoration: underline;
  }
</style>
