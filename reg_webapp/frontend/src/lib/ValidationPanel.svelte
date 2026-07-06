<script lang="ts">
import type { ValidationResultModel } from "./api";
import type { SafeSource } from "./project_data";
import type { ValidationStatus } from "./project_store.svelte";
import { Button } from "./ui";
import {
  codeLabel,
  findingLocation,
  type ValidationIssue,
  type WindowCoverageHint,
} from "./validation";

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
const { result, status, requestError, windowHints, sources, onRetry } = $props<{
  result: ValidationResultModel | null;
  status: ValidationStatus;
  requestError: string | null;
  windowHints: readonly WindowCoverageHint[];
  sources: readonly SafeSource[];
  onRetry?: () => void;
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
    <div class="banner request-error" role="alert">
      <span class="banner-text">
        <strong>Request failed:</strong>
        {requestError}
      </span>
      {#if onRetry}
        <Button variant="default" size="sm" onclick={onRetry}>
          Retry validation
        </Button>
      {/if}
    </div>
  {/if}

  {#if status === "checking"}
    <p class="summary checking" role="status" aria-busy="true">
      Checking the current project…
    </p>
  {:else if result == null}
    <p class="muted">Validation has not run for this draft yet.</p>
  {:else}
    <p
      class="summary {status === 'warnings' ? 'warning' : result.ok ? 'ok' : 'fail'}"
      role="status"
    >
      {#if result.ok}
        {status === "warnings" ? "Valid with warnings" : "Valid"} — no errors.
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
          <h4 class="micro-label">{LEVEL_LABEL[level]} ({grouped[level].length})</h4>
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
                  <div class="locators">
                    <!-- Click-to-locate: scrolls to + flashes the source/binding card
                         instead of leaking the raw JSON pointer. -->
                    <button type="button" class="locate" onclick={() => locate(loc.anchorId)}>
                      {loc.label}
                    </button>
                    {#if loc.catalogHref}
                      <!-- The cart is read-only (#991): fixes happen on the catalog
                           subject page, so link out to it. Omitted when the finding
                           resolves no catalog coordinate (an unpicked row). -->
                      <a class="catalog-link" href={loc.catalogHref}>
                        Fix in catalog: <code>{loc.catalogLabel}</code>
                      </a>
                    {/if}
                  </div>
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

  {#if windowHints.length > 0}
    <div class="group warning">
      <h4 class="micro-label">
        Study window coverage ({windowHints.length})
      </h4>
      <ul>
        {#each windowHints as hint, i (i)}
          <li>
            <p class="message">{hint.message}</p>
          <div class="locators">
            {#if hint.catalogHref}
              <a class="catalog-link" href={hint.catalogHref}>
                Extend in catalog: <code>{hint.catalogLabel}</code>
              </a>
            {:else}
              <span class="path muted">{hint.label}</span>
            {/if}
          </div>
          </li>
        {/each}
      </ul>
    </div>
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
  .banner-text {
    flex: 1;
  }
  .request-error {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
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
  .summary.checking {
    background: var(--surface-sunken);
    border: 1px solid var(--border);
    color: var(--text-muted);
  }
  .summary.warning {
    background: var(--warn-bg);
    border: 1px solid var(--warn);
    color: var(--warn);
  }
  .summary.fail {
    background: var(--err-bg);
    border: 1px solid var(--red-border);
    color: var(--err);
  }
  .group {
    margin-top: var(--space-4);
  }
  .group h4 {
    margin: 0 0 var(--space-2);
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
    /* The JSON-pointer / window-hint label carries a raw machine string. The
       `.path.muted` variant is a flex child of `.locators` (window hints) that
       defaults to `min-width: auto`, and the raw `<code class="path">` pointer
       fallback is a long unbroken run; both must break in-place rather than
       overflow the card on mobile. `min-width: 0` lets the flex-child variant
       shrink; `overflow-wrap: anywhere` lowers the run's min-content contribution so
       it breaks instead of clipping (#1112). */
    min-width: 0;
    overflow-wrap: anywhere;
  }
  /* The raw JSON pointer is a machine identifier → mono; the "(whole document)"
     fallback is prose (a <span>) and stays in the UI face. */
  code.path {
    font-family: var(--font-mono);
  }
  /* The locate trigger + catalog link sit on one row (wrapping on narrow
     viewports), separated so the "where" (locate) reads before the "fix it"
     (catalog) affordance. */
  .locators {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--space-1) var(--space-3);
  }
  /* The locate trigger AND the catalog link are interactive chrome → brand accent
     is correct here (links/click-to-locate), not a status color. */
  .locate,
  .catalog-link {
    font: inherit;
    font-size: var(--text-sm);
    padding: 0;
    border: none;
    background: none;
    color: var(--accent);
    cursor: pointer;
    text-align: left;
    /* As flex children of `.locators` (flex-wrap: wrap) these default to
       `min-width: auto`, so a long unbroken locate label or catalog FQID would
       refuse to shrink and overflow the card on mobile. `min-width: 0` lets each
       shrink; `overflow-wrap: anywhere` (inherited by the child `<code>` FQID run in
       the catalog link) lowers the text's min-content contribution so it breaks
       within the row instead of clipping (#1112). */
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .locate:hover,
  .catalog-link:hover {
    text-decoration: underline;
  }
  /* The catalog target is a machine FQID/coordinate → mono, like every identifier. */
  .catalog-link code {
    font-family: var(--font-mono);
    font-size: 0.9em;
  }
</style>
