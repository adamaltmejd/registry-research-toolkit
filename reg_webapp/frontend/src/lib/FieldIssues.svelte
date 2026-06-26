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
//
// Each issue ALSO leads with a redundant, non-color severity cue — an
// `aria-hidden` glyph (✕ ▲ ⓘ — the canonical status glyphs, DESIGN.md
// accent-vs-status) PLUS a visible level word ("Error" / "Warning" / "Info").
// Color alone would trip WCAG 1.4.1 (Use of Color); the glyph is decorative so the
// VISIBLE WORD is what carries the severity to assistive tech. The word mirrors
// ValidationPanel's level→label mapping, compacted to the singular for one issue.
const LEVELS = ["error", "warning", "info"] as const;
type Level = (typeof LEVELS)[number];

const LEVEL_CUE: Record<Level, { glyph: string; word: string }> = {
  error: { glyph: "✕", word: "Error" },
  warning: { glyph: "▲", word: "Warning" },
  info: { glyph: "ⓘ", word: "Info" },
};

// An unknown/unset level degrades to the `info` cue (forward-compat), matching the
// border-color fallback below — never the brand accent.
function levelCue(level: string): { glyph: string; word: string } {
  return LEVEL_CUE[level as Level] ?? LEVEL_CUE.info;
}

const { issues } = $props<{ issues: ValidationIssue[] }>();
</script>

{#if issues.length > 0}
  <ul class="field-issues" role="alert">
    {#each issues as issue, i (`${issue.code}|${i}`)}
      {@const cue = levelCue(issue.level)}
      <li class="issue {issue.level}">
        <span class="level-cue">
          <span class="glyph" aria-hidden="true">{cue.glyph}</span>
          <span class="word">{cue.word}</span>
        </span>
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
  /* The non-color severity cue: glyph + visible level word, sharing the level's
     status color with the code label below. Inline so the compact single-issue
     look is preserved (no pill). */
  .level-cue {
    font-weight: 600;
    white-space: nowrap;
  }
  .glyph {
    /* Decorative (aria-hidden) — the visible `.word` carries severity to AT. */
    font-size: 0.85em;
    margin-right: 0.15em;
  }
  .label {
    font-weight: 600;
  }
  /* Default/unknown level shares the --info color with the border fallback. */
  .level-cue,
  .label {
    color: var(--info);
  }
  .issue.error .level-cue,
  .issue.error .label {
    color: var(--err);
  }
  .issue.warning .level-cue,
  .issue.warning .label {
    color: var(--warn);
  }
  .issue.info .level-cue,
  .issue.info .label {
    color: var(--info);
  }
  .message {
    color: var(--text-muted);
  }
</style>
