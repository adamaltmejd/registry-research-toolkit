<script lang="ts">
import type { Snippet } from "svelte";
import type { TagTone } from "./types";

// A small inline label (#804). One component, `tone`-driven, covering the three
// disjoint color sub-systems (DESIGN.md → Color): brand chrome (neutral/accent),
// the categorical TYPE palette (reg/var/code/class/group → --cat-*), and the
// status set (error/warn/info/ok → status fg over its *-bg fill).
//
// Status tones pair with a GLYPH, never hue alone (the accent-vs-status rule):
// status callers MUST pass a leading-glyph snippet (✕ ▲ i ✓ — see DESIGN.md).
// The component can't synthesize one (the glyph is caller-domain), so it just
// renders the slot ahead of the label; categorical/chrome tones omit it.
//
// a11y: status meaning must ALSO be carried by the label text or surrounding
// context — the tone (color) and the glyph (`aria-hidden`) are visual-only, so an
// assistive-tech user gets only the label.
//
// `mono` faces code-like tags (a value-set code shown as a tag) in --font-mono.
// A single component (not a separate Badge): a count/status pill and a type tag
// differ only by tone + glyph, so a `tone` prop spans both with no semantic split.

interface Props {
  tone?: TagTone;
  mono?: boolean;
  /** Leading glyph (required for status tones; ignored visually otherwise). */
  glyph?: Snippet;
  children: Snippet;
}

let { tone = "neutral", mono = false, glyph, children }: Props = $props();
</script>

<span class="tag tone-{tone}" class:mono>
  {#if glyph}<span class="glyph" aria-hidden="true">{@render glyph()}</span>{/if}
  <span class="label">{@render children()}</span>
</span>

<style>
  .tag {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
    padding: 0.1em 0.5em;
    border-radius: var(--radius-sm);
    font-size: var(--text-sm);
    line-height: 1.4;
    white-space: nowrap;
    border: 1px solid transparent;
  }
  .mono .label {
    font-family: var(--font-mono);
    font-size: 0.8em;
  }
  .glyph {
    display: inline-flex;
    font-size: 0.85em;
  }

  /* Chrome tones. */
  .tone-neutral {
    background: var(--surface-sunken);
    color: var(--text-muted);
    border-color: var(--border);
  }
  .tone-accent {
    background: var(--accent-bg);
    color: var(--accent-ink);
  }

  /* Categorical TYPE tones — soft tint of the cat hue (color-mix keeps one ramp
     stop as both border ink and fill, so no second token per hue). Each tone only
     selects its hue into `--tone-hue`; one shared rule paints all five.
     The TEXT darkens the hue 15% toward black (mirrors --accent-ink): the raw
     cat hues are tuned as fills, and teal/gold fall just under WCAG AA (4.1–4.5:1)
     as text on their own 10% tint — the 15% darkening clears AA (≥5.3:1) for all
     five while the border + fill keep the full hue, so type identity is intact. */
  .tone-reg {
    --tone-hue: var(--cat-reg);
  }
  .tone-var {
    --tone-hue: var(--cat-var);
  }
  .tone-code {
    --tone-hue: var(--cat-code);
  }
  .tone-class {
    --tone-hue: var(--cat-class);
  }
  .tone-group {
    --tone-hue: var(--cat-group);
  }
  .tone-reg,
  .tone-var,
  .tone-code,
  .tone-class,
  .tone-group {
    color: color-mix(in srgb, var(--tone-hue) 85%, black);
    border-color: color-mix(in srgb, var(--tone-hue) 35%, transparent);
    background: color-mix(in srgb, var(--tone-hue) 10%, var(--surface));
  }

  /* Status tones — status fg over its fill tint (the *-bg roles). */
  .tone-error {
    color: var(--err);
    background: var(--err-bg);
  }
  .tone-warn {
    color: var(--warn);
    background: var(--warn-bg);
  }
  .tone-info {
    color: var(--info);
    background: var(--info-bg);
  }
  .tone-ok {
    color: var(--ok);
    background: var(--ok-bg);
  }
</style>
