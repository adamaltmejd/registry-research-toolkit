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
     stop as both border ink and fill, so no second token per hue). */
  .tone-reg {
    color: var(--cat-reg);
    border-color: color-mix(in srgb, var(--cat-reg) 35%, transparent);
    background: color-mix(in srgb, var(--cat-reg) 10%, var(--surface));
  }
  .tone-var {
    color: var(--cat-var);
    border-color: color-mix(in srgb, var(--cat-var) 35%, transparent);
    background: color-mix(in srgb, var(--cat-var) 10%, var(--surface));
  }
  .tone-code {
    color: var(--cat-code);
    border-color: color-mix(in srgb, var(--cat-code) 35%, transparent);
    background: color-mix(in srgb, var(--cat-code) 10%, var(--surface));
  }
  .tone-class {
    color: var(--cat-class);
    border-color: color-mix(in srgb, var(--cat-class) 35%, transparent);
    background: color-mix(in srgb, var(--cat-class) 10%, var(--surface));
  }
  .tone-group {
    color: var(--cat-group);
    border-color: color-mix(in srgb, var(--cat-group) 35%, transparent);
    background: color-mix(in srgb, var(--cat-group) 10%, var(--surface));
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
