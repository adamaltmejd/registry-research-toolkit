<script lang="ts">
import type { Snippet } from "svelte";
import type { PlainTone, StatusTone } from "./types";

// A small inline label (#804). One component, `tone`-driven, covering the three
// disjoint color sub-systems (DESIGN.md → Color): brand chrome (neutral/accent),
// the categorical TYPE palette (reg/var/code/class/group → --cat-*), and the
// status set (error/warn/info/ok → status fg over its *-bg fill).
//
// Status tones pair with a GLYPH, never hue alone (the accent-vs-status rule):
// status callers MUST pass a leading-glyph snippet (✕ ▲ i ✓ — see DESIGN.md).
// The component can't synthesize one (the glyph is caller-domain), so it just
// renders the slot ahead of the label; categorical/chrome tones omit it. The
// `Props` union makes `glyph` REQUIRED at the type level for a status `tone` and
// optional otherwise, so a color-only status tag won't compile.
//
// a11y: status meaning must ALSO be carried by the label text or surrounding
// context — the tone (color) and the glyph (`aria-hidden`) are visual-only, so an
// assistive-tech user gets only the label.
//
// `mono` faces code-like tags (a value-set code shown as a tag) in --font-mono.
// A single component (not a separate Badge): a count/status pill and a type tag
// differ only by tone + glyph, so a `tone` prop spans both with no semantic split.

// Discriminated on `tone`: a status tone REQUIRES `glyph`, a plain tone makes it
// optional. `tone` defaults to "neutral" (a plain tone) when omitted.
type Props = { mono?: boolean; children: Snippet } & (
  | { tone?: PlainTone; glyph?: Snippet }
  | { tone: StatusTone; glyph: Snippet }
);

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
     stop as both border ink and fill, so no second token per hue). Each tone
     selects its FILL hue into `--tone-hue` AND its AA-cleared TEXT stop into
     `--tone-ink`; one shared rule paints all five. The `--cat-*-ink` roles
     (tokens.css, mirroring --accent-ink) carry the 15%-toward-black darkening
     that clears WCAG AA — the raw --cat-* hues are tuned as fills and teal/gold
     fall just under AA as text on their own 10% tint. The border + fill keep the
     full hue (`--tone-hue`), so type identity is intact. */
  .tone-reg {
    --tone-hue: var(--cat-reg);
    --tone-ink: var(--cat-reg-ink);
  }
  .tone-var {
    --tone-hue: var(--cat-var);
    --tone-ink: var(--cat-var-ink);
  }
  .tone-code {
    --tone-hue: var(--cat-code);
    --tone-ink: var(--cat-code-ink);
  }
  .tone-class {
    --tone-hue: var(--cat-class);
    --tone-ink: var(--cat-class-ink);
  }
  .tone-group {
    --tone-hue: var(--cat-group);
    --tone-ink: var(--cat-group-ink);
  }
  .tone-reg,
  .tone-var,
  .tone-code,
  .tone-class,
  .tone-group {
    color: var(--tone-ink);
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
