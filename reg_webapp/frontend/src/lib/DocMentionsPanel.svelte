<script lang="ts">
import { type BindingNodeData, getDocsForVariable } from "./api";
import { asyncResource } from "./async.svelte";
import { fqidSegments, showingOf } from "./catalog";
import { parseInlineMarkdown } from "./inline_markdown";

// The binding-leaf "Mentioned in documentation" panel (#402). A deliberately
// SEPARATE component over a SEPARATE optional DB (the docs FTS index), so it's
// an independent FAILURE DOMAIN: a docs fetch
// error / timeout / absent index must NEVER blank or wedge the leaf (mirrors how
// #394's SearchView fires a second independent asyncResource for its docs group).
// The whole panel's worst case is one inline muted/error line inside this section.
//
// The hook is FUZZY (every result is `fuzzy:true`) — a name/provider_key text
// match, NOT an authoritative variable→doc link; the section-level caption marks
// the list as heuristic.
//
// Omit-when-empty: the WHOLE section
// is omitted when there's nothing usable to show — no docs index, no docs for
// this register, or zero hits — but NOT while still loading or on error (we never
// hide a section whose state is unknown, which would read as a confirmed absence).
let { node }: { node: BindingNodeData } = $props();

// The bare register slug = 2nd FQID segment (`scb/lisa/kon` → `lisa`); the
// backend matches it verbatim against the bare register slug, NOT `scb/lisa`.
const register = $derived(fqidSegments(node.fqid)[1]);
// The query: the variable's display name when present, else its slug (3rd
// segment). $derived off `node` so the fetch refetches when the leaf changes.
const q = $derived(node.name?.trim() || fqidSegments(node.fqid)[2]);

// #670: a grouped member shares its concept `node.name` with ~31 siblings, and
// the FTS runs on that shared name — so a member's hits are CONCEPT-grain
// (identical across the group), not member-specific. Caption them as such so the
// shared matches don't read as member-specific. Conditioned on `node.group` (the
// grouped-member marker); ungrouped variables keep only the fuzzy note.
const grouped = $derived(!!node.group);

// Read `q`/`register` SYNCHRONOUSLY inside `fn` so the effect tracks them and
// refetches when the leaf changes (same pattern as BindingLeafView's period
// resource). The teardown `signal` aborts a superseded request;
// `getDocsForVariable` layers its ~12s timeout on top.
const resource = asyncResource((signal) =>
  getDocsForVariable(q, { register, limit: 5, signal }),
);

const data = $derived(resource.data);
const results = $derived(data?.results ?? []);
// Show the section while loading / on error / when it has usable hits; omit it
// once we KNOW there's nothing to show — no docs index, no docs for this register,
// or zero hits (those resolved-empty states are noise on the subject page).
const show = $derived(
  resource.loading ||
    !!resource.error ||
    (!!data && data.ingested && data.register_ingested && results.length > 0),
);
</script>

{#if show}
  <section class="doc-mentions" aria-labelledby="doc-mentions-heading">
    <h3 id="doc-mentions-heading">Mentioned in documentation</h3>

    {#if resource.loading}
      <p class="muted" aria-busy="true">Loading…</p>
    {:else if resource.error}
      <!-- Any docs failure (error / timeout / 5xx / network drop) stays INLINE —
           it never throws past this section and never blanks the leaf. -->
      <p class="error" role="alert">
        Failed to load documentation mentions: {resource.error}
      </p>
    {:else if data}
      {#if grouped}
        <!-- #670: a grouped member's hits are CONCEPT-grain — the FTS matched the
             shared concept name, so these are identical across the group's
             members, not member-specific. -->
        <p class="muted concept-grain-note">
          Matches on the shared concept name — these mentions are common to the
          group's members, not specific to this variable.
        </p>
      {/if}
      <!-- One section-level caption marks the WHOLE list as fuzzy/heuristic name
           matches (every result is `fuzzy:true`) — cleaner than per-row badges. -->
      <p class="muted fuzzy-note">
        Heuristic name matches — not authoritative variable→documentation links.
      </p>
      <ul class="mentions">
        <!-- key by array INDEX — the list is replaced wholesale per fetch; a
             natural key like filename could collide and crash the keyed each, per
             the #391 lesson. -->
        {#each results as r, i (i)}
          <li>
            <!-- Links to the minimal /doc viewer; the App shell's use:link
                 intercepts same-origin SPA links (mirrors SearchView's docHit). -->
            <a href={`/doc/${encodeURIComponent(r.filename)}`}>
              <span class="label">{r.display_name ?? r.filename}</span>
            </a>
            {#if r.snippet}
              <!-- An FTS EXCERPT rendered through a SAFE inline-emphasis subset
                   (parseInlineMarkdown → DATA segments, never HTML). Each `{seg.text}`
                   is normal Svelte interpolation, so it is auto-escaped — NEVER
                   {@html} (the full body lives at the SCB source; republication
                   policy, see reg_webapp/DESIGN.md → Docs library endpoints). The
                   `**…**` markers are dominantly the FTS highlight delimiter wrapping
                   the matched term (reg_meta/doc_queries.py), so a `strong` segment
                   renders as <mark> ("matched term"); `*…*`/`_…_` render as <em>. -->
              <span class="hit-detail muted"
                >{#each parseInlineMarkdown(r.snippet) as seg, si (si)}{#if seg.emphasis === "strong"}<mark
                    >{seg.text}</mark
                  >{:else if seg.emphasis === "em"}<em>{seg.text}</em
                  >{:else}{seg.text}{/if}{/each}</span
              >
            {/if}
          </li>
        {/each}
      </ul>
      {@const caption = showingOf(results.length, data.total_count)}
      {#if caption}
        <p class="muted count">{caption}</p>
      {/if}
    {/if}
  </section>
{/if}

<style>
  .doc-mentions {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .fuzzy-note {
    margin: 0 0 0.5rem;
    font-size: 0.85em;
  }
  /* #670: the concept-grain caption — same muted/small treatment as the fuzzy
     note it sits above. */
  .concept-grain-note {
    margin: 0 0 0.5rem;
    font-size: 0.85em;
  }
  .mentions {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .mentions li {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.5rem 0.75rem;
  }
  .label {
    font-weight: 600;
  }
  .hit-detail {
    flex-basis: 100%;
    font-size: 0.9em;
  }
  /* The FTS matched-term highlight (`**…**` → <mark>): the accent tint already in
     the palette, NOT the browser default yellow, so it reads as one family with the
     rest of the app. */
  .hit-detail mark {
    background: var(--accent-bg);
    color: var(--accent);
    border-radius: var(--radius);
    padding: 0 0.1em;
  }
  .count {
    margin: 0.5rem 0 0;
    font-size: 0.85em;
  }
</style>
