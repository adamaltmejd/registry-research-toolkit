<script lang="ts">
import { getDoc } from "./api";
import { asyncResource } from "./async.svelte";

// The minimal documentation viewer (#394): metadata + a source POINTER + a
// BOUNDED excerpt for one doc. There is NO full-body endpoint and we never fetch
// or render one — the excerpt is shown as TEXT and the source pointer sends the
// reader to the SCB original. NEVER {@html}: `excerpt` may carry FTS highlight
// markers and Svelte's `{value}` auto-escaping is the republication guard.
let { identifier }: { identifier: string } = $props();

const resource = asyncResource(() => getDoc(identifier));
const doc = $derived(resource.data);
</script>

{#if resource.loading}
  <p class="muted" aria-busy="true">Loading…</p>
{:else if resource.status === 404}
  <!-- 404 covers BOTH "index not ingested" and "no doc for this id"; the backend
       `detail` (resource.error) distinguishes them — surface it verbatim. -->
  <p class="error" role="alert">{resource.error}</p>
  <p class="muted">Documentation coverage is LISA-only today.</p>
{:else if resource.error}
  <p class="error" role="alert">{resource.error}</p>
{:else if doc}
  <article class="doc-view">
    <h2>{doc.display_name ?? doc.filename}</h2>

    {#if doc.register || doc.variable || doc.tags.length > 0}
      <dl class="meta">
        {#if doc.register}
          <dt>Register</dt>
          <dd>{doc.register}</dd>
        {/if}
        {#if doc.variable}
          <dt>Variable</dt>
          <dd>{doc.variable}</dd>
        {/if}
        {#if doc.tags.length > 0}
          <!-- Tags are plain TEXT, not facet/chip controls — faceting rides #311. -->
          <dt>Tags</dt>
          <dd>{doc.tags.join(", ")}</dd>
        {/if}
      </dl>
    {/if}

    <!-- Source pointer: an off-site link to the SCB PDF when a curated URL was
         resolved at doc-DB build (#372), else the bare source identifier as text.
         Prefer the human title as the label; off-site link so target=_blank. -->
    {#if doc.source_url}
      <p class="source">
        <a href={doc.source_url} target="_blank" rel="noopener noreferrer">
          {doc.source_title ?? doc.source ?? doc.source_url}
        </a>
      </p>
    {:else if doc.source}
      <p class="source">{doc.source}</p>
    {/if}

    {#if doc.excerpt}
      <blockquote class="excerpt">{doc.excerpt}</blockquote>
      <p class="muted">
        Excerpt only — see the SCB source for the full document.
      </p>
    {:else}
      <p class="muted">No preview available.</p>
    {/if}
  </article>
{/if}

<style>
  .doc-view h2 {
    margin-bottom: 0.5rem;
  }
  .meta {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.35rem 1rem;
    margin: 1rem 0;
  }
  .meta dt {
    font-weight: 600;
  }
  .source {
    margin: 0.5rem 0;
  }
  .excerpt {
    margin: 1rem 0 0.5rem;
    padding: 0.5rem 1rem;
    border-left: 3px solid var(--border);
    color: var(--text-muted);
    white-space: pre-wrap;
  }
</style>
