<script lang="ts">
import {
  getRelatedDocuments,
  type RelatedDocument,
  relatedDocumentFileHref,
} from "./api";
import { asyncResource } from "./async.svelte";

// Rehosted register-version PDFs for a register page (#742/#967). This is
// authoritative register-level source metadata, distinct from the fuzzy FTS
// mentions in `DocMentionsPanel`, and must not be inherited by variable pages.
let { register }: { register: string } = $props();

const resource = asyncResource((signal) =>
  getRelatedDocuments(register, { signal }),
);
const data = $derived(resource.data);
const documents = $derived(data?.documents ?? []);
const show = $derived(
  resource.loading ||
    !!resource.error ||
    (!!data && data.ingested && documents.length > 0),
);

function bytesLabel(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kib = bytes / 1024;
  if (kib < 1024) {
    return `${kib.toFixed(kib < 10 ? 1 : 0)} KB`;
  }
  const mib = kib / 1024;
  return `${mib.toFixed(mib < 10 ? 1 : 0)} MB`;
}

function sourceHost(doc: RelatedDocument): string {
  try {
    return new URL(doc.source_url).hostname;
  } catch {
    return "source";
  }
}
</script>

{#if show}
  <section class="related-docs" aria-labelledby="related-docs-heading">
    <h3 id="related-docs-heading">Source documents</h3>

    {#if resource.loading}
      <p class="muted" aria-busy="true">Loading…</p>
    {:else if resource.error}
      <p class="error" role="alert">
        Failed to load source documents: {resource.error}
      </p>
    {:else}
      <ul class="documents">
        {#each documents as doc (doc.filename)}
          <li>
            <a class="title" href={relatedDocumentFileHref(register, doc.filename)}>
              {doc.title}
            </a>
            <p class="attribution">
              Källa: SCB · {doc.license}
              <a href={doc.source_url} target="_blank" rel="noreferrer">
                {sourceHost(doc)}
              </a>
            </p>
            <p class="provenance muted">
              {bytesLabel(doc.byte_size)} · fetched {doc.fetched}
            </p>
          </li>
        {/each}
      </ul>
    {/if}
  </section>
{/if}

<style>
  .related-docs {
    margin-top: 1.5rem;
  }
  h3 {
    margin: 0 0 0.5rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid var(--border);
  }
  .documents {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  .documents li {
    display: grid;
    gap: 0.2rem;
  }
  .title {
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  .attribution,
  .provenance {
    margin: 0;
    font-size: 0.9em;
  }
  .attribution {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 0.75rem;
    color: var(--text-muted);
  }
</style>
