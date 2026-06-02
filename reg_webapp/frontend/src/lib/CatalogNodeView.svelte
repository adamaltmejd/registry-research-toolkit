<script lang="ts">
import { getCatalogNode } from "./api";
import { asyncResource } from "./async.svelte";
import { breadcrumbs, catalogHref, nodeLabel } from "./catalog";
import VariantBrowser from "./VariantBrowser.svelte";

// Fetches and renders one catalog node by FQID path, switching on the `kind`
// discriminator. A5.3a is READ-ONLY browse: a binding leaf shows BASIC metadata
// only (no period/states picker — that is A5.3b). The catch-all response is
// narrowed on `kind` at the fetch boundary (the union has no `StatesResponse`
// arm here because A5.3a never passes `?period`).
let { fqidPath }: { fqidPath: string } = $props();

const resource = asyncResource(() => getCatalogNode(fqidPath));
const node = $derived(resource.data);
const crumbs = $derived(breadcrumbs(fqidPath));
</script>

<nav class="breadcrumbs" aria-label="Breadcrumb">
  <a href="/catalog">catalog</a>
  {#each crumbs as crumb (crumb.fqidPath)}
    <span class="sep" aria-hidden="true">/</span>
    <a href={catalogHref(crumb.fqidPath)}>{crumb.label}</a>
  {/each}
</nav>

{#if resource.loading}
  <p class="muted">Loading…</p>
{:else if resource.error}
  <p class="error" role="alert">
    {#if resource.status === 404}
      Not found: <code>{fqidPath}</code>
    {:else}
      {resource.error}
    {/if}
  </p>
{:else if node}
  <article>
    {#if node.kind === "provider"}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      <h3>Registers</h3>
      {#if node.children.length > 0}
        <ul class="children">
          {#each node.children as register (register.fqid)}
            <li>
              <a href={catalogHref(register.fqid)}>
                <span class="label">{register.name ?? register.fqid}</span>
                <code class=" child-fqid">{register.fqid}</code>
              </a>
              {#if register.purpose}<p class="muted">{register.purpose}</p>{/if}
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No registers.</p>
      {/if}
    {:else if node.kind === "register"}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      {#if node.purpose}<p>{node.purpose}</p>{/if}
      <h3>Bindings</h3>
      {#if node.children.some((c) => c.kind === "binding")}
        <ul class="children">
          {#each node.children as child (child.kind === "binding" ? child.fqid : "variants-ref")}
            {#if child.kind === "binding"}
              <li>
                <a href={catalogHref(child.fqid)}>
                  <span class="label">{child.name ?? child.fqid}</span>
                  <code class="child-fqid">{child.fqid}</code>
                </a>
              </li>
            {/if}
          {/each}
        </ul>
      {:else}
        <p class="muted">No bindings.</p>
      {/if}
      <VariantBrowser registerFqid={node.fqid} />
    {:else if node.kind === "binding"}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      <!-- A5.3a: BASIC metadata only. The period/states picker + lineage are A5.3b. -->
      <dl class="meta">
        {#if node.definition}
          <dt>Definition</dt>
          <dd>{node.definition}</dd>
        {/if}
        {#if node.description}
          <dt>Description</dt>
          <dd>{node.description}</dd>
        {/if}
        {#if node.measurement_unit}
          <dt>Unit</dt>
          <dd>{node.measurement_unit}</dd>
        {/if}
        <dt>Sensitive</dt>
        <dd>{node.is_sensitive ? "yes" : "no"}</dd>
        <dt>Identifier</dt>
        <dd>{node.is_identifier ? "yes" : "no"}</dd>
        <dt>States</dt>
        <dd>{node.states.length}</dd>
      </dl>
      <p class="muted note">
        State history, variants and lineage for this variable are shown in the
        states view (coming in A5.3b).
      </p>
    {:else if node.kind === "classification-root"}
      <h2>{nodeLabel(node)}</h2>
      <h3>Classifications</h3>
      {#if node.children.length > 0}
        <ul class="children">
          {#each node.children as classification (classification.fqid)}
            <li>
              <a href={catalogHref(classification.fqid)}>
                <span class="label">{classification.name}</span>
                <code class="child-fqid">{classification.short_name}</code>
              </a>
            </li>
          {/each}
        </ul>
      {:else}
        <p class="muted">No classifications.</p>
      {/if}
    {:else if node.kind === "classification"}
      <h2>{nodeLabel(node)}</h2>
      <p class="fqid"><code>{node.fqid}</code></p>
      <dl class="meta">
        <dt>Short name</dt>
        <dd>{node.short_name}</dd>
      </dl>
    {/if}
  </article>
{/if}

<style>
  .breadcrumbs {
    font-size: 0.9rem;
    margin-bottom: 1rem;
  }
  .breadcrumbs .sep {
    color: var(--muted);
    margin: 0 0.25rem;
  }
  .fqid {
    margin-top: -0.25rem;
    color: var(--muted);
  }
  .children {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }
  .children li a {
    display: flex;
    align-items: baseline;
    gap: 0.75rem;
  }
  .children .label {
    font-weight: 600;
  }
  .child-fqid {
    color: var(--muted);
    font-size: 0.85em;
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
  .note {
    font-style: italic;
  }
</style>
