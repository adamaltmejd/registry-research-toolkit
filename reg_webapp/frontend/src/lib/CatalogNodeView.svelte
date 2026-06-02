<script lang="ts">
import { getCatalogNode, isCatalogNode } from "./api";
import { asyncResource } from "./async.svelte";
import BindingLeafView from "./BindingLeafView.svelte";
import { breadcrumbs, catalogHref, nodeLabel } from "./catalog";
import VariantBrowser from "./VariantBrowser.svelte";

// Fetches and renders one catalog node by FQID path, switching on the `kind`
// discriminator. The provider/register/classification browse fetch is a plain
// (no-query) resolve; a binding leaf delegates to `BindingLeafView`, which owns
// the period/variant resolution + states + lineage (A5.3b). The browse fetch
// here never passes `?period`, so this catch-all response is always a `kind`-
// tagged node (the `StatesResponse` arm — a no-`kind` resolve_at subset — is
// only reachable WITH a query, so it's filtered to `null` and never rendered).
let { fqidPath }: { fqidPath: string } = $props();

const resource = asyncResource(() => getCatalogNode(fqidPath));
// A browsable path resolves to a `kind`-tagged CatalogNode. A SUB-ENDPOINT path
// (e.g. a deep-link to `.../states` or `.../variants`) hits that endpoint and
// returns a no-`kind` StatesResponse/VariantsResponse — narrow it OUT of `node`
// (so the kind-switch type-checks) and flag it as `notBrowsable` so we render a
// clear message instead of a blank page.
const node = $derived.by(() => {
  const data = resource.data;
  return data !== null && isCatalogNode(data) ? data : null;
});
const notBrowsable = $derived(
  resource.data !== null && !isCatalogNode(resource.data),
);
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
                <code class="child-fqid">{register.fqid}</code>
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
      <!-- Pass the full node down: this no-query browse fetch already resolved
           the variable's metadata + embedded edges + default states. BindingLeafView
           renders those from `node` (always present — so a cold deep-link with
           `?period` isn't blank) and fetches only the period-NARROWED states from
           the URL query, reactive without a remount. -->
      <BindingLeafView {fqidPath} {node} />
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
{:else if notBrowsable}
  <!-- A no-`kind` response: a deep-link to a SUB-ENDPOINT path (e.g.
       `.../states`, `.../variants`) hits that endpoint and returns a
       StatesResponse/VariantsResponse, not a browsable node. Render a clear
       message instead of a blank page. -->
  <p class="error" role="alert">
    <code>{fqidPath}</code> isn't a browsable catalog node.
  </p>
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
</style>
