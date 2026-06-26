<script lang="ts">
import type { BreadcrumbItem } from "./types";

// The topbar breadcrumb (#804 / DESIGN.md → App shell). `<nav
// aria-label="Breadcrumb">` over an ordered list; the LAST item is the current
// page — rendered as plain text with `aria-current="page"`, never a link.
// The separator is a CSS `::before` (not a DOM character) so a screen reader
// reads only the labels, not "slash" between each.
//
// Links carry the focus ring + accent-on-hover (the shared interactive-primitive
// focus convention, #804). The caller owns the `href` shape (the router's
// `use:link` intercepts internal anchors elsewhere — Breadcrumbs is purely
// presentational and does no navigation itself).

interface Props {
  items: BreadcrumbItem[];
}

let { items }: Props = $props();
</script>

<nav aria-label="Breadcrumb" class="breadcrumbs">
  <ol>
    {#each items as item, i (i)}
      {@const isLast = i === items.length - 1}
      <li>
        {#if item.href && !isLast}
          <a href={item.href}>{item.label}</a>
        {:else}
          <!-- Only the LAST item is the current page. An intentionally-plain
               non-final item (no href) is a plain span with NO aria-current, so a
               second "current page" can't leak into the a11y tree. -->
          <span aria-current={isLast ? "page" : undefined} class:current={isLast}>
            {item.label}
          </span>
        {/if}
      </li>
    {/each}
  </ol>
</nav>

<style>
  .breadcrumbs ol {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--space-1);
    font-size: var(--text-sm);
  }
  li {
    display: inline-flex;
    align-items: center;
    gap: var(--space-1);
  }
  /* Separator as CSS content (aria-hidden by being generated, not in the a11y
     tree as text). Precedes every item except the first. */
  li:not(:first-child)::before {
    content: "/";
    color: var(--text-faint);
    margin-right: var(--space-1);
  }
  a {
    color: var(--text-muted);
    text-decoration: none;
    border-radius: var(--radius-sm);
  }
  a:hover {
    color: var(--accent-ink);
    text-decoration: underline;
  }
  a:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .current {
    color: var(--text);
    font-weight: 500;
  }
</style>
