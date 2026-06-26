<script lang="ts">
import type { Snippet } from "svelte";

// Styled empty state (#804) — replaces unstyled "none found" text. Centered,
// muted, restrained (the data-tool ethos: an empty region is quiet, not a
// billboard). `title` is the headline; the optional `description` adds a line;
// the optional `icon` snippet leads above the title; the optional `action`
// snippet (typically a Button) sits below.

interface Props {
  title: string;
  description?: string;
  icon?: Snippet;
  action?: Snippet;
}

let { title, description, icon, action }: Props = $props();
</script>

<div class="empty-state">
  {#if icon}<div class="icon" aria-hidden="true">{@render icon()}</div>{/if}
  <p class="title">{title}</p>
  {#if description}<p class="description">{description}</p>{/if}
  {#if action}<div class="action">{@render action()}</div>{/if}
</div>

<style>
  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
    gap: var(--space-2);
    padding: var(--space-4);
    color: var(--text-muted);
  }
  .icon {
    color: var(--text-faint);
    font-size: var(--text-h2);
  }
  .title {
    margin: 0;
    color: var(--text);
    font-weight: 500;
  }
  .description {
    margin: 0;
    font-size: var(--text-sm);
    max-width: 32rem;
  }
  .action {
    margin-top: var(--space-2);
  }
</style>
