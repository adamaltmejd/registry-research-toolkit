<script lang="ts">
import { getRegisterVariants } from "./api";
import { asyncResource } from "./async.svelte";
import { catalogHref } from "./catalog";
import SubjectView from "./SubjectView.svelte";
import { Button, EmptyState, KeyValue, type KeyValueRow, Skeleton } from "./ui";
import { foldVersions, groupVariants, showsDistinctGroup } from "./variants";

// The register's variants PAGE (Y-79): `/catalog/<provider>/<register>/variants`,
// the route behind the register page's compact `VariantsSummary`. The variant
// axis is a register SUB-RESOURCE (NOT an FQID path segment; see reg_meta/DESIGN.md
// → Two-level variable model), so it has its own fixed-shape route rather than a
// catalog node. A5.3a DISPLAYS the variants for a register; the selection + the
// period/state resolution that consumes `?variant` is A5.3b.
//
// Versions are FOLDED (`variants.ts`): SCB delivers one `register_version` per
// year, so LISA's seven variants carry 105 of them, each repeating the previous
// one's prose with the year moved on. A run that says the same thing renders as
// ONE block spanning its years; a changed description, population or object type
// opens the next block, which is the only part a reader came for.
//
// Unlike the register page's summary, this page does NOT hide a `_default`
// variant (#673/M4): the summary suppresses itself because a `_default`-only
// register has no variant axis worth a section, but its `register_version` prose
// is real and this deep-linkable page is the only place it can be read.
const { registerFqid }: { registerFqid: string } = $props();

const variants = asyncResource(() => getRegisterVariants(registerFqid));
// Fold in the derived, not in the template: `foldKey` stringifies every version
// body, so folding under `{#each}` would re-walk the whole wall on each render.
const entries = $derived(
  groupVariants(variants.data?.variants ?? []).map((group) => ({
    ...group,
    segments: group.segments.map((segment) => ({
      ...segment,
      blocks: foldVersions(segment.variant.versions ?? []),
    })),
  })),
);

function rows(...rows: KeyValueRow[]): KeyValueRow[] {
  return rows.filter((row) => row.value);
}

function versionRows(version: {
  description?: string | null;
  measurement_information?: string | null;
}): KeyValueRow[] {
  return rows(
    { label: "Description", value: version.description ?? undefined },
    {
      label: "Measurement",
      value: version.measurement_information ?? undefined,
    },
  );
}

function populationRows(population: {
  name: string;
  definition?: string | null;
  comment?: string | null;
  date_range?: string | null;
}): KeyValueRow[] {
  return rows(
    { label: "Name", value: population.name },
    { label: "Definition", value: population.definition ?? undefined },
    { label: "Comment", value: population.comment ?? undefined },
    // A bare delivery year — mono, like every other year on the page.
    {
      label: "Date range",
      value: population.date_range ?? undefined,
      mono: true,
    },
  );
}

function objectTypeRows(objectType: {
  name: string;
  definition?: string | null;
}): KeyValueRow[] {
  return rows(
    { label: "Name", value: objectType.name },
    { label: "Definition", value: objectType.definition ?? undefined },
  );
}
</script>

<!-- The page rides `SubjectView` for the article wrapper + title/fqid header
     every catalog subject page shares; the variant list is its leading section,
     so it goes in the `description` slot (the same loose use as the
     classification-group route). -->
{#snippet body()}
  {#if variants.loading}
    <div class="loading" aria-busy="true" aria-live="polite">
      <p class="muted">Loading variants…</p>
      <Skeleton count={3} />
    </div>
  {:else if variants.error}
    <p class="error" role="alert">Failed to load variants: {variants.error}</p>
  {:else if entries.length === 0}
    <!-- A register with no slugged variant has no variant axis at all — say so
         and point back at the register rather than leaving a bare page. -->
    <EmptyState title="No variants for this register.">
      {#snippet action()}
        <Button href={catalogHref(registerFqid)}>Back to the register</Button>
      {/snippet}
    </EmptyState>
  {:else}
    <ul class="variant-list">
      {#each entries as entry (entry.key)}
        <li class="variant-entry">
          <h3 class="entry-name">{entry.label}</h3>
          {#if entry.isFamily}
            <!-- A curated succession family reads as ONE variant with a changed
                 frame: its segments name what the family label doesn't already
                 say, plus the years each was delivered. One LINE per segment —
                 the boundary between two frames is markup, not a separator
                 glyph, so it survives a narrow width and reads as two items to
                 a screen reader. -->
            <ul class="entry-segments">
              {#each entry.segments as segment (segment.variant.slug)}
                <li>
                  {segment.name}
                  <span class="years">{segment.span}</span>
                </li>
              {/each}
            </ul>
          {:else if showsDistinctGroup(entry.label, entry.segments[0].variant.display_group)}
            <!-- Omit display_group when it just repeats `name` (the common case;
                 "Arbetsställen Arbetsställen") — trimmed compare, see the helper. -->
            <p class="entry-meta">{entry.segments[0].variant.display_group}</p>
          {/if}
          {#each entry.segments as segment (segment.variant.slug)}
            <section class="segment">
              <!-- The concrete coordinate a project source extracts. The years
                   live on the folded blocks below (and, for a family, in the
                   segments list above), so the slug is all this line owes. -->
              <h4 class="segment-id">
                <code class="slug">{segment.variant.slug}</code>
              </h4>
              {#if segment.variant.description}
                <p class="desc muted">{segment.variant.description}</p>
              {/if}
              {#each segment.blocks as block}
                {@const vRows = versionRows(block.version)}
                {@const populations = block.version.populations ?? []}
                {@const objectTypes = block.version.object_types ?? []}
                <section class="version-meta">
                  <h5 class="version-title">
                    <span class="micro-label"
                      >{block.count === 1 ? "Version" : "Versions"}</span
                    >
                    <span class="version-name">{block.label}</span>
                  </h5>
                  {#if block.count > 1 && block.version.name}
                    <!-- The block spans years but prints ONE delivery's text
                         verbatim (years inside it included), so name which. -->
                    <p class="as-delivered muted">
                      Wording as delivered for <code>{block.version.name}</code>.
                    </p>
                  {/if}
                  {#if vRows.length > 0}
                    <KeyValue rows={vRows} />
                  {/if}
                  {#if populations.length > 0}
                    <div class="metadata-group">
                      <h6 class="micro-label metadata-heading">Population</h6>
                      {#each populations as population}
                        <KeyValue rows={populationRows(population)} />
                      {/each}
                    </div>
                  {/if}
                  {#if objectTypes.length > 0}
                    <div class="metadata-group">
                      <h6 class="micro-label metadata-heading">Object type</h6>
                      {#each objectTypes as objectType}
                        <KeyValue rows={objectTypeRows(objectType)} />
                      {/each}
                    </div>
                  {/if}
                </section>
              {/each}
            </section>
          {/each}
        </li>
      {/each}
    </ul>
  {/if}
{/snippet}

<SubjectView title="Variants" fqid={registerFqid} description={body} />

<style>
  .loading {
    display: grid;
    gap: var(--space-3);
  }
  .variant-list {
    list-style: none;
    padding: 0;
    margin: 0;
    display: grid;
    /* Entries must separate MORE than the segments inside one entry
       (`.segment`, --space-3), or a family runs into its neighbour. */
    gap: calc(var(--space-4) * 1.5);
  }
  /* The page is nothing but register-version prose, so the ENTRY carries the
     measure: without it a description runs ~155 characters at 1920. */
  .variant-entry {
    max-width: 70ch;
  }
  .entry-name {
    margin: 0;
    font-size: var(--text-h3);
    font-weight: var(--heading-weight);
  }
  .entry-meta,
  .entry-segments {
    margin: var(--space-1) 0 0;
    color: var(--text-muted);
    font-size: var(--text-sm);
  }
  .entry-segments {
    list-style: none;
    padding: 0;
  }
  /* A year is a machine value wherever it lands. */
  .years {
    font-family: var(--font-mono);
  }
  .segment {
    margin-top: var(--space-3);
  }
  .segment-id {
    margin: 0;
    font-size: var(--text-sm);
    font-weight: var(--heading-weight);
  }
  .slug {
    font-family: var(--font-mono);
  }
  .desc {
    margin: var(--space-1) 0 0;
    font-size: var(--text-sm);
  }
  .version-meta {
    margin-top: var(--space-3);
    padding-left: var(--space-3);
    border-left: 2px solid var(--border);
  }
  .version-title {
    margin: 0 0 var(--space-2);
    display: flex;
    align-items: baseline;
    gap: var(--space-2);
    font-size: var(--text-sm);
    font-weight: var(--heading-weight);
  }
  .version-name {
    font-family: var(--font-mono);
    color: var(--text);
  }
  .as-delivered {
    margin: 0 0 var(--space-2);
    font-size: var(--text-sm);
  }
  .as-delivered code {
    font-family: var(--font-mono);
  }
  .metadata-group {
    margin-top: var(--space-3);
    display: grid;
    gap: var(--space-2);
  }
  .metadata-heading {
    margin: 0;
  }
</style>
