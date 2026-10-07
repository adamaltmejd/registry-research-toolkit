<script lang="ts">
import type { ConceptGroup } from "./api";
import { distinctMemberCount } from "./catalog";
import { Tag } from "./ui";

// One concept-group row (#303) in the register-browse and classification-umbrella
// arms. It LINKS to the group's own subject page (#673/#756): register groups →
// `/catalog/group/<p>/<r>/<key>`, classification umbrellas →
// `/catalog/group/class/<key>`. The members, their facets and any picking live on
// that page (ConceptGroupView), not inline here.
let {
  group,
  noun = "variables",
  href,
}: {
  group: ConceptGroup;
  noun?: string;
  href: string;
} = $props();
</script>

<!-- One interactive element per row (no nested controls). -->
<a class="group-link" {href}>
  <span class="label">{group.label}</span>
  <!-- #819: count DISTINCT member FQIDs (the variable identity), not raw member
       rows — a representation group carries several members on one variable (one
       `fqid`, distinct delivery columns), so `members.length` overstates the
       "N variables" readout. Distinct-by-fqid is a no-op for axis groups (each
       facet value is its own variable) and umbrellas (distinct classifications).
       The count is a neutral chrome pill (Tag tone="neutral"): a quantity, not a
       TYPE, so it must not borrow the categorical group hue. -->
  <span class="count"><Tag tone="neutral">{distinctMemberCount(group.members)} {noun}</Tag></span>
</a>

<style>
  .group-link {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    cursor: pointer;
    color: var(--accent);
  }
  /* Keyboard focus: the shared --focus-ring (matching the search owner/leaf rows,
     #808), replacing the UA default outline. */
  .group-link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .group-link .label {
    font-weight: 600;
  }
  /* The count Tag doesn't shrink the row on wrap. */
  .group-link .count {
    white-space: nowrap;
  }
</style>
