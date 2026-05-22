"""Bundle-wide privacy constants.

``SUPPRESS_K`` is the global k-anonymity floor for any cell-count
suppression the bundle does: categorical frequency cutoff, per-period
``n_entity_ids`` drop, null-count censoring. Lives here (rather than
inside ``reg_monabundle.runtime.summarize``) so the namespaced-block
validator can enforce the "overrides may only raise the floor" rule
without importing the runtime tier, and so the same constant is
amalgamated into the MONA bundle alongside the runtime modules that
consult it.

A future steward-tier override would relax the floor by raising it
globally; lowering it requires a spec change, not a config knob.
"""

from __future__ import annotations

SUPPRESS_K: int = 10
"""k-anonymity floor. Cell counts < SUPPRESS_K are suppressed."""
