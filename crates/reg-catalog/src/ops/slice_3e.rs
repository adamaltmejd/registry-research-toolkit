//! Slice 3e's operations, on the `order` tool: `validate`.

use super::validate::{Validation, validate};
use super::{Cache, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "validate",
    paths: &["/api/project/validate"],
    tool: Some("order"),
    description: "Validate a `project_data.json` document (the JSON body; the `project` \
        argument over MCP): every issue, with `ok` false when one is an error. A \
        `schema_version` other than 3.0.0 is reported alone; then the document's \
        structure, then each source's variant and period and each binding's variable, \
        availability in the period (a narrower availability is an `info` clip), \
        representation and value set, and on a steward catalog what the steward holds. \
        An invalid project is a result, not an error.",
    params: &[Param::required("project", Type::Project)],
    // simplify: unused, a body operation is answered uncached; give `Operation` a
    // method if a cached POST ever appears.
    cache: Cache::Revalidate,
    run: validate,
    result: component::<Validation>,
}];
