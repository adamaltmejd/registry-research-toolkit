//! Slice 3e's operation, on the `order` tool: `validate`.

use super::validate::validate;
use super::{Cache, Operation, Param, Type, component};
use reg_core::ValidationResult;

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "validate",
    paths: &["/api/project/validate"],
    tool: Some("order"),
    description: "Validate a `project_data.json` document (the JSON body; the \
        `project` argument over MCP): every issue, `ok` false when one is an error. To \
        draft a project, use the `register-project` skill of the microdata-tools-se plugin.",
    params: &[Param::required("project", Type::Project)],
    // simplify: unused, a body operation is answered uncached; give `Operation` a
    // method if a cached POST ever appears.
    cache: Cache::Revalidate,
    run: validate,
    result: component::<ValidationResult>,
}];
