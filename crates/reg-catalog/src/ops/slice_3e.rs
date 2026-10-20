//! Slice 3e's operations, on the `order` tool: `validate` and `order`, and the order
//! manifest download.

use super::order::{self, Manifest};
use super::validate::validate;
use super::{Cache, Download, Operation, Param, Type, component};
use reg_core::ValidationResult;

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "validate",
        paths: &["/api/project/validate"],
        tool: Some("order"),
        description: "Validate a `project_data.json` document (the JSON body; the \
            `project` argument over MCP): every issue, `ok` false when one is an error. To \
            draft a project, use the microdata-tools-se plugin's project skill.",
        params: &[Param::required("project", Type::Project)],
        // simplify: unused, a body operation is answered uncached; give `Operation` a
        // method if a cached POST ever appears.
        cache: Cache::Revalidate,
        run: validate,
        result: component::<ValidationResult>,
    },
    Operation {
        name: "order",
        paths: &["/api/project/order"],
        tool: Some("order"),
        description: "Preview the order manifest of a `project_data.json` document: the \
            table and column delivering each binding, clipped where unavailable; a \
            partly undeliverable request is `order_blocked`.",
        params: &[Param::required("project", Type::Project)],
        // simplify: unused, as `validate`'s.
        cache: Cache::Revalidate,
        run: order::order,
        result: component::<Manifest>,
    },
];

pub const DOWNLOADS: &[Download] = &[Download {
    path: "/api/project/order/manifest",
    description: "`order`'s manifest as the exact `order.json` bytes, an attachment.",
    media_type: "application/json",
    params: &[Param::required("project", Type::Project)],
    // simplify: unused, a body download is answered uncached, as a body operation.
    cache: Cache::Revalidate,
    run: order::manifest,
}];
