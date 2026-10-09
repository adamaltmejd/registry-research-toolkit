//! Slice 3e's operations, on the `order` tool: `validate` and `order`, and the order
//! manifest download.

use super::order::{self, Manifest};
use super::validate::{Validation, validate};
use super::{Cache, Download, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[
    Operation {
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
    },
    Operation {
        name: "order",
        paths: &["/api/project/order"],
        tool: Some("order"),
        description: "The order manifest of a `project_data.json` document (the JSON body; \
            the `project` argument over MCP): per source and binding, each table and \
            column that delivers it, clipped to where the binding is available (each \
            clip listed), from the steward's holdings on a steward catalog and by \
            canonical column on the global catalog. A project `validate` rejects \
            structurally is `project_invalid`; any finding that leaves part of the \
            request undeliverable blocks the whole order (`order_blocked`, with every \
            finding). POST /api/project/order/manifest serves the same manifest as the \
            exact `order.json` bytes.",
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
