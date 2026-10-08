//! Slice 3d's operations, on the `graph` tool: `graph` and `lineage`.

use super::graph::{Graph, graph};
use super::lineage::{Lineage, lineage};
use super::{Cache, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "graph",
        paths: &["/api/graph/{ref}"],
        tool: Some("graph"),
        description: "The succession graph of a variable, a classification or a group \
            (`group/<provider>/<register>/<key>`, `group/class/<key>`): variable nodes \
            with their states folded into representation runs, classification edition \
            nodes, and directed succession edges. A variable draws its concept group's \
            members, a classification its classification groups; `focus_id` is the \
            requested node. No nodes means nothing to draw.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: graph,
        result: component::<Graph>,
    },
    Operation {
        name: "lineage",
        paths: &["/api/lineage/{ref}"],
        tool: Some("graph"),
        description: "A variable's lineage: the source states feeding each of its states \
            (`edges`), the build's lineage `warnings`, and, for every variable in scope \
            with its name, its register's provenance role (`registers`).",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: lineage,
        result: component::<Lineage>,
    },
];
