//! Slice 3c's operations: `schema` and `diff`, both on the `schema` tool.

use super::schema::{Diff, SchemaPage, diff, schema};
use super::{Cache, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "schema",
        paths: &["/api/schema/{ref}"],
        tool: Some("schema"),
        description: "The delivered columns of a register, or of one variable (a FQID or \
            a bare name): one row per register variant, window and column, with its type, \
            width and concept group. `period` (2019, 2015..2019) keeps rows overlapping \
            its years; `variant` keeps one variant (a slug); pass `next_cursor` back as \
            `cursor` for the next page.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("variant", Type::String),
            Param::optional("scope", Type::Scope),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: schema,
        result: component::<SchemaPage>,
    },
    Operation {
        name: "diff",
        paths: &["/api/diff/{ref}"],
        tool: Some("schema"),
        description: "A register's columns between two periods, per variant: the \
            variables added at `to`, removed since `from`, and changed in type, width or \
            column (a variable compares by its first column in each period). Variants \
            without changes are left out.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::required("from", Type::Period),
            Param::required("to", Type::Period),
            Param::optional("variant", Type::String),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: diff,
        result: component::<Diff>,
    },
];
