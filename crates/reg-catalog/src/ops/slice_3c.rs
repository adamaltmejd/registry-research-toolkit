//! Slice 3c's operations: `schema`, `diff` and `coded_variables` on the `schema`
//! tool, and `coverage` and `resolve` on tools of their own.

use super::coded::{CodedPage, coded_variables};
use super::coverage::{Coverage, coverage};
use super::resolve::{Resolved, resolve};
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
    Operation {
        name: "coded_variables",
        paths: &["/api/coded-variables"],
        tool: Some("schema"),
        description: "The variables with coded value sets, by common name: each name's \
            distinct codes over every coded state under it, its registers and its coded \
            states, ordered by distinct codes (ties by name); pass `next_cursor` back as \
            `cursor` for the next page.",
        params: &[
            Param::optional("scope", Type::Scope),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: coded_variables,
        result: component::<CodedPage>,
    },
    Operation {
        name: "coverage",
        paths: &["/api/coverage/{ref}"],
        tool: Some("coverage"),
        description: "The calendar years a register or a variable (a FQID or a bare \
            name) is delivered in: their span and gaps, and the years per register \
            variant, or for a variable the columns delivered each year. An open-ended \
            delivery counts its opening year; a ref with no dated delivery is not found.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: coverage,
        result: component::<Coverage>,
    },
    Operation {
        name: "resolve",
        paths: &["/api/resolve"],
        tool: Some("resolve"),
        description: "Delivered column names (data-file headers, 1 to 200 of them) to \
            the variables delivering them, by exact case-insensitive match: one row per \
            name, `matched` with its variables or `no_match`. `register` (a FQID or a \
            name) keeps matches to one register. Not ref resolution: use `search` to \
            discover names.",
        params: &[
            Param::required("columns", Type::Strings),
            Param::optional("register", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: resolve,
        result: component::<Resolved>,
    },
];
