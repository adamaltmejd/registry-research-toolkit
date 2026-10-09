//! Slice 3b's operations: `show`, `states`, `warnings`, `values`, `docs_get` and
//! `docs_related`, and the related-document download.

use super::docs::{self, DocDetail};
use super::show::{Show, show};
use super::states::{self, StatesPage};
use super::values::{self, ValuesPage};
use super::warnings;
use super::{Cache, Download, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "show",
        paths: &["/api/catalog", "/api/catalog/{ref}"],
        tool: Some("show"),
        description: "The summary of any ref, by `kind`: a provider, register, variable, \
            classification, `class` (every classification) or a group \
            (`group/<provider>/<register>/<key>`, `group/class/<key>`); no ref is the \
            catalog root. A register lists its variables, groups and variants; a \
            classification its owning variables. `ref` is a FQID or a bare name; a \
            retired FQID answers for its successor, under the successor's `fqid`.",
        params: &[
            Param::optional("ref", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: show,
        result: component::<Show>,
    },
    Operation {
        name: "states",
        paths: &["/api/states/{ref}"],
        tool: Some("states"),
        description: "A variable's states: each representation it was delivered in, with \
            its variant, bounds, column, coding and the ids of the data warnings that \
            apply to it. Without `period`, the whole history; with `period`, the dated \
            states overlapping it, a state's alias windows standing in for it where they \
            overlap. `variant` and `value_set_version` (`_none` for the empty label) \
            narrow the list. In holdings scope, only what is held, clipped to the held \
            periods. `ref` is a variable FQID or a bare name.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("variant", Type::String),
            Param::optional("value_set_version", Type::String),
            Param::optional("scope", Type::Scope),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: states::states,
        result: component::<StatesPage>,
    },
    Operation {
        name: "warnings",
        paths: &["/api/warnings/{ref}"],
        tool: None,
        description: "A register's or variable's data warnings, ordered by id: a \
            variable's include its register's unassigned ones. `period`, `variant` and \
            `representation` (a delivery column) keep the warnings that may apply to \
            them; `unassigned_only` keeps the register's unassigned ones. In holdings \
            scope, a variable's warnings apply only to what is held.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("variant", Type::String),
            Param::optional("representation", Type::String),
            Param::optional("unassigned_only", Type::Boolean),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: warnings::warnings,
        result: warnings::schema,
    },
    Operation {
        name: "values",
        paths: &["/api/values/{ref}"],
        tool: Some("values"),
        description: "A classification's codes (code, label, level, validity), or the \
            value set of one `state` of a variable (a `state_id` from `states`), ordered \
            by code and label. `column` with `alias_window_from` (a state's \
            `coding_window_from`) reads the coded alias window's set instead. With \
            `classification`, a book the coding declares, `partition` picks its part: \
            `source_extensions` (default; the codes outside the book, nonstandard and \
            sentinel), `nonstandard`, `sentinels`, or `canonical` (the delivered pairs \
            whose code the book holds). `q` keeps the rows whose code or label contains \
            it, case and diacritics folded; `total` counts them. In holdings scope, only \
            a held state. `ref` is a classification or variable FQID or a bare name.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("state", Type::StorageId),
            Param::optional("partition", Type::Enum(values::PARTITIONS)),
            Param::optional("classification", Type::Ref),
            Param::optional("column", Type::String),
            Param::optional("alias_window_from", Type::String),
            Param::optional("q", Type::String),
            Param::optional("scope", Type::Scope),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: values::values,
        result: component::<ValuesPage>,
    },
    Operation {
        name: "docs_get",
        paths: &["/api/docs/doc/{identifier}"],
        tool: Some("docs"),
        description: "One documentation entry by variable name or filename (with or \
            without `.md`): its register, tags and source, a 500-character `excerpt` and \
            the full markdown `body`.",
        params: &[
            Param::required("identifier", Type::String),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Day,
        run: docs::get,
        result: component::<DocDetail>,
    },
    Operation {
        name: "docs_related",
        paths: &["/api/docs/related/{ref}"],
        tool: Some("docs"),
        description: "A register's related documents (rehosted PDFs): title, source, \
            license, and the `sha256` and `byte_size` of the bytes \
            `/api/docs/file/{ref}/{filename}` serves. `ref` is a register FQID or a bare \
            name.",
        params: &[
            Param::required("ref", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Day,
        run: docs::related,
        result: docs::related_schema,
    },
];

pub const DOWNLOADS: &[Download] = &[Download {
    path: "/api/docs/file/{ref}/{filename}",
    description: "A related document's bytes, served inline.",
    media_type: "application/pdf",
    params: &[
        Param::required("ref", Type::Ref),
        Param::required("filename", Type::String),
        Param::optional("scope", Type::Scope),
    ],
    cache: Cache::Day,
    run: docs::file,
}];
