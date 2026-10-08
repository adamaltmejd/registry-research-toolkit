//! Slice 3b's operations: `docs_get` and `docs_related`, and the related-document
//! download.

use super::docs::{self, DocDetail};
use super::{Cache, Download, Operation, Param, Type, component};

pub const OPERATIONS: &[Operation] = &[
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
